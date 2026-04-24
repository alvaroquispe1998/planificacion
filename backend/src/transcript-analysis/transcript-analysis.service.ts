import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RunTranscriptAnalysisDto } from './dto/run-analysis.dto';

export type TranscriptTopicStatus = 'covered' | 'partial' | 'missing';

export interface TranscriptAnalysisTopic {
  title: string;
  status: TranscriptTopicStatus;
  evidence?: string;
  timestamp?: string;
  note?: string;
}

export interface TranscriptKeyMoment {
  timestamp?: string;
  summary: string;
}

export interface TranscriptAnalysisResult {
  coverageScore: number; // 0-100
  overallAssessment: string;
  topics: TranscriptAnalysisTopic[];
  extraTopics: string[];
  keyMoments: TranscriptKeyMoment[];
  strengths: string[];
  gaps: string[];
  pedagogyNotes?: string;
  language?: string;
  rawLlmResponse?: unknown;
  meta: {
    model: string;
    tookMs: number;
    transcriptChars: number;
    syllabusChars: number;
    promptChars: number;
    courseLabel?: string;
    sessionLabel?: string;
    teacherLabel?: string;
    generatedAt: string;
  };
}

const GEMINI_API_URL =
  'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent';

const SYSTEM_INSTRUCTION = `Eres un asistente pedagogico que analiza la transcripcion de una clase universitaria
en comparacion con un syllabus. Tu tarea es evaluar la cobertura de temas del syllabus,
identificar temas extra no planeados, resaltar momentos clave y dar una valoracion pedagogica.

Reglas:
- Responde SIEMPRE en espanol.
- Responde SIEMPRE con un unico objeto JSON valido que siga el esquema solicitado, sin texto alrededor.
- No inventes temas que no esten en el syllabus ni en la transcripcion.
- Si el transcript trae marcas de tiempo (VTT, SRT o [HH:MM:SS]), inclulye el timestamp mas cercano como referencia.
- Si la transcripcion es incoherente o demasiado corta, declara coverageScore=0 y explicalo en overallAssessment.`;

const RESPONSE_SCHEMA = {
  type: 'object',
  properties: {
    coverageScore: { type: 'number', description: 'Entre 0 y 100' },
    overallAssessment: { type: 'string' },
    topics: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          title: { type: 'string' },
          status: { type: 'string', enum: ['covered', 'partial', 'missing'] },
          evidence: { type: 'string' },
          timestamp: { type: 'string' },
          note: { type: 'string' },
        },
        required: ['title', 'status'],
      },
    },
    extraTopics: { type: 'array', items: { type: 'string' } },
    keyMoments: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          timestamp: { type: 'string' },
          summary: { type: 'string' },
        },
        required: ['summary'],
      },
    },
    strengths: { type: 'array', items: { type: 'string' } },
    gaps: { type: 'array', items: { type: 'string' } },
    pedagogyNotes: { type: 'string' },
    language: { type: 'string' },
  },
  required: ['coverageScore', 'overallAssessment', 'topics', 'extraTopics', 'keyMoments', 'strengths', 'gaps'],
} as const;

@Injectable()
export class TranscriptAnalysisService {
  private readonly logger = new Logger(TranscriptAnalysisService.name);

  constructor(private readonly configService: ConfigService) {}

  async analyze(dto: RunTranscriptAnalysisDto): Promise<TranscriptAnalysisResult> {
    const apiKey = this.configService.get<string>('GEMINI_API_KEY');
    if (!apiKey || !apiKey.trim()) {
      throw new BadRequestException(
        'Falta la variable de entorno GEMINI_API_KEY en el backend. Obten una en https://aistudio.google.com.',
      );
    }

    const syllabus = dto.syllabusText.trim();
    const transcript = dto.transcriptText.trim();

    if (syllabus.length < 10) {
      throw new BadRequestException('El syllabus es demasiado corto.');
    }
    if (transcript.length < 10) {
      throw new BadRequestException('El transcript es demasiado corto.');
    }

    const header = [
      dto.courseLabel ? `Curso: ${dto.courseLabel}` : null,
      dto.sessionLabel ? `Sesion: ${dto.sessionLabel}` : null,
      dto.teacherLabel ? `Docente: ${dto.teacherLabel}` : null,
    ]
      .filter(Boolean)
      .join('\n');

    const userPrompt = [
      header ? `Contexto:\n${header}` : null,
      '### SYLLABUS DE REFERENCIA',
      syllabus,
      '### TRANSCRIPT DE LA CLASE',
      transcript,
      '### TAREA',
      'Compara el transcript contra el syllabus y responde con el JSON solicitado.',
    ]
      .filter(Boolean)
      .join('\n\n');

    const model = 'gemini-2.0-flash';
    const requestBody = {
      systemInstruction: {
        role: 'system',
        parts: [{ text: SYSTEM_INSTRUCTION }],
      },
      contents: [
        {
          role: 'user',
          parts: [{ text: userPrompt }],
        },
      ],
      generationConfig: {
        temperature: 0.2,
        topP: 0.9,
        maxOutputTokens: 4096,
        responseMimeType: 'application/json',
        responseSchema: RESPONSE_SCHEMA,
      },
    };

    const startedAt = Date.now();
    let response: Response;
    try {
      response = await fetch(`${GEMINI_API_URL}?key=${encodeURIComponent(apiKey)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(requestBody),
      });
    } catch (error) {
      this.logger.error('Gemini fetch error', error as Error);
      throw new BadRequestException(
        `No se pudo contactar a Gemini: ${(error as Error).message || 'error desconocido'}`,
      );
    }

    const rawText = await response.text();
    if (!response.ok) {
      this.logger.error(`Gemini responded ${response.status}: ${rawText.slice(0, 500)}`);
      throw new BadRequestException(
        `Gemini devolvio ${response.status}: ${rawText.slice(0, 300)}`,
      );
    }

    let apiJson: unknown;
    try {
      apiJson = JSON.parse(rawText);
    } catch {
      throw new BadRequestException('Gemini devolvio una respuesta no JSON.');
    }

    const candidateText = this.extractCandidateText(apiJson);
    if (!candidateText) {
      throw new BadRequestException('Gemini devolvio una respuesta vacia.');
    }

    const parsed = this.parseJsonLoose(candidateText);
    if (!parsed || typeof parsed !== 'object') {
      throw new BadRequestException('No se pudo parsear la respuesta JSON de Gemini.');
    }

    const normalized = this.normalizeResult(parsed as Record<string, unknown>);
    const tookMs = Date.now() - startedAt;

    return {
      ...normalized,
      rawLlmResponse: apiJson,
      meta: {
        model,
        tookMs,
        transcriptChars: transcript.length,
        syllabusChars: syllabus.length,
        promptChars: userPrompt.length,
        courseLabel: dto.courseLabel,
        sessionLabel: dto.sessionLabel,
        teacherLabel: dto.teacherLabel,
        generatedAt: new Date().toISOString(),
      },
    };
  }

  private extractCandidateText(apiJson: unknown): string | null {
    if (!apiJson || typeof apiJson !== 'object') {
      return null;
    }
    const root = apiJson as Record<string, unknown>;
    const candidates = Array.isArray(root.candidates) ? root.candidates : [];
    for (const candidate of candidates) {
      if (!candidate || typeof candidate !== 'object') continue;
      const content = (candidate as Record<string, unknown>).content as
        | Record<string, unknown>
        | undefined;
      const parts = Array.isArray(content?.parts) ? content?.parts : [];
      for (const part of parts ?? []) {
        if (part && typeof part === 'object' && typeof (part as { text?: unknown }).text === 'string') {
          const text = (part as { text: string }).text;
          if (text.trim()) return text;
        }
      }
    }
    return null;
  }

  private parseJsonLoose(text: string): unknown {
    const trimmed = text.trim();
    try {
      return JSON.parse(trimmed);
    } catch {
      // Fallback: extract first JSON object from the text
      const start = trimmed.indexOf('{');
      const end = trimmed.lastIndexOf('}');
      if (start >= 0 && end > start) {
        try {
          return JSON.parse(trimmed.slice(start, end + 1));
        } catch {
          return null;
        }
      }
      return null;
    }
  }

  private normalizeResult(data: Record<string, unknown>): Omit<TranscriptAnalysisResult, 'meta' | 'rawLlmResponse'> {
    const coverageRaw = Number(data.coverageScore);
    const coverageScore = Number.isFinite(coverageRaw)
      ? Math.max(0, Math.min(100, Math.round(coverageRaw)))
      : 0;

    const topicsArr = Array.isArray(data.topics) ? data.topics : [];
    const topics: TranscriptAnalysisTopic[] = topicsArr
      .map((t) => (t && typeof t === 'object' ? (t as Record<string, unknown>) : null))
      .filter((t): t is Record<string, unknown> => t !== null)
      .map((t) => {
        const statusRaw = String(t.status || '').toLowerCase();
        const status: TranscriptTopicStatus =
          statusRaw === 'covered' || statusRaw === 'partial' || statusRaw === 'missing'
            ? statusRaw
            : 'missing';
        return {
          title: String(t.title || '').trim() || 'Sin titulo',
          status,
          evidence: typeof t.evidence === 'string' ? t.evidence : undefined,
          timestamp: typeof t.timestamp === 'string' ? t.timestamp : undefined,
          note: typeof t.note === 'string' ? t.note : undefined,
        };
      });

    const extraTopics = Array.isArray(data.extraTopics)
      ? data.extraTopics.filter((x): x is string => typeof x === 'string' && x.trim().length > 0)
      : [];

    const keyMomentsArr = Array.isArray(data.keyMoments) ? data.keyMoments : [];
    const keyMoments: TranscriptKeyMoment[] = keyMomentsArr
      .map((k) => (k && typeof k === 'object' ? (k as Record<string, unknown>) : null))
      .filter((k): k is Record<string, unknown> => k !== null)
      .map((k) => ({
        timestamp: typeof k.timestamp === 'string' ? k.timestamp : undefined,
        summary: String(k.summary || '').trim(),
      }))
      .filter((k) => k.summary.length > 0);

    const strengths = Array.isArray(data.strengths)
      ? data.strengths.filter((x): x is string => typeof x === 'string' && x.trim().length > 0)
      : [];
    const gaps = Array.isArray(data.gaps)
      ? data.gaps.filter((x): x is string => typeof x === 'string' && x.trim().length > 0)
      : [];

    return {
      coverageScore,
      overallAssessment: String(data.overallAssessment || '').trim(),
      topics,
      extraTopics,
      keyMoments,
      strengths,
      gaps,
      pedagogyNotes: typeof data.pedagogyNotes === 'string' ? data.pedagogyNotes : undefined,
      language: typeof data.language === 'string' ? data.language : undefined,
    };
  }
}
