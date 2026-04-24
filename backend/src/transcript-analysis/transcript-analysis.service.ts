import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  MeetingInstanceEntity,
  MeetingRecordingEntity,
} from '../entities/audit.entities';
import { ZoomAccountService } from '../videoconference/zoom-account.service';
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
  transcriptPreview?: string;
  transcriptSource?: 'manual' | 'zoom-recording';
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

export interface TranscriptAvailability {
  videoconferenceId: string;
  available: boolean;
  message: string;
  recordings: Array<{
    recordingId: string;
    instanceId: string;
    recordingType: string;
    fileExtension: string | null;
    startTime: string | null;
    hasDownloadUrl: boolean;
  }>;
}

const GEMINI_API_BASE =
  'https://generativelanguage.googleapis.com/v1beta/models';
const DEFAULT_GEMINI_MODEL = 'gemini-1.5-flash';
const ALLOWED_GEMINI_MODELS = new Set([
  'gemini-1.5-flash',
  'gemini-1.5-flash-8b',
  'gemini-1.5-pro',
  'gemini-2.0-flash',
  'gemini-2.0-flash-lite',
]);

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

  constructor(
    @InjectRepository(MeetingInstanceEntity)
    private readonly instancesRepo: Repository<MeetingInstanceEntity>,
    @InjectRepository(MeetingRecordingEntity)
    private readonly recordingsRepo: Repository<MeetingRecordingEntity>,
    private readonly zoomAccount: ZoomAccountService,
  ) {}

  async getAvailability(videoconferenceId: string): Promise<TranscriptAvailability> {
    if (!videoconferenceId) {
      throw new BadRequestException('videoconferenceId requerido.');
    }
    const instances = await this.instancesRepo.find({
      where: { planning_subsection_videoconference_id: videoconferenceId },
      select: { id: true },
    });
    const instanceIds = instances.map((i) => i.id);
    if (!instanceIds.length) {
      return {
        videoconferenceId,
        available: false,
        message: 'La videoconferencia no tiene instancias registradas. Sincroniza datos Zoom primero.',
        recordings: [],
      };
    }
    const all = await this.recordingsRepo
      .createQueryBuilder('r')
      .where('r.meeting_instance_id IN (:...ids)', { ids: instanceIds })
      .orderBy('r.start_time', 'DESC')
      .getMany();
    const transcripts = all.filter((r) => this.isTranscriptRecording(r));
    const mapped = transcripts.map((r) => ({
      recordingId: r.id,
      instanceId: r.meeting_instance_id,
      recordingType: r.recording_type,
      fileExtension: r.file_extension,
      startTime: r.start_time ? r.start_time.toISOString() : null,
      hasDownloadUrl: Boolean(r.download_url),
    }));
    if (!transcripts.length) {
      return {
        videoconferenceId,
        available: false,
        message:
          'No se encontraron grabaciones de tipo TRANSCRIPT/VTT. Verifica que la clase tenga Cloud Recording con Audio Transcript habilitado y vuelve a sincronizar.',
        recordings: [],
      };
    }
    return {
      videoconferenceId,
      available: transcripts.some((r) => Boolean(r.download_url)),
      message: `Se encontraron ${transcripts.length} archivos de transcript.`,
      recordings: mapped,
    };
  }

  async analyze(dto: RunTranscriptAnalysisDto): Promise<TranscriptAnalysisResult> {
    const apiKey = (dto.apiKey || '').trim();
    if (!apiKey) {
      throw new BadRequestException(
        'Falta el apiKey de Gemini. Pega tu clave de Google AI Studio antes de procesar.',
      );
    }

    const syllabus = (dto.syllabusText || '').trim();
    if (syllabus.length < 10) {
      throw new BadRequestException('El syllabus es demasiado corto (minimo 10 caracteres).');
    }

    let transcript = (dto.transcriptText || '').trim();
    let transcriptSource: 'manual' | 'zoom-recording' = 'manual';
    if (!transcript && dto.videoconferenceId) {
      transcript = await this.fetchTranscriptFromVideoconference(dto.videoconferenceId);
      transcriptSource = 'zoom-recording';
    }
    if (transcript.length < 10) {
      throw new BadRequestException(
        dto.videoconferenceId
          ? 'No se pudo obtener un transcript utilizable desde la videoconferencia. Revisa que exista una grabacion con transcript.'
          : 'El transcript es demasiado corto (minimo 10 caracteres).',
      );
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

    const model =
      dto.model && ALLOWED_GEMINI_MODELS.has(dto.model) ? dto.model : DEFAULT_GEMINI_MODEL;
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
      response = await fetch(
        `${GEMINI_API_BASE}/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(requestBody),
        },
      );
    } catch (error) {
      this.logger.error('Gemini fetch error', error as Error);
      throw new BadRequestException(
        `No se pudo contactar a Gemini: ${(error as Error).message || 'error desconocido'}`,
      );
    }

    const rawText = await response.text();
    if (!response.ok) {
      this.logger.error(`Gemini responded ${response.status}: ${rawText.slice(0, 1000)}`);
      const detail = this.extractGeminiErrorDetail(rawText);
      if (response.status === 429) {
        throw new BadRequestException(
          `Cuota de Gemini agotada para el modelo ${model} (429). ${detail} Sugerencia: espera unos minutos o cambia el modelo (ej. gemini-1.5-flash tiene cuota gratis mas amplia).`,
        );
      }
      if (response.status === 400 || response.status === 401 || response.status === 403) {
        throw new BadRequestException(
          `Gemini rechazo la peticion (${response.status}). ${detail} Verifica que la apiKey sea valida y tenga acceso al modelo ${model}.`,
        );
      }
      throw new BadRequestException(
        `Gemini devolvio ${response.status} con modelo ${model}. ${detail}`,
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
      transcriptSource,
      transcriptPreview: transcript.length > 500 ? transcript.slice(0, 500) + '...' : transcript,
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

  private extractGeminiErrorDetail(rawText: string): string {
    try {
      const parsed = JSON.parse(rawText) as { error?: { message?: string; status?: string } };
      const msg = parsed?.error?.message;
      const status = parsed?.error?.status;
      if (msg) {
        return status ? `${status}: ${msg}` : msg;
      }
    } catch {
      /* fallthrough */
    }
    return rawText.slice(0, 600);
  }

  private isTranscriptRecording(r: MeetingRecordingEntity): boolean {
    const type = String(r.recording_type || '').toUpperCase();
    const ext = String(r.file_extension || '').toUpperCase();
    return type === 'TRANSCRIPT' || type === 'VTT' || ext === 'VTT';
  }

  private async fetchTranscriptFromVideoconference(videoconferenceId: string): Promise<string> {
    const availability = await this.getAvailability(videoconferenceId);
    if (!availability.recordings.length) {
      throw new BadRequestException(availability.message);
    }
    const withUrl = availability.recordings.find((r) => r.hasDownloadUrl);
    if (!withUrl) {
      throw new BadRequestException(
        'Hay grabaciones de transcript pero ninguna tiene download_url registrada. Sincroniza datos Zoom de la reunion.',
      );
    }
    const recording = await this.recordingsRepo.findOne({ where: { id: withUrl.recordingId } });
    if (!recording?.download_url) {
      throw new BadRequestException('Grabacion sin download_url.');
    }
    let raw: string;
    try {
      raw = await this.zoomAccount.downloadRecordingAsText(recording.download_url);
    } catch (error) {
      throw new BadRequestException(
        `No se pudo descargar el transcript desde Zoom: ${(error as Error).message || 'error desconocido'}`,
      );
    }
    return this.flattenVtt(raw);
  }

  /** Convert a WEBVTT blob into plain text with [HH:MM:SS] markers. If already plain, return as-is. */
  private flattenVtt(raw: string): string {
    const text = raw.replace(/\r\n/g, '\n').trim();
    if (!/^WEBVTT/i.test(text) && !text.includes('-->')) {
      return text;
    }
    const lines = text.split('\n');
    const out: string[] = [];
    let currentTs: string | null = null;
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || /^WEBVTT/i.test(trimmed) || /^NOTE\b/i.test(trimmed)) continue;
      if (/^\d+$/.test(trimmed)) continue;
      const tsMatch = trimmed.match(/^(\d{1,2}:\d{2}(?::\d{2})?)[.,]?\d*\s*-->/);
      if (tsMatch) {
        currentTs = tsMatch[1];
        continue;
      }
      const prefix = currentTs ? `[${currentTs}] ` : '';
      out.push(`${prefix}${trimmed}`);
      currentTs = null;
    }
    return out.join('\n').trim() || text;
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

  private normalizeResult(data: Record<string, unknown>): Omit<TranscriptAnalysisResult, 'meta' | 'rawLlmResponse' | 'transcriptPreview' | 'transcriptSource'> {
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
