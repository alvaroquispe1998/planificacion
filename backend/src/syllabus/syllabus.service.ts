import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { newId } from '../common';
import { MeetingTranscriptEntity } from '../entities/audit.entities';
import {
  ClassSyllabusKeywordEntity,
  ClassSyllabusSessionEntity,
  MatchMethodValues,
  MatchStatusValues,
  MeetingSummaryEntity,
  MeetingSyllabusMatchEntity,
  SummaryTypeValues,
} from '../entities/syllabus.entities';
import {
  CreateSyllabusKeywordDto,
  CreateSyllabusSessionDto,
  GenerateSummaryDto,
  MatchSyllabusDto,
  UpdateSyllabusSessionDto,
} from './dto/syllabus.dto';

type MatchMethod = (typeof MatchMethodValues)[number];
type SummaryType = (typeof SummaryTypeValues)[number];

@Injectable()
export class SyllabusService {
  constructor(
    @InjectRepository(ClassSyllabusSessionEntity)
    private readonly sessionsRepo: Repository<ClassSyllabusSessionEntity>,
    @InjectRepository(ClassSyllabusKeywordEntity)
    private readonly keywordsRepo: Repository<ClassSyllabusKeywordEntity>,
    @InjectRepository(MeetingSummaryEntity)
    private readonly summariesRepo: Repository<MeetingSummaryEntity>,
    @InjectRepository(MeetingSyllabusMatchEntity)
    private readonly matchesRepo: Repository<MeetingSyllabusMatchEntity>,
    @InjectRepository(MeetingTranscriptEntity)
    private readonly transcriptsRepo: Repository<MeetingTranscriptEntity>,
  ) {}

  listSessions(classOfferingId?: string) {
    return this.sessionsRepo.find({
      where: classOfferingId ? { class_offering_id: classOfferingId } : {},
      order: { updated_at: 'DESC' },
    });
  }

  createSession(dto: CreateSyllabusSessionDto) {
    const now = new Date();
    return this.sessionsRepo.save(
      this.sessionsRepo.create({
        ...dto,
        created_at: now,
        updated_at: now,
      }),
    );
  }

  async updateSession(id: string, dto: UpdateSyllabusSessionDto) {
    await this.sessionsRepo.update({ id }, { ...dto, updated_at: new Date() });
    return this.sessionsRepo.findOne({ where: { id } });
  }

  async deleteSession(id: string) {
    await this.sessionsRepo.delete({ id });
    return { deleted: true, id };
  }

  listKeywords(syllabusSessionId?: string) {
    return this.keywordsRepo.find({
      where: syllabusSessionId ? { syllabus_session_id: syllabusSessionId } : {},
      order: { keyword: 'ASC' },
    });
  }

  createKeyword(dto: CreateSyllabusKeywordDto) {
    return this.keywordsRepo.save(
      this.keywordsRepo.create({
        ...dto,
        weight: dto.weight !== undefined ? dto.weight.toFixed(2) : null,
      }),
    );
  }

  async deleteKeyword(id: string) {
    await this.keywordsRepo.delete({ id });
    return { deleted: true, id };
  }

  listSummaries(meetingInstanceId?: string) {
    return this.summariesRepo.find({
      where: meetingInstanceId ? { meeting_instance_id: meetingInstanceId } : {},
      order: { created_at: 'DESC' },
    });
  }

  async generateSummary(meetingInstanceId: string, dto: GenerateSummaryDto) {
    const transcript = await this.transcriptsRepo.findOne({
      where: { meeting_instance_id: meetingInstanceId },
      order: { created_at: 'DESC' },
    });
    if (!transcript?.transcript_text?.trim()) {
      throw new NotFoundException(`meeting_transcripts not found for meeting_instance_id ${meetingInstanceId}`);
    }

    const summaryType: SummaryType = dto.summary_type ?? 'EXTRACTIVE';
    const analysis = analyzeText(transcript.transcript_text);

    let summaryText = '';
    if (summaryType === 'EXTRACTIVE') {
      summaryText = analysis.bestSentences.join(' ');
    } else if (summaryType === 'KEYWORDS') {
      summaryText = analysis.topKeywords.map((item) => item.keyword).join(', ');
    } else {
      summaryText = analysis.bestSentences.map((sentence, index) => `${index + 1}. ${sentence}`).join('\n');
    }

    const summary = this.summariesRepo.create({
      id: newId(),
      meeting_instance_id: meetingInstanceId,
      summary_type: summaryType,
      summary_text: summaryText,
      keywords_json: {
        top_keywords: analysis.topKeywords,
      },
      created_at: new Date(),
    });

    return this.summariesRepo.save(summary);
  }

  listMatches(meetingInstanceId?: string) {
    return this.matchesRepo.find({
      where: meetingInstanceId ? { meeting_instance_id: meetingInstanceId } : {},
      order: { created_at: 'DESC' },
    });
  }

  async runMatch(meetingInstanceId: string, syllabusSessionId: string, dto: MatchSyllabusDto) {
    const session = await this.sessionsRepo.findOne({ where: { id: syllabusSessionId } });
    if (!session) {
      throw new NotFoundException(`class_syllabus_sessions ${syllabusSessionId} not found`);
    }

    const transcript = await this.transcriptsRepo.findOne({
      where: { meeting_instance_id: meetingInstanceId },
      order: { created_at: 'DESC' },
    });

    if (!transcript?.transcript_text?.trim()) {
      throw new NotFoundException(`meeting_transcripts not found for meeting_instance_id ${meetingInstanceId}`);
    }

    const keywords = await this.keywordsRepo.find({ where: { syllabus_session_id: syllabusSessionId } });
    const method: MatchMethod = dto.method ?? 'HYBRID';

    const transcriptTokens = tokenize(transcript.transcript_text);
    const transcriptSet = new Set(transcriptTokens);

    let totalWeight = 0;
    let matchedWeight = 0;
    const matchedKeywords: Array<{ keyword: string; weight: number }> = [];

    for (const keywordRow of keywords) {
      const keyword = keywordRow.keyword.toLowerCase();
      const weight = keywordRow.weight ? Number(keywordRow.weight) : 1;
      totalWeight += weight;
      if (transcriptSet.has(keyword)) {
        matchedWeight += weight;
        matchedKeywords.push({ keyword: keywordRow.keyword, weight });
      }
    }

    const keywordScore = totalWeight > 0 ? (matchedWeight / totalWeight) * 100 : 0;
    const tfidfScore = tfidfCosine(session.expected_content, transcript.transcript_text) * 100;

    let finalScore = 0;
    if (method === 'TFIDF') {
      finalScore = tfidfScore;
    } else if (method === 'KEYWORD_OVERLAP') {
      finalScore = keywordScore;
    } else {
      finalScore = (tfidfScore + keywordScore) / 2;
    }

    const status = scoreToStatus(finalScore);

    const match = this.matchesRepo.create({
      id: newId(),
      meeting_instance_id: meetingInstanceId,
      syllabus_session_id: syllabusSessionId,
      method,
      score: finalScore.toFixed(2),
      matched_keywords_json: {
        matched_keywords: matchedKeywords,
        keyword_score: Number(keywordScore.toFixed(2)),
        tfidf_score: Number(tfidfScore.toFixed(2)),
      },
      notes: `method=${method}; tfidf=${tfidfScore.toFixed(2)}; keyword_overlap=${keywordScore.toFixed(2)}`,
      status,
      created_at: new Date(),
    });

    return this.matchesRepo.save(match);
  }
}

function scoreToStatus(score: number): (typeof MatchStatusValues)[number] {
  if (score >= 70) {
    return 'OK';
  }
  if (score >= 40) {
    return 'REVIEW';
  }
  return 'MISMATCH';
}

function analyzeText(text: string) {
  const tokens = tokenize(text);
  const keywordCounts = new Map<string, number>();
  for (const token of tokens) {
    keywordCounts.set(token, (keywordCounts.get(token) ?? 0) + 1);
  }

  const topKeywords = [...keywordCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([keyword, count]) => ({ keyword, count }));

  const topKeywordSet = new Set(topKeywords.map((item) => item.keyword));
  const sentences = text
    .split(/(?<=[.!?])\s+/)
    .map((sentence) => sentence.trim())
    .filter(Boolean);

  const scoredSentences = sentences.map((sentence) => {
    const sentenceTokens = tokenize(sentence);
    const score = sentenceTokens.reduce((sum, token) => sum + (topKeywordSet.has(token) ? 1 : 0), 0);
    return { sentence, score };
  });

  const bestSentences = scoredSentences
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((item) => item.sentence);

  return {
    topKeywords,
    bestSentences: bestSentences.length > 0 ? bestSentences : sentences.slice(0, 3),
  };
}

function tokenize(text: string): string[] {
  return normalizeText(text)
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 2 && !STOPWORDS_ES.has(token));
}

function normalizeText(text: string): string {
  return text
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tfidfCosine(a: string, b: string): number {
  const tokensA = tokenize(a);
  const tokensB = tokenize(b);

  if (tokensA.length === 0 || tokensB.length === 0) {
    return 0;
  }

  const vocab = [...new Set([...tokensA, ...tokensB])];
  const tfA = termFrequency(tokensA);
  const tfB = termFrequency(tokensB);
  const docs = [new Set(tokensA), new Set(tokensB)];

  const vectorA: number[] = [];
  const vectorB: number[] = [];

  for (const term of vocab) {
    const docFreq = docs.reduce((acc, doc) => acc + (doc.has(term) ? 1 : 0), 0);
    const idf = Math.log((1 + docs.length) / (1 + docFreq)) + 1;
    vectorA.push((tfA.get(term) ?? 0) * idf);
    vectorB.push((tfB.get(term) ?? 0) * idf);
  }

  const dot = vectorA.reduce((sum, value, idx) => sum + value * vectorB[idx], 0);
  const normA = Math.sqrt(vectorA.reduce((sum, value) => sum + value ** 2, 0));
  const normB = Math.sqrt(vectorB.reduce((sum, value) => sum + value ** 2, 0));

  if (normA === 0 || normB === 0) {
    return 0;
  }

  return dot / (normA * normB);
}

function termFrequency(tokens: string[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const token of tokens) {
    counts.set(token, (counts.get(token) ?? 0) + 1);
  }

  const total = tokens.length;
  const tf = new Map<string, number>();
  for (const [token, count] of counts.entries()) {
    tf.set(token, count / total);
  }

  return tf;
}

const STOPWORDS_ES = new Set([
  'que',
  'para',
  'con',
  'como',
  'esta',
  'este',
  'del',
  'los',
  'las',
  'por',
  'una',
  'uno',
  'unos',
  'unas',
  'sobre',
  'desde',
  'hasta',
  'entre',
  'tambien',
  'donde',
  'cuando',
  'porque',
  'pero',
  'muy',
  'son',
  'fue',
  'han',
  'hay',
  'sus',
  'mas',
  'sin',
  'esto',
  'estos',
  'estas',
  'ese',
  'esa',
  'esos',
  'esas',
]);
