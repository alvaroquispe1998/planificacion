import { Column, Entity, Index, PrimaryColumn } from 'typeorm';

export const MatchMethodValues = ['TFIDF', 'KEYWORD_OVERLAP', 'HYBRID'] as const;
export const MatchStatusValues = ['OK', 'REVIEW', 'MISMATCH'] as const;
export const SummaryTypeValues = ['EXTRACTIVE', 'KEYPOINTS', 'KEYWORDS'] as const;

@Entity({ name: 'class_syllabus_sessions' })
@Index(['class_offering_id', 'semester_week_id'], { unique: true })
export class ClassSyllabusSessionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_offering_id!: string;

  @Column({ type: 'varchar', length: 36 })
  semester_week_id!: string;

  @Column({ type: 'varchar', length: 200 })
  session_title!: string;

  @Column({ type: 'text' })
  expected_content!: string;

  @Column({ type: 'text', nullable: true })
  bibliography!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'class_syllabus_keywords' })
@Index(['syllabus_session_id', 'keyword'], { unique: true })
export class ClassSyllabusKeywordEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  syllabus_session_id!: string;

  @Column({ type: 'varchar', length: 80 })
  keyword!: string;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  weight!: string | null;
}

@Entity({ name: 'meeting_syllabus_match' })
export class MeetingSyllabusMatchEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_instance_id!: string;

  @Column({ type: 'varchar', length: 36 })
  syllabus_session_id!: string;

  @Column({ type: 'enum', enum: MatchMethodValues })
  method!: (typeof MatchMethodValues)[number];

  @Column({ type: 'decimal', precision: 5, scale: 2 })
  score!: string;

  @Column({ type: 'json', nullable: true })
  matched_keywords_json!: Record<string, unknown> | null;

  @Column({ type: 'text', nullable: true })
  notes!: string | null;

  @Column({ type: 'enum', enum: MatchStatusValues })
  status!: (typeof MatchStatusValues)[number];

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'meeting_summaries' })
export class MeetingSummaryEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_instance_id!: string;

  @Column({ type: 'enum', enum: SummaryTypeValues })
  summary_type!: (typeof SummaryTypeValues)[number];

  @Column({ type: 'text' })
  summary_text!: string;

  @Column({ type: 'json', nullable: true })
  keywords_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}
