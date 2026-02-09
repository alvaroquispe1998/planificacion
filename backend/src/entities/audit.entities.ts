import { Column, Entity, Index, PrimaryColumn } from 'typeorm';

export const VideoConferenceStatusValues = ['PROGRAMADA', 'REPROGRAMADA', 'CANCELADA'] as const;
export const MeetingInstanceStatusValues = ['CREATED', 'IN_PROGRESS', 'ENDED', 'ERROR'] as const;
export const ParticipantRoleValues = ['HOST', 'CO_HOST', 'PANELIST', 'ATTENDEE', 'UNKNOWN'] as const;
export const RecordingTypeValues = ['MP4', 'M4A', 'CHAT', 'TRANSCRIPT', 'VTT', 'OTHER'] as const;
export const RecordingStatusValues = ['AVAILABLE', 'DELETED', 'EXPIRED', 'ERROR'] as const;
export const TranscriptFormatValues = ['TEXT', 'VTT', 'SRT', 'JSON'] as const;

@Entity({ name: 'zoom_users' })
export class ZoomUserEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 150 })
  name!: string;

  @Column({ type: 'varchar', length: 190 })
  email!: string;
}

@Entity({ name: 'video_conferences' })
export class VideoConferenceEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_offering_id!: string;

  @Column({ type: 'varchar', length: 36 })
  semester_week_id!: string;

  @Column({ type: 'varchar', length: 36 })
  zoom_user_id!: string;

  @Column({ type: 'varchar', length: 50 })
  zoom_meeting_id!: string;

  @Column({ type: 'varchar', length: 200 })
  topic!: string;

  @Column({ type: 'datetime' })
  start_time!: Date;

  @Column({ type: 'datetime' })
  end_time!: Date;

  @Column({ type: 'varchar', length: 500, nullable: true })
  join_url!: string | null;

  @Column({ type: 'enum', enum: VideoConferenceStatusValues, nullable: true })
  status!: (typeof VideoConferenceStatusValues)[number] | null;

  @Column({ type: 'text', nullable: true })
  note!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'class_zoom_meetings' })
@Index(['class_offering_id', 'zoom_meeting_id'], { unique: true })
export class ClassZoomMeetingEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_offering_id!: string;

  @Column({ type: 'varchar', length: 50 })
  zoom_meeting_id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  zoom_user_id!: string | null;

  @Column({ type: 'boolean' })
  is_active!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'meeting_instances' })
export class MeetingInstanceEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  video_conference_id!: string;

  @Column({ type: 'varchar', length: 50 })
  zoom_meeting_id!: string;

  @Column({ type: 'varchar', length: 128 })
  zoom_meeting_uuid!: string;

  @Column({ type: 'datetime', nullable: true })
  scheduled_start!: Date | null;

  @Column({ type: 'datetime', nullable: true })
  scheduled_end!: Date | null;

  @Column({ type: 'datetime', nullable: true })
  actual_start!: Date | null;

  @Column({ type: 'datetime', nullable: true })
  actual_end!: Date | null;

  @Column({ type: 'int', nullable: true })
  duration_minutes!: number | null;

  @Column({ type: 'enum', enum: MeetingInstanceStatusValues })
  status!: (typeof MeetingInstanceStatusValues)[number];

  @Column({ type: 'json', nullable: true })
  raw_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'meeting_participants' })
export class MeetingParticipantEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_instance_id!: string;

  @Column({ type: 'varchar', length: 128, nullable: true })
  zoom_participant_id!: string | null;

  @Column({ type: 'varchar', length: 128, nullable: true })
  zoom_user_id!: string | null;

  @Column({ type: 'varchar', length: 200 })
  display_name!: string;

  @Column({ type: 'varchar', length: 190, nullable: true })
  email!: string | null;

  @Column({ type: 'enum', enum: ParticipantRoleValues })
  role!: (typeof ParticipantRoleValues)[number];

  @Column({ type: 'varchar', length: 36, nullable: true })
  teacher_id!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'meeting_attendance_segments' })
export class MeetingAttendanceSegmentEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_instance_id!: string;

  @Column({ type: 'varchar', length: 36 })
  participant_id!: string;

  @Column({ type: 'datetime' })
  join_time!: Date;

  @Column({ type: 'datetime', nullable: true })
  leave_time!: Date | null;

  @Column({ type: 'int', nullable: true })
  minutes!: number | null;

  @Column({ type: 'varchar', length: 40, nullable: true })
  connection_type!: string | null;

  @Column({ type: 'varchar', length: 45, nullable: true })
  ip_address!: string | null;

  @Column({ type: 'json', nullable: true })
  raw_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'meeting_teacher_metrics' })
export class MeetingTeacherMetricEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_instance_id!: string;

  @Column({ type: 'varchar', length: 36 })
  teacher_id!: string;

  @Column({ type: 'int' })
  join_count!: number;

  @Column({ type: 'int' })
  total_minutes!: number;

  @Column({ type: 'datetime' })
  first_join!: Date;

  @Column({ type: 'datetime', nullable: true })
  last_leave!: Date | null;

  @Column({ type: 'boolean' })
  is_host!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'meeting_recordings' })
export class MeetingRecordingEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_instance_id!: string;

  @Column({ type: 'varchar', length: 128, nullable: true })
  zoom_recording_id!: string | null;

  @Column({ type: 'enum', enum: RecordingTypeValues })
  recording_type!: (typeof RecordingTypeValues)[number];

  @Column({ type: 'varchar', length: 10, nullable: true })
  file_extension!: string | null;

  @Column({ type: 'bigint', nullable: true })
  file_size_bytes!: string | null;

  @Column({ type: 'varchar', length: 500, nullable: true })
  download_url!: string | null;

  @Column({ type: 'varchar', length: 500, nullable: true })
  play_url!: string | null;

  @Column({ type: 'datetime', nullable: true })
  start_time!: Date | null;

  @Column({ type: 'datetime', nullable: true })
  end_time!: Date | null;

  @Column({ type: 'enum', enum: RecordingStatusValues })
  status!: (typeof RecordingStatusValues)[number];

  @Column({ type: 'json', nullable: true })
  raw_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'meeting_transcripts' })
export class MeetingTranscriptEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_instance_id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  recording_id!: string | null;

  @Column({ type: 'enum', enum: TranscriptFormatValues })
  format!: (typeof TranscriptFormatValues)[number];

  @Column({ type: 'varchar', length: 10, nullable: true })
  language!: string | null;

  @Column({ type: 'longtext', nullable: true })
  transcript_text!: string | null;

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  confidence_avg!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}
