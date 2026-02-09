import { Type } from 'class-transformer';
import {
  IsBoolean,
  IsDateString,
  IsEnum,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Min,
} from 'class-validator';
import {
  MeetingInstanceStatusValues,
  ParticipantRoleValues,
  RecordingStatusValues,
  RecordingTypeValues,
  TranscriptFormatValues,
  VideoConferenceStatusValues,
} from '../../entities/audit.entities';

export class CreateVideoConferenceDto {
  @IsString()
  id!: string;

  @IsString()
  class_offering_id!: string;

  @IsString()
  semester_week_id!: string;

  @IsString()
  zoom_user_id!: string;

  @IsString()
  zoom_meeting_id!: string;

  @IsString()
  topic!: string;

  @IsDateString()
  start_time!: string;

  @IsDateString()
  end_time!: string;

  @IsOptional()
  @IsString()
  join_url?: string;

  @IsOptional()
  @IsEnum(VideoConferenceStatusValues)
  status?: (typeof VideoConferenceStatusValues)[number];

  @IsOptional()
  @IsString()
  note?: string;
}

export class CreateClassZoomMeetingDto {
  @IsString()
  id!: string;

  @IsString()
  class_offering_id!: string;

  @IsString()
  zoom_meeting_id!: string;

  @IsOptional()
  @IsString()
  zoom_user_id?: string;

  @IsBoolean()
  is_active!: boolean;
}

export class CreateMeetingInstanceDto {
  @IsString()
  id!: string;

  @IsString()
  video_conference_id!: string;

  @IsString()
  zoom_meeting_id!: string;

  @IsString()
  zoom_meeting_uuid!: string;

  @IsOptional()
  @IsDateString()
  scheduled_start?: string;

  @IsOptional()
  @IsDateString()
  scheduled_end?: string;

  @IsOptional()
  @IsDateString()
  actual_start?: string;

  @IsOptional()
  @IsDateString()
  actual_end?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  duration_minutes?: number;

  @IsEnum(MeetingInstanceStatusValues)
  status!: (typeof MeetingInstanceStatusValues)[number];

  @IsOptional()
  raw_json?: Record<string, unknown>;
}

export class UpdateMeetingInstanceDto {
  @IsOptional()
  @IsDateString()
  scheduled_start?: string;

  @IsOptional()
  @IsDateString()
  scheduled_end?: string;

  @IsOptional()
  @IsDateString()
  actual_start?: string;

  @IsOptional()
  @IsDateString()
  actual_end?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  duration_minutes?: number;

  @IsOptional()
  @IsEnum(MeetingInstanceStatusValues)
  status?: (typeof MeetingInstanceStatusValues)[number];

  @IsOptional()
  raw_json?: Record<string, unknown>;
}

export class CreateMeetingParticipantDto {
  @IsString()
  id!: string;

  @IsString()
  meeting_instance_id!: string;

  @IsOptional()
  @IsString()
  zoom_participant_id?: string;

  @IsOptional()
  @IsString()
  zoom_user_id?: string;

  @IsString()
  @IsNotEmpty()
  display_name!: string;

  @IsOptional()
  @IsString()
  email?: string;

  @IsEnum(ParticipantRoleValues)
  role!: (typeof ParticipantRoleValues)[number];

  @IsOptional()
  @IsString()
  teacher_id?: string;
}

export class CreateMeetingAttendanceSegmentDto {
  @IsString()
  id!: string;

  @IsString()
  meeting_instance_id!: string;

  @IsString()
  participant_id!: string;

  @IsDateString()
  join_time!: string;

  @IsOptional()
  @IsDateString()
  leave_time?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  minutes?: number;

  @IsOptional()
  @IsString()
  connection_type?: string;

  @IsOptional()
  @IsString()
  ip_address?: string;

  @IsOptional()
  raw_json?: Record<string, unknown>;
}

export class CreateMeetingRecordingDto {
  @IsString()
  id!: string;

  @IsString()
  meeting_instance_id!: string;

  @IsOptional()
  @IsString()
  zoom_recording_id?: string;

  @IsEnum(RecordingTypeValues)
  recording_type!: (typeof RecordingTypeValues)[number];

  @IsOptional()
  @IsString()
  file_extension?: string;

  @IsOptional()
  @IsString()
  file_size_bytes?: string;

  @IsOptional()
  @IsString()
  download_url?: string;

  @IsOptional()
  @IsString()
  play_url?: string;

  @IsOptional()
  @IsDateString()
  start_time?: string;

  @IsOptional()
  @IsDateString()
  end_time?: string;

  @IsEnum(RecordingStatusValues)
  status!: (typeof RecordingStatusValues)[number];

  @IsOptional()
  raw_json?: Record<string, unknown>;
}

export class CreateMeetingTranscriptDto {
  @IsString()
  id!: string;

  @IsString()
  meeting_instance_id!: string;

  @IsOptional()
  @IsString()
  recording_id?: string;

  @IsEnum(TranscriptFormatValues)
  format!: (typeof TranscriptFormatValues)[number];

  @IsOptional()
  @IsString()
  language?: string;

  @IsOptional()
  @IsString()
  transcript_text?: string;

  @IsOptional()
  @IsString()
  confidence_avg?: string;
}
