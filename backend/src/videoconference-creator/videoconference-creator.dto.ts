import { Type } from 'class-transformer';
import { IsBoolean, IsDateString, IsEnum, IsInt, IsNotEmpty, IsOptional, IsString, IsUUID, Max, MaxLength, Min } from 'class-validator';
import { ManualVideoconferenceTypeValues } from '../entities/videoconference-creator.entities';

// ── Create Meeting ────────────────────────────────────────────────────────────

export class CreateManualVideoconferenceDto {
    @IsEnum(ManualVideoconferenceTypeValues)
    type!: (typeof ManualVideoconferenceTypeValues)[number];

    @IsUUID()
    zoom_group_id!: string;

    @IsString()
    @IsNotEmpty()
    @MaxLength(300)
    topic!: string;

    @IsOptional()
    @IsString()
    @MaxLength(2000)
    agenda?: string;

    /**
     * ISO-8601 start datetime. The server stores it as-is and passes it to Zoom.
     * Example: "2026-06-15T10:00:00"
     */
    @IsDateString()
    start_time!: string;

    /** Duration in minutes (Zoom requires this field). */
    @IsInt()
    @Min(15)
    @Max(720)
    duration_minutes!: number;

    /**
     * Only required when type === 'WEEKLY'.
     * ISO-8601 end date for the recurrence series.
     * Example: "2026-12-31"
     */
    @IsOptional()
    @IsDateString()
    recurrence_end_date?: string;

    /**
     * Days of the week the recurrence repeats (1=Sunday … 7=Saturday).
     * Only relevant when type === 'WEEKLY'.
     * Zoom expects a comma-separated string like "2,4" for Mon + Wed.
     */
    @IsOptional()
    @IsString()
    recurrence_weekly_days?: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    request_approval_if_no_host?: boolean;
}

// ── Approve Backup ────────────────────────────────────────────────────────────

export class ApproveDraftBackupDto {
    /**
     * Override the backup host. When omitted the service uses the group's
     * configured backup_zoom_user_id.
     */
    @IsOptional()
    @IsUUID()
    override_backup_zoom_user_id?: string;
}

// ── Assign User Zoom Groups (security panel) ──────────────────────────────────

export class SetUserZoomGroupsDto {
    @IsUUID('4', { each: true })
    zoom_group_ids!: string[];
}
