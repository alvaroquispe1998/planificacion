import { Column, Entity, Index, PrimaryColumn } from 'typeorm';

export const ManualVideoconferenceTypeValues = ['UNIQUE', 'WEEKLY'] as const;
export const ManualVideoconferenceStatusValues = [
    'CREATED',
    'DRAFT_NO_HOST',
    'APPROVED_WITH_BACKUP',
    'ERROR',
    'CANCELLED',
] as const;

/**
 * Stores manually created videoconferences (not tied to planning or Aula Virtual).
 */
@Entity({ name: 'manual_videoconferences' })
@Index(['created_by_user_id'])
@Index(['zoom_group_id'])
@Index(['status'])
@Index(['start_time'])
export class ManualVideoconferenceEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    /** System user that requested the meeting. */
    @Column({ type: 'varchar', length: 36 })
    created_by_user_id!: string;

    /** Zoom group used to find a host. */
    @Column({ type: 'varchar', length: 36 })
    zoom_group_id!: string;

    /** The Zoom user ultimately assigned as host (null when DRAFT_NO_HOST). */
    @Column({ type: 'varchar', length: 36, nullable: true })
    assigned_zoom_user_id!: string | null;

    /** The backup Zoom user used when the primary was unavailable. */
    @Column({ type: 'varchar', length: 36, nullable: true })
    backup_zoom_user_id!: string | null;

    @Column({ type: 'enum', enum: ManualVideoconferenceTypeValues })
    type!: (typeof ManualVideoconferenceTypeValues)[number];

    @Column({ type: 'varchar', length: 300 })
    topic!: string;

    @Column({ type: 'text', nullable: true })
    agenda!: string | null;

    @Column({ type: 'datetime' })
    start_time!: Date;

    @Column({ type: 'datetime' })
    end_time!: Date;

    @Column({ type: 'int', unsigned: true })
    duration_minutes!: number;

    /**
     * JSON blob with recurrence settings for WEEKLY meetings.
     * Example: { end_date_time: '2026-12-31T23:59:00Z', weekly_days: '2' }
     */
    @Column({ type: 'json', nullable: true })
    recurrence_json!: Record<string, unknown> | null;

    @Column({
        type: 'enum',
        enum: ManualVideoconferenceStatusValues,
        default: 'CREATED',
    })
    status!: (typeof ManualVideoconferenceStatusValues)[number];

    /** Zoom meeting numeric ID (string because it can be large). */
    @Column({ type: 'varchar', length: 50, nullable: true })
    zoom_meeting_id!: string | null;

    @Column({ type: 'varchar', length: 512, nullable: true })
    join_url!: string | null;

    @Column({ type: 'varchar', length: 1024, nullable: true })
    start_url!: string | null;

    /** The exact payload sent to Zoom when creating the meeting. */
    @Column({ type: 'json', nullable: true })
    zoom_payload_json!: Record<string, unknown> | null;

    /** Raw Zoom API response for auditing. */
    @Column({ type: 'json', nullable: true })
    zoom_response_json!: Record<string, unknown> | null;

    @Column({ type: 'text', nullable: true })
    error_message!: string | null;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

/**
 * Assigns a system user to one or more Zoom groups so the host selector
 * in the creator UI only shows allowed groups.
 */
@Entity({ name: 'manual_videoconference_user_zoom_groups' })
@Index(['user_id', 'zoom_group_id'], { unique: true })
@Index(['user_id'])
export class ManualVideoconferenceUserZoomGroupEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    user_id!: string;

    @Column({ type: 'varchar', length: 36 })
    zoom_group_id!: string;

    @Column({ type: 'boolean', default: true })
    is_active!: boolean;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}
