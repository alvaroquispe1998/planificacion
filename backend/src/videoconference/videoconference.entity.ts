import { Column, Entity, Index, JoinColumn, ManyToOne, OneToMany, PrimaryColumn } from 'typeorm';

export const PlanningSubsectionVideoconferenceStatusValues = [
    'CREATING',
    'CREATED_UNMATCHED',
    'MATCHED',
    'ERROR',
] as const;

export const PlanningSubsectionVideoconferenceAuditStatusValues = [
    'PENDING',
    'SYNCED',
    'ERROR',
] as const;

export const PlanningSubsectionVideoconferenceAkademicCopyStatusValues = [
    'PENDING',
    'COPIED',
    'ERROR',
] as const;

export const PlanningSubsectionVideoconferenceDeleteStatusValues = [
    'PENDING',
    'DELETING',
    'DELETED',
    'ERROR',
] as const;

export const PlanningSubsectionVideoconferenceLinkModeValues = [
    'OWNED',
    'INHERITED',
] as const;

export const PlanningSubsectionVideoconferenceOverrideActionValues = [
    'KEEP',
    'SKIP',
    'RESCHEDULE',
] as const;

export const PlanningSubsectionVideoconferenceOverrideReasonValues = [
    'HOLIDAY',
    'WEATHER',
    'OTHER',
] as const;

export const VideoconferenceGenerationBatchStatusValues = [
    'PENDING',
    'RUNNING',
    'DONE',
    'FAILED',
] as const;

export const VideoconferenceGenerationResultStatusValues = [
    'MATCHED',
    'CREATED_UNMATCHED',
    'BLOCKED_EXISTING',
    'NO_AVAILABLE_ZOOM_USER',
    'VALIDATION_ERROR',
    'ERROR',
] as const;

@Entity({ name: 'videoconferences' })
@Index(['class_meeting_id'])
@Index(['start_time', 'end_time'])
export class VideoconferenceEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    class_meeting_id!: string;

    @Column({ type: 'varchar', length: 150, nullable: true })
    zoom_account_email!: string | null;

    @Column({ type: 'varchar', length: 255, nullable: true })
    zoom_join_url!: string | null;

    @Column({ type: 'varchar', length: 255, nullable: true })
    zoom_start_url!: string | null;

    @Column({ type: 'varchar', length: 50, nullable: true })
    zoom_meeting_id!: string | null;

    @Column({ type: 'datetime' })
    start_time!: Date;

    @Column({ type: 'datetime' })
    end_time!: Date;

    @Column({ type: 'varchar', length: 20, default: 'GENERATED' })
    status!: string;

    @Column({ type: 'json', nullable: true })
    metadata!: Record<string, unknown> | null;

    @Column({ type: 'datetime' })
    created_at!: Date;
}

@Entity({ name: 'vc_periods' })
export class VcPeriodEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 50, nullable: true })
    text!: string | null;

    @Column({ type: 'boolean', default: false })
    selected!: boolean;

    @Column({ type: 'boolean', default: true })
    is_active!: boolean;
}

@Entity({ name: 'vc_faculties' })
export class VcFacultyEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 150 })
    name!: string;

    @OneToMany(() => VcAcademicProgramEntity, (program) => program.faculty)
    programs!: VcAcademicProgramEntity[];
}

@Entity({ name: 'vc_academic_programs' })
export class VcAcademicProgramEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 200 })
    name!: string;

    @Column({ type: 'varchar', length: 36 })
    faculty_id!: string;

    @ManyToOne(() => VcFacultyEntity, (faculty) => faculty.programs)
    @JoinColumn({ name: 'faculty_id' })
    faculty!: VcFacultyEntity;

    @OneToMany(() => VcCourseEntity, (course) => course.program)
    courses!: VcCourseEntity[];
}

@Entity({ name: 'vc_courses' })
export class VcCourseEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 50, nullable: true })
    code!: string | null;

    @Column({ type: 'varchar', length: 200 })
    name!: string;

    @Column({ type: 'varchar', length: 36 })
    program_id!: string;

    @ManyToOne(() => VcAcademicProgramEntity, (program) => program.courses)
    @JoinColumn({ name: 'program_id' })
    program!: VcAcademicProgramEntity;

    @OneToMany(() => VcSectionEntity, (section) => section.course)
    sections!: VcSectionEntity[];
}

@Entity({ name: 'vc_sections' })
export class VcSectionEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 50 })
    name!: string;

    @Column({ type: 'varchar', length: 36 })
    course_id!: string;

    @ManyToOne(() => VcCourseEntity, (course) => course.sections)
    @JoinColumn({ name: 'course_id' })
    course!: VcCourseEntity;

    @Column({ type: 'json', nullable: true })
    teachers_json!: Record<string, unknown>[] | null;

    @Column({ type: 'int', unsigned: true, nullable: true })
    student_count!: number | null;

    @Column({ type: 'datetime', nullable: true })
    roster_synced_at!: Date | null;
}

@Entity({ name: 'vc_section_students' })
@Index(['section_id'])
@Index(['dni'])
export class VcSectionStudentEntity {
    @PrimaryColumn({ type: 'varchar', length: 80 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    section_id!: string;

    @Column({ type: 'varchar', length: 36, nullable: true })
    student_id!: string | null;

    @Column({ type: 'varchar', length: 30, nullable: true })
    dni!: string | null;

    @Column({ type: 'varchar', length: 255, nullable: true })
    full_name!: string | null;

    @Column({ type: 'varchar', length: 255, nullable: true })
    email!: string | null;

    @Column({ type: 'varchar', length: 60, nullable: true })
    state!: string | null;

    @Column({ type: 'json', nullable: true })
    raw_json!: Record<string, unknown> | null;

    @Column({ type: 'datetime' })
    synced_at!: Date;
}

@Entity({ name: 'zoom_config' })
export class ZoomConfigEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 255, default: '' })
    accountId!: string;

    @Column({ type: 'varchar', length: 255, default: '' })
    clientId!: string;

    @Column({ type: 'varchar', length: 512, default: '' })
    clientSecret!: string;

    @Column({ type: 'int', unsigned: true, default: 2 })
    maxConcurrent!: number;

    @Column({ type: 'int', unsigned: true, default: 20 })
    pageSize!: number;

    @Column({ type: 'varchar', length: 60, default: 'America/Lima' })
    timezone!: string;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'videoconference_zoom_pool_users' })
@Index(['zoom_user_id'], { unique: true })
@Index(['sort_order'])
export class VideoconferenceZoomPoolUserEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    zoom_user_id!: string;

    @Column({ type: 'int', unsigned: true, default: 1 })
    sort_order!: number;

    @Column({ type: 'boolean', default: true })
    is_active!: boolean;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'zoom_groups' })
@Index(['code'], { unique: true })
export class ZoomGroupEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 80 })
    name!: string;

    @Column({ type: 'varchar', length: 40 })
    code!: string;

    @Column({ type: 'boolean', default: true })
    is_active!: boolean;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'zoom_group_users' })
@Index(['group_id', 'zoom_user_id'], { unique: true })
@Index(['group_id', 'sort_order'])
export class ZoomGroupUserEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    group_id!: string;

    @Column({ type: 'varchar', length: 36 })
    zoom_user_id!: string;

    @Column({ type: 'int', unsigned: true, default: 1 })
    sort_order!: number;

    @Column({ type: 'boolean', default: true })
    is_active!: boolean;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'planning_subsection_schedule_vc_inheritances' })
@Index(['parent_schedule_id'])
@Index(['child_schedule_id'], { unique: true })
export class PlanningSubsectionScheduleVcInheritanceEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    parent_schedule_id!: string;

    @Column({ type: 'varchar', length: 36 })
    child_schedule_id!: string;

    @Column({ type: 'varchar', length: 500, nullable: true })
    notes!: string | null;

    @Column({ type: 'boolean', default: true })
    is_active!: boolean;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceAkademicCopyStatusValues,
        nullable: true,
        default: null,
    })
    akademic_copy_status!: (typeof PlanningSubsectionVideoconferenceAkademicCopyStatusValues)[number] | null;

    @Column({ type: 'datetime', nullable: true })
    akademic_copied_at!: Date | null;

    @Column({ type: 'json', nullable: true })
    akademic_copy_payload_json!: Record<string, unknown> | null;

    @Column({ type: 'json', nullable: true })
    akademic_copy_response_json!: Record<string, unknown> | null;

    @Column({ type: 'text', nullable: true })
    akademic_copy_error!: string | null;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'planning_subsection_videoconferences' })
@Index(['planning_subsection_schedule_id', 'conference_date'], { unique: true })
@Index(['zoom_user_id', 'conference_date'])
@Index(['zoom_meeting_id'])
@Index(['planning_offer_id', 'scheduled_start'])
@Index(['planning_section_id', 'conference_date'])
@Index(['status', 'audit_sync_status'])
@Index(['owner_videoconference_id'])
@Index(['inheritance_mapping_id'])
export class PlanningSubsectionVideoconferenceEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    planning_offer_id!: string;

    @Column({ type: 'varchar', length: 36 })
    planning_section_id!: string;

    @Column({ type: 'varchar', length: 36 })
    planning_subsection_id!: string;

    @Column({ type: 'varchar', length: 36 })
    planning_subsection_schedule_id!: string;

    @Column({ type: 'date' })
    conference_date!: string;

    @Column({ type: 'varchar', length: 20 })
    day_of_week!: string;

    @Column({ type: 'time' })
    start_time!: string;

    @Column({ type: 'time' })
    end_time!: string;

    @Column({ type: 'datetime' })
    scheduled_start!: Date;

    @Column({ type: 'datetime' })
    scheduled_end!: Date;

    @Column({ type: 'varchar', length: 36, nullable: true })
    zoom_user_id!: string | null;

    @Column({ type: 'varchar', length: 190, nullable: true })
    zoom_user_email!: string | null;

    @Column({ type: 'varchar', length: 150, nullable: true })
    zoom_user_name!: string | null;

    @Column({ type: 'varchar', length: 50, nullable: true })
    zoom_meeting_id!: string | null;

    @Column({ type: 'varchar', length: 255, nullable: true })
    topic!: string | null;

    @Column({ type: 'varchar', length: 255, nullable: true })
    aula_virtual_name!: string | null;

    @Column({ type: 'varchar', length: 1024, nullable: true })
    join_url!: string | null;

    @Column({ type: 'varchar', length: 2048, nullable: true })
    start_url!: string | null;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceLinkModeValues,
        default: 'OWNED',
    })
    link_mode!: (typeof PlanningSubsectionVideoconferenceLinkModeValues)[number];

    @Column({ type: 'varchar', length: 36, nullable: true })
    owner_videoconference_id!: string | null;

    @Column({ type: 'varchar', length: 36, nullable: true })
    inheritance_mapping_id!: string | null;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceStatusValues,
        default: 'CREATING',
    })
    status!: (typeof PlanningSubsectionVideoconferenceStatusValues)[number];

    @Column({ type: 'int', unsigned: true, default: 0 })
    match_attempts!: number;

    @Column({ type: 'datetime', nullable: true })
    matched_at!: Date | null;

    @Column({ type: 'text', nullable: true })
    error_message!: string | null;

    @Column({ type: 'json', nullable: true })
    payload_json!: Record<string, unknown> | null;

    @Column({ type: 'json', nullable: true })
    response_json!: Record<string, unknown> | null;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceAuditStatusValues,
        default: 'PENDING',
    })
    audit_sync_status!: (typeof PlanningSubsectionVideoconferenceAuditStatusValues)[number];

    @Column({ type: 'datetime', nullable: true })
    audit_synced_at!: Date | null;

    @Column({ type: 'text', nullable: true })
    audit_sync_error!: string | null;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceAkademicCopyStatusValues,
        nullable: true,
        default: null,
    })
    akademic_copy_status!: (typeof PlanningSubsectionVideoconferenceAkademicCopyStatusValues)[number] | null;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceDeleteStatusValues,
        nullable: true,
        default: null,
    })
    delete_status!: (typeof PlanningSubsectionVideoconferenceDeleteStatusValues)[number] | null;

    @Column({ type: 'datetime', nullable: true })
    deleted_at!: Date | null;

    @Column({ type: 'varchar', length: 120, nullable: true })
    deleted_by!: string | null;

    @Column({ type: 'text', nullable: true })
    delete_error!: string | null;

    @Column({ type: 'datetime', nullable: true })
    zoom_deleted_at!: Date | null;

    @Column({ type: 'datetime', nullable: true })
    akademic_deleted_at!: Date | null;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'planning_subsection_videoconference_overrides' })
@Index(['planning_subsection_schedule_id', 'conference_date'], { unique: true })
export class PlanningSubsectionVideoconferenceOverrideEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    planning_subsection_schedule_id!: string;

    @Column({ type: 'date' })
    conference_date!: string;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceOverrideActionValues,
        default: 'KEEP',
    })
    action!: (typeof PlanningSubsectionVideoconferenceOverrideActionValues)[number];

    @Column({ type: 'date', nullable: true })
    override_date!: string | null;

    @Column({ type: 'time', nullable: true })
    override_start_time!: string | null;

    @Column({ type: 'time', nullable: true })
    override_end_time!: string | null;

    @Column({
        type: 'enum',
        enum: PlanningSubsectionVideoconferenceOverrideReasonValues,
        default: 'OTHER',
    })
    reason_code!: (typeof PlanningSubsectionVideoconferenceOverrideReasonValues)[number];

    @Column({ type: 'varchar', length: 500, nullable: true })
    notes!: string | null;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'videoconference_generation_batches' })
@Index(['status', 'created_at'])
export class VideoconferenceGenerationBatchEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({
        type: 'enum',
        enum: VideoconferenceGenerationBatchStatusValues,
        default: 'PENDING',
    })
    status!: (typeof VideoconferenceGenerationBatchStatusValues)[number];

    @Column({ type: 'int', unsigned: true, default: 0 })
    requested_occurrences!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    processed_occurrences!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    matched!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    created_unmatched!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    blocked_existing!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    no_available_zoom_user!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    validation_errors!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    errors!: number;

    @Column({ type: 'int', unsigned: true, default: 0 })
    progress_percent!: number;

    @Column({ type: 'varchar', length: 255, nullable: true })
    current_message!: string | null;

    @Column({ type: 'json', nullable: true })
    params_json!: Record<string, unknown> | null;

    @Column({ type: 'json', nullable: true })
    summary_json!: Record<string, unknown> | null;

    @Column({ type: 'text', nullable: true })
    error_message!: string | null;

    @Column({ type: 'varchar', length: 36, nullable: true })
    created_by!: string | null;

    @Column({ type: 'datetime', nullable: true })
    started_at!: Date | null;

    @Column({ type: 'datetime', nullable: true })
    finished_at!: Date | null;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

@Entity({ name: 'videoconference_generation_batch_results' })
@Index(['batch_id', 'created_at'])
@Index(['batch_id', 'status'])
export class VideoconferenceGenerationBatchResultEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    batch_id!: string;

    @Column({ type: 'varchar', length: 36, nullable: true })
    schedule_id!: string | null;

    @Column({ type: 'varchar', length: 120, nullable: true })
    occurrence_key!: string | null;

    @Column({ type: 'date', nullable: true })
    conference_date!: string | null;

    @Column({
        type: 'enum',
        enum: VideoconferenceGenerationResultStatusValues,
    })
    status!: (typeof VideoconferenceGenerationResultStatusValues)[number];

    @Column({ type: 'text' })
    message!: string;

    @Column({ type: 'varchar', length: 36, nullable: true })
    record_id!: string | null;

    @Column({ type: 'varchar', length: 36, nullable: true })
    zoom_user_id!: string | null;

    @Column({ type: 'varchar', length: 190, nullable: true })
    zoom_user_email!: string | null;

    @Column({ type: 'varchar', length: 50, nullable: true })
    zoom_meeting_id!: string | null;

    @Column({ type: 'datetime' })
    created_at!: Date;
}

@Entity({ name: 'vc_schedule_host_rules' })
@Index(['schedule_id'], { unique: true })
@Index(['zoom_group_id'])
export class VcScheduleHostRuleEntity {
    @PrimaryColumn({ type: 'varchar', length: 36 })
    id!: string;

    @Column({ type: 'varchar', length: 36 })
    schedule_id!: string;

    @Column({ type: 'varchar', length: 36, nullable: true })
    zoom_group_id!: string | null;

    @Column({ type: 'varchar', length: 36, nullable: true })
    zoom_user_id!: string | null;

    @Column({ type: 'varchar', length: 500, nullable: true })
    notes!: string | null;

    @Column({ type: 'boolean', default: true })
    is_active!: boolean;

    @Column({ type: 'boolean', default: false })
    lock_host!: boolean;

    @Column({ type: 'boolean', default: false })
    skip_zoom!: boolean;

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}

