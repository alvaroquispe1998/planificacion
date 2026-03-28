import { Column, Entity, Index, JoinColumn, ManyToOne, OneToMany, PrimaryColumn } from 'typeorm';

export const PlanningSubsectionVideoconferenceStatusValues = [
    'CREATING',
    'CREATED_UNMATCHED',
    'MATCHED',
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

@Entity({ name: 'planning_subsection_videoconferences' })
@Index(['planning_subsection_schedule_id', 'conference_date'], { unique: true })
@Index(['zoom_user_id', 'conference_date'])
@Index(['zoom_meeting_id'])
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

    @Column({ type: 'datetime' })
    created_at!: Date;

    @Column({ type: 'datetime' })
    updated_at!: Date;
}
