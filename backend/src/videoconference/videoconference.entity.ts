import { Column, Entity, Index, JoinColumn, ManyToOne, OneToMany, PrimaryColumn } from 'typeorm';

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
