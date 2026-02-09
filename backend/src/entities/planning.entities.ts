import { Column, Entity, Index, PrimaryColumn } from 'typeorm';

export const GroupTypeValues = ['THEORY', 'PRACTICE', 'LAB'] as const;
export const DayOfWeekValues = [
  'LUNES',
  'MARTES',
  'MIERCOLES',
  'JUEVES',
  'VIERNES',
  'SABADO',
  'DOMINGO',
] as const;
export const ClassTeacherRoleValues = ['TITULAR', 'PRACTICE_HEAD', 'ASSISTANT'] as const;
export const ClassGroupTeacherRoleValues = [
  'PRIMARY',
  'SUPPORT',
  'ASSISTANT',
  'PRACTICE_HEAD',
  'LAB_INSTRUCTOR',
] as const;
export const CourseFormatValues = ['T', 'P', 'TP', 'LAB', 'MIXED'] as const;
export const ConflictTypeValues = [
  'TEACHER_OVERLAP',
  'CLASSROOM_OVERLAP',
  'GROUP_OVERLAP',
  'SECTION_OVERLAP',
] as const;
export const ConflictSeverityValues = ['INFO', 'WARNING', 'CRITICAL'] as const;

@Entity({ name: 'class_offerings' })
export class ClassOfferingEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  semester_id!: string;

  @Column({ type: 'varchar', length: 36 })
  study_plan_id!: string;

  @Column({ type: 'varchar', length: 36 })
  academic_program_id!: string;

  @Column({ type: 'varchar', length: 36 })
  course_id!: string;

  @Column({ type: 'varchar', length: 36 })
  course_section_id!: string;

  @Column({ type: 'varchar', length: 36 })
  campus_id!: string;

  @Column({ type: 'varchar', length: 36 })
  delivery_modality_id!: string;

  @Column({ type: 'varchar', length: 36 })
  shift_id!: string;

  @Column({ type: 'int', nullable: true })
  projected_vacancies!: number | null;

  @Column({ type: 'boolean' })
  status!: boolean;
}

@Entity({ name: 'class_groups' })
export class ClassGroupEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_offering_id!: string;

  @Column({ type: 'enum', enum: GroupTypeValues })
  group_type!: (typeof GroupTypeValues)[number];

  @Column({ type: 'varchar', length: 20 })
  code!: string;

  @Column({ type: 'int', nullable: true })
  capacity!: number | null;

  @Column({ type: 'varchar', length: 200, nullable: true })
  note!: string | null;
}

@Entity({ name: 'class_meetings' })
export class ClassMeetingEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_offering_id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_group_id!: string;

  @Column({ type: 'enum', enum: DayOfWeekValues })
  day_of_week!: (typeof DayOfWeekValues)[number];

  @Column({ type: 'time' })
  start_time!: string;

  @Column({ type: 'time' })
  end_time!: string;

  @Column({ type: 'int', nullable: true })
  minutes!: number | null;

  @Column({ type: 'int', nullable: true })
  academic_hours!: number | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  classroom_id!: string | null;
}

@Entity({ name: 'class_teachers' })
export class ClassTeacherEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_offering_id!: string;

  @Column({ type: 'varchar', length: 36 })
  teacher_id!: string;

  @Column({ type: 'enum', enum: ClassTeacherRoleValues })
  role!: (typeof ClassTeacherRoleValues)[number];

  @Column({ type: 'boolean' })
  is_primary!: boolean;
}

@Entity({ name: 'class_group_teachers' })
@Index(['class_group_id', 'teacher_id'], { unique: true })
@Index(['teacher_id'])
export class ClassGroupTeacherEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  class_group_id!: string;

  @Column({ type: 'varchar', length: 36 })
  teacher_id!: string;

  @Column({ type: 'enum', enum: ClassGroupTeacherRoleValues })
  role!: (typeof ClassGroupTeacherRoleValues)[number];

  @Column({ type: 'boolean', default: false })
  is_primary!: boolean;

  @Column({ type: 'datetime', nullable: true })
  assigned_from!: Date | null;

  @Column({ type: 'datetime', nullable: true })
  assigned_to!: Date | null;

  @Column({ type: 'varchar', length: 300, nullable: true })
  notes!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'course_section_hour_requirements' })
@Index(['course_section_id'], { unique: true })
export class CourseSectionHourRequirementEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  course_section_id!: string;

  @Column({ type: 'enum', enum: CourseFormatValues })
  course_format!: (typeof CourseFormatValues)[number];

  @Column({ type: 'int', default: 0 })
  theory_hours_academic!: number;

  @Column({ type: 'int', default: 0 })
  practice_hours_academic!: number;

  @Column({ type: 'int', default: 0 })
  lab_hours_academic!: number;

  @Column({ type: 'int', default: 50 })
  academic_minutes_per_hour!: number;

  @Column({ type: 'varchar', length: 300, nullable: true })
  notes!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'schedule_conflicts' })
@Index(['semester_id', 'conflict_type'])
@Index(['teacher_id'])
@Index(['classroom_id'])
@Index(['class_group_id'])
@Index(['class_offering_id'])
export class ScheduleConflictEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  semester_id!: string;

  @Column({ type: 'enum', enum: ConflictTypeValues })
  conflict_type!: (typeof ConflictTypeValues)[number];

  @Column({ type: 'enum', enum: ConflictSeverityValues })
  severity!: (typeof ConflictSeverityValues)[number];

  @Column({ type: 'varchar', length: 36, nullable: true })
  teacher_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  classroom_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  class_group_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  class_offering_id!: string | null;

  @Column({ type: 'varchar', length: 36 })
  meeting_a_id!: string;

  @Column({ type: 'varchar', length: 36 })
  meeting_b_id!: string;

  @Column({ type: 'int', default: 0 })
  overlap_minutes!: number;

  @Column({ type: 'json', nullable: true })
  detail_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  detected_at!: Date;

  @Column({ type: 'datetime' })
  created_at!: Date;
}
