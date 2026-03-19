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
export const PlanningOfferStatusValues = ['DRAFT', 'ACTIVE', 'OBSERVED', 'CLOSED'] as const;
export const PlanningSubsectionKindValues = ['THEORY', 'PRACTICE', 'MIXED'] as const;
export const PlanningPlanWorkflowStatusValues = [
  'DRAFT',
  'IN_REVIEW',
  'APPROVED',
  'IN_CORRECTION',
] as const;
export const PlanningV2ConflictTypeValues = [
  'TEACHER_OVERLAP',
  'CLASSROOM_OVERLAP',
  'SUBSECTION_OVERLAP',
  'SECTION_OVERLAP',
] as const;
export const PlanningChangeActionValues = ['CREATE', 'UPDATE', 'DELETE'] as const;

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

@Entity({ name: 'study_types' })
@Index(['code'], { unique: true })
export class StudyTypeEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 40 })
  code!: string;

  @Column({ type: 'varchar', length: 120 })
  name!: string;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'course_modalities' })
@Index(['code'], { unique: true })
export class CourseModalityEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 40 })
  code!: string;

  @Column({ type: 'varchar', length: 120 })
  name!: string;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'planning_cycle_plan_rules' })
@Index(['semester_id', 'academic_program_id', 'cycle'])
export class PlanningCyclePlanRuleEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  semester_id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  campus_id!: string | null;

  @Column({ type: 'varchar', length: 36 })
  academic_program_id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  faculty_id!: string | null;

  @Column({ type: 'varchar', length: 200, nullable: true })
  career_name!: string | null;

  @Column({ type: 'int' })
  cycle!: number;

  @Column({ type: 'varchar', length: 36 })
  study_plan_id!: string;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'enum', enum: PlanningPlanWorkflowStatusValues, default: 'DRAFT' })
  workflow_status!: (typeof PlanningPlanWorkflowStatusValues)[number];

  @Column({ type: 'datetime', nullable: true })
  submitted_at!: Date | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  submitted_by_user_id!: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  submitted_by!: string | null;

  @Column({ type: 'datetime', nullable: true })
  reviewed_at!: Date | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  reviewed_by_user_id!: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  reviewed_by!: string | null;

  @Column({ type: 'varchar', length: 500, nullable: true })
  review_comment!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'planning_offers' })
@Index(['semester_id', 'campus_id', 'study_plan_course_id'], { unique: true })
@Index(['semester_id', 'academic_program_id', 'cycle'])
export class PlanningOfferEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  semester_id!: string;

  @Column({ type: 'varchar', length: 36 })
  campus_id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  faculty_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  academic_program_id!: string | null;

  @Column({ type: 'varchar', length: 36 })
  study_plan_id!: string;

  @Column({ type: 'int' })
  cycle!: number;

  @Column({ type: 'varchar', length: 36 })
  study_plan_course_id!: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  course_code!: string | null;

  @Column({ type: 'varchar', length: 200, nullable: true })
  course_name!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  study_type_id!: string | null;

  @Column({ type: 'varchar', length: 30 })
  course_type!: string;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  theoretical_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  practical_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  total_hours!: number;

  @Column({ type: 'enum', enum: PlanningOfferStatusValues, default: 'DRAFT' })
  status!: (typeof PlanningOfferStatusValues)[number];

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'planning_sections' })
@Index(['planning_offer_id', 'code'], { unique: true })
export class PlanningSectionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  planning_offer_id!: string;

  @Column({ type: 'varchar', length: 20 })
  code!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  teacher_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  course_modality_id!: string | null;

  @Column({ type: 'int', nullable: true })
  projected_vacancies!: number | null;

  @Column({ type: 'boolean', default: false })
  has_subsections!: boolean;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  default_theoretical_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  default_practical_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  default_virtual_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  default_seminar_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  default_total_hours!: number;

  @Column({ type: 'enum', enum: PlanningOfferStatusValues, default: 'DRAFT' })
  status!: (typeof PlanningOfferStatusValues)[number];

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'planning_subsections' })
@Index(['planning_section_id', 'code'], { unique: true })
export class PlanningSubsectionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  planning_section_id!: string;

  @Column({ type: 'varchar', length: 20 })
  code!: string;

  @Column({ type: 'enum', enum: PlanningSubsectionKindValues })
  kind!: (typeof PlanningSubsectionKindValues)[number];

  @Column({ type: 'varchar', length: 36, nullable: true })
  responsible_teacher_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  course_modality_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  building_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  classroom_id!: string | null;

  @Column({ type: 'int', nullable: true })
  capacity_snapshot!: number | null;

  @Column({ type: 'varchar', length: 30, nullable: true })
  shift!: string | null;

  @Column({ type: 'int', nullable: true })
  projected_vacancies!: number | null;

  @Column({ type: 'varchar', length: 30 })
  course_type!: string;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  assigned_theoretical_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  assigned_practical_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  assigned_virtual_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  assigned_seminar_hours!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  assigned_total_hours!: number;

  @Column({ type: 'varchar', length: 255, nullable: true })
  denomination!: string | null;

  @Column({ type: 'enum', enum: PlanningOfferStatusValues, default: 'DRAFT' })
  status!: (typeof PlanningOfferStatusValues)[number];

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'planning_subsection_schedules' })
@Index(['planning_subsection_id', 'day_of_week'])
export class PlanningSubsectionScheduleEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  planning_subsection_id!: string;

  @Column({ type: 'enum', enum: DayOfWeekValues })
  day_of_week!: (typeof DayOfWeekValues)[number];

  @Column({ type: 'time' })
  start_time!: string;

  @Column({ type: 'time' })
  end_time!: string;

  @Column({ type: 'int' })
  duration_minutes!: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 0 })
  academic_hours!: number;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'planning_schedule_conflicts_v2' })
@Index(['semester_id', 'conflict_type'])
@Index(['planning_subsection_id'])
@Index(['teacher_id'])
@Index(['classroom_id'])
export class PlanningScheduleConflictV2Entity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  semester_id!: string;

  @Column({ type: 'enum', enum: PlanningV2ConflictTypeValues })
  conflict_type!: (typeof PlanningV2ConflictTypeValues)[number];

  @Column({ type: 'enum', enum: ConflictSeverityValues })
  severity!: (typeof ConflictSeverityValues)[number];

  @Column({ type: 'varchar', length: 36, nullable: true })
  planning_offer_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  planning_section_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  planning_subsection_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  teacher_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  classroom_id!: string | null;

  @Column({ type: 'varchar', length: 36 })
  schedule_a_id!: string;

  @Column({ type: 'varchar', length: 36 })
  schedule_b_id!: string;

  @Column({ type: 'int', default: 0 })
  overlap_minutes!: number;

  @Column({ type: 'json', nullable: true })
  detail_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  detected_at!: Date;

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'planning_change_logs' })
@Index(['entity_type', 'entity_id', 'changed_at'])
export class PlanningChangeLogEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 50 })
  entity_type!: string;

  @Column({ type: 'varchar', length: 36 })
  entity_id!: string;

  @Column({ type: 'enum', enum: PlanningChangeActionValues })
  action!: (typeof PlanningChangeActionValues)[number];

  @Column({ type: 'json', nullable: true })
  before_json!: Record<string, unknown> | null;

  @Column({ type: 'json', nullable: true })
  after_json!: Record<string, unknown> | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  changed_by_user_id!: string | null;

  @Column({ type: 'varchar', length: 80, nullable: true })
  changed_by!: string | null;

  @Column({ type: 'json', nullable: true })
  context_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  changed_at!: Date;
}
