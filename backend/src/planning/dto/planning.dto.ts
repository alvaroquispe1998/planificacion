import { Type } from 'class-transformer';
import {
  ArrayMinSize,
  IsArray,
  IsBoolean,
  IsDateString,
  IsEnum,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  MaxLength,
  Min,
} from 'class-validator';
import {
  ClassGroupTeacherRoleValues,
  ClassTeacherRoleValues,
  ConflictSeverityValues,
  ConflictTypeValues,
  CourseFormatValues,
  PlanningChangeActionValues,
  PlanningOfferStatusValues,
  PlanningSubsectionKindValues,
  DayOfWeekValues,
  GroupTypeValues,
} from '../../entities/planning.entities';

export class CreateClassOfferingDto {
  @IsString()
  @IsNotEmpty()
  id!: string;

  @IsString()
  semester_id!: string;

  @IsString()
  study_plan_id!: string;

  @IsString()
  academic_program_id!: string;

  @IsString()
  course_id!: string;

  @IsString()
  course_section_id!: string;

  @IsString()
  campus_id!: string;

  @IsString()
  delivery_modality_id!: string;

  @IsString()
  shift_id!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  projected_vacancies?: number;

  @IsBoolean()
  status!: boolean;
}

export class UpdateClassOfferingDto {
  @IsOptional()
  @IsString()
  semester_id?: string;

  @IsOptional()
  @IsString()
  study_plan_id?: string;

  @IsOptional()
  @IsString()
  academic_program_id?: string;

  @IsOptional()
  @IsString()
  course_id?: string;

  @IsOptional()
  @IsString()
  course_section_id?: string;

  @IsOptional()
  @IsString()
  campus_id?: string;

  @IsOptional()
  @IsString()
  delivery_modality_id?: string;

  @IsOptional()
  @IsString()
  shift_id?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  projected_vacancies?: number;

  @IsOptional()
  @IsBoolean()
  status?: boolean;
}

export class CreateClassGroupDto {
  @IsString()
  id!: string;

  @IsString()
  class_offering_id!: string;

  @IsEnum(GroupTypeValues)
  group_type!: (typeof GroupTypeValues)[number];

  @IsString()
  @MaxLength(20)
  code!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  capacity?: number;

  @IsOptional()
  @IsString()
  @MaxLength(200)
  note?: string;
}

export class UpdateClassGroupDto {
  @IsOptional()
  @IsEnum(GroupTypeValues)
  group_type?: (typeof GroupTypeValues)[number];

  @IsOptional()
  @IsString()
  @MaxLength(20)
  code?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  capacity?: number;

  @IsOptional()
  @IsString()
  @MaxLength(200)
  note?: string;
}

export class CreateClassMeetingDto {
  @IsString()
  id!: string;

  @IsString()
  class_offering_id!: string;

  @IsString()
  class_group_id!: string;

  @IsEnum(DayOfWeekValues)
  day_of_week!: (typeof DayOfWeekValues)[number];

  @IsString()
  start_time!: string;

  @IsString()
  end_time!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  minutes?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  academic_hours?: number;

  @IsOptional()
  @IsString()
  classroom_id?: string;
}

export class UpdateClassMeetingDto {
  @IsOptional()
  @IsEnum(DayOfWeekValues)
  day_of_week?: (typeof DayOfWeekValues)[number];

  @IsOptional()
  @IsString()
  start_time?: string;

  @IsOptional()
  @IsString()
  end_time?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  minutes?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  academic_hours?: number;

  @IsOptional()
  @IsString()
  classroom_id?: string;
}

export class CreateClassTeacherDto {
  @IsString()
  id!: string;

  @IsString()
  class_offering_id!: string;

  @IsString()
  teacher_id!: string;

  @IsEnum(ClassTeacherRoleValues)
  role!: (typeof ClassTeacherRoleValues)[number];

  @IsBoolean()
  is_primary!: boolean;
}

export class UpdateClassTeacherDto {
  @IsOptional()
  @IsString()
  teacher_id?: string;

  @IsOptional()
  @IsEnum(ClassTeacherRoleValues)
  role?: (typeof ClassTeacherRoleValues)[number];

  @IsOptional()
  @IsBoolean()
  is_primary?: boolean;
}

export class CreateClassGroupTeacherDto {
  @IsString()
  id!: string;

  @IsString()
  class_group_id!: string;

  @IsString()
  teacher_id!: string;

  @IsEnum(ClassGroupTeacherRoleValues)
  role!: (typeof ClassGroupTeacherRoleValues)[number];

  @IsBoolean()
  is_primary!: boolean;

  @IsOptional()
  @IsDateString()
  assigned_from?: string;

  @IsOptional()
  @IsDateString()
  assigned_to?: string;

  @IsOptional()
  @IsString()
  notes?: string;
}

export class UpdateClassGroupTeacherDto {
  @IsOptional()
  @IsEnum(ClassGroupTeacherRoleValues)
  role?: (typeof ClassGroupTeacherRoleValues)[number];

  @IsOptional()
  @IsBoolean()
  is_primary?: boolean;

  @IsOptional()
  @IsDateString()
  assigned_from?: string;

  @IsOptional()
  @IsDateString()
  assigned_to?: string;

  @IsOptional()
  @IsString()
  notes?: string;
}

export class CreateCourseSectionHourRequirementDto {
  @IsString()
  id!: string;

  @IsString()
  course_section_id!: string;

  @IsEnum(CourseFormatValues)
  course_format!: (typeof CourseFormatValues)[number];

  @Type(() => Number)
  @IsInt()
  @Min(0)
  theory_hours_academic!: number;

  @Type(() => Number)
  @IsInt()
  @Min(0)
  practice_hours_academic!: number;

  @Type(() => Number)
  @IsInt()
  @Min(0)
  lab_hours_academic!: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  academic_minutes_per_hour!: number;

  @IsOptional()
  @IsString()
  notes?: string;
}

export class UpdateCourseSectionHourRequirementDto {
  @IsOptional()
  @IsEnum(CourseFormatValues)
  course_format?: (typeof CourseFormatValues)[number];

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  theory_hours_academic?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  practice_hours_academic?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  lab_hours_academic?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  academic_minutes_per_hour?: number;

  @IsOptional()
  @IsString()
  notes?: string;
}

export class CreateScheduleConflictDto {
  @IsString()
  id!: string;

  @IsString()
  semester_id!: string;

  @IsEnum(ConflictTypeValues)
  conflict_type!: (typeof ConflictTypeValues)[number];

  @IsEnum(ConflictSeverityValues)
  severity!: (typeof ConflictSeverityValues)[number];

  @IsOptional()
  @IsString()
  teacher_id?: string;

  @IsOptional()
  @IsString()
  classroom_id?: string;

  @IsOptional()
  @IsString()
  class_group_id?: string;

  @IsOptional()
  @IsString()
  class_offering_id?: string;

  @IsString()
  meeting_a_id!: string;

  @IsString()
  meeting_b_id!: string;

  @Type(() => Number)
  @IsInt()
  @Min(0)
  overlap_minutes!: number;
}

export class UpdatePlanningWorkspaceRowDto {
  @IsOptional()
  @IsEnum(DayOfWeekValues)
  day_of_week?: (typeof DayOfWeekValues)[number];

  @IsOptional()
  @IsString()
  start_time?: string;

  @IsOptional()
  @IsString()
  end_time?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  minutes?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  academic_hours?: number;

  @IsOptional()
  @IsString()
  classroom_id?: string;

  @IsOptional()
  @IsBoolean()
  clear_classroom?: boolean;

  @IsOptional()
  @IsString()
  teacher_id?: string;

  @IsOptional()
  @IsEnum(ClassGroupTeacherRoleValues)
  teacher_role?: (typeof ClassGroupTeacherRoleValues)[number];

  @IsOptional()
  @IsBoolean()
  clear_teacher?: boolean;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  capacity?: number;

  @IsOptional()
  @IsString()
  @MaxLength(20)
  group_code?: string;

  @IsOptional()
  @IsString()
  @MaxLength(200)
  group_note?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  projected_vacancies?: number;

  @IsOptional()
  @IsBoolean()
  offering_status?: boolean;
}

export class BulkAssignTeacherDto {
  @IsArray()
  @ArrayMinSize(1)
  @IsString({ each: true })
  row_ids!: string[];

  @IsString()
  teacher_id!: string;

  @IsOptional()
  @IsEnum(ClassGroupTeacherRoleValues)
  role?: (typeof ClassGroupTeacherRoleValues)[number];

  @IsOptional()
  @IsBoolean()
  is_primary?: boolean;
}

export class BulkAssignClassroomDto {
  @IsArray()
  @ArrayMinSize(1)
  @IsString({ each: true })
  row_ids!: string[];

  @IsOptional()
  @IsString()
  classroom_id?: string;

  @IsOptional()
  @IsBoolean()
  clear_classroom?: boolean;
}

export class BulkDuplicateDto {
  @IsOptional()
  @IsString()
  source_row_id?: string;

  @IsOptional()
  @IsString()
  source_group_id?: string;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  target_group_ids?: string[];

  @IsOptional()
  @IsBoolean()
  duplicate_group?: boolean;
}

export class CreatePlanningCyclePlanRuleDto {
  @IsOptional()
  @IsString()
  id?: string;

  @IsString()
  semester_id!: string;

  @IsString()
  campus_id!: string;

  @IsString()
  academic_program_id!: string;

  @IsOptional()
  @IsString()
  faculty_id?: string;

  @IsOptional()
  @IsString()
  career_name?: string;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  cycle!: number;

  @IsString()
  study_plan_id!: string;

  @IsOptional()
  @IsBoolean()
  is_active?: boolean;
}

export class UpdatePlanningCyclePlanRuleDto {
  @IsOptional()
  @IsString()
  semester_id?: string;

  @IsOptional()
  @IsString()
  campus_id?: string;

  @IsOptional()
  @IsString()
  academic_program_id?: string;

  @IsOptional()
  @IsString()
  faculty_id?: string;

  @IsOptional()
  @IsString()
  career_name?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  cycle?: number;

  @IsOptional()
  @IsString()
  study_plan_id?: string;

  @IsOptional()
  @IsBoolean()
  is_active?: boolean;
}

export class CreatePlanningOfferDto {
  @IsOptional()
  @IsString()
  id?: string;

  @IsString()
  semester_id!: string;

  @IsString()
  campus_id!: string;

  @IsOptional()
  @IsString()
  faculty_id?: string;

  @IsOptional()
  @IsString()
  academic_program_id?: string;

  @IsString()
  study_plan_id!: string;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  cycle!: number;

  @IsString()
  study_plan_course_id!: string;

  @IsOptional()
  @IsString()
  study_type_id?: string;

  @IsOptional()
  @IsEnum(PlanningOfferStatusValues)
  status?: (typeof PlanningOfferStatusValues)[number];
}

export class UpdatePlanningOfferDto {
  @IsOptional()
  @IsString()
  study_type_id?: string;

  @IsOptional()
  @IsEnum(PlanningOfferStatusValues)
  status?: (typeof PlanningOfferStatusValues)[number];
}

export class CreatePlanningSectionDto {
  @IsOptional()
  @IsString()
  id?: string;

  @IsOptional()
  @IsString()
  teacher_id?: string;

  @IsOptional()
  @IsString()
  course_modality_id?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  projected_vacancies?: number;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  subsection_count!: number;
}

export class UpdatePlanningSectionDto {
  @IsOptional()
  @IsString()
  code?: string;

  @IsOptional()
  @IsString()
  teacher_id?: string;

  @IsOptional()
  @IsString()
  course_modality_id?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  projected_vacancies?: number;

  @IsOptional()
  @IsBoolean()
  has_subsections?: boolean;

  @IsOptional()
  @IsEnum(PlanningOfferStatusValues)
  status?: (typeof PlanningOfferStatusValues)[number];
}

export class CreatePlanningSubsectionDto {
  @IsOptional()
  @IsString()
  id?: string;

  @IsOptional()
  @IsString()
  code?: string;

  @IsEnum(PlanningSubsectionKindValues)
  kind!: (typeof PlanningSubsectionKindValues)[number];

  @IsOptional()
  @IsString()
  responsible_teacher_id?: string;

  @IsOptional()
  @IsString()
  course_modality_id?: string;

  @IsOptional()
  @IsString()
  building_id?: string;

  @IsOptional()
  @IsString()
  classroom_id?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  capacity_snapshot?: number;

  @IsOptional()
  @IsString()
  shift?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  projected_vacancies?: number;

  @IsOptional()
  @IsString()
  denomination?: string;
}

export class UpdatePlanningSubsectionDto {
  @IsOptional()
  @IsString()
  code?: string;

  @IsOptional()
  @IsEnum(PlanningSubsectionKindValues)
  kind?: (typeof PlanningSubsectionKindValues)[number];

  @IsOptional()
  @IsString()
  responsible_teacher_id?: string;

  @IsOptional()
  @IsString()
  course_modality_id?: string;

  @IsOptional()
  @IsString()
  building_id?: string;

  @IsOptional()
  @IsString()
  classroom_id?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  capacity_snapshot?: number;

  @IsOptional()
  @IsString()
  shift?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  projected_vacancies?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  assigned_theoretical_hours?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  assigned_practical_hours?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  assigned_virtual_hours?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  assigned_seminar_hours?: number;

  @IsOptional()
  @IsString()
  denomination?: string;

  @IsOptional()
  @IsEnum(PlanningOfferStatusValues)
  status?: (typeof PlanningOfferStatusValues)[number];
}

export class CreatePlanningSubsectionScheduleDto {
  @IsOptional()
  @IsString()
  id?: string;

  @IsEnum(DayOfWeekValues)
  day_of_week!: (typeof DayOfWeekValues)[number];

  @IsString()
  start_time!: string;

  @IsString()
  end_time!: string;
}

export class UpdatePlanningSubsectionScheduleDto {
  @IsOptional()
  @IsEnum(DayOfWeekValues)
  day_of_week?: (typeof DayOfWeekValues)[number];

  @IsOptional()
  @IsString()
  start_time?: string;

  @IsOptional()
  @IsString()
  end_time?: string;
}
