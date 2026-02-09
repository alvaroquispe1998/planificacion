import { Type } from 'class-transformer';
import {
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
