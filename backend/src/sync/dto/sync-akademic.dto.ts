import { Type } from 'class-transformer';
import { IsArray, IsBoolean, IsOptional, IsString, ValidateNested } from 'class-validator';
import {
  CreateClassGroupDto,
  CreateClassGroupTeacherDto,
  CreateClassMeetingDto,
  CreateClassOfferingDto,
  CreateClassTeacherDto,
  CreateCourseSectionHourRequirementDto,
} from '../../planning/dto/planning.dto';

export class SyncAkademicDto {
  @IsOptional()
  @IsString()
  semester_id?: string;

  @IsOptional()
  @IsBoolean()
  replace_semester?: boolean;

  @IsOptional()
  @IsString()
  source_system?: string;

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateClassOfferingDto)
  class_offerings?: CreateClassOfferingDto[];

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateClassGroupDto)
  class_groups?: CreateClassGroupDto[];

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateClassMeetingDto)
  class_meetings?: CreateClassMeetingDto[];

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateClassTeacherDto)
  class_teachers?: CreateClassTeacherDto[];

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateClassGroupTeacherDto)
  class_group_teachers?: CreateClassGroupTeacherDto[];

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateCourseSectionHourRequirementDto)
  course_section_hour_requirements?: CreateCourseSectionHourRequirementDto[];
}
