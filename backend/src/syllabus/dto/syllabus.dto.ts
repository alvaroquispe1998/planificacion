import { Type } from 'class-transformer';
import {
  IsDateString,
  IsEnum,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Max,
  Min,
} from 'class-validator';
import { MatchMethodValues, SummaryTypeValues } from '../../entities/syllabus.entities';

export class CreateSyllabusSessionDto {
  @IsString()
  id!: string;

  @IsString()
  class_offering_id!: string;

  @IsString()
  semester_week_id!: string;

  @IsString()
  @IsNotEmpty()
  session_title!: string;

  @IsString()
  @IsNotEmpty()
  expected_content!: string;

  @IsOptional()
  @IsString()
  bibliography?: string;
}

export class UpdateSyllabusSessionDto {
  @IsOptional()
  @IsString()
  session_title?: string;

  @IsOptional()
  @IsString()
  expected_content?: string;

  @IsOptional()
  @IsString()
  bibliography?: string;
}

export class CreateSyllabusKeywordDto {
  @IsString()
  id!: string;

  @IsString()
  syllabus_session_id!: string;

  @IsString()
  keyword!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  @Max(100)
  weight?: number;
}

export class GenerateSummaryDto {
  @IsOptional()
  @IsEnum(SummaryTypeValues)
  summary_type?: (typeof SummaryTypeValues)[number];
}

export class MatchSyllabusDto {
  @IsOptional()
  @IsEnum(MatchMethodValues)
  method?: (typeof MatchMethodValues)[number];
}
