import { Type } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsEnum,
  IsNotEmpty,
  IsOptional,
  IsString,
  MaxLength,
} from 'class-validator';
import { SyncJobModeValues } from '../../entities/catalog-sync.entities';

export class UpsertSourceSessionDto {
  @IsString()
  @IsNotEmpty()
  cookie_text!: string;
}

export class RunSettingsSyncDto {
  @IsOptional()
  @IsEnum(SyncJobModeValues)
  mode?: (typeof SyncJobModeValues)[number];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  resources?: string[];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  campus_ids?: string[];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  course_ids?: string[];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  classroom_ids?: string[];

  @IsOptional()
  @IsString()
  @MaxLength(36)
  semester_id?: string;

  @IsOptional()
  @IsString()
  schedule_start?: string;

  @IsOptional()
  @IsString()
  schedule_end?: string;

  @IsOptional()
  @Type(() => Number)
  limit?: number;
}

export class PreviewPlanningWorkspaceResetDto {
  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  wipe_config?: boolean;
}

export class ExecutePlanningWorkspaceResetDto extends PreviewPlanningWorkspaceResetDto {
  @IsString()
  @IsNotEmpty()
  confirm_token!: string;
}
