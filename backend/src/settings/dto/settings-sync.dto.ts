import { Type } from 'class-transformer';
import {
  IsArray,
  IsDateString,
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

  @IsOptional()
  @IsDateString()
  expires_at?: string;
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
  @IsString()
  @MaxLength(36)
  semester_id?: string;

  @IsOptional()
  @Type(() => Number)
  limit?: number;
}
