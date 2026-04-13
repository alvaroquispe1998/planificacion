import { Type } from 'class-transformer';
import {
    ArrayUnique,
    IsArray,
    IsBoolean,
    IsDateString,
    IsInt,
    IsOptional,
    IsString,
    Min,
    ValidateNested,
} from 'class-validator';

export class FilterOptionsDto {
    @IsOptional()
    @IsString()
    semesterId?: string;

    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    campusIds?: string[];

    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    facultyIds?: string[];

    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    programIds?: string[];

    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    courseIds?: string[];

    @IsOptional()
    @IsString()
    modality?: string;

    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    modalities?: string[];

    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    days?: string[];
}

export class PreviewVideoconferenceDto extends FilterOptionsDto {
    @IsOptional()
    @IsDateString()
    startDate?: string;

    @IsOptional()
    @IsDateString()
    endDate?: string;
}

export class AssignmentPreviewVideoconferenceDto extends FilterOptionsDto {
    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    selectAllVisible?: boolean;

    @IsOptional()
    @IsArray()
    @ArrayUnique()
    @IsString({ each: true })
    scheduleIds?: string[];

    @IsOptional()
    @IsArray()
    @ArrayUnique()
    @IsString({ each: true })
    occurrenceKeys?: string[];

    @IsOptional()
    @IsDateString()
    startDate?: string;

    @IsOptional()
    @IsDateString()
    endDate?: string;
}

export class GenerateVideoconferenceDto {
    @IsOptional()
    @IsArray()
    @ArrayUnique()
    @IsString({ each: true })
    scheduleIds?: string[];

    @IsOptional()
    @IsArray()
    @ArrayUnique()
    @IsString({ each: true })
    occurrenceKeys?: string[];

    @IsDateString()
    startDate!: string;

    @IsDateString()
    endDate!: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    allowPoolWarnings?: boolean;
}

export class UpsertVideoconferenceOverrideDto {
    @IsString()
    scheduleId!: string;

    @IsDateString()
    conferenceDate!: string;

    @IsString()
    action!: string;

    @IsOptional()
    @IsDateString()
    overrideDate?: string;

    @IsOptional()
    @IsString()
    overrideStartTime?: string;

    @IsOptional()
    @IsString()
    overrideEndTime?: string;

    @IsOptional()
    @IsString()
    reasonCode?: string;

    @IsOptional()
    @IsString()
    notes?: string;
}

export class UpdateZoomConfigDto {
    @IsOptional()
    @IsString()
    accountId?: string;

    @IsOptional()
    @IsString()
    clientId?: string;

    @IsOptional()
    @IsString()
    clientSecret?: string;

    @IsOptional()
    @Type(() => Number)
    @IsInt()
    @Min(1)
    maxConcurrent?: number;

    @IsOptional()
    @Type(() => Number)
    @IsInt()
    @Min(1)
    pageSize?: number;

    @IsOptional()
    @IsString()
    timezone?: string;
}

export class UpdateZoomPoolItemDto {
    @IsString()
    zoom_user_id!: string;

    @Type(() => Number)
    @IsInt()
    @Min(1)
    sort_order!: number;

    @Type(() => Boolean)
    @IsBoolean()
    is_active!: boolean;
}

export class UpdateZoomPoolDto {
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => UpdateZoomPoolItemDto)
    items!: UpdateZoomPoolItemDto[];
}

export class VideoconferenceInheritanceCatalogDto {
    @IsString()
    semesterId!: string;

    @IsOptional()
    @IsString()
    campusId?: string;

    @IsString()
    facultyId!: string;

    @IsOptional()
    @IsString()
    programId?: string;
}

export class PreviewVideoconferenceInheritanceCandidatesDto {
    @IsString()
    semesterId!: string;

    @IsString()
    facultyId!: string;
}

export class CreateVideoconferenceInheritanceDto {
    @IsString()
    parentScheduleId!: string;

    @IsString()
    childScheduleId!: string;

    @IsOptional()
    @IsString()
    notes?: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    isActive?: boolean;
}

export class UpdateVideoconferenceInheritanceDto {
    @IsOptional()
    @IsString()
    parentScheduleId?: string;

    @IsOptional()
    @IsString()
    childScheduleId?: string;

    @IsOptional()
    @IsString()
    notes?: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    isActive?: boolean;
}
