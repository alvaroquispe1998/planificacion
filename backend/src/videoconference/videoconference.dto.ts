import { Type } from 'class-transformer';
import {
    ArrayUnique,
    IsArray,
    IsBoolean,
    IsDateString,
    IsInt,
    IsNotEmpty,
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

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    includeSplit?: boolean;

    /** Returns all schedules regardless of split rules; each row includes host_rule if one exists */
    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    includeAll?: boolean;

    /**
     * Disables the automatic continuous-block grouping so that each schedule_id
     * is returned as its own row (used by the Cursos Especiales "Por horario" config).
     */
    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    expandGroups?: boolean;
}

export class AssignmentPreviewVideoconferenceDto extends FilterOptionsDto {
    @IsString()
    @IsNotEmpty()
    zoomGroupId!: string;

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

    @IsOptional()
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => TemporaryVideoconferenceOverrideDto)
    temporaryOverrides?: TemporaryVideoconferenceOverrideDto[];
}

export class GenerateVideoconferenceDto {
    @IsString()
    @IsNotEmpty()
    zoomGroupId!: string;

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

    @IsOptional()
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => GeneratePreferredHostDto)
    preferredHosts?: GeneratePreferredHostDto[];

    @IsOptional()
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => TemporaryVideoconferenceOverrideDto)
    temporaryOverrides?: TemporaryVideoconferenceOverrideDto[];
}

export class GeneratePreferredHostDto {
    @IsString()
    scheduleId!: string;

    @IsOptional()
    @IsDateString()
    conferenceDate?: string;

    @IsString()
    zoomUserId!: string;
}

export class CheckExistingVideoconferencesDto {
    @IsOptional()
    @IsArray()
    @ArrayUnique()
    @IsString({ each: true })
    occurrenceKeys?: string[];

    @IsOptional()
    @IsArray()
    @ArrayUnique()
    @IsString({ each: true })
    scheduleIds?: string[];

    @IsOptional()
    @IsDateString()
    startDate?: string;

    @IsOptional()
    @IsDateString()
    endDate?: string;
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

export class TemporaryVideoconferenceOverrideDto {
    @IsString()
    scheduleId!: string;

    @IsDateString()
    conferenceDate!: string;

    @IsDateString()
    overrideDate!: string;

    @IsString()
    overrideStartTime!: string;

    @IsString()
    overrideEndTime!: string;

    @IsOptional()
    @IsString()
    reasonCode?: string;

    @IsOptional()
    @IsString()
    notes?: string;

    @IsOptional()
    @IsString()
    topicOverride?: string;
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

export class CreateZoomGroupDto {
    @IsString()
    @IsNotEmpty()
    name!: string;

    @IsOptional()
    @IsString()
    code?: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    is_active?: boolean;
}

export class UpdateZoomGroupDto {
    @IsOptional()
    @IsString()
    @IsNotEmpty()
    name?: string;

    @IsOptional()
    @IsString()
    @IsNotEmpty()
    code?: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    is_active?: boolean;
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

export class CreateHostRuleDto {
    @IsString()
    @IsNotEmpty()
    scheduleId!: string;

    @IsOptional()
    @IsString()
    zoomGroupId?: string;

    @IsOptional()
    @IsString()
    zoomUserId?: string;

    @IsOptional()
    @IsString()
    notes?: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    lockHost?: boolean;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    skipZoom?: boolean;
}

export class UpdateHostRuleDto {
    @IsOptional()
    @IsString()
    zoomGroupId?: string;

    @IsOptional()
    @IsString()
    zoomUserId?: string;

    @IsOptional()
    @IsString()
    notes?: string;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    isActive?: boolean;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    lockHost?: boolean;

    @IsOptional()
    @Type(() => Boolean)
    @IsBoolean()
    skipZoom?: boolean;
}
