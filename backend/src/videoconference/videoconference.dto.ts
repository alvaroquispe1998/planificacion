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
    days?: string[];
}

export class GenerateVideoconferenceDto {
    @IsArray()
    @ArrayUnique()
    @IsString({ each: true })
    scheduleIds!: string[];

    @IsDateString()
    startDate!: string;

    @IsDateString()
    endDate!: string;
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
