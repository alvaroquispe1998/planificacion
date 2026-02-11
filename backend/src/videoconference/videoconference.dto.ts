import { IsArray, IsOptional, IsString } from 'class-validator';

export class FilterOptionsDto {
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
    @IsArray()
    @IsString({ each: true })
    sectionIds?: string[];

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
    @IsString({ each: true })
    meetings!: string[];

    @IsString()
    startDate!: string;

    @IsString()
    endDate!: string;
}
