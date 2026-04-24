import { IsOptional, IsString, MaxLength, MinLength } from 'class-validator';

export class RunTranscriptAnalysisDto {
  @IsString()
  @MinLength(10, { message: 'El syllabus debe tener al menos 10 caracteres.' })
  @MaxLength(60000, { message: 'El syllabus excede 60000 caracteres.' })
  syllabusText!: string;

  @IsString()
  @MinLength(10, { message: 'El transcript debe tener al menos 10 caracteres.' })
  @MaxLength(300000, { message: 'El transcript excede 300000 caracteres.' })
  transcriptText!: string;

  @IsOptional()
  @IsString()
  @MaxLength(300)
  courseLabel?: string;

  @IsOptional()
  @IsString()
  @MaxLength(300)
  sessionLabel?: string;

  @IsOptional()
  @IsString()
  @MaxLength(300)
  teacherLabel?: string;
}
