import { IsOptional, IsString, MaxLength, MinLength } from 'class-validator';

export class RunTranscriptAnalysisDto {
  /** Gemini API key provided by the user (not persisted). */
  @IsString()
  @MinLength(10)
  @MaxLength(200)
  apiKey!: string;

  @IsString()
  @MinLength(10, { message: 'El syllabus debe tener al menos 10 caracteres.' })
  @MaxLength(60000, { message: 'El syllabus excede 60000 caracteres.' })
  syllabusText!: string;

  /** Either provide transcriptText directly or videoconferenceId to auto-fetch. */
  @IsOptional()
  @IsString()
  @MaxLength(300000, { message: 'El transcript excede 300000 caracteres.' })
  transcriptText?: string;

  @IsOptional()
  @IsString()
  @MaxLength(36)
  videoconferenceId?: string;

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
