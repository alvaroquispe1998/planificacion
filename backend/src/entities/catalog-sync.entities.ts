import { Column, Entity, Index, PrimaryColumn } from 'typeorm';

export const ExternalSessionStatusValues = ['ACTIVE', 'EXPIRED', 'ERROR'] as const;
export const SyncJobModeValues = ['FULL', 'INCREMENTAL'] as const;
export const SyncJobStatusValues = ['PENDING', 'RUNNING', 'DONE', 'FAILED'] as const;
export const SyncLogLevelValues = ['INFO', 'WARN', 'ERROR'] as const;

@Entity({ name: 'semesters' })
export class SemesterEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 30, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 40, nullable: true })
  state!: string | null;

  @Column({ type: 'date', nullable: true })
  start_date!: string | null;

  @Column({ type: 'date', nullable: true })
  end_date!: string | null;

  @Column({ type: 'date', nullable: true })
  class_start_date!: string | null;

  @Column({ type: 'date', nullable: true })
  class_end_date!: string | null;
}

@Entity({ name: 'campuses' })
export class CampusEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 20, nullable: true })
  code!: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 255, nullable: true })
  address!: string | null;

  @Column({ type: 'boolean', default: true })
  is_valid!: boolean;

  @Column({ type: 'boolean', default: false })
  is_principal!: boolean;

  @Column({ type: 'varchar', length: 100, nullable: true })
  district!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  province!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  department!: string | null;
}

@Entity({ name: 'faculties' })
export class FacultyEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 20, nullable: true })
  code!: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 20, nullable: true })
  abbreviation!: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  institucional_email!: string | null;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'boolean', default: true })
  is_valid!: boolean;
}

@Entity({ name: 'academic_programs' })
export class AcademicProgramEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 20, nullable: true })
  code!: string | null;

  @Column({ type: 'varchar', length: 200, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  faculty_id!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  faculty!: string | null;

  @Column({ type: 'text', nullable: true })
  graduate_profile!: string | null;

  @Column({ type: 'text', nullable: true })
  general_information!: string | null;

  @Column({ type: 'text', nullable: true })
  comments!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  decanal_resolution!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  rectoral_resolution!: string | null;
}

@Entity({ name: 'study_plans' })
export class StudyPlanEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 150, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 50, nullable: true })
  curriculum!: string | null;

  @Column({ type: 'varchar', length: 20, nullable: true })
  curriculum_code!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  faculty_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  academic_program_id!: string | null;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;
}

@Entity({ name: 'sections' })
export class SectionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  description!: string | null;
}

@Entity({ name: 'courses' })
export class CourseEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  code!: string | null;

  @Column({ type: 'varchar', length: 200, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 200, nullable: true })
  area_career!: string | null;

  @Column({ type: 'varchar', length: 10, nullable: true })
  cycle!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  program!: string | null;

  @Column({ type: 'varchar', length: 20, nullable: true })
  type!: string | null;

  @Column({ type: 'boolean', default: false })
  has_syllabus!: boolean;

  @Column({ type: 'boolean', default: false })
  can_edit!: boolean;

  @Column({ type: 'varchar', length: 36, nullable: true })
  career_id!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  area!: string | null;

  @Column({ type: 'int', nullable: true })
  academic_year!: number | null;
}

@Entity({ name: 'teachers' })
export class TeacherEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 20, nullable: true })
  dni!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  paternal_surname!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  maternal_surname!: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 200, nullable: true })
  full_name!: string | null;

  @Column({ type: 'varchar', length: 30, nullable: true })
  phone_number!: string | null;

  @Column({ type: 'varchar', length: 255, nullable: true })
  picture!: string | null;

  @Column({ type: 'varchar', length: 80, nullable: true })
  user_name!: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  institutional_email!: string | null;
}

@Entity({ name: 'classroom_types' })
export class ClassroomTypeEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 80, nullable: true })
  name!: string | null;
}

@Entity({ name: 'buildings' })
export class BuildingEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  campus_id!: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  name!: string | null;
}

@Entity({ name: 'classrooms' })
export class ClassroomEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  name!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  building_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  campus_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  type_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  faculty_id!: string | null;

  @Column({ type: 'int', nullable: true })
  capacity!: number | null;

  @Column({ type: 'boolean', default: true })
  status!: boolean;

  @Column({ type: 'varchar', length: 45, nullable: true })
  ip_address!: string | null;

  @Column({ type: 'varchar', length: 20, nullable: true })
  code!: string | null;

  @Column({ type: 'varchar', length: 10, nullable: true })
  floor!: string | null;

  @Column({ type: 'int', nullable: true })
  number!: number | null;
}

@Entity({ name: 'academic_program_campuses' })
@Index(['academic_program_id', 'campus_id'], { unique: true })
export class AcademicProgramCampusEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  academic_program_id!: string;

  @Column({ type: 'varchar', length: 36 })
  campus_id!: string;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;
}

@Entity({ name: 'course_sections' })
export class CourseSectionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  course_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  section_id!: string | null;

  @Column({ type: 'varchar', length: 50, nullable: true })
  text!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  teacher_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  semester_id!: string | null;
}

@Entity({ name: 'external_sources' })
@Index(['code'], { unique: true })
export class ExternalSourceEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 20 })
  code!: string;

  @Column({ type: 'varchar', length: 150 })
  name!: string;

  @Column({ type: 'varchar', length: 255 })
  base_url!: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  login_url!: string | null;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'external_sessions' })
@Index(['source_id'], { unique: true })
export class ExternalSessionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  source_id!: string;

  @Column({ type: 'longtext' })
  cookie_jar_encrypted!: string;

  @Column({ type: 'enum', enum: ExternalSessionStatusValues })
  status!: (typeof ExternalSessionStatusValues)[number];

  @Column({ type: 'datetime', nullable: true })
  last_validated_at!: Date | null;

  @Column({ type: 'datetime', nullable: true })
  expires_at!: Date | null;

  @Column({ type: 'text', nullable: true })
  error_last!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'sync_jobs' })
@Index(['source_id', 'resource', 'status'])
export class SyncJobEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  source_id!: string | null;

  @Column({ type: 'varchar', length: 60 })
  resource!: string;

  @Column({ type: 'enum', enum: SyncJobModeValues })
  mode!: (typeof SyncJobModeValues)[number];

  @Column({ type: 'enum', enum: SyncJobStatusValues })
  status!: (typeof SyncJobStatusValues)[number];

  @Column({ type: 'json', nullable: true })
  params_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime', nullable: true })
  started_at!: Date | null;

  @Column({ type: 'datetime', nullable: true })
  finished_at!: Date | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  created_by!: string | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'sync_logs' })
@Index(['job_id', 'created_at'])
export class SyncLogEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  job_id!: string;

  @Column({ type: 'enum', enum: SyncLogLevelValues })
  level!: (typeof SyncLogLevelValues)[number];

  @Column({ type: 'text' })
  message!: string;

  @Column({ type: 'json', nullable: true })
  meta_json!: Record<string, unknown> | null;

  @Column({ type: 'datetime' })
  created_at!: Date;
}
