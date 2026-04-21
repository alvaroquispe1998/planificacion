import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { createHash } from 'crypto';
import { EntityManager, EntityTarget, In, ObjectLiteral, Repository } from 'typeorm';
import * as XLSX from 'xlsx';
import { newId } from '../common';
import { SettingsSyncService } from '../settings/settings-sync.service';
import {
  AcademicProgramEntity,
  BuildingEntity,
  CampusEntity,
  ClassroomEntity,
  FacultyEntity,
  SemesterEntity,
  StudyPlanCourseDetailEntity,
  StudyPlanCourseEntity,
  StudyPlanEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
  CourseModalityEntity,
  DayOfWeekValues,
  PlanningChangeLogEntity,
  PlanningCampusVcLocationMappingEntity,
  PlanningCyclePlanRuleEntity,
  PlanningImportAliasMappingEntity,
  PlanningImportBatchEntity,
  PlanningImportIssueSeverityValues,
  PlanningImportRowEntity,
  PlanningImportRowIssueEntity,
  PlanningImportSourceKindValues,
  PlanningImportScopeDecisionEntity,
  PlanningImportScopeDecisionValues,
  PlanningOfferEntity,
  PlanningSessionTypeValues,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionKindValues,
  PlanningSubsectionScheduleEntity,
  PlanningSourceSystemValues,
  StudyTypeEntity,
} from '../entities/planning.entities';
import {
  ComparePlanningExcelDto,
  CreatePlanningImportAliasDto,
  ExportPlanningWorkspaceDto,
  PreviewPlanningAkademicImportDto,
  UpdatePlanningImportAliasDto,
  UpdatePlanningImportScopeDecisionsDto,
} from './dto/planning.dto';
import {
  PlanningSubsectionVideoconferenceEntity,
  PlanningSubsectionVideoconferenceOverrideEntity,
  PlanningSubsectionScheduleVcInheritanceEntity,
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcFacultyEntity,
  VcPeriodEntity,
} from '../videoconference/videoconference.entity';
import { PlanningManualService } from './planning-manual.service';

type ImportActor = {
  user_id?: string | null;
  username?: string | null;
  display_name?: string | null;
  ip_address?: string | null;
};

type PlanningSubsectionKind = (typeof PlanningSubsectionKindValues)[number];
type PlanningImportIssueSeverity = (typeof PlanningImportIssueSeverityValues)[number];
type PlanningImportSourceKind = (typeof PlanningImportSourceKindValues)[number];
type PlanningSourceSystem = (typeof PlanningSourceSystemValues)[number];
type PlanningSessionType = (typeof PlanningSessionTypeValues)[number];

type AliasNamespace =
  | 'vc_period'
  | 'campus'
  | 'faculty_code'
  | 'academic_program_code'
  | 'study_plan_code'
  | 'course_code'
  | 'course_modality'
  | 'shift'
  | 'building'
  | 'classroom'
  | 'laboratory';

type AliasSourceContext = {
  academic_program_code_raw?: string | null;
  study_plan_code_raw?: string | null;
  cycle_raw?: number | null;
};

type ImportCatalog = {
  semesters: SemesterEntity[];
  vcPeriods: VcPeriodEntity[];
  campuses: CampusEntity[];
  campusVcLocations: PlanningCampusVcLocationMappingEntity[];
  faculties: FacultyEntity[];
  programs: AcademicProgramEntity[];
  studyPlans: StudyPlanEntity[];
  studyPlanById: Map<string, StudyPlanEntity>;
  studyPlanCourses: StudyPlanCourseEntity[];
  studyPlanCourseDetails: StudyPlanCourseDetailEntity[];
  studyPlanCourseDetailById: Map<string, StudyPlanCourseDetailEntity>;
  teachers: TeacherEntity[];
  courseModalities: CourseModalityEntity[];
  studyTypes: StudyTypeEntity[];
  buildings: BuildingEntity[];
  classrooms: ClassroomEntity[];
  defaultStudyTypeId: string | null;
  aliasMap: Map<AliasNamespace, Map<string, PlanningImportAliasMappingEntity>>;
  teacherMatchCache: Map<string, MappingResolution>;
  intranetTeacherSearchCache: Map<string, TeacherEntity[]>;
};

type ImportExecutionCaches = {
  vcContextByScopeKey: Map<
    string,
    {
      vcFacultyId: string | null;
      vcAcademicProgramId: string | null;
    }
  >;
  vcCoursesByProgramId: Map<string, VcCourseEntity[]>;
  vcFaculties?: VcFacultyEntity[] | null;
  vcAcademicPrograms?: VcAcademicProgramEntity[] | null;
};

type NormalizedImportRow = {
  row_number: number;
  semester_raw: string;
  study_plan_code_raw: string;
  campus_raw: string;
  faculty_code_raw: string;
  academic_program_code_raw: string;
  academic_program_code_akademic_raw: string;
  cycle_raw: number | null;
  section_raw: string;
  course_code_raw: string;
  course_name_raw: string;
  study_type_raw: string;
  course_requirement_raw: string;
  course_modality_raw: string;
  delivery_modality_raw: string;
  theory_hours: number;
  practical_hours: number;
  total_hours: number;
  credits: number | null;
  projected_vacancies: number | null;
  teacher_dni_raw: string;
  teacher_name_raw: string;
  shift_raw: string;
  building_raw: string;
  classroom_raw: string;
  laboratory_raw: string;
  day_raw: string;
  start_hour_raw: string;
  start_minute_raw: string;
  end_hour_raw: string;
  end_minute_raw: string;
  academic_hours_raw: number | null;
  denomination_raw: string;
  raw_row: Record<string, unknown>;
};

type MappingResolution = {
  source_value: string;
  target_id: string | null;
  target_label: string | null;
  match_source: 'alias' | 'catalog' | 'heuristic' | 'manual_value' | 'none';
  target_extra?: Record<string, unknown> | null;
};

type ParsedSchedule = {
  day_of_week: (typeof DayOfWeekValues)[number];
  start_time: string;
  end_time: string;
  duration_minutes: number;
  academic_hours: number;
  signature: string;
};

type PreviewIssue = {
  severity: PlanningImportIssueSeverity;
  issue_code: string;
  field_name?: string | null;
  message: string;
  meta_json?: Record<string, unknown> | null;
};

type PreviewRow = {
  row_number: number;
  source_json: Record<string, unknown>;
  normalized_json: Record<string, unknown>;
  resolution_json: Record<string, unknown>;
  scope_key: string | null;
  row_hash: string | null;
  can_import: boolean;
  issues: PreviewIssue[];
};

type AkademicCourseRow = {
  id: string;
  code: string;
  name: string;
  career_name: string | null;
  credits: number | null;
  academic_year: number | null;
  type_raw: string | null;
  raw: Record<string, unknown>;
};

type AkademicSectionRow = {
  id: string;
  course_id: string;
  external_code: string;
  modality_raw: number | string | null;
  vacancies: number | null;
  teacher_names: string[];
  teacher_ids: string[];
  raw: Record<string, unknown>;
};

type AkademicScheduleRow = {
  id: string;
  section_id: string;
  day_of_week: (typeof DayOfWeekValues)[number];
  start_time: string;
  end_time: string;
  duration_minutes: number;
  academic_hours: number;
  session_type: PlanningSessionType;
  source_session_type_code: string | null;
  teacher_external_id: string | null;
  teacher_dni: string | null;
  teacher_name: string | null;
  teacher_username: string | null;
  teacher_email: string | null;
  classroom_external_id: string | null;
  building_external_id: string | null;
  classroom_description: string | null;
  building_description: string | null;
  capacity: number | null;
  section_group_id: string | null;
  raw: Record<string, unknown>;
};

type AkademicPreviewInput = {
  course: AkademicCourseRow;
  section: AkademicSectionRow;
  schedules: AkademicScheduleRow[];
  import_section_code: string;
  import_subsection_code: string;
  subsection_kind: PlanningSubsectionKind;
  scope: ImportScope;
  semester_resolution: MappingResolution;
  vc_period_resolution: MappingResolution;
  campus_resolution: MappingResolution;
  faculty_resolution: MappingResolution;
  program_resolution: MappingResolution;
  study_plan_resolution: MappingResolution;
  course_resolution: MappingResolution;
  teacher_resolution: MappingResolution;
  modality_resolution: MappingResolution;
  shift_resolution: MappingResolution;
  building_resolution: MappingResolution;
  classroom_resolution: MappingResolution;
  is_cepea: boolean;
  heuristic_warning?: string | null;
};

type ImportScope = {
  semester_id: string;
  vc_period_id: string | null;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
  study_plan_id: string;
  cycle: number;
  semester_name: string | null;
  vc_period_name: string | null;
  campus_name: string | null;
  faculty_name: string | null;
  academic_program_name: string | null;
  study_plan_name: string | null;
  study_plan_year: string | null;
};

type BatchComposition = {
  summary: Record<string, unknown>;
  scopes: Record<string, unknown>[];
  unresolved_mappings: Record<string, unknown>[];
  resolved_mappings: Record<string, unknown>[];
  issue_summary: Record<string, unknown>[];
  row_preview: Record<string, unknown>[];
};

type WorkspaceExportFilters = {
  semester_id?: string;
  campus_id?: string;
  faculty_id?: string;
  academic_program_id?: string;
  study_plan_id?: string;
  delivery_modality_id?: string;
  shift_id?: string;
  search?: string;
};

type ManualWorkspaceComparableRow = {
  semester_id: string | null;
  semester_name: string | null;
  study_plan_code: string | null;
  rcu: string | null;
  campus_name: string | null;
  faculty_code: string | null;
  faculty_name: string | null;
  academic_program_code: string | null;
  academic_program_code_akademic: string | null;
  cycle: number | null;
  section_code: string | null;
  subsection_code: string | null;
  course_code: string | null;
  course_name: string | null;
  study_type: string | null;
  plan_course_type: string | null;
  course_modality_name: string | null;
  theory_hours: number | null;
  practical_hours: number | null;
  total_hours: number | null;
  credits: number | null;
  projected_vacancies: number | null;
  teacher_dni: string | null;
  teacher_name: string | null;
  delivery_modality_id: string | null;
  delivery_modality_name: string | null;
  shift: string | null;
  shift_id: string | null;
  assistant_name: string | null;
  building_name: string | null;
  classroom_name: string | null;
  classroom_capacity: number | null;
  laboratory_name: string | null;
  laboratory_capacity: number | null;
  second_shift: string | null;
  subsection_kind_label: string | null;
  day_of_week: string | null;
  day_number: number | null;
  start_hour: number | null;
  start_minute: number | null;
  end_hour: number | null;
  end_minute: number | null;
  academic_hours_value: number | null;
  start_time_label: string | null;
  end_time_label: string | null;
  denomination: string | null;
  source_row_id: string;
  comparison_key: string;
  row_label: string;
};

type ExcelComparisonDifference = {
  key: string;
  row_label: string;
  fields: Array<{
    field: string;
    label: string;
    excel_value: string;
    system_value: string;
  }>;
};

type ExcelComparisonResult = {
  summary: Record<string, unknown>;
  warnings: Array<Record<string, unknown>>;
  only_in_excel: ManualWorkspaceComparableRow[];
  only_in_system: ManualWorkspaceComparableRow[];
  differences: ExcelComparisonDifference[];
};

type SnapshotEntry = {
  key: string;
  label: string;
};

type ExistingScopeSnapshot = {
  offer_keys: string[];
  section_keys: string[];
  subsection_keys: string[];
  schedule_keys: string[];
  offer_entries?: SnapshotEntry[];
  section_entries?: SnapshotEntry[];
  subsection_entries?: SnapshotEntry[];
  schedule_entries?: SnapshotEntry[];
};

const IMPORT_SHEET_NAME = 'Hoja1';
const AKADEMIC_IMPORT_SOURCE_NAME = 'Akademic';
const ISSUE_PREVIEW_LIMIT = 200;
const SHIFT_OPTIONS = ['DIURNO', 'MANANA', 'TARDE', 'NOCHE', 'NOCTURNO', 'TARDE/NOCHE'] as const;
const AKADEMIC_COURSE_CONCURRENCY = 12;
const AKADEMIC_SECTION_CONCURRENCY = 10;
const AKADEMIC_SCOPE_DECISION_CONCURRENCY = 10;

@Injectable()
export class PlanningImportService {
  constructor(
    private readonly settingsSyncService: SettingsSyncService,
    private readonly planningManualService: PlanningManualService,
    @InjectRepository(SemesterEntity)
    private readonly semestersRepo: Repository<SemesterEntity>,
    @InjectRepository(VcPeriodEntity)
    private readonly vcPeriodsRepo: Repository<VcPeriodEntity>,
    @InjectRepository(VcFacultyEntity)
    private readonly vcFacultiesRepo: Repository<VcFacultyEntity>,
    @InjectRepository(VcAcademicProgramEntity)
    private readonly vcAcademicProgramsRepo: Repository<VcAcademicProgramEntity>,
    @InjectRepository(VcCourseEntity)
    private readonly vcCoursesRepo: Repository<VcCourseEntity>,
    @InjectRepository(CampusEntity)
    private readonly campusesRepo: Repository<CampusEntity>,
    @InjectRepository(FacultyEntity)
    private readonly facultiesRepo: Repository<FacultyEntity>,
    @InjectRepository(AcademicProgramEntity)
    private readonly programsRepo: Repository<AcademicProgramEntity>,
    @InjectRepository(StudyPlanEntity)
    private readonly studyPlansRepo: Repository<StudyPlanEntity>,
    @InjectRepository(StudyPlanCourseEntity)
    private readonly studyPlanCoursesRepo: Repository<StudyPlanCourseEntity>,
    @InjectRepository(StudyPlanCourseDetailEntity)
    private readonly studyPlanCourseDetailsRepo: Repository<StudyPlanCourseDetailEntity>,
    @InjectRepository(TeacherEntity)
    private readonly teachersRepo: Repository<TeacherEntity>,
    @InjectRepository(BuildingEntity)
    private readonly buildingsRepo: Repository<BuildingEntity>,
    @InjectRepository(ClassroomEntity)
    private readonly classroomsRepo: Repository<ClassroomEntity>,
    @InjectRepository(StudyTypeEntity)
    private readonly studyTypesRepo: Repository<StudyTypeEntity>,
    @InjectRepository(CourseModalityEntity)
    private readonly courseModalitiesRepo: Repository<CourseModalityEntity>,
    @InjectRepository(PlanningCampusVcLocationMappingEntity)
    private readonly campusVcLocationMappingsRepo: Repository<PlanningCampusVcLocationMappingEntity>,
    @InjectRepository(PlanningCyclePlanRuleEntity)
    private readonly planRulesRepo: Repository<PlanningCyclePlanRuleEntity>,
    @InjectRepository(PlanningOfferEntity)
    private readonly offersRepo: Repository<PlanningOfferEntity>,
    @InjectRepository(PlanningSectionEntity)
    private readonly sectionsRepo: Repository<PlanningSectionEntity>,
    @InjectRepository(PlanningSubsectionEntity)
    private readonly subsectionsRepo: Repository<PlanningSubsectionEntity>,
    @InjectRepository(PlanningSubsectionScheduleEntity)
    private readonly schedulesRepo: Repository<PlanningSubsectionScheduleEntity>,
    @InjectRepository(PlanningSubsectionVideoconferenceEntity)
    private readonly planningVideoconferencesRepo: Repository<PlanningSubsectionVideoconferenceEntity>,
    @InjectRepository(PlanningSubsectionVideoconferenceOverrideEntity)
    private readonly planningVideoconferenceOverridesRepo: Repository<PlanningSubsectionVideoconferenceOverrideEntity>,
    @InjectRepository(PlanningChangeLogEntity)
    private readonly changeLogsRepo: Repository<PlanningChangeLogEntity>,
    @InjectRepository(PlanningImportBatchEntity)
    private readonly importBatchesRepo: Repository<PlanningImportBatchEntity>,
    @InjectRepository(PlanningImportRowEntity)
    private readonly importRowsRepo: Repository<PlanningImportRowEntity>,
    @InjectRepository(PlanningImportRowIssueEntity)
    private readonly importRowIssuesRepo: Repository<PlanningImportRowIssueEntity>,
    @InjectRepository(PlanningImportScopeDecisionEntity)
    private readonly importScopeDecisionsRepo: Repository<PlanningImportScopeDecisionEntity>,
    @InjectRepository(PlanningImportAliasMappingEntity)
    private readonly importAliasMappingsRepo: Repository<PlanningImportAliasMappingEntity>,
  ) {}

  async previewExcelImport(file: any, actor?: ImportActor | null) {
    if (!file?.buffer?.length) {
      throw new BadRequestException('Debes adjuntar un archivo Excel para generar el preview.');
    }

    const now = new Date();
    const batch = this.importBatchesRepo.create({
      id: newId(),
      file_name: file.originalname ?? 'import.xlsx',
      sheet_name: IMPORT_SHEET_NAME,
      file_hash: createHash('sha1').update(file.buffer).digest('hex'),
      status: 'PREVIEW_PROCESSING',
      total_row_count: 0,
      importable_row_count: 0,
      blocked_row_count: 0,
      warning_row_count: 0,
      preview_summary_json: null,
      preview_progress_json: this.buildPreviewProgress('Archivo recibido. Preparando preview...', 4, {
        stage_code: 'QUEUED',
      }),
      execution_summary_json: null,
      error_message: null,
      uploaded_by_user_id: actor?.user_id ?? null,
      uploaded_by: actor?.display_name || actor?.username || 'SYSTEM',
      uploaded_from_ip: actor?.ip_address ?? null,
      executed_at: null,
      created_at: now,
      updated_at: now,
    });

    await this.importBatchesRepo.save(batch);
    void this.processPreviewBatch(batch.id, Buffer.from(file.buffer));
    return this.getBatch(batch.id);
  }

  async exportWorkspaceExcel(dto: ExportPlanningWorkspaceDto) {
    const rows = await this.buildWorkspaceComparableRows(dto);
    const workbook = XLSX.utils.book_new();
    const sheet = XLSX.utils.aoa_to_sheet([
      this.exportWorkbookHeaders(),
      ...rows.map((row) => this.exportWorkbookRow(row)),
    ]);
    XLSX.utils.book_append_sheet(workbook, sheet, 'planificacion');
    const buffer = XLSX.write(workbook, {
      type: 'buffer',
      bookType: 'xlsx',
    }) as Buffer;
    const semesterLabel = sanitizeFileName(
      rows[0]?.semester_name ??
        (dto.semester_id
          ? (await this.semestersRepo.findOne({ where: { id: dto.semester_id } }))?.name
          : null) ??
        'planning',
    );
    return {
      file_name: `planificacion-${semesterLabel}.xlsx`,
      buffer,
    };
  }

  async compareExcelWithSystem(file: any, dto: ComparePlanningExcelDto): Promise<ExcelComparisonResult> {
    if (!file?.buffer?.length) {
      throw new BadRequestException('Debes adjuntar un archivo Excel para comparar.');
    }
    const semester = await this.requireEntity(this.semestersRepo, dto.semester_id, 'semester');
    const excelRows = await this.parseComparisonExcelRows(file.buffer, dto.semester_id);
    const systemRows = await this.buildWorkspaceComparableRows({ semester_id: dto.semester_id });
    return this.compareComparableRows(excelRows, systemRows, {
      semester_id: semester.id,
      semester_name: semester.name,
      file_name: file.originalname ?? 'planificacion.xlsx',
    });
  }

  async exportExcelComparisonReport(file: any, dto: ComparePlanningExcelDto) {
    const comparison = await this.compareExcelWithSystem(file, dto);
    const workbook = this.buildComparisonWorkbook(comparison);
    const semester = await this.requireEntity(this.semestersRepo, dto.semester_id, 'semester');
    return {
      file_name: `comparacion-planificacion-${sanitizeFileName(semester.name ?? dto.semester_id)}.xlsx`,
      buffer: XLSX.write(workbook, {
        type: 'buffer',
        bookType: 'xlsx',
      }) as Buffer,
    };
  }

  async previewAkademicImport(
    dto: PreviewPlanningAkademicImportDto,
    actor?: ImportActor | null,
  ) {
    const now = new Date();
    const semester = dto.semester_id
      ? await this.semestersRepo.findOne({ where: { id: dto.semester_id } })
      : null;
    const batch = this.importBatchesRepo.create({
      id: newId(),
      file_name: semester?.name
        ? `${AKADEMIC_IMPORT_SOURCE_NAME} ${semester.name}`
        : AKADEMIC_IMPORT_SOURCE_NAME,
      source_kind: 'AKADEMIC',
      sheet_name: null,
      file_hash: null,
      status: 'PREVIEW_PROCESSING',
      total_row_count: 0,
      importable_row_count: 0,
      blocked_row_count: 0,
      warning_row_count: 0,
      preview_summary_json: null,
      preview_progress_json: this.buildPreviewProgress('Consultando informacion desde Akademic...', 4, {
        stage_code: 'QUEUED',
      }),
      execution_summary_json: null,
      source_scope_json: {
        semester_id: dto.semester_id,
        vc_period_id: dto.vc_period_id ?? null,
        campus_id: dto.campus_id ?? null,
        faculty_id: dto.faculty_id ?? null,
        academic_program_id: dto.academic_program_id ?? null,
        study_plan_id: dto.study_plan_id ?? null,
        cycle: dto.cycle ?? null,
        study_plan_course_id: dto.study_plan_course_id ?? null,
        course_code: dto.course_code ?? null,
      },
      error_message: null,
      uploaded_by_user_id: actor?.user_id ?? null,
      uploaded_by: actor?.display_name || actor?.username || 'SYSTEM',
      uploaded_from_ip: actor?.ip_address ?? null,
      executed_at: null,
      created_at: now,
      updated_at: now,
    });

    await this.importBatchesRepo.save(batch);
    void this.processAkademicPreviewBatch(batch.id, dto);
    return this.getBatch(batch.id);
  }

  async getBatch(batchId: string) {
    const current = await this.requireBatch(batchId);
    const batch = await this.reconcileFinishedExecutionBatch(current);
    if (
      batch.status === 'PREVIEW_PROCESSING' ||
      batch.status === 'PREVIEW_FAILED' ||
      batch.status === 'EXECUTING'
    ) {
      return this.composePendingBatchResponse(batch);
    }
    const [rows, issues, scopeDecisions] = await Promise.all([
      this.importRowsRepo.find({
        where: { batch_id: batchId },
        order: { row_number: 'ASC' },
      }),
      this.importRowIssuesRepo.find({
        where: { batch_id: batchId },
        order: { row_number: 'ASC', created_at: 'ASC' },
      }),
      this.importScopeDecisionsRepo.find({
        where: { batch_id: batchId },
        order: { scope_key: 'ASC' },
      }),
    ]);

    return this.composeBatchResponse(batch, rows, issues, scopeDecisions);
  }

  private async reconcileFinishedExecutionBatch(batch: PlanningImportBatchEntity) {
    if (batch.status !== 'EXECUTING') {
      return batch;
    }
    const report = asRecord(batch.execution_summary_json);
    const processedScopeCount = numberValue(report.processed_scope_count);
    const totalScopeCount = numberValue(report.total_scope_count);
    if (!totalScopeCount || processedScopeCount < totalScopeCount) {
      return batch;
    }

    batch.status = 'EXECUTED';
    batch.error_message = null;
    batch.executed_at = batch.executed_at ?? new Date();
    batch.execution_summary_json = this.buildExecutionProgress(
      'Carga masiva aplicada correctamente.',
      100,
      {
        ...report,
        stage_code: 'DONE',
        finished_at: recordString(report, 'finished_at') ?? new Date().toISOString(),
      },
    );
    batch.updated_at = new Date();
    await this.importBatchesRepo.save(batch);
    return batch;
  }

  async getBatchReport(batchId: string) {
    return this.getBatch(batchId);
  }

  async updateScopeDecisions(batchId: string, dto: UpdatePlanningImportScopeDecisionsDto) {
    const batch = await this.requireBatch(batchId);
    if (batch.status !== 'PREVIEW_READY') {
      throw new BadRequestException('El preview aun no esta listo para guardar decisiones.');
    }
    const decisions = await this.importScopeDecisionsRepo.find({
      where: { batch_id: batchId },
    });
    const decisionMap = new Map(decisions.map((item) => [item.scope_key, item]));
    const now = new Date();

    for (const input of dto.decisions) {
      const current = decisionMap.get(input.scope_key);
      if (!current) {
        throw new NotFoundException(`No existe un scope ${input.scope_key} dentro del batch.`);
      }
      current.decision = input.decision;
      current.notes = emptyToNull(input.notes);
      current.updated_at = now;
    }

    await this.importScopeDecisionsRepo.save([...decisionMap.values()]);
    batch.updated_at = now;
    await this.importBatchesRepo.save(batch);
    return this.getBatch(batchId);
  }

  async executeBatch(batchId: string, actor?: ImportActor | null) {
    const batch = await this.requireBatch(batchId);
    if (batch.status === 'EXECUTING') {
      return this.getBatch(batchId);
    }
    if (batch.status !== 'PREVIEW_READY') {
      throw new BadRequestException('El preview aun no esta listo o fallo durante el procesamiento.');
    }
    const scopeDecisions = await this.importScopeDecisionsRepo.find({
      where: { batch_id: batchId },
      order: { scope_key: 'ASC' },
    });
    const blockingScopes = scopeDecisions.filter(
      (item) =>
        hasExistingData(item.existing_summary_json) &&
        item.decision === 'PENDING',
    );
    if (blockingScopes.length > 0) {
      throw new BadRequestException(
        'Debes decidir si reemplazas o saltas los scopes que ya tienen informacion cargada.',
      );
    }

    batch.status = 'EXECUTING';
    batch.error_message = null;
    batch.execution_summary_json = this.buildExecutionProgress(
      'Preparando la carga en la plataforma...',
      2,
      {
        stage_code: 'QUEUED',
        started_at: new Date().toISOString(),
        processed_scope_count: 0,
        total_scope_count: 0,
        imported_scope_count: 0,
        skipped_scope_count: 0,
        replaced_scope_count: 0,
        created_plan_rule_count: 0,
        created_offer_count: 0,
        created_section_count: 0,
        created_subsection_count: 0,
        created_schedule_count: 0,
        skipped_row_count: 0,
        executed_scope_keys: [] as string[],
      },
    );
    batch.updated_at = new Date();
    await this.importBatchesRepo.save(batch);
    void this.processExecuteBatch(batchId, actor);
    return this.getBatch(batchId);
  }

  private async processExecuteBatch(batchId: string, actor?: ImportActor | null) {
    let batch: PlanningImportBatchEntity | null = null;
    let executionSummary: Record<string, unknown> = {};
    let totalScopeCount = 0;

    try {
      batch = await this.requireBatch(batchId);
      const [rows, scopeDecisions] = await Promise.all([
        this.importRowsRepo.find({
          where: { batch_id: batchId, can_import: true },
          order: { row_number: 'ASC' },
        }),
        this.importScopeDecisionsRepo.find({
          where: { batch_id: batchId },
          order: { scope_key: 'ASC' },
        }),
      ]);

      const groupedRows = groupBy(rows, (row) => row.scope_key ?? '__NO_SCOPE__');
      const scopeEntries = [...groupedRows.entries()].filter(([scopeKey]) => scopeKey !== '__NO_SCOPE__');
      totalScopeCount = scopeEntries.length;
      const scopeDecisionMap = new Map(scopeDecisions.map((item) => [item.scope_key, item]));
      executionSummary = {
        ...(asRecord(batch.execution_summary_json) ?? {}),
        processed_scope_count: 0,
        total_scope_count: scopeEntries.length,
        imported_scope_count: 0,
        skipped_scope_count: 0,
        replaced_scope_count: 0,
        created_plan_rule_count: 0,
        created_offer_count: 0,
        created_section_count: 0,
        created_subsection_count: 0,
        created_schedule_count: 0,
        skipped_row_count: 0,
        executed_scope_keys: [] as string[],
      };

      executionSummary = await this.updateExecutionBatchProgress(
        batch,
        executionSummary,
        8,
        'Cargando catalogos para ejecutar la carga...',
        {
          stage_code: 'LOADING_CATALOG',
          total_scope_count: scopeEntries.length,
        },
      );

      const catalog = await this.loadImportCatalog();
      const executionCaches: ImportExecutionCaches = {
        vcContextByScopeKey: new Map(),
        vcCoursesByProgramId: new Map(),
        vcFaculties: null,
        vcAcademicPrograms: null,
      };

      for (let index = 0; index < scopeEntries.length; index += 1) {
        const [scopeKey, scopeRows] = scopeEntries[index];
        const decision = scopeDecisionMap.get(scopeKey);
        const firstResolution = asRecord(scopeRows[0].resolution_json);
        const scope = this.scopeFromResolution(firstResolution);
        const scopeLabel = this.buildExecutionScopeLabel(scope);
        const progressPercent =
          10 + Math.round(((index + 1) / Math.max(scopeEntries.length, 1)) * 85);

        if (decision?.decision === 'SKIP_SCOPE') {
          executionSummary.skipped_scope_count = numberValue(executionSummary.skipped_scope_count) + 1;
          executionSummary.skipped_row_count = numberValue(executionSummary.skipped_row_count) + scopeRows.length;
          executionSummary.processed_scope_count = index + 1;
          executionSummary = await this.updateExecutionBatchProgress(
            batch,
            executionSummary,
            progressPercent,
            `Omitiendo grupo ${index + 1}/${scopeEntries.length}: ${scopeLabel}.`,
            {
              stage_code: 'SKIPPING_SCOPE',
              current_scope_key: scopeKey,
              current_scope_label: scopeLabel,
            },
          );
          continue;
        }

        if (!scope) {
          executionSummary.skipped_row_count = numberValue(executionSummary.skipped_row_count) + scopeRows.length;
          executionSummary.processed_scope_count = index + 1;
          executionSummary = await this.updateExecutionBatchProgress(
            batch,
            executionSummary,
            progressPercent,
            `Saltando grupo ${index + 1}/${scopeEntries.length} porque no tiene un scope valido.`,
            {
              stage_code: 'SKIPPING_SCOPE',
              current_scope_key: scopeKey,
              current_scope_label: scopeLabel,
            },
          );
          continue;
        }

        let created: { plan_rules: number; offers: number; sections: number; subsections: number; schedules: number };

        if (batch.source_kind === 'AKADEMIC') {
          // Akademic: merge in-place. Updates existing records (keeping IDs stable so
          // videoconferences keep pointing to valid schedules), creates new ones that
          // appeared in Akademic, and deletes stale ones that were removed from Akademic.
          executionSummary = await this.updateExecutionBatchProgress(
            batch,
            executionSummary,
            Math.max(10, progressPercent - 1),
            `Sincronizando grupo ${index + 1}/${scopeEntries.length}: ${scopeLabel}.`,
            {
              stage_code: 'IMPORTING_SCOPE',
              current_scope_key: scopeKey,
              current_scope_label: scopeLabel,
            },
          );
          created = await this.mergeAkademicScopeRows(
            batchId,
            scope,
            scopeRows,
            catalog,
            executionCaches,
            actor,
          );
          executionSummary.replaced_scope_count = numberValue(executionSummary.replaced_scope_count) + 1;
        } else {
          if (hasExistingData(decision?.existing_summary_json) && decision?.decision === 'REPLACE_SCOPE') {
            executionSummary = await this.updateExecutionBatchProgress(
              batch,
              executionSummary,
              Math.max(10, progressPercent - 1),
              `Reemplazando datos existentes del grupo ${index + 1}/${scopeEntries.length}: ${scopeLabel}.`,
              {
                stage_code: 'REPLACING_SCOPE',
                current_scope_key: scopeKey,
                current_scope_label: scopeLabel,
              },
            );
            const replaced = await this.replaceExistingScope(scope, actor);
            executionSummary.replaced_scope_count = numberValue(executionSummary.replaced_scope_count) + (replaced ? 1 : 0);
          }

          executionSummary = await this.updateExecutionBatchProgress(
            batch,
            executionSummary,
            Math.max(10, progressPercent - 1),
            `Importando grupo ${index + 1}/${scopeEntries.length}: ${scopeLabel}.`,
            {
              stage_code: 'IMPORTING_SCOPE',
              current_scope_key: scopeKey,
              current_scope_label: scopeLabel,
            },
          );
          created = await this.importRowsForScope(
            batchId,
            scope,
            scopeRows,
            catalog,
            executionCaches,
            actor,
          );
        }
        executionSummary.processed_scope_count = index + 1;
        executionSummary.imported_scope_count = numberValue(executionSummary.imported_scope_count) + 1;
        executionSummary.created_plan_rule_count = numberValue(executionSummary.created_plan_rule_count) + created.plan_rules;
        executionSummary.created_offer_count = numberValue(executionSummary.created_offer_count) + created.offers;
        executionSummary.created_section_count = numberValue(executionSummary.created_section_count) + created.sections;
        executionSummary.created_subsection_count = numberValue(executionSummary.created_subsection_count) + created.subsections;
        executionSummary.created_schedule_count = numberValue(executionSummary.created_schedule_count) + created.schedules;
        (executionSummary.executed_scope_keys as string[]).push(scopeKey);
        executionSummary = await this.updateExecutionBatchProgress(
          batch,
          executionSummary,
          progressPercent,
          `Grupo ${index + 1}/${scopeEntries.length} completado: ${scopeLabel}.`,
          {
            stage_code: 'IMPORTING_SCOPE',
            current_scope_key: scopeKey,
            current_scope_label: scopeLabel,
          },
        );
      }

      batch.status = 'EXECUTED';
      batch.error_message = null;
      batch.execution_summary_json = this.buildExecutionProgress(
        'Carga masiva aplicada correctamente.',
        100,
        {
          ...executionSummary,
          stage_code: 'DONE',
          finished_at: new Date().toISOString(),
          processed_scope_count: scopeEntries.length,
          total_scope_count: scopeEntries.length,
        },
      );
      batch.executed_at = new Date();
      batch.updated_at = new Date();
      await this.importBatchesRepo.save(batch);
    } catch (error) {
      if (!batch) {
        return;
      }
      const errorMessage =
        error instanceof Error ? error.message : 'No se pudo ejecutar la importacion.';
      batch.status = 'FAILED';
      batch.error_message = errorMessage;
      batch.execution_summary_json = this.buildExecutionProgress(
        errorMessage,
        100,
        {
          ...(executionSummary ?? {}),
          stage_code: 'FAILED',
          error_message: errorMessage,
          finished_at: new Date().toISOString(),
          total_scope_count:
            totalScopeCount || numberValue(executionSummary.total_scope_count),
        },
      );
      batch.updated_at = new Date();
      await this.importBatchesRepo.save(batch);
    }
  }

  private async processPreviewBatch(batchId: string, fileBuffer: Buffer) {
    const batch = await this.requireBatch(batchId);

    try {
      await this.updatePreviewBatchProgress(batch, 8, 'Leyendo la hoja principal del Excel...', {
        stage_code: 'READING_WORKBOOK',
      });

      const workbook = XLSX.read(fileBuffer, { type: 'buffer', raw: true });
      const sheetName = workbook.SheetNames.find((name) => name === IMPORT_SHEET_NAME) ?? workbook.SheetNames[0] ?? null;
      const sheet = sheetName ? workbook.Sheets[sheetName] ?? null : null;
      if (!sheet) {
        throw new BadRequestException('No se encontro una hoja valida para procesar el archivo.');
      }

      const rawRows = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
        defval: null,
        raw: true,
      });
      batch.sheet_name = sheetName;
      batch.total_row_count = rawRows.length;
      batch.updated_at = new Date();
      await this.importBatchesRepo.save(batch);

      await this.updatePreviewBatchProgress(batch, 18, 'Cargando catalogos y mapeos...', {
        stage_code: 'LOADING_CATALOG',
        total_rows: rawRows.length,
      });

      const catalog = await this.loadImportCatalog();

      const normalizedRows = rawRows
        .map((row, index) => this.normalizeExcelRow(row, index + 2))
        .filter((row): row is NormalizedImportRow => Boolean(row));
      const previewRows: PreviewRow[] = [];
      const progressStep = Math.max(75, Math.floor(normalizedRows.length / 8));

      for (let index = 0; index < normalizedRows.length; index += 1) {
        previewRows.push(this.resolvePreviewRow(normalizedRows[index], catalog));
        const processedCount = index + 1;
        if (
          processedCount === normalizedRows.length ||
          processedCount % progressStep === 0
        ) {
          const percent = 22 + Math.round((processedCount / Math.max(normalizedRows.length, 1)) * 34);
          await this.updatePreviewBatchProgress(
            batch,
            Math.min(percent, 56),
            `Normalizando filas y validando matches (${processedCount}/${normalizedRows.length})...`,
            {
              stage_code: 'NORMALIZING_ROWS',
              processed_rows: processedCount,
              total_rows: normalizedRows.length,
            },
          );
        }
      }

      this.assignStructuralCodes(previewRows);
      this.applyScheduleWarnings(previewRows);

      batch.importable_row_count = previewRows.filter((row) => row.can_import).length;
      batch.blocked_row_count = previewRows.filter((row) => !row.can_import).length;
      batch.warning_row_count = previewRows.filter((row) =>
        row.issues.some((issue) => issue.severity === 'WARNING'),
      ).length;
      batch.preview_summary_json = this.buildStoredSummary(previewRows);
      batch.updated_at = new Date();
      await this.importBatchesRepo.save(batch);

      await this.updatePreviewBatchProgress(batch, 62, 'Guardando filas del preview...', {
        stage_code: 'STAGING_ROWS',
        importable_rows: batch.importable_row_count,
        blocked_rows: batch.blocked_row_count,
      });

      await this.stagePreviewRowsAndFinalizeBatch(batch, previewRows, {
        stagingMessage: 'Guardando filas del preview...',
        readyMessage: 'Preview listo.',
      });
    } catch (error) {
      batch.status = 'PREVIEW_FAILED';
      batch.error_message =
        error instanceof Error ? error.message : 'No se pudo generar el preview del archivo.';
      batch.preview_progress_json = this.buildPreviewProgress(batch.error_message, 100, {
        stage_code: 'FAILED',
      });
      batch.updated_at = new Date();
      await this.importBatchesRepo.save(batch);
    }
  }

  private async processAkademicPreviewBatch(
    batchId: string,
    dto: PreviewPlanningAkademicImportDto,
  ) {
    const batch = await this.requireBatch(batchId);

    try {
      await this.updatePreviewBatchProgress(batch, 8, 'Validando filtros y catalogos locales...', {
        stage_code: 'VALIDATING_SCOPE',
      });

      const catalog = await this.loadImportCatalog();
      this.validateAkademicPreviewDto(dto, catalog);

      await this.updatePreviewBatchProgress(batch, 18, 'Consultando cursos y secciones desde Akademic...', {
        stage_code: 'FETCHING_AKADEMIC',
      });

      const previewRows = await this.buildAkademicPreviewRows(dto, catalog, async (current, total, message) => {
        const percent = 18 + Math.round((current / Math.max(total, 1)) * 44);
        await this.updatePreviewBatchProgress(batch, Math.min(percent, 62), message, {
          stage_code: 'FETCHING_AKADEMIC',
          processed_courses: current,
          total_courses: total,
        });
      });

      batch.total_row_count = previewRows.length;
      batch.importable_row_count = previewRows.filter((row) => row.can_import).length;
      batch.blocked_row_count = previewRows.filter((row) => !row.can_import).length;
      batch.warning_row_count = previewRows.filter((row) =>
        row.issues.some((issue) => issue.severity === 'WARNING'),
      ).length;
      batch.preview_summary_json = this.buildStoredSummary(previewRows);
      batch.updated_at = new Date();
      await this.importBatchesRepo.save(batch);

      await this.stagePreviewRowsAndFinalizeBatch(batch, previewRows, {
        stagingMessage: 'Guardando filas del preview Akademic...',
        readyMessage: 'Preview Akademic listo.',
      });
    } catch (error) {
      batch.status = 'PREVIEW_FAILED';
      batch.error_message =
        error instanceof Error ? error.message : 'No se pudo generar el preview desde Akademic.';
      batch.preview_progress_json = this.buildPreviewProgress(batch.error_message, 100, {
        stage_code: 'FAILED',
      });
      batch.updated_at = new Date();
      await this.importBatchesRepo.save(batch);
    }
  }

  private async stagePreviewRowsAndFinalizeBatch(
    batch: PlanningImportBatchEntity,
    previewRows: PreviewRow[],
    options: {
      stagingMessage: string;
      readyMessage: string;
    },
  ) {
    await this.updatePreviewBatchProgress(batch, 62, options.stagingMessage, {
      stage_code: 'STAGING_ROWS',
      importable_rows: batch.importable_row_count,
      blocked_rows: batch.blocked_row_count,
    });

    const now = new Date();
    await this.importBatchesRepo.manager.transaction(async (manager) => {
      if (previewRows.length > 0) {
        const rowEntities = previewRows.map((row) =>
          manager.create(PlanningImportRowEntity, {
            id: newId(),
            batch_id: batch.id,
            row_number: row.row_number,
            scope_key: row.scope_key,
            row_hash: row.row_hash,
            source_json: row.source_json,
            normalized_json: row.normalized_json,
            resolution_json: row.resolution_json,
            can_import: row.can_import,
            imported: false,
            created_at: now,
            updated_at: now,
          }),
        );
        await manager.save(PlanningImportRowEntity, rowEntities, { chunk: 250 });

        const issues = rowEntities.flatMap((entity, index) =>
          previewRows[index].issues.map((issue) =>
            manager.create(PlanningImportRowIssueEntity, {
              id: newId(),
              batch_id: batch.id,
              row_id: entity.id,
              row_number: previewRows[index].row_number,
              severity: issue.severity,
              issue_code: issue.issue_code,
              field_name: issue.field_name ?? null,
              message: issue.message,
              meta_json: issue.meta_json ?? null,
              created_at: now,
            }),
          ),
        );
        if (issues.length > 0) {
          await manager.save(PlanningImportRowIssueEntity, issues, { chunk: 250 });
        }
      }
    });

    await this.updatePreviewBatchProgress(batch, 76, 'Analizando scopes y data existente...', {
      stage_code: 'CHECKING_SCOPES',
    });

    const scopeEntities = await this.buildScopeDecisionEntities(batch.id, previewRows, now, async (current, total) => {
      const percent = 76 + Math.round((current / Math.max(total, 1)) * 18);
      await this.updatePreviewBatchProgress(
        batch,
        Math.min(percent, 94),
        `Revisando data existente por scope (${current}/${total})...`,
        {
          stage_code: 'CHECKING_SCOPES',
          processed_scopes: current,
          total_scopes: total,
        },
      );
    });
    if (scopeEntities.length > 0) {
      await this.importScopeDecisionsRepo.save(scopeEntities, { chunk: 100 });
    }

    batch.status = 'PREVIEW_READY';
    batch.preview_progress_json = this.buildPreviewProgress(options.readyMessage, 100, {
      stage_code: 'READY',
      total_rows: batch.total_row_count,
      importable_rows: batch.importable_row_count,
      blocked_rows: batch.blocked_row_count,
    });
    batch.error_message = null;
    batch.updated_at = new Date();
    await this.importBatchesRepo.save(batch);
  }

  private validateAkademicPreviewDto(
    dto: PreviewPlanningAkademicImportDto,
    catalog: ImportCatalog,
  ) {
    if (!catalog.semesters.some((item) => item.id === dto.semester_id)) {
      throw new BadRequestException('El semestre seleccionado no existe en el catalogo local.');
    }
    if (dto.vc_period_id && !catalog.vcPeriods.some((item) => item.id === dto.vc_period_id)) {
      throw new BadRequestException('El periodo VC seleccionado no existe en el catalogo local.');
    }
    if (dto.campus_id && !catalog.campuses.some((item) => item.id === dto.campus_id)) {
      throw new BadRequestException('La sede seleccionada no existe en el catalogo local.');
    }
    if (dto.faculty_id && !catalog.faculties.some((item) => item.id === dto.faculty_id)) {
      throw new BadRequestException('La facultad seleccionada no existe en el catalogo local.');
    }
    if (
      dto.academic_program_id &&
      !catalog.programs.some((item) => item.id === dto.academic_program_id)
    ) {
      throw new BadRequestException('El programa seleccionado no existe en el catalogo local.');
    }
    if (dto.study_plan_id && !catalog.studyPlans.some((item) => item.id === dto.study_plan_id)) {
      throw new BadRequestException('El plan de estudios seleccionado no existe en el catalogo local.');
    }
    if (
      dto.study_plan_course_id &&
      !catalog.studyPlanCourses.some((item) => item.id === dto.study_plan_course_id)
    ) {
      throw new BadRequestException('El curso seleccionado no existe dentro del catalogo local.');
    }
  }

  private async buildAkademicPreviewRows(
    dto: PreviewPlanningAkademicImportDto,
    catalog: ImportCatalog,
    onProgress?: (current: number, total: number, message: string) => Promise<void> | void,
  ): Promise<PreviewRow[]> {
    const requestedCourseCode =
      dto.course_code?.trim() ||
      recordString(
        asRecord(
          catalog.studyPlanCourseDetails.find(
            (item) => item.study_plan_course_id === dto.study_plan_course_id,
          ) ?? null,
        ),
        'short_code',
      ) ||
      catalog.studyPlanCourses.find((item) => item.id === dto.study_plan_course_id)?.course_code ||
      '';

    const courses = (await this.fetchAkademicCourses(dto.semester_id, requestedCourseCode)).filter((course) => {
      if (!requestedCourseCode) {
        return true;
      }
      return courseCodeLikeMatch(course.code, normalizeCourseCodeValue(requestedCourseCode));
    });

    const totalCourses = courses.length;
    const byCourse = await runWithConcurrency(courses, AKADEMIC_COURSE_CONCURRENCY, async (course, index) => {
      const enrichedCourse = await this.enrichAkademicCourse(course, dto.semester_id);
      if (dto.cycle && enrichedCourse.academic_year && dto.cycle !== enrichedCourse.academic_year) {
        await onProgress?.(
          index + 1,
          totalCourses,
          `Filtrando cursos por ciclo (${index + 1}/${Math.max(totalCourses, 1)})...`,
        );
        return [] as PreviewRow[];
      }

      const sections = await this.fetchAkademicSections(enrichedCourse.id, dto.semester_id);
      const sectionRows = await runWithConcurrency(sections, AKADEMIC_SECTION_CONCURRENCY, async (section) => {
        const [detail, schedules] = await Promise.all([
          this.fetchAkademicSectionDetail(section.id),
          this.fetchAkademicSectionSchedules(section.id),
        ]);
        return this.buildAkademicPreviewRowsForSection(
          dto,
          catalog,
          enrichedCourse,
          section,
          detail,
          schedules,
        );
      });

      await onProgress?.(
        index + 1,
        totalCourses,
        `Procesando cursos y secciones (${index + 1}/${Math.max(totalCourses, 1)})...`,
      );
      return sectionRows.flat();
    });

    return byCourse
      .flat()
      .map((row, index) => ({
        ...row,
        row_number: index + 2,
      }))
      .sort((left, right) => left.row_number - right.row_number);
  }

  private async fetchAkademicCourses(termId: string, search = ''): Promise<AkademicCourseRow[]> {
    const rows = await this.fetchAkademicRows('/admin/secciones-profesores/cursos/get', {
      draw: '1',
      tid: termId,
      acid: '0',
      acaprog: 'null',
      ayid: 'null',
      onlyWithSections: 'true',
      onlyWithoutCoordinator: 'false',
      start: '0',
      length: '5000',
      'search[value]': '',
      'search[regex]': 'false',
      keyName: search || '',
      curriculumId: 'null',
      _: `${Date.now()}`,
    });
    return deduplicateRows(rows)
      .map((row) => ({
        id: asString(row.id),
        code: asString(row.code),
        name: asString(row.name),
        career_name:
          asNullableString(pick(asRecord(row.career) ?? {}, 'name')) ??
          asNullableString(pick(row, 'careerName', 'areaCareer', 'program')),
        credits: asNullableNumber(row.credits),
        academic_year: asNullableInt(pick(row, 'academicYear')),
        type_raw: asNullableString(pick(row, 'type', 'courseType.name')),
        raw: row,
      }))
      .filter((item) => item.id && item.code && item.name);
  }

  private async enrichAkademicCourse(
    course: AkademicCourseRow,
    termId: string,
  ): Promise<AkademicCourseRow> {
    if (course.academic_year && course.type_raw) {
      return course;
    }
    try {
      const rows = await this.fetchAkademicRows(
        '/admin/cursos/get',
        {
          start: '0',
          length: '5000',
          search: course.code,
          tid: termId,
        },
      );
      const detail = rows.find((item) => normalizeLoose(asString(item.code)) === normalizeLoose(course.code)) ?? null;
      if (!detail) {
        return course;
      }
      return {
        ...course,
        career_name:
          course.career_name ??
          asNullableString(pick(detail, 'areaCareer', 'program')) ??
          course.career_name,
        academic_year: asNullableInt(pick(detail, 'academicYear')) ?? course.academic_year,
        type_raw: asNullableString(pick(detail, 'type')) ?? course.type_raw,
        raw: {
          ...course.raw,
          detail,
        },
      };
    } catch {
      return course;
    }
  }

  private async fetchAkademicSections(courseId: string, termId: string): Promise<AkademicSectionRow[]> {
    const rows = await this.fetchAkademicRows(
      `/admin/secciones-profesores/cursos/${encodeURIComponent(courseId)}/secciones/get`,
      {
        tid: termId,
        start: '0',
        length: '5000',
      },
    );
    return deduplicateRows(rows)
      .map((row) => {
        const rawModality = pick(row, 'modality');
        return {
          id: asString(row.id),
          course_id: courseId,
          external_code: asString(row.code),
          modality_raw:
            typeof rawModality === 'string' || typeof rawModality === 'number'
              ? rawModality
              : null,
          vacancies: asNullableInt(pick(row, 'vacancies')),
          teacher_names: asStringArray(row.teacherNames) ?? [],
          teacher_ids: (Array.isArray(row.teacherSections) ? row.teacherSections : [])
          .map((item) => asNullableString(pick(asRecord(item) ?? {}, 'teacherId', 'userId')))
          .filter((item): item is string => Boolean(item)),
          raw: row,
        };
      })
      .filter((item) => item.id && item.external_code);
  }

  private async fetchAkademicSectionDetail(
    sectionId: string,
  ): Promise<Record<string, unknown> | null> {
    const payload = await this.fetchAkademicPayload(
      '/admin/secciones-profesores/cursos/0/secciones/seccion/get',
      { id: sectionId },
    );
    return asRecord(payload);
  }

  private async fetchAkademicSectionSchedules(sectionId: string): Promise<AkademicScheduleRow[]> {
    const rows = await this.fetchAkademicRows(
      `/admin/secciones-profesores/cursos/0/secciones/${encodeURIComponent(sectionId)}/horarios/get`,
    );

    return deduplicateRows(rows)
      .map((row) => this.normalizeAkademicScheduleRow(row, sectionId))
      .filter((item): item is AkademicScheduleRow => Boolean(item));
  }

  private buildFallbackAkademicCourseForSectionSync(
    offer: PlanningOfferEntity,
    detail: Record<string, unknown> | null,
    catalog: ImportCatalog,
  ): AkademicCourseRow | null {
    const courseId =
      recordString(detail ?? {}, 'courseId') ??
      offer.source_course_id ??
      null;
    const courseCode = offer.course_code ?? recordString(detail ?? {}, 'courseCode');
    const courseName = offer.course_name ?? recordString(detail ?? {}, 'courseName');
    if (!courseId || !courseCode || !courseName) {
      return null;
    }

    const programName =
      (offer.academic_program_id
        ? catalog.programs.find((item) => item.id === offer.academic_program_id)?.name ?? null
        : null) ??
      recordString(detail ?? {}, 'careerName') ??
      recordString(detail ?? {}, 'programName');

    return {
      id: courseId,
      code: courseCode,
      name: courseName,
      career_name: programName,
      credits: null,
      academic_year: offer.cycle ?? null,
      type_raw: offer.course_type ?? null,
      raw: {
        id: courseId,
        code: courseCode,
        name: courseName,
        careerName: programName,
        detail,
      },
    };
  }

  private buildFallbackAkademicSectionForSectionSync(
    currentSection: PlanningSectionEntity,
    sourceCourseId: string,
    detail: Record<string, unknown> | null,
  ): AkademicSectionRow | null {
    const sectionId = recordString(detail ?? {}, 'id') ?? currentSection.source_section_id ?? null;
    const externalCode =
      recordString(detail ?? {}, 'code') ??
      currentSection.external_code ??
      null;
    if (!sectionId || !externalCode) {
      return null;
    }

    const teacherSections = Array.isArray(pick(detail ?? {}, 'teacherSections'))
      ? (pick(detail ?? {}, 'teacherSections') as unknown[])
      : [];

    return {
      id: sectionId,
      course_id: sourceCourseId,
      external_code: externalCode,
      modality_raw:
        pick(detail ?? {}, 'modality', 'sessionType') as number | string | null,
      vacancies: this.resolveAkademicSectionVacancies(
        {
          id: sectionId,
          course_id: sourceCourseId,
          external_code: externalCode,
          modality_raw: null,
          vacancies: null,
          teacher_names: [],
          teacher_ids: [],
          raw: detail ?? {},
        },
        detail,
      ),
      teacher_names: teacherSections
        .map((item) => asNullableString(pick(asRecord(item) ?? {}, 'fullName', 'teacherName')))
        .filter((item): item is string => Boolean(item)),
      teacher_ids: teacherSections
        .map((item) =>
          asNullableString(pick(asRecord(item) ?? {}, 'teacherId', 'userId')),
        )
        .filter((item): item is string => Boolean(item)),
      raw: detail ?? {},
    };
  }

  private shouldFetchAkademicSectionDetail(
    course: AkademicCourseRow,
    section: AkademicSectionRow,
  ) {
    if (!extractAkademicProgramIdFromSource(course.raw, null)) {
      return true;
    }
    if (!extractAkademicFacultyIdFromSource(course.raw, null)) {
      return true;
    }
    if (!section.teacher_ids.length && !section.teacher_names.length) {
      return true;
    }
    return false;
  }

  private async fetchAkademicPayload(path: string, query?: Record<string, string>) {
    return this.retryAkademicRequest(() =>
      this.settingsSyncService.fetchSourcePayloadByCode('DOCENTE', path, query, {
        skipProbe: true,
      }),
    );
  }

  private async fetchAkademicRows(path: string, query?: Record<string, string>) {
    return this.retryAkademicRequest(() =>
      this.settingsSyncService.fetchSourceRowsByCode('DOCENTE', path, query, {
        skipProbe: true,
      }),
    );
  }

  private async fetchAkademicRowsPaged(
    path: string,
    queryBuilder: (start: number) => Record<string, string>,
    pageSize: number,
  ) {
    const firstPayload = await this.fetchAkademicPayload(path, queryBuilder(0));
    const firstRecord = asRecord(firstPayload);
    const firstRows = Array.isArray(firstRecord.data) ? (firstRecord.data as Record<string, unknown>[]) : [];

    const recordsTotal = numberValue(firstRecord.recordsTotal, 0);
    const recordsFiltered = numberValue(firstRecord.recordsFiltered, 0);
    // Some Akademic/datatables endpoints report the page size in `recordsTotal`
    // and the real result-set size in `recordsFiltered`. Prefer the larger value
    // so we do not stop after the first page in full-semester syncs.
    const totalCount = Math.max(recordsTotal, recordsFiltered, firstRows.length);
    if (totalCount <= pageSize || firstRows.length === 0) {
      return firstRows;
    }

    const rows: Record<string, unknown>[] = [...firstRows];
    const maxPages = 100; // Safeguard
    const pageOffsets: number[] = [];

    for (let offset = pageSize; offset < totalCount && offset < maxPages * pageSize; offset += pageSize) {
      pageOffsets.push(offset);
    }

    if (pageOffsets.length > 0) {
      const remainingPages = await runWithConcurrency(pageOffsets, 5, async (offset) => {
        return this.fetchAkademicRows(path, queryBuilder(offset));
      });
      for (const pageRows of remainingPages) {
        rows.push(...pageRows);
      }
    }

    return rows;
  }

  private async retryAkademicRequest<T>(operation: () => Promise<T>) {
    let lastError: unknown = null;
    const maxAttempts = 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        if (!isTransientAkademicRequestError(error) || attempt === maxAttempts) {
          throw error;
        }
        await waitFor(500 * attempt);
      }
    }
    throw lastError;
  }

  private async buildAkademicPreviewRowsForSection(
    dto: PreviewPlanningAkademicImportDto,
    catalog: ImportCatalog,
    course: AkademicCourseRow,
    section: AkademicSectionRow,
    detail: Record<string, unknown> | null,
    schedules: AkademicScheduleRow[],
  ): Promise<PreviewRow[]> {
    const issues: PreviewIssue[] = [];
    const resolvedSectionVacancies = this.resolveAkademicSectionVacancies(section, detail);
    const mergedSectionSourcePayload = this.buildAkademicSectionSourcePayload(
      section.raw,
      detail,
      resolvedSectionVacancies,
    );
    const parsedSection = parseAkademicExternalSectionCode(section.external_code);
    if (!parsedSection.section_code) {
      return [
        {
          row_number: 0,
          source_json: {
            course: course.raw,
            section: mergedSectionSourcePayload,
            detail,
            schedules: schedules.map((item) => item.raw),
          },
          normalized_json: {
            source_kind: 'AKADEMIC',
            semester_raw: dto.semester_id,
            section_raw: section.external_code,
            course_code_raw: course.code,
            course_name_raw: course.name,
            academic_program_code_raw: course.career_name ?? '',
            cycle_raw: course.academic_year,
          },
          resolution_json: {
            source_kind: 'AKADEMIC',
            source_system: 'AKADEMIC',
            external_section_code: section.external_code,
          },
          scope_key: null,
          row_hash: null,
          can_import: false,
          issues: [
            {
              severity: 'BLOCKING',
              issue_code: 'INVALID_EXTERNAL_SECTION',
              field_name: 'SECCION',
              message: `No se pudo interpretar el codigo ${section.external_code} de Akademic.`,
            },
          ],
        },
      ];
    }

    const sectionTeacherIds = [
      ...section.teacher_ids,
      ...(Array.isArray(detail?.teacherSections)
        ? detail.teacherSections
            .map((item) => asNullableString(pick(asRecord(item) ?? {}, 'userId', 'teacherId')))
            .filter((item): item is string => Boolean(item))
        : []),
    ];
    const sectionTeacherDnis = [
      ...(Array.isArray(detail?.teacherSections)
        ? detail.teacherSections
            .map((item) =>
              asNullableString(
                pick(asRecord(item) ?? {}, 'dni', 'teacher.dni', 'teacher.user.dni', 'teacher.documentNumber'),
              ),
            )
            .filter((item): item is string => Boolean(item))
        : []),
    ];
    const sectionTeacherNames = [
      ...section.teacher_names,
      ...(Array.isArray(detail?.teacherSections)
        ? detail.teacherSections
            .map((item) => asNullableString(pick(asRecord(item) ?? {}, 'fullName')))
            .filter((item): item is string => Boolean(item))
        : []),
    ];
    const semester = catalog.semesters.find((item) => item.id === dto.semester_id) ?? null;
    const sourceVcAcademicProgramId = extractAkademicProgramIdFromSource(course.raw, detail);
    const sourceVcFacultyId = extractAkademicFacultyIdFromSource(course.raw, detail);
    const semesterResolution = toResolution(
      dto.semester_id,
      dto.semester_id,
      semester?.name ?? dto.semester_id,
      'manual_value',
    );
    const vcPeriod =
      (dto.vc_period_id
        ? catalog.vcPeriods.find((item) => item.id === dto.vc_period_id) ?? null
        : findVcPeriodByValue(catalog.vcPeriods, semester?.name ?? dto.semester_id)) ?? null;
    const effectiveVcPeriodId = vcPeriod?.id ?? dto.vc_period_id ?? null;
    const vcPeriodResolution = effectiveVcPeriodId
      ? toResolution(
          dto.vc_period_id ?? semester?.name ?? dto.semester_id,
          effectiveVcPeriodId,
          vcPeriod?.text ?? effectiveVcPeriodId,
          dto.vc_period_id ? 'manual_value' : 'catalog',
        )
      : toResolution('', null, null, 'none');
    const campusResolution = this.resolveAkademicCampus(parsedSection.location_token, dto.campus_id, catalog, issues);
    if (dto.campus_id && campusResolution.target_id && dto.campus_id !== campusResolution.target_id) {
      issues.push({
        severity: 'BLOCKING',
        issue_code: 'CAMPUS_SUFFIX_MISMATCH',
        field_name: 'SECCION',
        message: `La seccion ${section.external_code} pertenece a otra sede segun su sufijo.`,
      });
    }

    const programResolution = this.resolveAkademicProgram(dto, course, catalog, issues);
    const facultyResolution = this.resolveAkademicFaculty(dto, programResolution, catalog, issues);
    const cycle = dto.cycle ?? course.academic_year ?? null;
    if (!cycle) {
      issues.push({
        severity: 'BLOCKING',
        issue_code: 'MISSING_CYCLE',
        field_name: 'CICLO',
        message: `No se pudo resolver el ciclo para ${course.code}.`,
      });
    }
    const studyPlanResolution = this.resolveAkademicStudyPlan(
      dto,
      course,
      programResolution,
      cycle,
      catalog,
      issues,
    );
    const courseResolution = this.resolveAkademicCourse(
      dto,
      course,
      studyPlanResolution,
      cycle,
      programResolution,
      catalog,
      issues,
    );
    const modalityResolution = this.resolveAkademicCourseModality(
      section.modality_raw,
      parsedSection.modality_token,
      catalog,
      issues,
    );
    const shiftResolution = this.resolveAkademicShift(schedules);
    const groupedSubsections = this.groupAkademicSubsections(
      parsedSection.section_code,
      schedules,
      this.normalizeAkademicCourseType(course.type_raw),
    );

    const allRows = [] as PreviewRow[];
    const subsectionGroups = groupedSubsections.length
      ? groupedSubsections
      : [
          {
            import_subsection_code: parsedSection.section_code,
            subsection_kind: this.deriveAkademicSubsectionKindFromSchedules(
              this.normalizeAkademicCourseType(course.type_raw),
              [],
            ),
            schedules: [] as AkademicScheduleRow[],
            warning: null,
          },
        ];

    for (const group of subsectionGroups) {
      const groupSchedules = group.schedules.length ? group.schedules : [null];
      const groupHours = this.computeAkademicAssignedHours(group.schedules);
      const summaryTeacherResolution = await this.resolveAkademicTeacherMatch(
        sectionTeacherIds[0] ?? null,
        sectionTeacherDnis[0] ?? null,
        sectionTeacherNames[0] ?? null,
        null,
        null,
        catalog,
        issues,
      );

      for (const schedule of groupSchedules) {
        const rowIssues = [...issues];
        if (group.warning) {
          rowIssues.push({
            severity: 'WARNING',
            issue_code: 'SUBSECTION_SPLIT_HEURISTIC',
            field_name: 'HORARIO',
            message: group.warning,
          });
        }
        const scheduleTeacherResolution = schedule
          ? await this.resolveAkademicTeacherMatch(
              schedule.teacher_external_id,
              schedule.teacher_dni,
              schedule.teacher_name ?? sectionTeacherNames[0] ?? null,
              schedule.teacher_username,
              schedule.teacher_email,
              catalog,
              rowIssues,
            )
          : summaryTeacherResolution;
        const locationResolution = schedule
          ? this.resolveAkademicScheduleLocation(schedule, campusResolution.target_id, catalog, rowIssues)
          : {
              building: toResolution('', null, null, 'none'),
              classroom: toResolution('', null, null, 'none'),
            };

        const canImport =
          Boolean(campusResolution.target_id) &&
          Boolean(facultyResolution.target_id) &&
          Boolean(programResolution.target_id) &&
          Boolean(studyPlanResolution.target_id) &&
          Boolean(courseResolution.target_id) &&
          Boolean(cycle) &&
          !rowIssues.some((issue) => issue.severity === 'BLOCKING');
        const scope =
          canImport && campusResolution.target_id && facultyResolution.target_id && programResolution.target_id && studyPlanResolution.target_id && cycle
            ? {
                semester_id: dto.semester_id,
                vc_period_id: effectiveVcPeriodId,
                campus_id: campusResolution.target_id,
                faculty_id: facultyResolution.target_id,
                academic_program_id: programResolution.target_id,
                study_plan_id: studyPlanResolution.target_id,
                cycle,
                semester_name: semester?.name ?? dto.semester_id,
                vc_period_name: vcPeriod?.text ?? null,
                campus_name: campusResolution.target_label,
                faculty_name: facultyResolution.target_label,
                academic_program_name: programResolution.target_label,
                study_plan_name: studyPlanResolution.target_label,
                study_plan_year: recordString(asRecord(studyPlanResolution.target_extra ?? {}), 'year'),
              }
            : null;
        const scopeKey = scope ? buildScopeKey(scope) : null;
        const schedulePayload = schedule
          ? {
              day_of_week: schedule.day_of_week,
              start_time: schedule.start_time,
              end_time: schedule.end_time,
              duration_minutes: schedule.duration_minutes,
              academic_hours: schedule.academic_hours,
              signature: `${schedule.day_of_week}|${schedule.start_time}|${schedule.end_time}`,
              session_type: schedule.session_type,
              source_session_type_code: schedule.source_session_type_code,
              teacher_id: scheduleTeacherResolution.target_id,
              building_id: locationResolution.building.target_id,
              classroom_id: locationResolution.classroom.target_id,
              source_schedule_id: schedule.id,
              source_payload_json: schedule.raw,
            }
          : null;

        allRows.push({
          row_number: 0,
          source_json: {
            course: course.raw,
            section: mergedSectionSourcePayload,
            detail,
            schedule: schedule?.raw ?? null,
          },
          normalized_json: {
            source_kind: 'AKADEMIC',
            semester_raw: dto.semester_id,
            campus_raw: parsedSection.location_token ?? '',
            faculty_code_raw: facultyResolution.source_value,
            academic_program_code_raw: course.career_name ?? '',
            cycle_raw: cycle,
            study_plan_code_raw: studyPlanResolution.source_value,
            section_raw: section.external_code,
            course_code_raw: course.code,
            course_name_raw: course.name,
            teacher_name_raw: schedule?.teacher_name ?? sectionTeacherNames[0] ?? '',
            day_raw: schedule?.day_of_week ?? '',
            denomination_raw: section.external_code,
          },
          resolution_json: {
            source_kind: 'AKADEMIC',
            source_system: 'AKADEMIC',
            semester_id: dto.semester_id,
            vc_period_id: effectiveVcPeriodId,
            source_vc_faculty_id: sourceVcFacultyId,
            source_vc_academic_program_id: sourceVcAcademicProgramId,
            source_vc_course_id: course.id,
            source_vc_section_id: section.id,
            campus_id: campusResolution.target_id,
            faculty_id: facultyResolution.target_id,
            academic_program_id: programResolution.target_id,
            study_plan_id: studyPlanResolution.target_id,
            study_plan_course_id: courseResolution.target_id,
            teacher_id: summaryTeacherResolution.target_id,
            schedule_teacher_id: scheduleTeacherResolution.target_id,
            course_modality_id: modalityResolution.target_id,
            building_id: locationResolution.building.target_id,
            classroom_id: locationResolution.classroom.target_id,
            shift: shiftResolution.target_id,
            cycle,
            scope_key: scopeKey,
            scope,
            section_base_code: (parsedSection.section_code ?? '') + (parsedSection.modality_token ?? ''),
            explicit_subsection_code: group.import_subsection_code,
            import_section_code: (parsedSection.section_code ?? '') + (parsedSection.modality_token ?? ''),
            import_subsection_code: group.import_subsection_code,
            external_section_code: section.external_code,
            source_section_id: section.id,
            source_course_id: course.id,
            source_term_id: dto.semester_id,
            is_cepea: parsedSection.is_cepea,
            offer_course_code:
              recordString(asRecord(courseResolution.target_extra ?? {}), 'course_code') ?? course.code,
            offer_course_name:
              recordString(asRecord(courseResolution.target_extra ?? {}), 'course_name') ?? course.name,
            offer_theoretical_hours: groupHours.offer_theoretical_hours,
            offer_practical_hours: groupHours.offer_practical_hours,
            offer_total_hours: groupHours.offer_total_hours,
            offer_course_type:
              groupHours.offer_theoretical_hours > 0 && groupHours.offer_practical_hours > 0
                ? 'TEORICO_PRACTICO'
                : groupHours.offer_practical_hours > 0
                  ? 'PRACTICO'
                  : 'TEORICO',
            projected_vacancies: resolvedSectionVacancies,
            capacity_snapshot: schedule?.capacity ?? resolvedSectionVacancies ?? null,
            subsection_kind: group.subsection_kind,
            assigned_theoretical_hours: groupHours.assigned_theoretical_hours,
            assigned_practical_hours: groupHours.assigned_practical_hours,
            assigned_total_hours: groupHours.assigned_total_hours,
            denomination: buildDenomination(
              course.code,
              course.name,
              parsedSection.section_code,
              group.import_subsection_code,
              campusResolution.target_id ?? '',
            ),
            schedule: schedulePayload,
            mapping_resolution: {
              vc_period: vcPeriodResolution,
              campus: campusResolution,
              faculty_code: facultyResolution,
              academic_program_code: programResolution,
              study_plan_code: studyPlanResolution,
              course_code: courseResolution,
              course_modality: modalityResolution,
              shift: shiftResolution,
              building: locationResolution.building,
              classroom: locationResolution.classroom,
            },
          },
          scope_key: scopeKey,
          row_hash: hashRowKey([
            scopeKey,
            courseResolution.target_id,
            parsedSection.section_code,
            group.import_subsection_code,
            schedule?.id ?? 'no-schedule',
          ]),
          can_import: canImport,
          issues: rowIssues,
        });
      }
    }

    return allRows.filter((row) => this.matchesAkademicFilterScope(dto, asRecord(row.resolution_json)));
  }

  private matchesAkademicFilterScope(
    dto: PreviewPlanningAkademicImportDto,
    resolution: Record<string, unknown>,
  ) {
    if (dto.faculty_id && recordString(resolution, 'faculty_id') && dto.faculty_id !== recordString(resolution, 'faculty_id')) {
      return false;
    }
    if (
      dto.academic_program_id &&
      recordString(resolution, 'academic_program_id') &&
      dto.academic_program_id !== recordString(resolution, 'academic_program_id')
    ) {
      return false;
    }
    if (dto.study_plan_id && recordString(resolution, 'study_plan_id') && dto.study_plan_id !== recordString(resolution, 'study_plan_id')) {
      return false;
    }
    return true;
  }

  private normalizeAkademicScheduleRow(
    row: Record<string, unknown>,
    sectionId: string,
  ): AkademicScheduleRow | null {
    const startTime =
      normalizeAkademicTimeValue(asNullableString(pick(row, 'startTimeText'))) ??
      normalizeAkademicTimeValue(asNullableString(pick(row, 'startTime')));
    const endTime =
      normalizeAkademicTimeValue(asNullableString(pick(row, 'endTimeText'))) ??
      normalizeAkademicTimeValue(asNullableString(pick(row, 'endTime')));
    const weekDay = asNullableInt(pick(row, 'weekDay'));
    if (!startTime || !endTime || weekDay === null) {
      return null;
    }

    const day_of_week = parseDayOfWeek(String(weekDay));
    if (!day_of_week) {
      return null;
    }

    const durationMinutes = Math.max(0, toMinutes(endTime) - toMinutes(startTime));
    const teacherSchedules = Array.isArray(row.teacherSchedules) ? row.teacherSchedules : [];
    const firstTeacherSchedule = asRecord(teacherSchedules[0]) ?? null;
    const teacherNode = asRecord(pick(firstTeacherSchedule ?? {}, 'teacher')) ?? null;
    const teacherUser = asRecord(pick(teacherNode ?? {}, 'user')) ?? null;
    const classroomNode = asRecord(pick(row, 'classroom')) ?? null;

    return {
      id: asString(row.id),
      section_id: sectionId,
      day_of_week,
      start_time: startTime,
      end_time: endTime,
      duration_minutes: durationMinutes,
      academic_hours: roundToTwo(durationMinutes / 50),
      session_type: this.normalizeAkademicSessionType(pick(row, 'sessionType')),
      source_session_type_code: asNullableString(pick(row, 'sessionType')),
      teacher_external_id:
        asNullableString(pick(firstTeacherSchedule ?? {}, 'teacherId')) ??
        asNullableString(pick(teacherNode ?? {}, 'userId')) ??
        asNullableString(pick(teacherUser ?? {}, 'id')),
      teacher_dni:
        asNullableString(
          pick(firstTeacherSchedule ?? {}, 'dni', 'teacher.dni', 'teacher.user.dni', 'teacher.documentNumber'),
        ) ??
        asNullableString(pick(teacherUser ?? {}, 'dni', 'documentNumber')) ??
        asNullableString(pick(teacherNode ?? {}, 'dni', 'documentNumber')),
      teacher_name:
        asNullableString(pick(teacherUser ?? {}, 'fullName')) ??
        asNullableString(pick(teacherNode ?? {}, 'fullName')),
      teacher_username:
        asNullableString(pick(teacherUser ?? {}, 'username', 'userName')) ??
        asNullableString(pick(teacherNode ?? {}, 'username', 'userName')),
      teacher_email:
        asNullableString(pick(teacherUser ?? {}, 'email', 'institutionalEmail')) ??
        asNullableString(pick(teacherNode ?? {}, 'email', 'institutionalEmail')),
      classroom_external_id:
        asNullableString(pick(row, 'classroomId')) ??
        asNullableString(pick(classroomNode ?? {}, 'id')),
      building_external_id:
        asNullableString(pick(classroomNode ?? {}, 'buildingId')) ??
        asNullableString(pick(classroomNode ?? {}, 'building.id')),
      classroom_description:
        asNullableString(pick(classroomNode ?? {}, 'description')) ??
        asNullableString(pick(classroomNode ?? {}, 'code')),
      building_description: asNullableString(pick(classroomNode ?? {}, 'building.description', 'building.name')),
      capacity: asNullableInt(pick(classroomNode ?? {}, 'capacity')),
      section_group_id: asNullableString(pick(row, 'sectionGroupId')),
      raw: row,
    };
  }

  private normalizeAkademicSessionType(value: unknown): PlanningSessionType {
    const normalized = normalizeLoose(`${value ?? ''}`);
    if (['TEORIA', 'THEORY'].includes(normalized)) {
      return 'THEORY';
    }
    if (['PRACTICA', 'PRACTICE'].includes(normalized)) {
      return 'PRACTICE';
    }
    if (['LAB', 'LABORATORIO', 'LABORATORY'].includes(normalized)) {
      return 'LAB';
    }
    return 'OTHER';
  }

  private resolveAkademicCampus(
    locationToken: string | null,
    requestedCampusId: string | undefined,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    const sourceValue = locationToken ?? requestedCampusId ?? '';
    const alias = resolveAlias(catalog.aliasMap, 'campus', sourceValue);
    if (alias?.target_id) {
      const campus = catalog.campuses.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(
        sourceValue,
        campus?.id ?? null,
        campus?.name ?? alias.target_label ?? null,
        campus ? 'alias' : 'none',
      );
    }
    if (requestedCampusId && !locationToken) {
      const campus = catalog.campuses.find((item) => item.id === requestedCampusId) ?? null;
      return toResolution(sourceValue, campus?.id ?? null, campus?.name ?? null, 'manual_value');
    }
    const mappedCampus =
      catalog.campusVcLocations.find((item) => item.vc_location_code === locationToken)?.campus_id ?? null;
    if (mappedCampus) {
      const campus = catalog.campuses.find((item) => item.id === mappedCampus) ?? null;
      return toResolution(sourceValue, campus?.id ?? null, campus?.name ?? null, 'catalog');
    }
    const campus =
      catalog.campuses.find((item) => normalizeLoose(item.code) === normalizeLoose(locationToken)) ??
      catalog.campuses.find((item) => normalizeLoose(item.name).includes(normalizeLoose(locationToken))) ??
      null;
    if (!campus) {
      issues.push(missingCriticalIssue('campus', sourceValue, 'No se pudo resolver la sede desde el codigo de Akademic.'));
    }
    return toResolution(sourceValue, campus?.id ?? null, campus?.name ?? null, campus ? 'heuristic' : 'none');
  }

  private resolveAkademicProgram(
    dto: PreviewPlanningAkademicImportDto,
    course: AkademicCourseRow,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    if (dto.academic_program_id) {
      const program = catalog.programs.find((item) => item.id === dto.academic_program_id) ?? null;
      return toResolution(
        course.career_name ?? dto.academic_program_id,
        program?.id ?? null,
        program?.name ?? null,
        'manual_value',
      );
    }
    return this.resolveAcademicProgram(course.career_name ?? '', catalog, issues);
  }

  private resolveAkademicFaculty(
    dto: PreviewPlanningAkademicImportDto,
    programResolution: MappingResolution,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    if (dto.faculty_id) {
      const faculty = catalog.faculties.find((item) => item.id === dto.faculty_id) ?? null;
      return toResolution(dto.faculty_id, faculty?.id ?? null, faculty?.name ?? null, 'manual_value');
    }
    const program = catalog.programs.find((item) => item.id === programResolution.target_id) ?? null;
    const faculty = program?.faculty_id
      ? catalog.faculties.find((item) => item.id === program.faculty_id) ?? null
      : null;
    if (!faculty) {
      issues.push(
        missingCriticalIssue(
          'faculty_code',
          programResolution.source_value,
          'No se pudo resolver la facultad del programa importado.',
        ),
      );
    }
    return toResolution(
      faculty?.code ?? programResolution.source_value,
      faculty?.id ?? null,
      faculty?.name ?? null,
      faculty ? 'catalog' : 'none',
    );
  }

  private resolveAkademicStudyPlan(
    dto: PreviewPlanningAkademicImportDto,
    course: AkademicCourseRow,
    programResolution: MappingResolution,
    cycle: number | null,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    if (dto.study_plan_id) {
      const plan = catalog.studyPlans.find((item) => item.id === dto.study_plan_id) ?? null;
      return toResolution(
        plan?.year ?? dto.study_plan_id,
        plan?.id ?? null,
        [plan?.year, plan?.career, plan?.name].filter(Boolean).join(' - ') || null,
        'manual_value',
        { year: plan?.year ?? null },
      );
    }
    const plans = this.findStudyPlansByCourseCode(
      course.code,
      programResolution.target_id,
      cycle,
      catalog,
    );
    if (plans.length === 1) {
      const plan = plans[0];
      return toResolution(
        plan.year ?? course.code,
        plan.id,
        [plan.year, plan.career, plan.name].filter(Boolean).join(' - ') || null,
        'catalog',
        { year: plan.year ?? null },
      );
    }
    issues.push(
      missingCriticalIssue(
        'study_plan_code',
        course.code,
        plans.length > 1
          ? `El curso ${course.code} coincide con mas de un plan de estudios para el ciclo ${cycle ?? '-'}.`
          : `No se encontro el plan de estudios para ${course.code}.`,
      ),
    );
    return toResolution(course.code, null, null, 'none');
  }

  private resolveAkademicCourse(
    dto: PreviewPlanningAkademicImportDto,
    course: AkademicCourseRow,
    studyPlanResolution: MappingResolution,
    cycle: number | null,
    programResolution: MappingResolution,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    if (dto.study_plan_course_id) {
      const item = catalog.studyPlanCourses.find((candidate) => candidate.id === dto.study_plan_course_id) ?? null;
      const detail = item ? catalog.studyPlanCourseDetailById.get(item.id) ?? null : null;
      return toResolution(
        course.code,
        item?.id ?? null,
        [detail?.short_code ?? item?.course_code, detail?.name ?? item?.course_name].filter(Boolean).join(' - ') || null,
        'manual_value',
        {
          course_code: detail?.short_code ?? item?.course_code ?? null,
          course_name: detail?.name ?? item?.course_name ?? null,
          cycle,
        },
      );
    }
    return this.resolveStudyPlanCourse(
      course.code,
      course.name,
      studyPlanResolution.target_id,
      cycle,
      {
        academic_program_code_raw: programResolution.source_value,
        study_plan_code_raw: studyPlanResolution.source_value,
        cycle_raw: cycle,
      },
      catalog,
      issues,
    );
  }

  private resolveAkademicCourseModality(
    rawValue: string | number | null,
    modalityToken: string | null,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    const normalizedToken = normalizeLoose(modalityToken);
    let value = '';
    if (normalizedToken === 'V') {
      value = 'VIRTUAL';
    } else if (normalizedToken === 'P') {
      value = 'PRESENCIAL';
    } else if (normalizedToken === 'HV') {
      value = 'HIBRIDO VIRTUAL';
    } else if (normalizedToken === 'HP') {
      value = 'HIBRIDO PRESENCIAL';
    } else {
      value = `${rawValue ?? ''}`;
    }
    const alias = resolveAlias(catalog.aliasMap, 'course_modality', value);
    if (alias?.target_id) {
      const modality = catalog.courseModalities.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(
        value,
        modality?.id ?? null,
        modality?.name ?? alias.target_label ?? null,
        modality ? 'alias' : 'none',
      );
    }
    const modality = catalog.courseModalities.find((item) => {
      const normalizedName = normalizeLoose(item.name);
      const normalizedCode = normalizeLoose(item.code);
      const normalized = normalizeLoose(item.name || item.code);
      if (normalizedToken === 'V') {
        return (
          normalizedCode === 'V' ||
          normalizedName === 'VIRTUAL' ||
          (normalized.includes('VIRTUAL') && !normalized.includes('HIBRIDO'))
        );
      }
      if (normalizedToken === 'P') {
        return (
          normalizedCode === 'P' ||
          normalizedName === 'PRESENCIAL' ||
          (normalized.includes('PRESENCIAL') && !normalized.includes('HIBRIDO'))
        );
      }
      if (normalizedToken === 'HV') {
        return normalized.includes('HIBRIDO') && normalized.includes('VIRTUAL');
      }
      if (normalizedToken === 'HP') {
        return normalized.includes('HIBRIDO') && normalized.includes('PRESENCIAL');
      }
      return normalized === normalizeLoose(value);
    }) ?? null;
    if (!modality && value) {
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_COURSE_MODALITY',
        field_name: 'MODALIDAD',
        message: `No se pudo resolver la modalidad enviada por Akademic (${value}).`,
        meta_json: {
          namespace: 'course_modality',
          source_value: value,
        },
      });
    }
    return toResolution(value, modality?.id ?? null, modality?.name ?? null, modality ? 'heuristic' : 'none');
  }

  private resolveAkademicShift(schedules: AkademicScheduleRow[]) {
    if (!schedules.length) {
      return toResolution('', null, null, 'none');
    }
    const firstStart = schedules
      .map((item) => toMinutes(item.start_time))
      .filter((value) => Number.isFinite(value))
      .sort((left, right) => left - right)[0];
    let shift = 'DIURNO';
    if (firstStart >= 18 * 60) {
      shift = 'NOCHE';
    } else if (firstStart >= 13 * 60) {
      shift = 'TARDE';
    }
    return toResolution(shift, shift, shift, 'heuristic');
  }

  private async resolveAkademicTeacherMatch(
    externalTeacherId: string | null,
    teacherDni: string | null,
    teacherName: string | null,
    teacherUsername: string | null,
    teacherEmail: string | null,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ): Promise<MappingResolution> {
    const cacheKey = [
      normalizeLoose(externalTeacherId),
      normalizeDni(teacherDni ?? null),
      normalizeLoose(teacherName),
      normalizeLoose(teacherUsername),
      normalizeLoose(teacherEmail),
    ].join('|');
    const cached = catalog.teacherMatchCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const localMatch = this.findTeacherCatalogMatch(
      catalog.teachers,
      externalTeacherId,
      teacherDni,
      teacherName,
      teacherUsername,
      teacherEmail,
    );
    if (localMatch) {
      const resolved = toResolution(
        teacherName ?? teacherDni ?? localMatch.full_name ?? localMatch.name ?? externalTeacherId ?? '',
        localMatch.id,
        this.teacherDisplayName(localMatch),
        'catalog',
      );
      catalog.teacherMatchCache.set(cacheKey, resolved);
      return resolved;
    }

    const intranetTeacher = await this.findTeacherFromIntranetSearch(
      externalTeacherId,
      teacherDni,
      teacherName,
      teacherUsername,
      teacherEmail,
      catalog,
    );
    if (intranetTeacher) {
      const resolved = toResolution(
        teacherName ?? intranetTeacher.full_name ?? intranetTeacher.name ?? intranetTeacher.id,
        intranetTeacher.id,
        this.teacherDisplayName(intranetTeacher),
        'heuristic',
      );
      catalog.teacherMatchCache.set(cacheKey, resolved);
      return resolved;
    }

    if (teacherName || teacherDni || externalTeacherId) {
      const teacherLabel = firstNonEmpty([
        teacherName,
        teacherDni ? `DNI ${teacherDni}` : null,
        teacherUsername,
        teacherEmail,
        externalTeacherId,
      ]);
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_TEACHER',
        field_name: 'DOCENTE',
        message: teacherLabel
          ? `No se pudo resolver el docente de Akademic (${teacherLabel}); se importara en null.`
          : 'No se pudo resolver el docente de Akademic; se importara en null.',
      });
    }
    const unresolved = toResolution(teacherName ?? teacherDni ?? externalTeacherId ?? '', null, null, 'none');
    catalog.teacherMatchCache.set(cacheKey, unresolved);
    return unresolved;
  }

  private resolveAkademicScheduleLocation(
    schedule: AkademicScheduleRow,
    campusId: string | null,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ): { building: MappingResolution; classroom: MappingResolution } {
    const buildingCandidates = buildLocationCandidateLabels(schedule.building_description);
    const classroomMatch = this.findCatalogClassroomMatch(schedule.classroom_description, catalog, {
      classroomId: schedule.classroom_external_id,
      campusId,
    });
    const classroom = classroomMatch.classroom;
    const building = classroom?.building_id
      ? catalog.buildings.find((item) => item.id === classroom.building_id) ?? null
      : schedule.building_external_id
        ? catalog.buildings.find((item) => item.id === schedule.building_external_id) ?? null
      : schedule.building_description
        ? catalog.buildings.find((item) => {
            if (campusId && item.campus_id !== campusId) {
              return false;
            }
            return buildingCandidates.some((candidate) => normalizeLoose(item.name) === normalizeLoose(candidate));
          }) ?? null
        : null;
    const classroomByName =
      classroom ??
      this.findCatalogClassroomMatch(schedule.classroom_description, catalog, {
        campusId,
        buildingId: building?.id ?? null,
        allowGlobalUniqueFallback: true,
      }).classroom;
    const resolvedBuilding = classroomByName?.building_id
      ? catalog.buildings.find((item) => item.id === classroomByName.building_id) ?? null
      : building;
    const buildingMatchSource =
      classroomByName?.building_id && resolvedBuilding
        ? classroomMatch.classroom === classroomByName
          ? classroomMatch.match_source
          : 'heuristic'
        : resolvedBuilding
          ? 'catalog'
          : 'none';
    const classroomMatchSource =
      classroomMatch.classroom === classroomByName && classroomByName
        ? classroomMatch.match_source
        : classroomByName
          ? 'heuristic'
          : 'none';

    if (!classroomByName && schedule.classroom_description) {
      const locationLabel = [
        schedule.classroom_description ? `aula ${schedule.classroom_description}` : null,
        schedule.building_description ? `pabellon ${schedule.building_description}` : null,
      ]
        .filter(Boolean)
        .join(', ');
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_CLASSROOM',
        field_name: 'AULA',
        message: locationLabel
          ? `No se pudo resolver el ${locationLabel} de Akademic; se importara en null.`
          : 'No se pudo resolver el aula de Akademic; se importara en null.',
        meta_json: {
          namespace: 'classroom',
          source_value: schedule.classroom_description,
          building_source_value: schedule.building_description ?? null,
        },
      });
    }

    return {
      building: toResolution(
        schedule.building_description ?? resolvedBuilding?.name ?? '',
        resolvedBuilding?.id ?? null,
        resolvedBuilding?.name ?? null,
        buildingMatchSource,
      ),
      classroom: toResolution(
        schedule.classroom_description ?? classroomByName?.name ?? '',
        classroomByName?.id ?? null,
        classroomByName?.name ?? classroomByName?.code ?? null,
        classroomMatchSource,
      ),
    };
  }

  private findCatalogClassroomMatch(
    value: string | null | undefined,
    catalog: ImportCatalog,
    options?: {
      classroomId?: string | null;
      campusId?: string | null;
      buildingId?: string | null;
      allowGlobalUniqueFallback?: boolean;
    },
  ): { classroom: ClassroomEntity | null; match_source: MappingResolution['match_source'] } {
    const classroomId = options?.classroomId ?? null;
    if (classroomId) {
      const byId = catalog.classrooms.find((item) => item.id === classroomId) ?? null;
      if (byId) {
        return { classroom: byId, match_source: 'catalog' };
      }
    }

    const normalizedCandidates = buildLocationCandidateLabels(value).map((item) => normalizeLoose(item)).filter(Boolean);
    if (!normalizedCandidates.length) {
      return { classroom: null, match_source: 'none' };
    }

    const matchesCandidate = (item: ClassroomEntity) =>
      normalizedCandidates.includes(normalizeLoose(item.code)) ||
      normalizedCandidates.includes(normalizeLoose(item.name));

    const scopedMatches = catalog.classrooms.filter((item) => {
      if (options?.campusId && item.campus_id !== options.campusId) {
        return false;
      }
      if (options?.buildingId && item.building_id && item.building_id !== options.buildingId) {
        return false;
      }
      return matchesCandidate(item);
    });
    if (scopedMatches.length > 0) {
      return { classroom: scopedMatches[0] ?? null, match_source: 'catalog' };
    }

    if (!options?.allowGlobalUniqueFallback) {
      return { classroom: null, match_source: 'none' };
    }

    const globalMatches = catalog.classrooms.filter(matchesCandidate);
    if (globalMatches.length === 1) {
      return { classroom: globalMatches[0] ?? null, match_source: 'heuristic' };
    }

    return { classroom: null, match_source: 'none' };
  }

  private normalizeAkademicCourseType(value: string | null | undefined) {
    const normalized = normalizeLoose(value);
    if (normalized === 'PRACTICO' || normalized === 'PRACTICA') {
      return 'PRACTICO';
    }
    if (normalized === 'TEORICO' || normalized === 'TEORIA') {
      return 'TEORICO';
    }
    return 'TEORICO_PRACTICO';
  }

  private deriveAkademicSubsectionKindFromSchedules(
    courseType: string,
    schedules: AkademicScheduleRow[],
  ): PlanningSubsectionKind {
    const hasTheory = schedules.some((item) => item.session_type === 'THEORY');
    const hasPractice = schedules.some((item) => item.session_type === 'PRACTICE' || item.session_type === 'LAB');
    if (hasTheory && hasPractice) {
      return 'MIXED';
    }
    if (hasPractice) {
      return 'PRACTICE';
    }
    if (hasTheory) {
      return 'THEORY';
    }
    return courseType === 'PRACTICO' ? 'PRACTICE' : courseType === 'TEORICO' ? 'THEORY' : 'MIXED';
  }

  private groupAkademicSubsections(
    sectionCode: string,
    schedules: AkademicScheduleRow[],
    courseType: string,
  ): Array<{
    import_subsection_code: string;
    subsection_kind: PlanningSubsectionKind;
    schedules: AkademicScheduleRow[];
    warning: string | null;
  }> {
    const sortedSchedules = [...schedules].sort((left, right) =>
      `${left.day_of_week}|${left.start_time}|${left.end_time}`.localeCompare(
        `${right.day_of_week}|${right.start_time}|${right.end_time}`,
      ),
    );
    const normalizedSchedules = this.applyAkademicScheduleHeuristics(sortedSchedules);
    if (normalizedSchedules.length <= 1) {
      return [
        {
          import_subsection_code: sectionCode,
          subsection_kind: this.deriveAkademicSubsectionKindFromSchedules(
            courseType,
            normalizedSchedules,
          ),
          schedules: normalizedSchedules,
          warning: null,
        },
      ];
    }

    return normalizedSchedules.map((schedule, index) => ({
      import_subsection_code: index === 0 ? sectionCode : `${sectionCode}${index}`,
      subsection_kind: this.deriveAkademicSubsectionKindFromSchedules(courseType, [schedule]),
      schedules: [schedule],
      warning: null,
    }));
  }

  private resolveAkademicSectionVacancies(
    section: AkademicSectionRow,
    detail: Record<string, unknown> | null,
  ) {
    const detailRecord = detail ?? {};
    const detailVacancies =
      [
        asNullableInt(pick(detailRecord, 'vacancies')),
        asNullableInt(pick(detailRecord, 'availableVacancies')),
        asNullableInt(pick(detailRecord, 'quota')),
        asNullableInt(pick(detailRecord, 'totalVacancies')),
        asNullableInt(pick(detailRecord, 'vacantSeats')),
        asNullableInt(pick(detailRecord, 'section.vacancies')),
        asNullableInt(pick(detailRecord, 'section.availableVacancies')),
        asNullableInt(pick(detailRecord, 'courseTerm.vacancies')),
        asNullableInt(pick(detailRecord, 'detail.vacancies')),
        asNullableInt(pick(detailRecord, 'data.vacancies')),
      ].find((value) => value !== null && value !== undefined) ?? null;
    return detailVacancies ?? section.vacancies ?? null;
  }

  private buildAkademicSectionSourcePayload(
    sectionRaw: Record<string, unknown> | null | undefined,
    detail: Record<string, unknown> | null,
    vacancies: number | null,
  ) {
    return {
      ...(sectionRaw ?? {}),
      ...(detail ?? {}),
      vacancies,
    };
  }

  private applyAkademicScheduleHeuristics(
    schedules: AkademicScheduleRow[],
  ): AkademicScheduleRow[] {
    if (!schedules.length) {
      return [];
    }
    const knownTypes = schedules.filter((item) => item.session_type !== 'OTHER').length;
    if (knownTypes > 0) {
      return schedules;
    }
    return schedules.map((item, index) => ({
      ...item,
      session_type:
        schedules.length === 1
          ? 'THEORY'
          : index === 0
            ? 'THEORY'
            : 'PRACTICE',
    }));
  }

  private computeAkademicAssignedHours(schedules: AkademicScheduleRow[]) {
    const theoretical = roundToTwo(
      schedules
        .filter((item) => item.session_type === 'THEORY')
        .reduce((sum, item) => sum + item.academic_hours, 0),
    );
    const practical = roundToTwo(
      schedules
        .filter((item) => item.session_type === 'PRACTICE' || item.session_type === 'LAB')
        .reduce((sum, item) => sum + item.academic_hours, 0),
    );
    const total = roundToTwo(schedules.reduce((sum, item) => sum + item.academic_hours, 0));
    return {
      offer_theoretical_hours: theoretical,
      offer_practical_hours: practical,
      offer_total_hours: total,
      assigned_theoretical_hours: theoretical,
      assigned_practical_hours: practical,
      assigned_total_hours: total,
    };
  }

  private async updatePreviewBatchProgress(
    batch: PlanningImportBatchEntity,
    percent: number,
    message: string,
    meta: Record<string, unknown> = {},
  ) {
    batch.preview_progress_json = this.buildPreviewProgress(message, percent, meta);
    batch.updated_at = new Date();
    await this.importBatchesRepo.save(batch);
  }

  private buildPreviewProgress(
    message: string,
    percent: number,
    meta: Record<string, unknown> = {},
  ) {
    return {
      ...meta,
      percent: Math.max(0, Math.min(100, Math.round(percent))),
      message,
      updated_at: new Date().toISOString(),
    };
  }

  private async updateExecutionBatchProgress(
    batch: PlanningImportBatchEntity,
    summary: Record<string, unknown>,
    percent: number,
    message: string,
    meta: Record<string, unknown> = {},
  ) {
    const nextSummary = this.buildExecutionProgress(message, percent, {
      ...summary,
      ...meta,
    });
    batch.execution_summary_json = nextSummary;
    batch.updated_at = new Date();
    await this.importBatchesRepo.save(batch);
    return nextSummary;
  }

  private buildExecutionProgress(
    message: string,
    percent: number,
    meta: Record<string, unknown> = {},
  ) {
    return {
      ...meta,
      percent: Math.max(0, Math.min(100, Math.round(percent))),
      message,
      updated_at: new Date().toISOString(),
    };
  }

  private buildExecutionScopeLabel(scope: ImportScope | null) {
    if (!scope) {
      return 'grupo sin contexto';
    }
    const parts = [
      scope.campus_name,
      scope.academic_program_name,
      scope.study_plan_year ? `Plan ${scope.study_plan_year}` : null,
      scope.cycle ? `Ciclo ${scope.cycle}` : null,
    ]
      .map((item) => String(item ?? '').trim())
      .filter(Boolean);
    return parts.length ? parts.join(' | ') : 'grupo sin contexto';
  }

  async listAliasMappings(namespace?: string, search?: string) {
    const mappings = await this.importAliasMappingsRepo.find({
      where: {
        ...(namespace ? { namespace } : {}),
      },
      order: { namespace: 'ASC', source_value: 'ASC' },
    });

    const normalizedSearch = normalizeLoose(search);
    return mappings.filter((item) => {
      if (!normalizedSearch) {
        return true;
      }
      return (
        normalizeLoose(item.source_value).includes(normalizedSearch) ||
        normalizeLoose(item.target_label).includes(normalizedSearch) ||
        normalizeLoose(item.target_id).includes(normalizedSearch)
      );
    });
  }

  async createAliasMapping(dto: CreatePlanningImportAliasDto) {
    const namespace = normalizeAliasNamespace(dto.namespace);
    const sourceValue = normalizeSourceValue(dto.source_value);
    if (!namespace || !sourceValue) {
      throw new BadRequestException('Debes indicar namespace y source_value validos.');
    }
    const existing = await this.importAliasMappingsRepo.findOne({
      where: {
        namespace,
        source_value: sourceValue,
      },
    });
    if (existing) {
      throw new BadRequestException('Ya existe un alias para ese namespace y valor origen.');
    }
    const now = new Date();
    const entity = this.importAliasMappingsRepo.create({
      id: newId(),
      namespace,
      source_value: sourceValue,
      target_id: dto.target_id.trim(),
      target_label: emptyToNull(dto.target_label) ?? dto.target_id.trim(),
      is_active: dto.is_active ?? true,
      notes: emptyToNull(dto.notes),
      created_at: now,
      updated_at: now,
    });
    return this.importAliasMappingsRepo.save(entity);
  }

  async updateAliasMapping(id: string, dto: UpdatePlanningImportAliasDto) {
    const current = await this.importAliasMappingsRepo.findOne({ where: { id } });
    if (!current) {
      throw new NotFoundException(`No existe un alias ${id}.`);
    }
    const next = this.importAliasMappingsRepo.create({
      ...current,
      target_id: dto.target_id?.trim() ?? current.target_id,
      target_label:
        dto.target_label !== undefined ? emptyToNull(dto.target_label) : current.target_label,
      is_active: dto.is_active ?? current.is_active,
      notes: dto.notes !== undefined ? emptyToNull(dto.notes) : current.notes,
      updated_at: new Date(),
    });
    return this.importAliasMappingsRepo.save(next);
  }

  async getAliasCatalog() {
    const [vcPeriods, campuses, faculties, programs, studyPlans, studyPlanCourses, studyPlanCourseDetails, courseModalities, buildings, classrooms] =
      await Promise.all([
        this.vcPeriodsRepo.find({ where: { is_active: true }, order: { text: 'DESC' } }),
        this.campusesRepo.find({ order: { name: 'ASC' } }),
        this.facultiesRepo.find({ order: { name: 'ASC' } }),
        this.programsRepo.find({ order: { name: 'ASC' } }),
        this.studyPlansRepo.find({ order: { career: 'ASC', year: 'ASC' } }),
        this.studyPlanCoursesRepo.find({ order: { course_code: 'ASC', course_name: 'ASC' } }),
        this.studyPlanCourseDetailsRepo.find(),
        this.courseModalitiesRepo.find({ where: { is_active: true }, order: { name: 'ASC' } }),
        this.buildingsRepo.find({ order: { name: 'ASC' } }),
        this.classroomsRepo.find({ order: { name: 'ASC' } }),
      ]);

    const campusById = new Map(campuses.map((item) => [item.id, item] as const));
    const buildingById = new Map(buildings.map((item) => [item.id, item] as const));
    const studyPlanById = new Map(studyPlans.map((item) => [item.id, item] as const));
    const studyPlanCourseDetailById = new Map(
      studyPlanCourseDetails.map((item) => [item.study_plan_course_id, item] as const),
    );

    return {
      namespaces: [
        'vc_period',
        'campus',
        'faculty_code',
        'academic_program_code',
        'study_plan_code',
        'course_code',
        'course_modality',
        'shift',
        'building',
        'classroom',
        'laboratory',
      ],
      shift_options: SHIFT_OPTIONS.map((value) => ({ id: value, label: value })),
      vc_periods: vcPeriods.map((item) => ({
        id: item.id,
        label: item.text || item.id,
      })),
      campuses: campuses.map((item) => ({
        id: item.id,
        label: [item.code, item.name].filter(Boolean).join(' - '),
      })),
      faculties: faculties.map((item) => ({
        id: item.id,
        label: [item.code, item.name].filter(Boolean).join(' - '),
      })),
      academic_programs: programs.map((item) => ({
        id: item.id,
        label: [item.code, item.name].filter(Boolean).join(' - '),
      })),
      study_plans: studyPlans.map((item) => ({
        id: item.id,
        academic_program_id: item.academic_program_id ?? null,
        label: [item.year, item.career, item.name].filter(Boolean).join(' - '),
      })),
      course_targets: studyPlanCourses
        .map((item) => {
          const plan = studyPlanById.get(item.study_plan_id) ?? null;
          const detail = studyPlanCourseDetailById.get(item.id) ?? null;
          const cycle = detail?.academic_year ?? extractCycleFromStudyPlanCourse(item) ?? null;
          return {
            id: item.id,
            study_plan_id: item.study_plan_id,
            academic_program_id: plan?.academic_program_id ?? null,
            cycle,
            label: [
              detail?.short_code ?? item.course_code,
              detail?.name ?? item.course_name,
              plan?.career ?? plan?.academic_program,
              plan?.year,
              cycle ? `Ciclo ${cycle}` : null,
            ]
              .filter(Boolean)
              .join(' - '),
          };
        })
        .sort((left, right) => left.label.localeCompare(right.label)),
      course_modalities: courseModalities.map((item) => ({
        id: item.id,
        label: item.name ?? item.code ?? item.id,
      })),
      buildings: buildings.map((item) => {
        const campus = item.campus_id ? campusById.get(item.campus_id) ?? null : null;
        return {
          id: item.id,
          campus_id: item.campus_id ?? null,
          campus_label: campus ? [campus.code, campus.name].filter(Boolean).join(' - ') : null,
          label: [item.name, campus?.name].filter(Boolean).join(' - ') || item.id,
        };
      }),
      classrooms: classrooms.map((item) => {
        const campus = item.campus_id ? campusById.get(item.campus_id) ?? null : null;
        const building = item.building_id ? buildingById.get(item.building_id) ?? null : null;
        return {
          id: item.id,
          campus_id: item.campus_id ?? null,
          building_id: item.building_id ?? null,
          campus_label: campus ? [campus.code, campus.name].filter(Boolean).join(' - ') : null,
          building_label: building?.name ?? null,
          label: [item.code, item.name, building?.name, campus?.name].filter(Boolean).join(' - ') || item.id,
        };
      }),
    };
  }

  // ---------------------------------------------------------------------------
  // mergeAkademicScopeRows
  // ---------------------------------------------------------------------------
  // Akademic-specific upsert path: instead of deleting everything and recreating
  // from scratch (which orphans videoconferences), this method:
  //   1. Loads existing offers/sections/subsections/schedules for the scope.
  //   2. For each incoming entity, finds a match by stable key and UPDATES it
  //      in-place (keeping the same DB id), so existing videoconferences that
  //      reference planning_subsection_schedule_id remain valid.
  //   3. Creates brand-new records for entities that did not exist before.
  //   4. Deletes stale records that no longer appear in Akademic. Before
  //      deleting stale schedules it also deletes the associated videoconferences
  //      and overrides (clean break instead of silent orphan).
  // ---------------------------------------------------------------------------
  private async mergeAkademicScopeRows(
    batchId: string,
    scope: ImportScope,
    rows: PlanningImportRowEntity[],
    catalog: ImportCatalog,
    caches: ImportExecutionCaches,
    actor?: ImportActor | null,
  ): Promise<{ plan_rules: number; offers: number; sections: number; subsections: number; schedules: number }> {
    const now = new Date();

    // ── 1. Resolve VC context (identical to importRowsForScope) ───────────────
    const rowsByOffer = groupBy(rows, (row) => {
      const resolution = asRecord(row.resolution_json);
      return `${recordString(resolution, 'study_plan_course_id')}::${recordString(resolution, 'offer_course_code')}`;
    });
    const directScopeVcFacultyId = firstNonEmpty(
      rows.map((row) => recordString(asRecord(row.resolution_json), 'source_vc_faculty_id')),
    );
    const directScopeVcAcademicProgramId = firstNonEmpty(
      rows.map((row) => recordString(asRecord(row.resolution_json), 'source_vc_academic_program_id')),
    );
    const firstScopeSource = rows
      .map((row) => asRecord(row.source_json))
      .find((item) => Object.keys(item).length > 0);
    const scopeCourseSource =
      asRecord(recordValue(firstScopeSource ?? {}, 'course')) ?? firstScopeSource ?? null;
    const scopeDetailSource = asRecord(recordValue(firstScopeSource ?? {}, 'detail')) ?? null;
    const sourceScopeVcContext = await this.resolveVcContextFromSourceInput(
      {
        source_vc_faculty_id: directScopeVcFacultyId,
        source_vc_academic_program_id: directScopeVcAcademicProgramId,
        faculty_name: extractAkademicFacultyNameFromSource(scopeCourseSource, scopeDetailSource),
        academic_program_name: extractAkademicProgramNameFromSource(scopeCourseSource, scopeDetailSource),
      },
      caches,
    );
    const effectiveVcFacultyId = sourceScopeVcContext.vcFacultyId;
    const effectiveVcAcademicProgramId = sourceScopeVcContext.vcAcademicProgramId;
    const sourceVcFacultyIdByOfferKey = new Map<string, string | null>();
    const sourceVcAcademicProgramIdByOfferKey = new Map<string, string | null>();
    const vcFacultyIdByOfferKey = new Map<string, string | null>();
    const vcAcademicProgramIdByOfferKey = new Map<string, string | null>();
    const vcCourseIdByOfferKey = new Map<string, string | null>();

    for (const [offerKey, offerRows] of rowsByOffer.entries()) {
      const representative = offerRows.map((row) => asRecord(row.resolution_json)).find(Boolean);
      if (!representative) {
        sourceVcFacultyIdByOfferKey.set(offerKey, null);
        sourceVcAcademicProgramIdByOfferKey.set(offerKey, null);
        vcFacultyIdByOfferKey.set(offerKey, null);
        vcAcademicProgramIdByOfferKey.set(offerKey, null);
        vcCourseIdByOfferKey.set(offerKey, null);
        continue;
      }
      const firstOfferSource = asRecord(offerRows[0]?.source_json);
      const offerCourseSource =
        asRecord(recordValue(firstOfferSource, 'course')) ?? firstOfferSource ?? null;
      const offerDetailSource = asRecord(recordValue(firstOfferSource, 'detail')) ?? null;
      const sourceVcCourseId = recordString(representative, 'source_vc_course_id');
      const sourceVcCourse = sourceVcCourseId
        ? await this.vcCoursesRepo.findOne({ where: { id: sourceVcCourseId } })
        : null;
      const sourceNamedVcContext = await this.resolveVcContextFromSourceInput(
        {
          source_vc_faculty_id: recordString(representative, 'source_vc_faculty_id'),
          source_vc_academic_program_id: recordString(representative, 'source_vc_academic_program_id'),
          faculty_name: extractAkademicFacultyNameFromSource(offerCourseSource, offerDetailSource),
          academic_program_name: extractAkademicProgramNameFromSource(offerCourseSource, offerDetailSource),
        },
        caches,
      );
      const sourceResolvedVcAcademicProgramId =
        sourceVcCourse?.program_id ?? sourceNamedVcContext.vcAcademicProgramId ?? null;
      const sourceVcAcademicProgram = sourceResolvedVcAcademicProgramId
        ? await this.vcAcademicProgramsRepo.findOne({ where: { id: sourceResolvedVcAcademicProgramId } })
        : null;
      const sourceResolvedVcFacultyId =
        sourceVcAcademicProgram?.faculty_id ?? sourceNamedVcContext.vcFacultyId ?? null;
      sourceVcFacultyIdByOfferKey.set(offerKey, sourceResolvedVcFacultyId);
      sourceVcAcademicProgramIdByOfferKey.set(offerKey, sourceResolvedVcAcademicProgramId);
      const vcCourseId =
        sourceVcCourse?.id ??
        (await this.resolveVcCourseIdForImport(
          {
            course_code: recordString(representative, 'offer_course_code'),
            course_name: recordString(representative, 'offer_course_name'),
            study_plan_year: scope.study_plan_year,
            vc_academic_program_id: sourceResolvedVcAcademicProgramId ?? null,
          },
          caches,
        ));
      vcFacultyIdByOfferKey.set(offerKey, sourceResolvedVcFacultyId);
      vcAcademicProgramIdByOfferKey.set(offerKey, sourceResolvedVcAcademicProgramId);
      vcCourseIdByOfferKey.set(offerKey, vcCourseId);
    }

    // ── 2. Load existing DB state for this scope ──────────────────────────────
    const scopeWhere = {
      semester_id: scope.semester_id,
      campus_id: scope.campus_id,
      ...(scope.vc_period_id ? { vc_period_id: scope.vc_period_id } : {}),
      faculty_id: scope.faculty_id,
      academic_program_id: scope.academic_program_id,
      study_plan_id: scope.study_plan_id,
      cycle: scope.cycle,
    };
    const [existingRules, existingOffers] = await Promise.all([
      this.planRulesRepo.find({ where: scopeWhere }),
      this.offersRepo.find({ where: scopeWhere }),
    ]);
    const existingOfferByCourseId = new Map(existingOffers.map((o) => [o.study_plan_course_id, o]));
    const existingOfferIds = existingOffers.map((o) => o.id);

    const existingSections = existingOfferIds.length
      ? await this.sectionsRepo.find({ where: { planning_offer_id: In(existingOfferIds) } })
      : [];
    // key = `${offerId}::${sectionCode}`
    const existingSectionByKey = new Map(existingSections.map((s) => [`${s.planning_offer_id}::${s.code}`, s]));
    // key = `${offerId}::${sourceSectionId}` (preferred for stable matching when code/name changes)
    const existingSectionBySourceId = new Map<string, PlanningSectionEntity>();
    for (const section of existingSections) {
      if (!section.source_section_id) {
        continue;
      }
      existingSectionBySourceId.set(
        `${section.planning_offer_id}::${section.source_section_id}`,
        section,
      );
    }
    const existingSectionIds = existingSections.map((s) => s.id);

    const existingSubsections = existingSectionIds.length
      ? await this.subsectionsRepo.find({ where: { planning_section_id: In(existingSectionIds) } })
      : [];
    // key = `${sectionId}::${subsectionCode}`
    const existingSubsectionByKey = new Map(existingSubsections.map((sub) => [`${sub.planning_section_id}::${sub.code}`, sub]));
    const existingSubsectionIds = existingSubsections.map((sub) => sub.id);

    const existingSchedules = existingSubsectionIds.length
      ? await this.schedulesRepo.find({ where: { planning_subsection_id: In(existingSubsectionIds) } })
      : [];
    // Two lookup strategies for schedule matching
    const existingScheduleBySourceId = new Map<string, PlanningSubsectionScheduleEntity>();
    const existingScheduleBySlot = new Map<string, PlanningSubsectionScheduleEntity>();
    for (const sch of existingSchedules) {
      if (sch.source_schedule_id) {
        existingScheduleBySourceId.set(`${sch.planning_subsection_id}::${sch.source_schedule_id}`, sch);
      }
      existingScheduleBySlot.set(
        `${sch.planning_subsection_id}::${sch.day_of_week}::${sch.start_time}::${sch.end_time}`,
        sch,
      );
    }

    // ── 3. Track which existing IDs are still valid ───────────────────────────
    const touchedOfferIds = new Set<string>();
    const touchedSectionIds = new Set<string>();
    const touchedSubsectionIds = new Set<string>();
    const touchedScheduleIds = new Set<string>();

    // ── 4. Build upsert collections ───────────────────────────────────────────
    const result = { plan_rules: 0, offers: 0, sections: 0, subsections: 0, schedules: 0 };
    const changeLogs: Partial<PlanningChangeLogEntity>[] = [];
    const offersToSave: PlanningOfferEntity[] = [];
    const sectionsToSave: PlanningSectionEntity[] = [];
    const subsectionsToSave: PlanningSubsectionEntity[] = [];
    const schedulesToSave: PlanningSubsectionScheduleEntity[] = [];

    // Plan rule: update existing or create new
    const existingPlanRule = existingRules[0] ?? null;
    let planRule: PlanningCyclePlanRuleEntity;
    if (existingPlanRule) {
      existingPlanRule.vc_faculty_id = effectiveVcFacultyId;
      existingPlanRule.vc_academic_program_id = effectiveVcAcademicProgramId;
      existingPlanRule.updated_at = now;
      planRule = existingPlanRule;
    } else {
      planRule = this.planRulesRepo.create({
        id: newId(),
        semester_id: scope.semester_id,
        vc_period_id: scope.vc_period_id,
        campus_id: scope.campus_id,
        academic_program_id: scope.academic_program_id,
        faculty_id: scope.faculty_id,
        career_name: scope.academic_program_name ?? scope.study_plan_name ?? null,
        cycle: scope.cycle,
        study_plan_id: scope.study_plan_id,
        vc_faculty_id: effectiveVcFacultyId,
        vc_academic_program_id: effectiveVcAcademicProgramId,
        is_active: true,
        workflow_status: 'DRAFT',
        submitted_at: null,
        submitted_by_user_id: null,
        submitted_by: null,
        reviewed_at: null,
        reviewed_by_user_id: null,
        reviewed_by: null,
        review_comment: null,
        created_at: now,
        updated_at: now,
      });
      result.plan_rules += 1;
      changeLogs.push(
        this.buildChangeLogEntry(
          'planning_cycle_plan_rule',
          planRule.id,
          'CREATE',
          null,
          planRule,
          this.buildPlanRuleLogContext(planRule),
          actor,
        ),
      );
    }

    // Offers, sections, subsections, schedules
    for (const offerRows of rowsByOffer.values()) {
      const representative = offerRows.map((row) => asRecord(row.resolution_json)).find(Boolean);
      if (!representative) continue;

      const offerKey = `${recordString(representative, 'study_plan_course_id')}::${recordString(representative, 'offer_course_code')}`;
      const offerResolutions = offerRows.map((row) => asRecord(row.resolution_json));
      const sourceVcCourseId = recordString(representative, 'source_vc_course_id');
      const sourceResolvedVcFacultyId = sourceVcFacultyIdByOfferKey.get(offerKey) ?? null;
      const sourceResolvedVcAcademicProgramId = sourceVcAcademicProgramIdByOfferKey.get(offerKey) ?? null;
      const offerVcFacultyId = vcFacultyIdByOfferKey.get(offerKey) ?? effectiveVcFacultyId;
      const offerVcAcademicProgramId = vcAcademicProgramIdByOfferKey.get(offerKey) ?? effectiveVcAcademicProgramId;
      const resolvedVcCourseId = vcCourseIdByOfferKey.get(offerKey) ?? null;
      const sourceSystem = (firstNonEmpty(offerResolutions.map((row) => recordString(row, 'source_system'))) as PlanningSourceSystem | null) ?? 'AKADEMIC';
      const firstOfferSource = asRecord(offerRows[0]?.source_json);
      const offerVcSource =
        sourceResolvedVcFacultyId || sourceResolvedVcAcademicProgramId || sourceVcCourseId
          ? 'sync_source'
          : 'fallback_match';
      const studyPlanCourseId = recordString(representative, 'study_plan_course_id')!;
      const existingOffer = existingOfferByCourseId.get(studyPlanCourseId);

      let offer: PlanningOfferEntity;
      if (existingOffer) {
        // Update in-place — keep same id
        existingOffer.vc_faculty_id = offerVcFacultyId;
        existingOffer.vc_academic_program_id = offerVcAcademicProgramId;
        existingOffer.vc_course_id = resolvedVcCourseId;
        existingOffer.course_name =
          recordString(representative, 'offer_course_name') ?? existingOffer.course_name;
        existingOffer.course_type =
          recordString(representative, 'offer_course_type') ?? existingOffer.course_type;
        existingOffer.theoretical_hours =
          maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_theoretical_hours'))) ??
          existingOffer.theoretical_hours;
        existingOffer.practical_hours =
          maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_practical_hours'))) ??
          existingOffer.practical_hours;
        existingOffer.total_hours =
          maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_total_hours'))) ??
          existingOffer.total_hours;
        existingOffer.last_synced_at = now;
        existingOffer.updated_at = now;
        offer = existingOffer;
      } else {
        offer = this.offersRepo.create({
          id: newId(),
          semester_id: scope.semester_id,
          vc_period_id: scope.vc_period_id,
          campus_id: scope.campus_id,
          faculty_id: scope.faculty_id,
          academic_program_id: scope.academic_program_id,
          study_plan_id: scope.study_plan_id,
          cycle: scope.cycle,
          study_plan_course_id: studyPlanCourseId,
          vc_faculty_id: offerVcFacultyId,
          vc_academic_program_id: offerVcAcademicProgramId,
          vc_course_id: resolvedVcCourseId,
          course_code: recordString(representative, 'offer_course_code'),
          course_name: recordString(representative, 'offer_course_name'),
          study_type_id: catalog.defaultStudyTypeId,
          course_type: recordString(representative, 'offer_course_type') ?? 'TEORICO_PRACTICO',
          source_system: sourceSystem,
          source_course_id: firstNonEmpty(offerResolutions.map((row) => recordString(row, 'source_course_id'))),
          source_term_id: firstNonEmpty(offerResolutions.map((row) => recordString(row, 'source_term_id'))),
          last_synced_at: now,
          source_payload_json: attachVcContextMetadata(
            asRecord(recordValue(firstOfferSource, 'course')) ?? firstOfferSource ?? null,
            {
              vc_source: offerVcSource,
              source_vc_faculty_id: sourceResolvedVcFacultyId,
              source_vc_academic_program_id: sourceResolvedVcAcademicProgramId,
              source_vc_course_id: sourceVcCourseId,
              vc_context_message:
                offerVcSource === 'sync_source'
                  ? 'Contexto AV preservado desde la sincronizacion.'
                  : 'Contexto AV resuelto por nombre/codigo del origen.',
            },
          ),
          theoretical_hours:
            maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_theoretical_hours'))) ?? 0,
          practical_hours:
            maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_practical_hours'))) ?? 0,
          total_hours:
            maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_total_hours'))) ?? 0,
          status: 'DRAFT',
          created_at: now,
          updated_at: now,
        });
        result.offers += 1;
        changeLogs.push(
          this.buildChangeLogEntry(
            'planning_offer',
            offer.id,
            'CREATE',
            null,
            offer,
            this.buildOfferLogContext(offer),
            actor,
          ),
        );
      }
      touchedOfferIds.add(offer.id);
      offersToSave.push(offer);

      const rowsBySection = groupBy(offerRows, (row) => {
        const resolution = asRecord(row.resolution_json);
        return recordString(resolution, 'import_section_code') ?? '__NO_SECTION__';
      });

      for (const [sectionCode, sectionRows] of rowsBySection.entries()) {
        if (sectionCode === '__NO_SECTION__') continue;
        const sectionResolutions = sectionRows.map((row) => asRecord(row.resolution_json));
        const sourceSectionId = firstNonEmpty(
          sectionResolutions.map((row) => recordString(row, 'source_section_id')),
        );
        const sourceVcSectionId = firstNonEmpty(
          sectionResolutions.map((row) => recordString(row, 'source_vc_section_id')),
        );
        const sectionMapKey = `${offer.id}::${sectionCode}`;
        const sectionSourceMapKey = sourceSectionId ? `${offer.id}::${sourceSectionId}` : null;
        const existingSection =
          (sectionSourceMapKey ? existingSectionBySourceId.get(sectionSourceMapKey) : null) ??
          existingSectionByKey.get(sectionMapKey);

        let section: PlanningSectionEntity;
        if (existingSection) {
          // Update in-place — keep same id
          existingSection.code = sectionCode;
          existingSection.external_code =
            firstNonEmpty(sectionResolutions.map((row) => recordString(row, 'external_section_code'))) ??
            existingSection.external_code;
          existingSection.source_section_id =
            sourceSectionId ??
            existingSection.source_section_id;
          existingSection.teacher_id = firstNonEmpty(
            sectionResolutions.map((row) => recordString(row, 'teacher_id')),
          );
          existingSection.course_modality_id = firstNonEmpty(
            sectionResolutions.map((row) => recordString(row, 'course_modality_id')),
          );
          existingSection.projected_vacancies =
            maxNumber(sectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies'))) ??
            existingSection.projected_vacancies;
          existingSection.is_cepea = sectionResolutions.some((row) =>
            Boolean(recordBoolean(row, 'is_cepea')),
          );
          existingSection.has_subsections =
            new Set(
              sectionResolutions
                .map((row) => recordString(row, 'import_subsection_code'))
                .filter(Boolean),
            ).size > 1;
          existingSection.updated_at = now;
          section = existingSection;
        } else {
          section = this.sectionsRepo.create({
            id: newId(),
            planning_offer_id: offer.id,
            code: sectionCode,
            external_code: firstNonEmpty(
              sectionResolutions.map((row) => recordString(row, 'external_section_code')),
            ),
            source_section_id: firstNonEmpty(
              sectionResolutions.map((row) => recordString(row, 'source_section_id')),
            ),
            source_payload_json: attachVcContextMetadata(
              asRecord(recordValue(asRecord(sectionRows[0]?.source_json), 'section')) ??
                asRecord(sectionRows[0]?.source_json) ??
                null,
              {
                vc_source: sourceVcSectionId ? 'sync_source' : 'fallback_match',
                source_vc_section_id: sourceVcSectionId,
                vc_context_message: sourceVcSectionId
                  ? 'Seccion VC preservada desde la sincronizacion.'
                  : 'Seccion VC pendiente de resolver por fallback.',
              },
            ),
            teacher_id: firstNonEmpty(sectionResolutions.map((row) => recordString(row, 'teacher_id'))),
            course_modality_id: firstNonEmpty(
              sectionResolutions.map((row) => recordString(row, 'course_modality_id')),
            ),
            projected_vacancies: maxNumber(
              sectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies')),
            ),
            is_cepea: sectionResolutions.some((row) => Boolean(recordBoolean(row, 'is_cepea'))),
            has_subsections:
              new Set(
                sectionResolutions
                  .map((row) => recordString(row, 'import_subsection_code'))
                  .filter(Boolean),
              ).size > 1,
            default_theoretical_hours: offer.theoretical_hours,
            default_practical_hours: offer.practical_hours,
            default_virtual_hours: 0,
            default_seminar_hours: 0,
            default_total_hours: offer.total_hours,
            status: 'DRAFT',
            created_at: now,
            updated_at: now,
          });
          result.sections += 1;
          changeLogs.push(
            this.buildChangeLogEntry(
              'planning_section',
              section.id,
              'CREATE',
              null,
              section,
              this.buildSectionLogContext(section, offer),
              actor,
            ),
          );
        }
        touchedSectionIds.add(section.id);
        sectionsToSave.push(section);

        const rowsBySubsection = groupBy(sectionRows, (row) => {
          const resolution = asRecord(row.resolution_json);
          return recordString(resolution, 'import_subsection_code') ?? '__NO_SUBSECTION__';
        });

        for (const [subsectionCode, subsectionRows] of rowsBySubsection.entries()) {
          if (subsectionCode === '__NO_SUBSECTION__') continue;
          const subsectionResolutions = subsectionRows.map((row) => asRecord(row.resolution_json));
          const assignedTheoretical =
            maxNumber(subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_theoretical_hours'))) ?? 0;
          const assignedPractical =
            maxNumber(subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_practical_hours'))) ?? 0;
          const assignedTotal =
            maxNumber(subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_total_hours'))) ??
            assignedTheoretical + assignedPractical;
          const schedulePayloads = subsectionResolutions
            .map((row) => asRecord(recordValue(row, 'schedule')))
            .filter((item) => Object.keys(item).length > 0);
          const uniqueScheduleTeacherIds = uniqueIds(schedulePayloads.map((row) => recordString(row, 'teacher_id')));
          const uniqueScheduleBuildingIds = uniqueIds(schedulePayloads.map((row) => recordString(row, 'building_id')));
          const uniqueScheduleClassroomIds = uniqueIds(
            schedulePayloads.map((row) => recordString(row, 'classroom_id')),
          );
          const subsectionKind = (
            firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'subsection_kind'))) ??
            deriveSubsectionKind(assignedTheoretical, assignedPractical)
          ) as PlanningSubsectionKind;

          const subsectionMapKey = `${section.id}::${subsectionCode}`;
          const existingSubsection = existingSubsectionByKey.get(subsectionMapKey);

          let subsection: PlanningSubsectionEntity;
          if (existingSubsection) {
            // Update in-place — keep same id
            existingSubsection.kind = subsectionKind;
            existingSubsection.responsible_teacher_id =
              uniqueScheduleTeacherIds.length === 1
                ? uniqueScheduleTeacherIds[0]
                : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'teacher_id')));
            existingSubsection.course_modality_id = firstNonEmpty(
              subsectionResolutions.map((row) => recordString(row, 'course_modality_id')),
            );
            existingSubsection.building_id =
              uniqueScheduleBuildingIds.length === 1
                ? uniqueScheduleBuildingIds[0]
                : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'building_id')));
            existingSubsection.classroom_id =
              uniqueScheduleClassroomIds.length === 1
                ? uniqueScheduleClassroomIds[0]
                : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'classroom_id')));
            existingSubsection.capacity_snapshot =
              maxNumber(subsectionResolutions.map((row) => recordNumberOrNull(row, 'capacity_snapshot'))) ??
              existingSubsection.capacity_snapshot;
            existingSubsection.shift = firstNonEmpty(
              subsectionResolutions.map((row) => recordString(row, 'shift')),
            );
            existingSubsection.projected_vacancies = maxNumber(
              subsectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies')),
            );
            existingSubsection.assigned_theoretical_hours = assignedTheoretical;
            existingSubsection.assigned_practical_hours = assignedPractical;
            existingSubsection.assigned_total_hours = assignedTotal;
            existingSubsection.vc_section_id =
              firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'source_vc_section_id'))) ??
              existingSubsection.vc_section_id;
            existingSubsection.updated_at = now;
            subsection = existingSubsection;
          } else {
            subsection = this.subsectionsRepo.create({
              id: newId(),
              planning_section_id: section.id,
              code: subsectionCode,
              kind: subsectionKind,
              responsible_teacher_id:
                uniqueScheduleTeacherIds.length === 1
                  ? uniqueScheduleTeacherIds[0]
                  : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'teacher_id'))),
              course_modality_id: firstNonEmpty(
                subsectionResolutions.map((row) => recordString(row, 'course_modality_id')),
              ),
              building_id:
                uniqueScheduleBuildingIds.length === 1
                  ? uniqueScheduleBuildingIds[0]
                  : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'building_id'))),
              classroom_id:
                uniqueScheduleClassroomIds.length === 1
                  ? uniqueScheduleClassroomIds[0]
                  : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'classroom_id'))),
              capacity_snapshot: maxNumber(
                subsectionResolutions.map((row) => recordNumberOrNull(row, 'capacity_snapshot')),
              ),
              shift: firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'shift'))),
              projected_vacancies: maxNumber(
                subsectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies')),
              ),
              course_type: offer.course_type,
              assigned_theoretical_hours: assignedTheoretical,
              assigned_practical_hours: assignedPractical,
              assigned_virtual_hours: 0,
              assigned_seminar_hours: 0,
              assigned_total_hours: assignedTotal,
              denomination:
                firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'denomination'))) ??
                buildDenomination(
                  offer.course_code,
                  offer.course_name,
                  section.code,
                  subsectionCode,
                  offer.campus_id,
                ),
              vc_section_id:
                firstNonEmpty(
                  subsectionResolutions.map((row) => recordString(row, 'source_vc_section_id')),
                ) ?? null,
              status: 'DRAFT',
              created_at: now,
              updated_at: now,
            });
            result.subsections += 1;
            changeLogs.push(
              this.buildChangeLogEntry(
                'planning_subsection',
                subsection.id,
                'CREATE',
                null,
                subsection,
                this.buildSubsectionLogContext(subsection, section, offer),
                actor,
              ),
            );
          }
          touchedSubsectionIds.add(subsection.id);
          subsectionsToSave.push(subsection);

          const uniqueSchedules = dedupeImportedSchedules(schedulePayloads);
          for (const selectedSchedule of uniqueSchedules) {
            const sourceScheduleId = recordString(selectedSchedule, 'source_schedule_id');
            const dayOfWeek = recordString(selectedSchedule, 'day_of_week') as (typeof DayOfWeekValues)[number];
            const startTime = recordString(selectedSchedule, 'start_time')!;
            const endTime = recordString(selectedSchedule, 'end_time')!;

            // Match by source_schedule_id first (reliable), then by time-slot (fallback)
            const existingSchedule =
              (sourceScheduleId
                ? existingScheduleBySourceId.get(`${subsection.id}::${sourceScheduleId}`)
                : null) ??
              existingScheduleBySlot.get(
                `${subsection.id}::${dayOfWeek}::${startTime}::${endTime}`,
              ) ??
              null;

            let schedule: PlanningSubsectionScheduleEntity;
            if (existingSchedule) {
              // Update in-place — SAME ID → existing videoconferences remain valid!
              existingSchedule.planning_subsection_id = subsection.id;
              existingSchedule.day_of_week = dayOfWeek;
              existingSchedule.start_time = startTime;
              existingSchedule.end_time = endTime;
              existingSchedule.session_type =
                (recordString(selectedSchedule, 'session_type') as PlanningSessionType | null) ?? 'OTHER';
              existingSchedule.source_session_type_code = recordString(
                selectedSchedule,
                'source_session_type_code',
              );
              existingSchedule.teacher_id = recordString(selectedSchedule, 'teacher_id');
              existingSchedule.building_id = recordString(selectedSchedule, 'building_id');
              existingSchedule.classroom_id = recordString(selectedSchedule, 'classroom_id');
              existingSchedule.source_schedule_id = sourceScheduleId;
              existingSchedule.source_payload_json =
                asRecord(recordValue(selectedSchedule, 'source_payload_json')) ?? null;
              existingSchedule.duration_minutes = recordNumber(selectedSchedule, 'duration_minutes');
              existingSchedule.academic_hours = recordNumber(selectedSchedule, 'academic_hours');
              existingSchedule.updated_at = now;
              schedule = existingSchedule;
            } else {
              schedule = this.schedulesRepo.create({
                id: newId(),
                planning_subsection_id: subsection.id,
                day_of_week: dayOfWeek,
                start_time: startTime,
                end_time: endTime,
                session_type:
                  (recordString(selectedSchedule, 'session_type') as PlanningSessionType | null) ?? 'OTHER',
                source_session_type_code: recordString(selectedSchedule, 'source_session_type_code'),
                teacher_id: recordString(selectedSchedule, 'teacher_id'),
                building_id: recordString(selectedSchedule, 'building_id'),
                classroom_id: recordString(selectedSchedule, 'classroom_id'),
                source_schedule_id: sourceScheduleId,
                source_payload_json:
                  asRecord(recordValue(selectedSchedule, 'source_payload_json')) ?? null,
                duration_minutes: recordNumber(selectedSchedule, 'duration_minutes'),
                academic_hours: recordNumber(selectedSchedule, 'academic_hours'),
                created_at: now,
                updated_at: now,
              });
              result.schedules += 1;
              changeLogs.push(
                this.buildChangeLogEntry(
                  'planning_subsection_schedule',
                  schedule.id,
                  'CREATE',
                  null,
                  schedule,
                  this.buildScheduleLogContext(schedule, subsection, section, offer),
                  actor,
                ),
              );
            }
            touchedScheduleIds.add(schedule.id);
            schedulesToSave.push(schedule);
          }
        }
      }
    }

    // ── 5. Identify stale records (existed before but not in incoming data) ────
    const staleScheduleIds = existingSchedules
      .map((s) => s.id)
      .filter((id) => !touchedScheduleIds.has(id));
    const staleSubsectionIds = existingSubsections
      .map((s) => s.id)
      .filter((id) => !touchedSubsectionIds.has(id));
    const staleSectionIds = existingSections
      .map((s) => s.id)
      .filter((id) => !touchedSectionIds.has(id));
    const staleOfferIds = existingOffers
      .map((o) => o.id)
      .filter((id) => !touchedOfferIds.has(id));
    const staleRuleIds = existingRules
      .filter((r) => r.id !== planRule.id)
      .map((r) => r.id);

    // ── 6. Persist all changes in a single transaction ────────────────────────
    await this.offersRepo.manager.transaction(async (manager) => {
      // Upsert plan rule
      await manager.save(PlanningCyclePlanRuleEntity, planRule);

      // Upsert offers / sections / subsections / schedules
      if (offersToSave.length) {
        await manager.save(PlanningOfferEntity, offersToSave);
      }
      if (sectionsToSave.length) {
        await manager.save(PlanningSectionEntity, sectionsToSave);
      }
      if (subsectionsToSave.length) {
        await manager.save(PlanningSubsectionEntity, subsectionsToSave);
      }
      if (schedulesToSave.length) {
        await manager.save(PlanningSubsectionScheduleEntity, schedulesToSave);
      }

      // Delete stale schedules: clean up videoconferences, overrides and inheritance mappings first
      if (staleScheduleIds.length) {
        await manager.delete(PlanningSubsectionVideoconferenceOverrideEntity, {
          planning_subsection_schedule_id: In(staleScheduleIds),
        });
        await manager.delete(PlanningSubsectionVideoconferenceEntity, {
          planning_subsection_schedule_id: In(staleScheduleIds),
        });
        await manager.delete(PlanningSubsectionScheduleVcInheritanceEntity, {
          parent_schedule_id: In(staleScheduleIds),
        });
        await manager.delete(PlanningSubsectionScheduleVcInheritanceEntity, {
          child_schedule_id: In(staleScheduleIds),
        });
        await manager.delete(PlanningSubsectionScheduleEntity, { id: In(staleScheduleIds) });
      }
      if (staleSubsectionIds.length) {
        await manager.delete(PlanningSubsectionEntity, { id: In(staleSubsectionIds) });
      }
      if (staleSectionIds.length) {
        await manager.delete(PlanningSectionEntity, { id: In(staleSectionIds) });
      }
      if (staleOfferIds.length) {
        await manager.delete(PlanningOfferEntity, { id: In(staleOfferIds) });
      }
      if (staleRuleIds.length) {
        await manager.delete(PlanningCyclePlanRuleEntity, { id: In(staleRuleIds) });
      }

      // Mark rows as imported
      await manager.update(
        PlanningImportRowEntity,
        { batch_id: batchId, id: In(rows.map((row) => row.id)) },
        { imported: true, updated_at: new Date() },
      );
      await this.saveChangeLogsBulk(changeLogs, manager);
    });

    // ── 7. Recalculate VC matches for all (new + updated) offers ─────────────
    for (const offer of offersToSave) {
      await this.planningManualService.recalculateVcMatches(actor as any, { offer_id: offer.id });
    }

    return result;
  }

  private async importRowsForScope(
    batchId: string,
    scope: ImportScope,
    rows: PlanningImportRowEntity[],
    catalog: ImportCatalog,
    caches: ImportExecutionCaches,
    actor?: ImportActor | null,
  ) {
    const rowsByOffer = groupBy(rows, (row) => {
      const resolution = asRecord(row.resolution_json);
      return `${recordString(resolution, 'study_plan_course_id')}::${recordString(resolution, 'offer_course_code')}`;
    });
    const directScopeVcFacultyId = firstNonEmpty(
      rows.map((row) => recordString(asRecord(row.resolution_json), 'source_vc_faculty_id')),
    );
    const directScopeVcAcademicProgramId = firstNonEmpty(
      rows.map((row) => recordString(asRecord(row.resolution_json), 'source_vc_academic_program_id')),
    );
    const firstScopeSource = rows
      .map((row) => asRecord(row.source_json))
      .find((item) => Object.keys(item).length > 0);
    const scopeCourseSource =
      asRecord(recordValue(firstScopeSource ?? {}, 'course')) ?? firstScopeSource ?? null;
    const scopeDetailSource = asRecord(recordValue(firstScopeSource ?? {}, 'detail')) ?? null;
    const sourceScopeVcContext = await this.resolveVcContextFromSourceInput(
      {
        source_vc_faculty_id: directScopeVcFacultyId,
        source_vc_academic_program_id: directScopeVcAcademicProgramId,
        faculty_name: extractAkademicFacultyNameFromSource(scopeCourseSource, scopeDetailSource),
        academic_program_name: extractAkademicProgramNameFromSource(
          scopeCourseSource,
          scopeDetailSource,
        ),
      },
      caches,
    );
    const effectiveVcFacultyId = sourceScopeVcContext.vcFacultyId;
    const effectiveVcAcademicProgramId = sourceScopeVcContext.vcAcademicProgramId;
    const sourceVcFacultyIdByOfferKey = new Map<string, string | null>();
    const sourceVcAcademicProgramIdByOfferKey = new Map<string, string | null>();
    const vcFacultyIdByOfferKey = new Map<string, string | null>();
    const vcAcademicProgramIdByOfferKey = new Map<string, string | null>();
    const vcCourseIdByOfferKey = new Map<string, string | null>();
    for (const [offerKey, offerRows] of rowsByOffer.entries()) {
      const representative = offerRows.map((row) => asRecord(row.resolution_json)).find(Boolean);
      if (!representative) {
        sourceVcFacultyIdByOfferKey.set(offerKey, null);
        sourceVcAcademicProgramIdByOfferKey.set(offerKey, null);
        vcFacultyIdByOfferKey.set(offerKey, null);
        vcAcademicProgramIdByOfferKey.set(offerKey, null);
        vcCourseIdByOfferKey.set(offerKey, null);
        continue;
      }
      const firstOfferSource = asRecord(offerRows[0]?.source_json);
      const offerCourseSource =
        asRecord(recordValue(firstOfferSource, 'course')) ?? firstOfferSource ?? null;
      const offerDetailSource = asRecord(recordValue(firstOfferSource, 'detail')) ?? null;
      const sourceVcCourseId = recordString(representative, 'source_vc_course_id');
      const sourceVcCourse = sourceVcCourseId
        ? await this.vcCoursesRepo.findOne({ where: { id: sourceVcCourseId } })
        : null;
      const sourceNamedVcContext = await this.resolveVcContextFromSourceInput(
        {
          source_vc_faculty_id: recordString(representative, 'source_vc_faculty_id'),
          source_vc_academic_program_id: recordString(representative, 'source_vc_academic_program_id'),
          faculty_name: extractAkademicFacultyNameFromSource(offerCourseSource, offerDetailSource),
          academic_program_name: extractAkademicProgramNameFromSource(
            offerCourseSource,
            offerDetailSource,
          ),
        },
        caches,
      );
      const sourceResolvedVcAcademicProgramId =
        sourceVcCourse?.program_id ??
        sourceNamedVcContext.vcAcademicProgramId ??
        null;
      const sourceVcAcademicProgram = sourceResolvedVcAcademicProgramId
        ? await this.vcAcademicProgramsRepo.findOne({ where: { id: sourceResolvedVcAcademicProgramId } })
        : null;
      const sourceResolvedVcFacultyId =
        sourceVcAcademicProgram?.faculty_id ??
        sourceNamedVcContext.vcFacultyId ??
        null;
      const offerVcAcademicProgramId =
        sourceResolvedVcAcademicProgramId ?? null;
      const offerVcFacultyId = sourceResolvedVcFacultyId ?? null;
      sourceVcFacultyIdByOfferKey.set(offerKey, sourceResolvedVcFacultyId);
      sourceVcAcademicProgramIdByOfferKey.set(offerKey, sourceResolvedVcAcademicProgramId);
      const vcCourseId =
        sourceVcCourse?.id ??
        (await this.resolveVcCourseIdForImport({
          course_code: recordString(representative, 'offer_course_code'),
          course_name: recordString(representative, 'offer_course_name'),
          study_plan_year: scope.study_plan_year,
          vc_academic_program_id: offerVcAcademicProgramId,
        }, caches));
      vcFacultyIdByOfferKey.set(offerKey, offerVcFacultyId);
      vcAcademicProgramIdByOfferKey.set(offerKey, offerVcAcademicProgramId);
      vcCourseIdByOfferKey.set(offerKey, vcCourseId);
    }

    const result = {
      plan_rules: 0,
      offers: 0,
      sections: 0,
      subsections: 0,
      schedules: 0,
    };
    const now = new Date();

    const changeLogs: Partial<PlanningChangeLogEntity>[] = [];
    const offersToInsert: PlanningOfferEntity[] = [];
    const sectionsToInsert: PlanningSectionEntity[] = [];
    const subsectionsToInsert: PlanningSubsectionEntity[] = [];
    const schedulesToInsert: PlanningSubsectionScheduleEntity[] = [];

    const planRule = this.planRulesRepo.create({
      id: newId(),
      semester_id: scope.semester_id,
      vc_period_id: scope.vc_period_id,
      campus_id: scope.campus_id,
      academic_program_id: scope.academic_program_id,
      faculty_id: scope.faculty_id,
      career_name: scope.academic_program_name ?? scope.study_plan_name ?? null,
      cycle: scope.cycle,
      study_plan_id: scope.study_plan_id,
      vc_faculty_id: effectiveVcFacultyId,
      vc_academic_program_id: effectiveVcAcademicProgramId,
      is_active: true,
      workflow_status: 'DRAFT',
      submitted_at: null,
      submitted_by_user_id: null,
      submitted_by: null,
      reviewed_at: null,
      reviewed_by_user_id: null,
      reviewed_by: null,
      review_comment: null,
      created_at: now,
      updated_at: now,
    });
    changeLogs.push(
      this.buildChangeLogEntry(
        'planning_cycle_plan_rule',
        planRule.id,
        'CREATE',
        null,
        planRule,
        this.buildPlanRuleLogContext(planRule),
        actor,
      ),
    );
    result.plan_rules += 1;

    for (const offerRows of rowsByOffer.values()) {
      const representative = offerRows.map((row) => asRecord(row.resolution_json)).find(Boolean);
      if (!representative) {
        continue;
      }
      const offerKey = `${recordString(representative, 'study_plan_course_id')}::${recordString(
        representative,
        'offer_course_code',
      )}`;
      const offerResolutions = offerRows.map((row) => asRecord(row.resolution_json));
      const sourceVcCourseId = recordString(representative, 'source_vc_course_id');
      const sourceResolvedVcFacultyId = sourceVcFacultyIdByOfferKey.get(offerKey) ?? null;
      const sourceResolvedVcAcademicProgramId =
        sourceVcAcademicProgramIdByOfferKey.get(offerKey) ?? null;
      const offerVcFacultyId = vcFacultyIdByOfferKey.get(offerKey) ?? effectiveVcFacultyId;
      const offerVcAcademicProgramId =
        vcAcademicProgramIdByOfferKey.get(offerKey) ?? effectiveVcAcademicProgramId;
      const resolvedVcCourseId = vcCourseIdByOfferKey.get(offerKey) ?? null;
      const sourceSystem =
        (firstNonEmpty(
          offerResolutions.map((row) => recordString(row, 'source_system')),
        ) as PlanningSourceSystem | null) ?? 'EXCEL';
      const firstOfferSource = asRecord(offerRows[0]?.source_json);
      const offerVcSource =
        sourceResolvedVcFacultyId || sourceResolvedVcAcademicProgramId || sourceVcCourseId
          ? 'sync_source'
          : resolvedVcCourseId
            ? 'fallback_match'
            : 'fallback_match';
      const offer = this.offersRepo.create({
        id: newId(),
        semester_id: scope.semester_id,
        vc_period_id: scope.vc_period_id,
        campus_id: scope.campus_id,
        faculty_id: scope.faculty_id,
        academic_program_id: scope.academic_program_id,
        study_plan_id: scope.study_plan_id,
        cycle: scope.cycle,
        study_plan_course_id: recordString(representative, 'study_plan_course_id')!,
        vc_faculty_id: offerVcFacultyId,
        vc_academic_program_id: offerVcAcademicProgramId,
        vc_course_id: resolvedVcCourseId,
        course_code: recordString(representative, 'offer_course_code'),
        course_name: recordString(representative, 'offer_course_name'),
        study_type_id: catalog.defaultStudyTypeId,
        course_type: recordString(representative, 'offer_course_type') ?? 'TEORICO_PRACTICO',
        source_system: sourceSystem,
        source_course_id: firstNonEmpty(
          offerResolutions.map((row) => recordString(row, 'source_course_id')),
        ),
        source_term_id: firstNonEmpty(
          offerResolutions.map((row) => recordString(row, 'source_term_id')),
        ),
        last_synced_at: sourceSystem === 'AKADEMIC' ? now : null,
        source_payload_json: attachVcContextMetadata(
          asRecord(recordValue(firstOfferSource, 'course')) ??
            firstOfferSource ??
            null,
          {
            vc_source: offerVcSource,
            source_vc_faculty_id: sourceResolvedVcFacultyId,
            source_vc_academic_program_id: sourceResolvedVcAcademicProgramId,
            source_vc_course_id: sourceVcCourseId,
            vc_context_message:
              offerVcSource === 'sync_source'
                ? 'Contexto AV preservado desde la sincronizacion.'
                : 'Contexto AV resuelto por nombre/codigo del origen.',
          },
        ),
        theoretical_hours:
          maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_theoretical_hours'))) ?? 0,
        practical_hours:
          maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_practical_hours'))) ?? 0,
        total_hours:
          maxNumber(offerResolutions.map((row) => recordNumberOrNull(row, 'offer_total_hours'))) ?? 0,
        status: 'DRAFT',
        created_at: now,
        updated_at: now,
      });
      offersToInsert.push(offer);
      changeLogs.push(
        this.buildChangeLogEntry(
          'planning_offer',
          offer.id,
          'CREATE',
          null,
          offer,
          this.buildOfferLogContext(offer),
          actor,
        ),
      );
      result.offers += 1;

      const rowsBySection = groupBy(offerRows, (row) => {
        const resolution = asRecord(row.resolution_json);
        return recordString(resolution, 'import_section_code') ?? '__NO_SECTION__';
      });

      for (const [sectionCode, sectionRows] of rowsBySection.entries()) {
        if (sectionCode === '__NO_SECTION__') {
          continue;
        }
        const sectionResolutions = sectionRows.map((row) => asRecord(row.resolution_json));
        const sourceVcSectionId = firstNonEmpty(
          sectionResolutions.map((row) => recordString(row, 'source_vc_section_id')),
        );
        const section = this.sectionsRepo.create({
          id: newId(),
          planning_offer_id: offer.id,
          code: sectionCode,
          external_code: firstNonEmpty(
            sectionResolutions.map((row) => recordString(row, 'external_section_code')),
          ),
          source_section_id: firstNonEmpty(
            sectionResolutions.map((row) => recordString(row, 'source_section_id')),
          ),
          source_payload_json: attachVcContextMetadata(
            asRecord(recordValue(asRecord(sectionRows[0]?.source_json), 'section')) ??
              asRecord(sectionRows[0]?.source_json) ??
              null,
            {
              vc_source: sourceVcSectionId ? 'sync_source' : 'fallback_match',
              source_vc_section_id: sourceVcSectionId,
              vc_context_message: sourceVcSectionId
                ? 'Seccion VC preservada desde la sincronizacion.'
                : 'Seccion VC pendiente de resolver por fallback.',
            },
          ),
          teacher_id: firstNonEmpty(sectionResolutions.map((row) => recordString(row, 'teacher_id'))),
          course_modality_id: firstNonEmpty(sectionResolutions.map((row) => recordString(row, 'course_modality_id'))),
          projected_vacancies: maxNumber(sectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies'))),
          is_cepea: sectionResolutions.some((row) => Boolean(recordBoolean(row, 'is_cepea'))),
          has_subsections:
            new Set(
              sectionResolutions
                .map((row) => recordString(row, 'import_subsection_code'))
                .filter(Boolean),
            ).size > 1,
          default_theoretical_hours: offer.theoretical_hours,
          default_practical_hours: offer.practical_hours,
          default_virtual_hours: 0,
          default_seminar_hours: 0,
          default_total_hours: offer.total_hours,
          status: 'DRAFT',
          created_at: now,
          updated_at: now,
        });
        sectionsToInsert.push(section);
        changeLogs.push(
          this.buildChangeLogEntry(
            'planning_section',
            section.id,
            'CREATE',
            null,
            section,
            this.buildSectionLogContext(section, offer),
            actor,
          ),
        );
        result.sections += 1;
        const rowsBySubsection = groupBy(sectionRows, (row) => {
          const resolution = asRecord(row.resolution_json);
          return recordString(resolution, 'import_subsection_code') ?? '__NO_SUBSECTION__';
        });

        for (const [subsectionCode, subsectionRows] of rowsBySubsection.entries()) {
          if (subsectionCode === '__NO_SUBSECTION__') {
            continue;
          }
          const subsectionResolutions = subsectionRows.map((row) => asRecord(row.resolution_json));
          const assignedTheoretical = maxNumber(
            subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_theoretical_hours')),
          ) ?? 0;
          const assignedPractical = maxNumber(
            subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_practical_hours')),
          ) ?? 0;
          const assignedTotal = maxNumber(
            subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_total_hours')),
          ) ?? assignedTheoretical + assignedPractical;
          const schedulePayloads = subsectionResolutions
            .map((row) => asRecord(recordValue(row, 'schedule')))
            .filter((item) => Object.keys(item).length > 0);
          const uniqueScheduleTeacherIds = uniqueIds(
            schedulePayloads.map((row) => recordString(row, 'teacher_id')),
          );
          const uniqueScheduleBuildingIds = uniqueIds(
            schedulePayloads.map((row) => recordString(row, 'building_id')),
          );
          const uniqueScheduleClassroomIds = uniqueIds(
            schedulePayloads.map((row) => recordString(row, 'classroom_id')),
          );
          const subsectionKind = (
            firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'subsection_kind'))) ??
            deriveSubsectionKind(assignedTheoretical, assignedPractical)
          ) as PlanningSubsectionKind;
          const subsection = this.subsectionsRepo.create({
            id: newId(),
            planning_section_id: section.id,
            code: subsectionCode,
            kind: subsectionKind,
            responsible_teacher_id:
              uniqueScheduleTeacherIds.length === 1
                ? uniqueScheduleTeacherIds[0]
                : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'teacher_id'))),
            course_modality_id: firstNonEmpty(
              subsectionResolutions.map((row) => recordString(row, 'course_modality_id')),
            ),
            building_id:
              uniqueScheduleBuildingIds.length === 1
                ? uniqueScheduleBuildingIds[0]
                : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'building_id'))),
            classroom_id:
              uniqueScheduleClassroomIds.length === 1
                ? uniqueScheduleClassroomIds[0]
                : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'classroom_id'))),
            capacity_snapshot: maxNumber(
              subsectionResolutions.map((row) => recordNumberOrNull(row, 'capacity_snapshot')),
            ),
            shift: firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'shift'))),
            projected_vacancies: maxNumber(
              subsectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies')),
            ),
            course_type: offer.course_type,
            assigned_theoretical_hours: assignedTheoretical,
            assigned_practical_hours: assignedPractical,
            assigned_virtual_hours: 0,
            assigned_seminar_hours: 0,
            assigned_total_hours: assignedTotal,
            denomination:
              firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'denomination'))) ??
              buildDenomination(offer.course_code, offer.course_name, section.code, subsectionCode, offer.campus_id),
            vc_section_id:
              firstNonEmpty(
                subsectionResolutions.map((row) => recordString(row, 'source_vc_section_id')),
              ) ?? null,
            status: 'DRAFT',
            created_at: now,
            updated_at: now,
          });
          subsectionsToInsert.push(subsection);
          changeLogs.push(
            this.buildChangeLogEntry(
              'planning_subsection',
              subsection.id,
              'CREATE',
              null,
              subsection,
              this.buildSubsectionLogContext(subsection, section, offer),
              actor,
            ),
          );
          result.subsections += 1;

          const uniqueSchedules = dedupeImportedSchedules(schedulePayloads);
          for (const selectedSchedule of uniqueSchedules) {
            const schedule = this.schedulesRepo.create({
              id: newId(),
              planning_subsection_id: subsection.id,
              day_of_week: recordString(selectedSchedule, 'day_of_week') as (typeof DayOfWeekValues)[number],
              start_time: recordString(selectedSchedule, 'start_time')!,
              end_time: recordString(selectedSchedule, 'end_time')!,
              session_type:
                (recordString(selectedSchedule, 'session_type') as PlanningSessionType | null) ?? 'OTHER',
              source_session_type_code: recordString(selectedSchedule, 'source_session_type_code'),
              teacher_id: recordString(selectedSchedule, 'teacher_id'),
              building_id: recordString(selectedSchedule, 'building_id'),
              classroom_id: recordString(selectedSchedule, 'classroom_id'),
              source_schedule_id: recordString(selectedSchedule, 'source_schedule_id'),
              source_payload_json:
                asRecord(recordValue(selectedSchedule, 'source_payload_json')) ??
                null,
              duration_minutes: recordNumber(selectedSchedule, 'duration_minutes'),
              academic_hours: recordNumber(selectedSchedule, 'academic_hours'),
              created_at: now,
              updated_at: now,
            });
            schedulesToInsert.push(schedule);
            changeLogs.push(
              this.buildChangeLogEntry(
                'planning_subsection_schedule',
                schedule.id,
                'CREATE',
                null,
                schedule,
                this.buildScheduleLogContext(schedule, subsection, section, offer),
                actor,
              ),
            );
            result.schedules += 1;
          }
        }
      }
    }

    await this.offersRepo.manager.transaction(async (manager) => {
      await this.insertInChunks(manager, PlanningCyclePlanRuleEntity, [planRule]);
      await this.insertInChunks(manager, PlanningOfferEntity, offersToInsert);
      await this.insertInChunks(manager, PlanningSectionEntity, sectionsToInsert);
      await this.insertInChunks(manager, PlanningSubsectionEntity, subsectionsToInsert);
      await this.insertInChunks(manager, PlanningSubsectionScheduleEntity, schedulesToInsert);
      await manager.update(
        PlanningImportRowEntity,
        { batch_id: batchId, id: In(rows.map((row) => row.id)) },
        { imported: true, updated_at: new Date() },
      );
      await this.saveChangeLogsBulk(changeLogs, manager);
    });

    for (const offer of offersToInsert) {
      await this.planningManualService.recalculateVcMatches(actor as any, { offer_id: offer.id });
    }

    return result;
  }

  private buildImportScopeCacheKey(scope: ImportScope) {
    return [
      scope.semester_id,
      scope.vc_period_id ?? '',
      scope.campus_id,
      scope.faculty_id,
      scope.academic_program_id,
      scope.study_plan_id,
      scope.cycle,
    ].join('::');
  }

  private async resolveVcContextForScope(scope: ImportScope, caches?: ImportExecutionCaches) {
    const scopeCacheKey = this.buildImportScopeCacheKey(scope);
    const cached = caches?.vcContextByScopeKey.get(scopeCacheKey);
    if (cached) {
      return cached;
    }
    const [vcFaculties, vcPrograms] = await Promise.all([
      this.vcFacultiesRepo.find({ order: { name: 'ASC' } }),
      this.vcAcademicProgramsRepo.find({ order: { name: 'ASC' } }),
    ]);

    const vcFaculty = this.matchVcFaculty(scope, vcFaculties);
    const vcAcademicProgram = this.matchVcAcademicProgram(
      scope,
      vcFaculty?.id ?? null,
      vcPrograms,
    );

    const resolved = {
      vcFacultyId: vcFaculty?.id ?? null,
      vcAcademicProgramId: vcAcademicProgram?.id ?? null,
    };
    caches?.vcContextByScopeKey.set(scopeCacheKey, resolved);
    return resolved;
  }

  private async resolveVcContextFromSourceInput(
    input: {
      source_vc_faculty_id?: string | null;
      source_vc_academic_program_id?: string | null;
      faculty_name?: string | null;
      academic_program_name?: string | null;
    },
    caches?: ImportExecutionCaches,
  ) {
    const [vcFaculties, vcPrograms] = await this.loadVcContextCatalogs(caches);
    const vcFaculty = this.findVcFacultyByIdOrName(
      input.source_vc_faculty_id ?? null,
      input.faculty_name ?? null,
      vcFaculties,
    );
    const vcAcademicProgram = this.findVcAcademicProgramByIdOrName(
      input.source_vc_academic_program_id ?? null,
      input.academic_program_name ?? null,
      vcFaculty?.id ?? null,
      vcPrograms,
    );
    const effectiveFaculty =
      vcAcademicProgram
        ? vcFaculties.find((item) => item.id === vcAcademicProgram.faculty_id) ?? vcFaculty
        : vcFaculty;

    return {
      vcFacultyId: effectiveFaculty?.id ?? null,
      vcAcademicProgramId: vcAcademicProgram?.id ?? null,
    };
  }

  private async loadVcContextCatalogs(caches?: ImportExecutionCaches) {
    if (!caches?.vcFaculties) {
      const vcFaculties = await this.vcFacultiesRepo.find({ order: { name: 'ASC' } });
      if (caches) {
        caches.vcFaculties = vcFaculties;
      } else {
        return [vcFaculties, await this.vcAcademicProgramsRepo.find({ order: { name: 'ASC' } })] as const;
      }
    }
    if (!caches?.vcAcademicPrograms) {
      const vcPrograms = await this.vcAcademicProgramsRepo.find({ order: { name: 'ASC' } });
      if (caches) {
        caches.vcAcademicPrograms = vcPrograms;
      } else {
        return [await this.vcFacultiesRepo.find({ order: { name: 'ASC' } }), vcPrograms] as const;
      }
    }
    return [caches?.vcFaculties ?? [], caches?.vcAcademicPrograms ?? []] as const;
  }

  private findVcFacultyByIdOrName(
    facultyId: string | null,
    facultyName: string | null,
    vcFaculties: VcFacultyEntity[],
  ) {
    if (facultyId) {
      const byId = vcFaculties.find((item) => item.id === facultyId) ?? null;
      if (byId) {
        return byId;
      }
    }
    const nameVariants = buildCatalogNameMatchVariants(facultyName);
    if (nameVariants.length === 0) {
      return null;
    }
    const matches = vcFaculties.filter((item) => catalogNameMatches(item.name, nameVariants));
    return matches.length === 1 ? matches[0] : null;
  }

  private findVcAcademicProgramByIdOrName(
    programId: string | null,
    programName: string | null,
    facultyId: string | null,
    vcPrograms: VcAcademicProgramEntity[],
  ) {
    if (programId) {
      const byId = vcPrograms.find((item) => item.id === programId) ?? null;
      if (byId && (!facultyId || byId.faculty_id === facultyId)) {
        return byId;
      }
    }
    const nameVariants = buildCatalogNameMatchVariants(programName);
    if (nameVariants.length === 0) {
      return null;
    }
    const matches = vcPrograms.filter(
      (item) =>
        (!facultyId || item.faculty_id === facultyId) &&
        catalogNameMatches(item.name, nameVariants),
    );
    return matches.length === 1 ? matches[0] : null;
  }

  private matchVcFaculty(scope: ImportScope, vcFaculties: VcFacultyEntity[]) {
    return this.findVcFacultyByIdOrName(scope.faculty_id ?? null, scope.faculty_name ?? null, vcFaculties);
  }

  private matchVcAcademicProgram(
    scope: ImportScope,
    vcFacultyId: string | null,
    vcPrograms: VcAcademicProgramEntity[],
  ) {
    return this.findVcAcademicProgramByIdOrName(
      scope.academic_program_id ?? null,
      scope.academic_program_name ?? null,
      vcFacultyId,
      vcPrograms,
    );
  }

  private async resolveVcCourseIdForImport(input: {
    course_name: string | null | undefined;
    course_code: string | null | undefined;
    study_plan_year: string | null | undefined;
    vc_academic_program_id: string | null;
  }, caches?: ImportExecutionCaches) {
    const normalizedCourseName = normalizeCourseName(input.course_name);
    if (!normalizedCourseName || !input.vc_academic_program_id) {
      return null;
    }

    let candidates = caches?.vcCoursesByProgramId.get(input.vc_academic_program_id) ?? null;
    if (!candidates) {
      candidates = await this.vcCoursesRepo.find({
        where: { program_id: input.vc_academic_program_id },
        order: { name: 'ASC' },
      });
      caches?.vcCoursesByProgramId.set(input.vc_academic_program_id, candidates);
    }
    const nameMatches = candidates.filter(
      (item) => normalizeCourseName(item.name) === normalizedCourseName,
    );
    if (nameMatches.length === 0) {
      return null;
    }

    const normalizedStudyPlanYear = normalizeLoose(input.study_plan_year);
    if (normalizedStudyPlanYear) {
      const yearMatches = nameMatches.filter((item) =>
        vcCourseCodeMatchesStudyPlanYear(item.code, normalizedStudyPlanYear),
      );
      if (yearMatches.length === 1) {
        return yearMatches[0].id;
      }
      if (yearMatches.length > 1) {
        return null;
      }
    }

    const normalizedCourseCode = normalizeCourseCodeValue(input.course_code);
    if (normalizedCourseCode) {
      const directCodeMatches = nameMatches.filter(
        (item) => normalizeCourseCodeValue(item.code) === normalizedCourseCode,
      );
      if (directCodeMatches.length === 1) {
        return directCodeMatches[0].id;
      }
      if (directCodeMatches.length > 1) {
        return null;
      }
    }

    return nameMatches.length === 1 ? nameMatches[0].id : null;
  }

  private async replaceExistingScope(scope: ImportScope, actor?: ImportActor | null) {
    const scopeWhere = {
      semester_id: scope.semester_id,
      ...(scope.vc_period_id ? { vc_period_id: scope.vc_period_id } : {}),
      campus_id: scope.campus_id,
      faculty_id: scope.faculty_id,
      academic_program_id: scope.academic_program_id,
      study_plan_id: scope.study_plan_id,
      cycle: scope.cycle,
    };
    const rules = await this.planRulesRepo.find({
      where: scopeWhere,
      order: { created_at: 'ASC' },
    });

    const offers = await this.offersRepo.find({
      where: scopeWhere,
      order: { created_at: 'ASC' },
    });
    if (rules.length === 0 && offers.length === 0) {
      return false;
    }

    const offerIds = offers.map((item) => item.id);
    const sections = offerIds.length
      ? await this.sectionsRepo.find({
          where: { planning_offer_id: In(offerIds) },
          order: { created_at: 'ASC', code: 'ASC' },
        })
      : [];
    const sectionIds = sections.map((item) => item.id);
    const subsections = sectionIds.length
      ? await this.subsectionsRepo.find({
          where: { planning_section_id: In(sectionIds) },
          order: { created_at: 'ASC', code: 'ASC' },
        })
      : [];
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = subsectionIds.length
      ? await this.schedulesRepo.find({
          where: { planning_subsection_id: In(subsectionIds) },
          order: { created_at: 'ASC' },
        })
      : [];

    await this.planRulesRepo.manager.transaction(async (manager) => {
      if (subsectionIds.length > 0) {
        await manager.delete(PlanningSubsectionScheduleEntity, {
          planning_subsection_id: In(subsectionIds),
        });
      }
      if (sectionIds.length > 0) {
        await manager.delete(PlanningSubsectionEntity, {
          planning_section_id: In(sectionIds),
        });
      }
      if (offerIds.length > 0) {
        await manager.delete(PlanningSectionEntity, {
          planning_offer_id: In(offerIds),
        });
        await manager.delete(PlanningOfferEntity, {
          id: In(offerIds),
        });
      }
      if (rules.length > 0) {
        await manager.delete(PlanningCyclePlanRuleEntity, {
          id: In(rules.map((item) => item.id)),
        });
      }
    });

    const offerMap = mapById(offers);
    const sectionMap = mapById(sections);
    const subsectionMap = mapById(subsections);
    const changeLogs: Partial<PlanningChangeLogEntity>[] = [];
    for (const schedule of schedules) {
      const subsection = subsectionMap.get(schedule.planning_subsection_id);
      const section = subsection ? sectionMap.get(subsection.planning_section_id) : null;
      const offer = section ? offerMap.get(section.planning_offer_id) : null;
      if (!subsection || !section || !offer) {
        continue;
      }
      changeLogs.push(
        this.buildChangeLogEntry(
          'planning_subsection_schedule',
          schedule.id,
          'DELETE',
          schedule,
          null,
          {
            ...this.buildScheduleLogContext(schedule, subsection, section, offer),
            source: 'planning_import_replace_scope',
          },
          actor,
        ),
      );
    }
    for (const subsection of subsections) {
      const section = sectionMap.get(subsection.planning_section_id);
      const offer = section ? offerMap.get(section.planning_offer_id) : null;
      if (!section || !offer) {
        continue;
      }
      changeLogs.push(
        this.buildChangeLogEntry(
          'planning_subsection',
          subsection.id,
          'DELETE',
          subsection,
          null,
          {
            ...this.buildSubsectionLogContext(subsection, section, offer),
            source: 'planning_import_replace_scope',
          },
          actor,
        ),
      );
    }
    for (const section of sections) {
      const offer = offerMap.get(section.planning_offer_id);
      if (!offer) {
        continue;
      }
      changeLogs.push(
        this.buildChangeLogEntry(
          'planning_section',
          section.id,
          'DELETE',
          section,
          null,
          {
            ...this.buildSectionLogContext(section, offer),
            source: 'planning_import_replace_scope',
          },
          actor,
        ),
      );
    }
    for (const offer of offers) {
      changeLogs.push(
        this.buildChangeLogEntry(
          'planning_offer',
          offer.id,
          'DELETE',
          offer,
          null,
          {
            ...this.buildOfferLogContext(offer),
            source: 'planning_import_replace_scope',
          },
          actor,
        ),
      );
    }
    for (const rule of rules) {
      changeLogs.push(
        this.buildChangeLogEntry(
          'planning_cycle_plan_rule',
          rule.id,
          'DELETE',
          rule,
          null,
          {
            ...this.buildPlanRuleLogContext(rule),
            source: 'planning_import_replace_scope',
          },
          actor,
        ),
      );
    }
    await this.saveChangeLogsBulk(changeLogs);

    return true;
  }

  private async buildScopeDecisionEntities(
    batchId: string,
    previewRows: PreviewRow[],
    now: Date,
    onProgress?: (current: number, total: number) => Promise<void> | void,
  ) {
    const autoReplaceScopes = previewRows.some(
      (row) => recordString(asRecord(row.resolution_json), 'source_kind') === 'AKADEMIC',
    );
    const rowsByScope = groupBy(
      previewRows.filter((row) => row.can_import && row.scope_key),
      (row) => row.scope_key!,
    );
    const totalScopes = rowsByScope.size;
    const scopeEntries = [...rowsByScope.entries()];
    let processedScopes = 0;
    const entities = await runWithConcurrency(
      scopeEntries,
      AKADEMIC_SCOPE_DECISION_CONCURRENCY,
      async ([scopeKey, rows]) => {
        const scope = this.scopeFromResolution(asRecord(rows[0].resolution_json));
        if (!scope) {
          return null;
        }
        const existing = await this.findExistingScopeSummary(scope);
        processedScopes += 1;
        if (onProgress && (processedScopes === totalScopes || processedScopes % 8 === 0)) {
          await onProgress(processedScopes, totalScopes);
        }
        return this.importScopeDecisionsRepo.create({
          id: newId(),
          batch_id: batchId,
          scope_key: scopeKey,
          scope_json: scope as unknown as Record<string, unknown>,
          existing_summary_json: existing,
          decision: autoReplaceScopes ? 'REPLACE_SCOPE' : hasExistingData(existing) ? 'PENDING' : 'REPLACE_SCOPE',
          notes: null,
          created_at: now,
          updated_at: now,
        });
      },
    );

    if (onProgress && totalScopes === 0) {
      await onProgress(0, 0);
    }

    return entities.filter((item): item is PlanningImportScopeDecisionEntity => Boolean(item));
  }

  private async findExistingScopeSummary(scope: ImportScope) {
    const scopeWhere = {
      semester_id: scope.semester_id,
      ...(scope.vc_period_id ? { vc_period_id: scope.vc_period_id } : {}),
      campus_id: scope.campus_id,
      faculty_id: scope.faculty_id,
      academic_program_id: scope.academic_program_id,
      study_plan_id: scope.study_plan_id,
      cycle: scope.cycle,
    };
    const [planRules, offers] = await Promise.all([
      this.planRulesRepo.find({
        where: scopeWhere,
        select: { id: true, created_at: true },
        order: { created_at: 'DESC' },
      }),
      this.offersRepo.find({
        where: scopeWhere,
        select: { id: true, study_plan_course_id: true, course_code: true, course_name: true },
      }),
    ]);
    const offerIds = offers.map((item) => item.id);
    const sectionRows = offerIds.length
      ? await this.sectionsRepo
          .createQueryBuilder('section')
          .innerJoin(PlanningOfferEntity, 'offer', 'offer.id = section.planning_offer_id')
          .select('section.id', 'section_id')
          .addSelect('offer.study_plan_course_id', 'study_plan_course_id')
          .addSelect('section.code', 'section_code')
          .where('section.planning_offer_id IN (:...offerIds)', { offerIds })
          .getRawMany<Record<string, unknown>>()
      : [];
    const sectionIds = sectionRows
      .map((item) => recordString(item, 'section_id'))
      .filter((item): item is string => Boolean(item));
    const subsectionRows = sectionIds.length
      ? await this.subsectionsRepo
          .createQueryBuilder('subsection')
          .innerJoin(PlanningSectionEntity, 'section', 'section.id = subsection.planning_section_id')
          .innerJoin(PlanningOfferEntity, 'offer', 'offer.id = section.planning_offer_id')
          .select('subsection.id', 'subsection_id')
          .addSelect('offer.study_plan_course_id', 'study_plan_course_id')
          .addSelect('section.code', 'section_code')
          .addSelect('subsection.code', 'subsection_code')
          .where('subsection.planning_section_id IN (:...sectionIds)', { sectionIds })
          .getRawMany<Record<string, unknown>>()
      : [];
    const subsectionIds = subsectionRows
      .map((item) => recordString(item, 'subsection_id'))
      .filter((item): item is string => Boolean(item));
    const scheduleRows = subsectionIds.length
      ? await this.schedulesRepo
          .createQueryBuilder('schedule')
          .innerJoin(PlanningSubsectionEntity, 'subsection', 'subsection.id = schedule.planning_subsection_id')
          .innerJoin(PlanningSectionEntity, 'section', 'section.id = subsection.planning_section_id')
          .innerJoin(PlanningOfferEntity, 'offer', 'offer.id = section.planning_offer_id')
          .select('offer.study_plan_course_id', 'study_plan_course_id')
          .addSelect('section.code', 'section_code')
          .addSelect('subsection.code', 'subsection_code')
          .addSelect('schedule.source_schedule_id', 'source_schedule_id')
          .addSelect('schedule.day_of_week', 'day_of_week')
          .addSelect('schedule.start_time', 'start_time')
          .addSelect('schedule.end_time', 'end_time')
          .addSelect('schedule.session_type', 'session_type')
          .where('schedule.planning_subsection_id IN (:...subsectionIds)', { subsectionIds })
          .getRawMany<Record<string, unknown>>()
      : [];

    const changeMapSnapshot: ExistingScopeSnapshot = {
      offer_keys: [...new Set(offers.map((item) => item.study_plan_course_id).filter(Boolean))],
      section_keys: [...new Set(sectionRows.map((item) => buildExistingSectionKey(item)).filter(Boolean))],
      subsection_keys: [...new Set(subsectionRows.map((item) => buildExistingSubsectionKey(item)).filter(Boolean))],
      schedule_keys: [...new Set(scheduleRows.map((item) => buildExistingScheduleKey(item)).filter(Boolean))],
      offer_entries: uniqueSnapshotEntries(
        offers.map((item) => ({
          key: item.study_plan_course_id,
          label: [
            item.course_code,
            item.course_name,
            item.study_plan_course_id,
          ]
            .filter(Boolean)
            .join(' | '),
        })),
      ),
      section_entries: uniqueSnapshotEntries(
        sectionRows.map((item) => ({
          key: buildExistingSectionKey(item),
          label: [
            recordString(item, 'section_code'),
            recordString(item, 'study_plan_course_id'),
          ]
            .filter(Boolean)
            .join(' | '),
        })),
      ),
      subsection_entries: uniqueSnapshotEntries(
        subsectionRows.map((item) => ({
          key: buildExistingSubsectionKey(item),
          label: [
            recordString(item, 'section_code'),
            recordString(item, 'subsection_code'),
            recordString(item, 'study_plan_course_id'),
          ]
            .filter(Boolean)
            .join(' | '),
        })),
      ),
      schedule_entries: uniqueSnapshotEntries(
        scheduleRows.map((item) => ({
          key: buildExistingScheduleKey(item),
          label: buildExistingScheduleLabel(item),
        })),
      ),
    };

    return {
      has_existing_data:
        planRules.length > 0 ||
        offers.length > 0 ||
        sectionRows.length > 0 ||
        subsectionRows.length > 0 ||
        scheduleRows.length > 0,
      plan_rule_id: planRules[0]?.id ?? null,
      plan_rule_count: planRules.length,
      offer_count: offers.length,
      section_count: sectionRows.length,
      subsection_count: subsectionRows.length,
      schedule_count: scheduleRows.length,
      change_map_snapshot: changeMapSnapshot,
    };
  }

  private composeBatchResponse(
    batch: PlanningImportBatchEntity,
    rows: PlanningImportRowEntity[],
    issues: PlanningImportRowIssueEntity[],
    scopeDecisions: PlanningImportScopeDecisionEntity[],
  ) {
    const composition = this.buildBatchComposition(batch, rows, issues, scopeDecisions);
    return {
      id: batch.id,
      file_name: batch.file_name,
      source_kind: batch.source_kind,
      source_scope: batch.source_scope_json ?? null,
      sheet_name: batch.sheet_name,
      status: batch.status,
      total_row_count: batch.total_row_count,
      importable_row_count: batch.importable_row_count,
      blocked_row_count: batch.blocked_row_count,
      warning_row_count: batch.warning_row_count,
      created_at: batch.created_at,
      updated_at: batch.updated_at,
      executed_at: batch.executed_at,
      progress: batch.preview_progress_json ?? null,
      error_message: batch.error_message ?? null,
      summary: composition.summary,
      scope_decisions: composition.scopes,
      unresolved_mappings: composition.unresolved_mappings,
      resolved_mappings: composition.resolved_mappings,
      issue_summary: composition.issue_summary,
      row_preview: composition.row_preview,
      report: batch.execution_summary_json ?? null,
    };
  }

  private composePendingBatchResponse(batch: PlanningImportBatchEntity) {
    return {
      id: batch.id,
      file_name: batch.file_name,
      source_kind: batch.source_kind,
      source_scope: batch.source_scope_json ?? null,
      sheet_name: batch.sheet_name,
      status: batch.status,
      total_row_count: batch.total_row_count,
      importable_row_count: batch.importable_row_count,
      blocked_row_count: batch.blocked_row_count,
      warning_row_count: batch.warning_row_count,
      created_at: batch.created_at,
      updated_at: batch.updated_at,
      executed_at: batch.executed_at,
      progress: batch.preview_progress_json ?? null,
      error_message: batch.error_message ?? null,
      summary: batch.preview_summary_json ?? null,
      scope_decisions: [],
      unresolved_mappings: [],
      resolved_mappings: [],
      issue_summary: [],
      row_preview: [],
      report: batch.execution_summary_json ?? null,
    };
  }

  private buildBatchComposition(
    batch: PlanningImportBatchEntity,
    rows: PlanningImportRowEntity[],
    issues: PlanningImportRowIssueEntity[],
    scopeDecisions: PlanningImportScopeDecisionEntity[],
  ): BatchComposition {
    const issuesByRowId = groupBy(issues, (issue) => issue.row_id ?? '__NO_ROW__');
    const rowsById = new Map(rows.map((row) => [row.id, row]));
    const rowsByScope = groupBy(
      rows.filter((row) => row.can_import && row.scope_key),
      (row) => row.scope_key!,
    );
    const unresolvedMappingMap = new Map<string, Record<string, unknown>>();
    const resolvedMappingMap = new Map<string, Record<string, unknown>>();

    for (const issue of issues) {
      const meta = asRecord(issue.meta_json);
      const namespace = recordString(meta, 'namespace');
      const sourceValue = recordString(meta, 'source_value');
      if (!namespace || !sourceValue) {
        continue;
      }
      const key = `${namespace}::${sourceValue}`;
      const current = unresolvedMappingMap.get(key) ?? {
        namespace,
        source_value: sourceValue,
        row_count: 0,
        issue_count: 0,
        blocking_count: 0,
        warning_count: 0,
        dependent_academic_programs: [],
        sample_rows: [],
      };
      const relatedRow = issue.row_id ? rowsById.get(issue.row_id) ?? null : null;
      current.row_count = numberValue(current.row_count) + 1;
      current.issue_count = numberValue(current.issue_count) + 1;
      if (issue.severity === 'BLOCKING') {
        current.blocking_count = numberValue(current.blocking_count) + 1;
      }
      if (issue.severity === 'WARNING') {
        current.warning_count = numberValue(current.warning_count) + 1;
      }
      if (relatedRow) {
        pushMappingSampleRow(current, relatedRow);
      }
      if (namespace === 'study_plan_code' && relatedRow) {
          const normalized = asRecord(relatedRow.normalized_json);
          const resolution = asRecord(relatedRow.resolution_json);
          const mappingResolution = asRecord(recordValue(resolution, 'mapping_resolution'));
          const programResolution = asRecord(recordValue(mappingResolution, 'academic_program_code'));
          const dependentPrograms = Array.isArray(current.dependent_academic_programs)
            ? current.dependent_academic_programs
            : [];
          const dependentProgram = {
            source_value: recordString(normalized, 'academic_program_code_raw') ?? '',
            target_id: recordString(programResolution, 'target_id'),
            target_label: recordString(programResolution, 'target_label'),
            match_source: recordString(programResolution, 'match_source') ?? 'none',
          };
          if (
            dependentProgram.source_value &&
            !dependentPrograms.some(
              (item: any) =>
                `${item?.source_value ?? ''}` === dependentProgram.source_value &&
                `${item?.target_id ?? ''}` === `${dependentProgram.target_id ?? ''}`,
            )
          ) {
            dependentPrograms.push(dependentProgram);
            current.dependent_academic_programs = dependentPrograms;
          }
      }
      if (namespace === 'course_code' && relatedRow) {
          const normalized = asRecord(relatedRow.normalized_json);
          const resolution = asRecord(relatedRow.resolution_json);
          const mappingResolution = asRecord(recordValue(resolution, 'mapping_resolution'));
          const programResolution = asRecord(recordValue(mappingResolution, 'academic_program_code'));
          const studyPlanResolution = asRecord(recordValue(mappingResolution, 'study_plan_code'));
          const dependentPrograms = Array.isArray(current.dependent_academic_programs)
            ? current.dependent_academic_programs
            : [];
          const dependentStudyPlans = Array.isArray(current.dependent_study_plans)
            ? current.dependent_study_plans
            : [];
          const dependentCycles = Array.isArray(current.dependent_cycles) ? current.dependent_cycles : [];
          const dependentProgram = {
            source_value: recordString(normalized, 'academic_program_code_raw') ?? '',
            target_id: recordString(programResolution, 'target_id'),
            target_label: recordString(programResolution, 'target_label'),
            match_source: recordString(programResolution, 'match_source') ?? 'none',
          };
          const dependentStudyPlan = {
            source_value: recordString(normalized, 'study_plan_code_raw') ?? '',
            target_id: recordString(studyPlanResolution, 'target_id'),
            target_label: recordString(studyPlanResolution, 'target_label'),
            match_source: recordString(studyPlanResolution, 'match_source') ?? 'none',
          };
          const cycle = recordNumberOrNull(normalized, 'cycle_raw');
          if (
            dependentProgram.source_value &&
            !dependentPrograms.some(
              (item: any) =>
                `${item?.source_value ?? ''}` === dependentProgram.source_value &&
                `${item?.target_id ?? ''}` === `${dependentProgram.target_id ?? ''}`,
            )
          ) {
            dependentPrograms.push(dependentProgram);
            current.dependent_academic_programs = dependentPrograms;
          }
          if (
            dependentStudyPlan.source_value &&
            !dependentStudyPlans.some(
              (item: any) =>
                `${item?.source_value ?? ''}` === dependentStudyPlan.source_value &&
                `${item?.target_id ?? ''}` === `${dependentStudyPlan.target_id ?? ''}`,
            )
          ) {
            dependentStudyPlans.push(dependentStudyPlan);
            current.dependent_study_plans = dependentStudyPlans;
          }
          if (cycle && !dependentCycles.includes(cycle)) {
            dependentCycles.push(cycle);
            current.dependent_cycles = dependentCycles;
          }
          current.source_value_display = buildCourseSourceValueDisplay(
            sourceValue,
            recordString(normalized, 'course_name_raw'),
          );
      }
      if (
        (namespace === 'building' || namespace === 'classroom' || namespace === 'laboratory') &&
        relatedRow
      ) {
        const normalized = asRecord(relatedRow.normalized_json);
        const resolution = asRecord(relatedRow.resolution_json);
        const mappingResolution = asRecord(recordValue(resolution, 'mapping_resolution'));
        const campusResolution = asRecord(recordValue(mappingResolution, 'campus'));
        pushDependencyEntry(current, 'dependent_campuses', {
          source_value:
            recordString(campusResolution, 'source_value') ?? recordString(normalized, 'campus_raw') ?? '',
          target_id: recordString(campusResolution, 'target_id'),
          target_label: recordString(campusResolution, 'target_label'),
          match_source: recordString(campusResolution, 'match_source') ?? 'none',
        });
        if (namespace === 'classroom' || namespace === 'laboratory') {
          const buildingResolution = asRecord(recordValue(mappingResolution, 'building'));
          pushDependencyEntry(current, 'dependent_buildings', {
            source_value:
              recordString(buildingResolution, 'source_value') ?? recordString(normalized, 'building_raw') ?? '',
            target_id: recordString(buildingResolution, 'target_id'),
            target_label: recordString(buildingResolution, 'target_label'),
            match_source: recordString(buildingResolution, 'match_source') ?? 'none',
          });
        }
      }
      unresolvedMappingMap.set(key, current);
    }

    for (const row of rows) {
      const resolution = asRecord(row.resolution_json);
      const mappingResolution = asRecord(recordValue(resolution, 'mapping_resolution'));
      for (const [namespace, payload] of Object.entries(mappingResolution)) {
        const details = asRecord(payload);
        const sourceValue = recordString(details, 'source_value');
        const targetLabel = recordString(details, 'target_label');
        const targetId = recordString(details, 'target_id');
        if (!sourceValue || !targetId) {
          continue;
        }
        if (requiresMappingConfirmation(namespace, targetId, recordString(details, 'match_source'))) {
          const confirmationKey = `${namespace}::${sourceValue}`;
          const current = unresolvedMappingMap.get(confirmationKey) ?? {
            namespace,
            source_value: sourceValue,
            row_count: 0,
            issue_count: 0,
            blocking_count: 0,
            warning_count: 0,
            target_id: targetId,
            target_label: targetLabel,
            match_source: recordString(details, 'match_source') ?? 'catalog',
            requires_confirmation: true,
            dependent_academic_programs: Array.isArray(details.dependent_academic_programs)
              ? details.dependent_academic_programs
              : [],
            dependent_study_plans: Array.isArray(details.dependent_study_plans)
              ? details.dependent_study_plans
              : [],
            dependent_cycles: Array.isArray(details.dependent_cycles) ? details.dependent_cycles : [],
            sample_rows: [],
          };
          current.row_count = numberValue(current.row_count) + 1;
          current.target_id = targetId;
          current.target_label = targetLabel;
          current.match_source = recordString(details, 'match_source') ?? 'catalog';
          current.requires_confirmation = true;
          pushMappingSampleRow(current, row);
          unresolvedMappingMap.set(confirmationKey, current);
        }
        const key = `${namespace}::${sourceValue}::${targetId}`;
        const current = resolvedMappingMap.get(key) ?? {
          namespace,
          source_value: sourceValue,
          target_id: targetId,
          target_label: targetLabel,
          row_count: 0,
          match_source: recordString(details, 'match_source') ?? 'catalog',
        };
        current.row_count = numberValue(current.row_count) + 1;
        if (namespace === 'course_code') {
          const normalized = asRecord(row.normalized_json);
          current.source_value_display = buildCourseSourceValueDisplay(
            sourceValue,
            recordString(normalized, 'course_name_raw'),
          );
        }
        resolvedMappingMap.set(key, current);
      }
    }

    const issueSummary = [...groupBy(issues, (issue) => `${issue.severity}::${issue.issue_code}`).entries()]
      .map(([key, grouped]) => {
        const [severity = '', issueCode = ''] = key.split('::');
        return {
          severity,
          issue_code: issueCode,
          count: grouped.length,
        };
      })
      .sort((left, right) => left.severity.localeCompare(right.severity) || left.issue_code.localeCompare(right.issue_code));

    const scopeSummaries = scopeDecisions.map((scopeDecision) => {
      const rowsForScope = rowsByScope.get(scopeDecision.scope_key) ?? [];
      const importableRows = rowsForScope.length;
      const warningRows = rowsForScope.filter((row) =>
        (issuesByRowId.get(row.id) ?? []).some((issue) => issue.severity === 'WARNING'),
      ).length;
      const resolutionRows = rowsForScope.map((row) => asRecord(row.resolution_json));
      const incomingSnapshot = buildIncomingScopeSnapshot(resolutionRows);
      const offerCount = incomingSnapshot.offer_keys.length;
      const sectionCount = incomingSnapshot.section_keys.length;
      const subsectionCount = incomingSnapshot.subsection_keys.length;
      const scheduleCount = incomingSnapshot.schedule_keys.length;
      const existingSummary = asRecord(scopeDecision.existing_summary_json);
      const changeMap = buildScopeChangeMap(existingSummary, incomingSnapshot, importableRows > 0);
      return {
        id: scopeDecision.id,
        scope_key: scopeDecision.scope_key,
        scope: scopeDecision.scope_json,
        existing_summary: scopeDecision.existing_summary_json,
        decision: scopeDecision.decision,
        notes: scopeDecision.notes,
        importable_row_count: importableRows,
        warning_row_count: warningRows,
        create_counts: {
          plan_rules: importableRows > 0 ? 1 : 0,
          offers: offerCount,
          sections: sectionCount,
          subsections: subsectionCount,
          schedules: scheduleCount,
        },
        change_map: changeMap,
      };
    });

    const effectiveScopes = scopeSummaries.filter((scope) => scope.decision !== 'SKIP_SCOPE');
    const effectiveCounts = effectiveScopes.reduce(
      (acc, scope) => ({
        plan_rules: acc.plan_rules + numberValue(asRecord(scope.create_counts).plan_rules),
        offers: acc.offers + numberValue(asRecord(scope.create_counts).offers),
        sections: acc.sections + numberValue(asRecord(scope.create_counts).sections),
        subsections: acc.subsections + numberValue(asRecord(scope.create_counts).subsections),
        schedules: acc.schedules + numberValue(asRecord(scope.create_counts).schedules),
      }),
      { plan_rules: 0, offers: 0, sections: 0, subsections: 0, schedules: 0 },
    );
    const totalCreateCounts = scopeSummaries.reduce(
      (acc, scope) => ({
        plan_rules: acc.plan_rules + numberValue(asRecord(scope.create_counts).plan_rules),
        offers: acc.offers + numberValue(asRecord(scope.create_counts).offers),
        sections: acc.sections + numberValue(asRecord(scope.create_counts).sections),
        subsections: acc.subsections + numberValue(asRecord(scope.create_counts).subsections),
        schedules: acc.schedules + numberValue(asRecord(scope.create_counts).schedules),
      }),
      { plan_rules: 0, offers: 0, sections: 0, subsections: 0, schedules: 0 },
    );
    const totalChangeMap = scopeSummaries.reduce(
      (acc, scope) => mergeScopeChangeMaps(acc, asRecord(scope.change_map)),
      emptyScopeChangeMap(),
    );
    const effectiveChangeMap = effectiveScopes.reduce(
      (acc, scope) => mergeScopeChangeMaps(acc, asRecord(scope.change_map)),
      emptyScopeChangeMap(),
    );

    const rowPreview = rows
      .filter((row) => {
        const rowIssues = issuesByRowId.get(row.id) ?? [];
        return !row.can_import || rowIssues.length > 0;
      })
      .slice(0, ISSUE_PREVIEW_LIMIT)
      .map((row) => {
        const normalized = asRecord(row.normalized_json);
        const resolution = asRecord(row.resolution_json);
        const source = asRecord(row.source_json);
        const scheduleSource = asRecord(recordValue(source, 'schedule'));
        return {
          row_id: row.id,
          row_number: row.row_number,
          can_import: row.can_import,
          semester: recordString(normalized, 'semester_raw'),
          campus: recordString(normalized, 'campus_raw'),
          faculty: recordString(normalized, 'faculty_code_raw'),
          academic_program: recordString(normalized, 'academic_program_code_raw'),
          cycle: recordNumberOrNull(normalized, 'cycle_raw'),
          course_code: recordString(normalized, 'course_code_raw'),
          course_name: recordString(normalized, 'course_name_raw'),
          section: recordString(normalized, 'section_raw'),
          import_section_code: recordString(resolution, 'import_section_code'),
          import_subsection_code: recordString(resolution, 'import_subsection_code'),
          teacher_name: recordString(normalized, 'teacher_name_raw'),
          teacher_dni:
            asNullableString(
              pick(scheduleSource, 'dni', 'teacher.dni', 'teacher.user.dni', 'teacher.documentNumber'),
            ) ?? null,
          issues: (issuesByRowId.get(row.id) ?? []).map((issue) => ({
            severity: issue.severity,
            issue_code: issue.issue_code,
            field_name: issue.field_name,
            message: issue.message,
          })),
        };
      });

    const rowsWithOptionalNulls = rows.filter((row) => {
      if (!row.can_import) {
        return false;
      }
      return (issuesByRowId.get(row.id) ?? []).some((issue) => issue.severity === 'WARNING');
    }).length;

    const pendingDecisions = scopeSummaries.filter(
      (scope) =>
        hasExistingData(scope.existing_summary as Record<string, unknown> | null | undefined) &&
        scope.decision === 'PENDING',
    ).length;
    const pendingMappingConfirmations = [...unresolvedMappingMap.values()].filter(
      (item) => recordBoolean(item, 'requires_confirmation'),
    ).length;

    return {
      summary: {
        total_row_count: batch.total_row_count,
        importable_row_count: batch.importable_row_count,
        blocked_row_count: batch.blocked_row_count,
        warning_row_count: batch.warning_row_count,
        optional_null_row_count: rowsWithOptionalNulls,
        detected_scope_count: scopeSummaries.length,
        pending_scope_decision_count: pendingDecisions,
        pending_mapping_confirmation_count: pendingMappingConfirmations,
        create_counts: totalCreateCounts,
        effective_create_counts: effectiveCounts,
        change_map: totalChangeMap,
        effective_change_map: effectiveChangeMap,
      },
      scopes: scopeSummaries,
      unresolved_mappings: [...unresolvedMappingMap.values()].sort((left, right) =>
        `${left.namespace}`.localeCompare(`${right.namespace}`) ||
        `${left.source_value}`.localeCompare(`${right.source_value}`),
      ),
      resolved_mappings: [...resolvedMappingMap.values()].sort((left, right) =>
        `${left.namespace}`.localeCompare(`${right.namespace}`) ||
        `${left.source_value}`.localeCompare(`${right.source_value}`),
      ),
      issue_summary: issueSummary,
      row_preview: rowPreview,
    };
  }

  private buildStoredSummary(previewRows: PreviewRow[]) {
    const canImportRows = previewRows.filter((row) => row.can_import);
    return {
      total_row_count: previewRows.length,
      importable_row_count: canImportRows.length,
      blocked_row_count: previewRows.length - canImportRows.length,
      warning_row_count: previewRows.filter((row) =>
        row.issues.some((issue) => issue.severity === 'WARNING'),
      ).length,
    };
  }

  private normalizeExcelRow(row: Record<string, unknown>, rowNumber: number) {
    const startTimeFormatted = extractFormattedTime(firstCellValue(row, 'HORA INICIO_1', 'HORA INICIO 2'));
    const endTimeFormatted = extractFormattedTime(firstCellValue(row, 'HORA FIN_1', 'HORA FIN 2'));
    const normalized: NormalizedImportRow = {
      row_number: rowNumber,
      semester_raw: stringifyCell(firstCellValue(row, 'SEMESTRE ', 'SEMESTRE')),
      study_plan_code_raw: stringifyCell(firstCellValue(row, 'CODIGO DE  PLAN', 'COD. PLAN')),
      campus_raw: stringifyCell(firstCellValue(row, 'LOCAL')),
      faculty_code_raw: stringifyCell(firstCellValue(row, 'Cod Facultad:', 'FACULTAD')),
      academic_program_code_akademic_raw: '',
      academic_program_code_raw: stringifyCell(
        firstCellValue(row, 'PROGRAMA ACADÉMICO', 'PROGRAMA ACADÃ‰MICO', 'COD PROGRAMA AKDEMIC', 'COD PROGRAMA'),
      ),
      cycle_raw: numberFromCell(firstCellValue(row, 'CICLO')),
      section_raw: stringifyCell(firstCellValue(row, 'SECCIÓN', 'SECCIÃ“N')),
      course_code_raw: stringifyCell(firstCellValue(row, 'CÓDIGO', 'CÃ“DIGO', 'CODIGO CURSO')),
      course_name_raw: stringifyCell(firstCellValue(row, 'NOMBRE DE CURSO')),
      study_type_raw: stringifyCell(firstCellValue(row, 'TIPO DE ESTUDIOS')),
      course_requirement_raw: stringifyCell(firstCellValue(row, 'TIPO DE CURSO PLAN', 'TIPO DE CURSO')),
      course_modality_raw: stringifyCell(firstCellValue(row, 'MODALIDAD DE CURSO')),
      delivery_modality_raw: stringifyCell(
        firstCellValue(row, 'MODALIDAD \r\n(Presenc./VIRTUAL/HIBRIDO)', 'MODALIDAD'),
      ),
      theory_hours: safeNumber(numberFromCell(firstCellValue(row, 'HORAS TEORÍA', 'HORAS TEORIA'))),
      practical_hours: safeNumber(numberFromCell(firstCellValue(row, 'HORAS PRÁCTICA', 'HORAS PRACTICA'))),
      total_hours: safeNumber(numberFromCell(firstCellValue(row, 'TOTAL DE HORAS'))),
      credits: numberFromCell(firstCellValue(row, 'TOTAL DE CREDITOS', 'TOTAL DE CRÉDITOS')),
      projected_vacancies: numberFromCell(
        firstCellValue(row, 'NÚMERO VACANTES PROYECTADAS 2025-2', 'VACANTES PROYECTADAS'),
      ),
      teacher_dni_raw: stringifyCell(firstCellValue(row, 'DNI')),
      teacher_name_raw: stringifyCell(firstCellValue(row, 'APELLIDOS Y NOMBRES', 'DOCENTE')),
      shift_raw:
        stringifyCell(firstCellValue(row, 'TURNO\r\n(DIURNO/\r\nMAÑANA/ TARDE/NOCHE)', 'TURNO', 'TURNO_1')),
      building_raw: stringifyCell(firstCellValue(row, 'PABELLON')),
      classroom_raw: stringifyCell(firstCellValue(row, 'AULA')),
      laboratory_raw: stringifyCell(firstCellValue(row, 'LABORATORIO')),
      day_raw: stringifyCell(firstCellValue(row, 'DIA')),
      start_hour_raw:
        stringifyCell(firstCellValue(row, 'HORA INICIO')) ||
        stringifyCell(startTimeFormatted?.hour),
      start_minute_raw:
        stringifyCell(firstCellValue(row, 'MINUTO INICIO')) ||
        stringifyCell(startTimeFormatted?.minute),
      end_hour_raw:
        stringifyCell(firstCellValue(row, 'HORA FIN')) ||
        stringifyCell(endTimeFormatted?.hour),
      end_minute_raw:
        stringifyCell(firstCellValue(row, 'MINUTO FIN')) ||
        stringifyCell(endTimeFormatted?.minute),
      academic_hours_raw: numberFromCell(firstCellValue(row, 'HORAS\r\nACADEM.', 'HORAS ACADEMICAS')),
      denomination_raw: stringifyCell(firstCellValue(row, 'DENOMINACIÓN', 'DENOMINACIÃ“N', 'DENOMINACION')),
      raw_row: row,
    };
    normalized.academic_program_code_akademic_raw = stringifyCell(
      firstCellValue(row, 'COD PROGRAMA AKDEMIC', 'PROGRAMA ACADÃ‰MICO', 'PROGRAMA ACADÃƒâ€°MICO'),
    );
    const explicitProgramCode = stringifyCell(firstCellValue(row, 'COD PROGRAMA'));
    if (explicitProgramCode) {
      normalized.academic_program_code_raw = explicitProgramCode;
    }

    if (shouldSkipExcelRow(normalized)) {
      return null;
    }

    if (normalized.total_hours <= 0) {
      normalized.total_hours = roundToTwo(normalized.theory_hours + normalized.practical_hours);
    }
    return normalized;
  }

  private resolvePreviewRow(row: NormalizedImportRow, catalog: ImportCatalog): PreviewRow {
    const issues: PreviewIssue[] = [];
    const aliasContext: AliasSourceContext = {
      academic_program_code_raw: row.academic_program_code_raw,
      study_plan_code_raw: row.study_plan_code_raw,
      cycle_raw: row.cycle_raw,
    };
    const semesterResolution = this.resolveSemester(row.semester_raw, catalog, issues);
    const vcPeriodResolution = this.resolveVcPeriod(row.semester_raw, catalog, issues);
    const campusResolution = this.resolveCampus(row.campus_raw, catalog, issues);
    const facultyResolution = this.resolveFaculty(row.faculty_code_raw, catalog, issues);
    const programResolution = this.resolveAcademicProgram(row.academic_program_code_raw, catalog, issues);
    const studyPlanResolution = this.resolveStudyPlan(
      row.study_plan_code_raw,
      programResolution.target_id,
      row.course_code_raw,
      row.cycle_raw,
      aliasContext,
      catalog,
      issues,
    );
    const courseResolution = this.resolveStudyPlanCourse(
      row.course_code_raw,
      row.course_name_raw,
      studyPlanResolution.target_id,
      row.cycle_raw,
      aliasContext,
      catalog,
      issues,
    );
    const teacherResolution = this.resolveTeacher(row.teacher_dni_raw, row.teacher_name_raw, catalog, issues);
    const modalityResolution = this.resolveCourseModality(
      row.delivery_modality_raw || row.course_modality_raw,
      catalog,
      issues,
    );
    const shiftResolution = this.resolveShift(row.shift_raw, catalog, issues);
    const buildingResolution = this.resolveBuilding(
      row.building_raw,
      campusResolution.target_id,
      catalog,
      issues,
    );
    const classroomResolution = this.resolveClassroom(
      row.classroom_raw || row.laboratory_raw,
      campusResolution.target_id,
      buildingResolution.target_id,
      catalog,
      issues,
      row.laboratory_raw ? 'laboratory' : 'classroom',
    );
    const section = parseSectionToken(row.section_raw);
    if (!section.section_code) {
      issues.push({
        severity: 'BLOCKING',
        issue_code: 'INVALID_SECTION',
        field_name: 'SECCIÓN',
        message: 'No se pudo interpretar la seccion del archivo.',
      });
    }
    if (!row.cycle_raw || row.cycle_raw < 1) {
      issues.push({
        severity: 'BLOCKING',
        issue_code: 'MISSING_CYCLE',
        field_name: 'CICLO',
        message: 'No se encontro un ciclo valido para la fila.',
      });
    }

    const schedule = this.resolveSchedule(row, issues);
    const offerTheoreticalHours = roundToTwo(row.theory_hours);
    const offerPracticalHours = roundToTwo(row.practical_hours);
    const offerTotalHours = roundToTwo(row.total_hours || row.theory_hours + row.practical_hours);
    const subsectionKind = deriveSubsectionKind(offerTheoreticalHours, offerPracticalHours);
    const canImport = !issues.some((issue) => issue.severity === 'BLOCKING');
    const scope =
      canImport &&
      semesterResolution.target_id &&
      vcPeriodResolution.target_id &&
      campusResolution.target_id &&
      facultyResolution.target_id &&
      programResolution.target_id &&
      studyPlanResolution.target_id &&
      row.cycle_raw
        ? {
            semester_id: semesterResolution.target_id,
            vc_period_id: vcPeriodResolution.target_id,
            campus_id: campusResolution.target_id,
            faculty_id: facultyResolution.target_id,
            academic_program_id: programResolution.target_id,
            study_plan_id: studyPlanResolution.target_id,
            cycle: row.cycle_raw,
            semester_name: semesterResolution.target_label,
            vc_period_name: vcPeriodResolution.target_label,
            campus_name: campusResolution.target_label,
            faculty_name: facultyResolution.target_label,
            academic_program_name: programResolution.target_label,
            study_plan_name: studyPlanResolution.target_label,
            study_plan_year: recordString(asRecord(studyPlanResolution.target_extra ?? {}), 'year'),
          }
        : null;
    const scopeKey = scope ? buildScopeKey(scope) : null;
    const resolution = {
      source_kind: 'EXCEL',
      source_system: 'EXCEL',
      semester_id: semesterResolution.target_id,
      vc_period_id: vcPeriodResolution.target_id,
      campus_id: campusResolution.target_id,
      faculty_id: facultyResolution.target_id,
      academic_program_id: programResolution.target_id,
      study_plan_id: studyPlanResolution.target_id,
      study_plan_course_id: courseResolution.target_id,
      teacher_id: teacherResolution.target_id,
      course_modality_id: modalityResolution.target_id,
      building_id: buildingResolution.target_id,
      classroom_id: classroomResolution.target_id,
      shift: shiftResolution.target_id,
      cycle: row.cycle_raw,
      scope_key: scopeKey,
      scope,
      section_base_code: section.section_code,
      explicit_subsection_code: section.explicit_subsection_code,
      import_section_code: null,
      import_subsection_code: null,
      external_section_code: null,
      source_section_id: null,
      source_course_id: null,
      source_term_id: null,
      is_cepea: section.is_cepea,
      offer_course_code:
        recordString(asRecord(courseResolution.target_extra ?? {}), 'course_code') ?? row.course_code_raw,
      offer_course_name:
        recordString(asRecord(courseResolution.target_extra ?? {}), 'course_name') ?? row.course_name_raw,
      offer_theoretical_hours: offerTheoreticalHours,
      offer_practical_hours: offerPracticalHours,
      offer_total_hours: offerTotalHours,
      offer_course_type: deriveCourseType(offerTheoreticalHours, offerPracticalHours),
      projected_vacancies: row.projected_vacancies,
      capacity_snapshot: row.projected_vacancies,
      subsection_kind: subsectionKind,
      assigned_theoretical_hours: offerTheoreticalHours,
      assigned_practical_hours: offerPracticalHours,
      assigned_total_hours: offerTotalHours,
      denomination:
        emptyToNull(row.denomination_raw) ??
        buildDenomination(
          recordString(asRecord(courseResolution.target_extra ?? {}), 'course_code') ?? row.course_code_raw,
          recordString(asRecord(courseResolution.target_extra ?? {}), 'course_name') ?? row.course_name_raw,
          section.section_code ?? '',
          section.explicit_subsection_code ?? section.section_code ?? '',
          campusResolution.target_id ?? '',
        ),
      schedule,
      mapping_resolution: {
        vc_period: vcPeriodResolution,
        campus: campusResolution,
        faculty_code: facultyResolution,
        academic_program_code: programResolution,
        study_plan_code: {
          ...studyPlanResolution,
          source_value: buildAliasSourceValue('study_plan_code', row.study_plan_code_raw, aliasContext),
          dependent_academic_programs: [
            {
              source_value: row.academic_program_code_raw,
              target_id: programResolution.target_id,
              target_label: programResolution.target_label,
              match_source: programResolution.match_source,
            },
          ],
        },
        course_code: {
          ...courseResolution,
          source_value: buildAliasSourceValue('course_code', row.course_code_raw, aliasContext),
          dependent_academic_programs: [
            {
              source_value: row.academic_program_code_raw,
              target_id: programResolution.target_id,
              target_label: programResolution.target_label,
              match_source: programResolution.match_source,
            },
          ],
          dependent_study_plans: [
            {
              source_value: row.study_plan_code_raw,
              target_id: studyPlanResolution.target_id,
              target_label: studyPlanResolution.target_label,
              match_source: studyPlanResolution.match_source,
            },
          ],
          dependent_cycles: row.cycle_raw ? [row.cycle_raw] : [],
        },
        course_modality: modalityResolution,
        shift: shiftResolution,
        building: buildingResolution,
        classroom: classroomResolution,
      },
    };

    return {
      row_number: row.row_number,
      source_json: row.raw_row,
      normalized_json: row as unknown as Record<string, unknown>,
      resolution_json: resolution as unknown as Record<string, unknown>,
      scope_key: scopeKey,
      row_hash: null,
      can_import: canImport,
      issues,
    };
  }

  private assignStructuralCodes(rows: PreviewRow[]) {
    const rowsByGroup = groupBy(
      rows.filter((row) => row.can_import),
      (row) => {
        const resolution = asRecord(row.resolution_json);
        return [
          recordString(resolution, 'scope_key'),
          recordString(resolution, 'study_plan_course_id'),
          recordString(resolution, 'section_base_code'),
        ].join('::');
      },
    );

    for (const groupRows of rowsByGroup.values()) {
      const usedCodes = new Set(
        groupRows
          .map((row) => asRecord(row.resolution_json))
          .map((resolution) => recordString(resolution, 'explicit_subsection_code'))
          .filter(Boolean),
      );
      const rowsWithExplicit = groupRows.filter((row) => {
        const resolution = asRecord(row.resolution_json);
        return Boolean(recordString(resolution, 'explicit_subsection_code'));
      });
      const rowsWithoutExplicit = groupRows
        .filter((row) => !rowsWithExplicit.includes(row))
        .sort((left, right) => left.row_number - right.row_number);

      for (const row of rowsWithExplicit) {
        const resolution = asRecord(row.resolution_json);
        const explicitSubsectionCode = recordString(resolution, 'explicit_subsection_code');
        const sectionBaseCode = recordString(resolution, 'section_base_code');
        resolution.import_section_code = sectionBaseCode;
        resolution.import_subsection_code = explicitSubsectionCode;
        row.row_hash = hashRowKey([
          recordString(resolution, 'scope_key'),
          recordString(resolution, 'study_plan_course_id'),
          sectionBaseCode,
          explicitSubsectionCode,
        ]);
      }

      let nextIndex = 1;
      for (let index = 0; index < rowsWithoutExplicit.length; index += 1) {
        const row = rowsWithoutExplicit[index];
        const resolution = asRecord(row.resolution_json);
        const sectionBaseCode = recordString(resolution, 'section_base_code');
        if (!sectionBaseCode) {
          continue;
        }
        let subsectionCode = sectionBaseCode;
        if (rowsWithoutExplicit.length > 1 || usedCodes.size > 0) {
          if (index === 0) {
            subsectionCode = sectionBaseCode;
          } else {
            while (usedCodes.has(`${sectionBaseCode}${nextIndex}`)) {
              nextIndex += 1;
            }
            subsectionCode = `${sectionBaseCode}${nextIndex}`;
            nextIndex += 1;
          }
        }
        usedCodes.add(subsectionCode);
        resolution.import_section_code = sectionBaseCode;
        resolution.import_subsection_code = subsectionCode;
        row.row_hash = hashRowKey([
          recordString(resolution, 'scope_key'),
          recordString(resolution, 'study_plan_course_id'),
          sectionBaseCode,
          subsectionCode,
        ]);
      }
    }
  }

  private applyScheduleWarnings(rows: PreviewRow[]) {
    const rowsBySubsection = groupBy(
      rows.filter((row) => row.can_import),
      (row) => row.row_hash ?? '__NO_HASH__',
    );

    for (const [key, groupRows] of rowsBySubsection.entries()) {
      if (key === '__NO_HASH__') {
        continue;
      }
      const seenSignatures = new Set<string>();
      let hasSelectedSchedule = false;
      for (const row of groupRows.sort((left, right) => left.row_number - right.row_number)) {
        const resolution = asRecord(row.resolution_json);
        const schedule = asRecord(recordValue(resolution, 'schedule'));
        const signature = recordString(schedule, 'signature');
        if (!signature) {
          continue;
        }
        if (!hasSelectedSchedule || !seenSignatures.has(signature)) {
          seenSignatures.add(signature);
          hasSelectedSchedule = true;
          continue;
        }
        row.issues.push({
          severity: 'WARNING',
          issue_code: 'DUPLICATED_SCHEDULE_SIGNATURE',
          field_name: 'HORARIO',
          message: 'Se detecto un horario repetido para el mismo grupo; solo se conservara una firma unica.',
        });
      }
    }
  }

  private resolveSemester(value: string, catalog: ImportCatalog, issues: PreviewIssue[]) {
    const semester = findSemesterByPeriodValue(catalog.semesters, value);
    if (!semester) {
      issues.push(missingCriticalIssue('semester', value, 'No se encontro el semestre del archivo.'));
    }
    return toResolution(value, semester?.id ?? null, semester?.name ?? null, semester ? 'catalog' : 'none');
  }

  private resolveVcPeriod(value: string, catalog: ImportCatalog, issues: PreviewIssue[]) {
    const alias = resolveAlias(catalog.aliasMap, 'vc_period', value);
    if (alias?.target_id) {
      const vcPeriod = catalog.vcPeriods.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(
        value,
        vcPeriod?.id ?? null,
        vcPeriod?.text ?? alias.target_label ?? null,
        vcPeriod ? 'alias' : 'none',
      );
    }
    const vcPeriod = findVcPeriodByValue(catalog.vcPeriods, value);
    if (!vcPeriod) {
      issues.push(missingCriticalIssue('vc_period', value, 'No se encontro el periodo VC del archivo.'));
    }
    return toResolution(value, vcPeriod?.id ?? null, vcPeriod?.text ?? null, vcPeriod ? 'catalog' : 'none');
  }

  private resolveCampus(value: string, catalog: ImportCatalog, issues: PreviewIssue[]) {
    const alias = resolveAlias(catalog.aliasMap, 'campus', value);
    if (alias?.target_id) {
      const campus = catalog.campuses.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(value, campus?.id ?? null, campus?.name ?? alias.target_label ?? null, campus ? 'alias' : 'none');
    }
    const normalized = normalizeLoose(value);
    const catalogCampus =
      catalog.campuses.find((item) => normalizeLoose(item.code) === normalized) ??
      catalog.campuses.find((item) => normalizeLoose(item.name) === normalized) ??
      null;
    if (catalogCampus) {
      return toResolution(value, catalogCampus.id, catalogCampus.name ?? null, 'catalog');
    }

    let campus: CampusEntity | null = null;
    if (!campus) {
      if (normalized === 'PRINCIPAL') {
        campus = catalog.campuses.find((item) => normalizeLoose(item.name).includes('SEDE CENTRAL')) ?? null;
      } else if (normalized === 'FILIAL') {
        campus = catalog.campuses.find((item) => normalizeLoose(item.name).includes('ICA')) ?? null;
      } else if (normalized === 'HUAURA') {
        campus = catalog.campuses.find((item) => normalizeLoose(item.name).includes('HUAURA')) ?? null;
      } else if (normalized === 'SUNAMPE') {
        campus = catalog.campuses.find((item) => normalizeLoose(item.name).includes('SUNAMPE')) ?? null;
      } else if (normalized === 'PORUMA') {
        campus = catalog.campuses.find((item) => normalizeLoose(item.name).includes('PORUMA')) ?? null;
      }
    }
    if (!campus) {
      issues.push(missingCriticalIssue('campus', value, 'No se encontro la sede/local del archivo.'));
    }
    return toResolution(value, campus?.id ?? null, campus?.name ?? null, campus ? 'heuristic' : 'none');
  }

  private resolveFaculty(value: string, catalog: ImportCatalog, issues: PreviewIssue[]) {
    const alias = resolveAlias(catalog.aliasMap, 'faculty_code', value);
    if (alias?.target_id) {
      const faculty = catalog.faculties.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(value, faculty?.id ?? null, faculty?.name ?? alias.target_label ?? null, faculty ? 'alias' : 'none');
    }
    const normalized = normalizeLoose(value);
    const faculty =
      catalog.faculties.find((item) => normalizeLoose(item.code) === normalized) ??
      catalog.faculties.find((item) => normalizeLoose(item.abbreviation) === normalized) ??
      catalog.faculties.find((item) => normalizeLoose(item.name) === normalized) ??
      null;
    if (!faculty) {
      issues.push(missingCriticalIssue('faculty_code', value, 'No se encontro la facultad del archivo.'));
    }
    return toResolution(value, faculty?.id ?? null, faculty?.name ?? null, faculty ? 'catalog' : 'none');
  }

  private resolveAcademicProgram(value: string, catalog: ImportCatalog, issues: PreviewIssue[]) {
    const alias = resolveAlias(catalog.aliasMap, 'academic_program_code', value);
    if (alias?.target_id) {
      const program = catalog.programs.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(value, program?.id ?? null, program?.name ?? alias.target_label ?? null, program ? 'alias' : 'none');
    }
    const normalized = normalizeLoose(value);
    const program =
      catalog.programs.find((item) => normalizeLoose(item.code) === normalized) ??
      catalog.programs.find((item) => normalizeLoose(item.name) === normalized) ??
      null;
    if (!program) {
      issues.push(
        missingCriticalIssue(
          'academic_program_code',
          value,
          'No se encontro el programa academico del archivo. Configura un mapeo para esta abreviatura.',
        ),
      );
    }
    return toResolution(value, program?.id ?? null, program?.name ?? null, program ? 'catalog' : 'none');
  }

  private findStudyPlansByCourseCode(
    courseCode: string,
    academicProgramId: string | null,
    cycle: number | null,
    catalog: ImportCatalog,
  ) {
    const normalizedCourseCode = normalizeCourseCodeValue(courseCode);
    if (!normalizedCourseCode) {
      return [];
    }

    const candidates = catalog.studyPlanCourses
      .map((item) => {
        const plan = catalog.studyPlanById.get(item.study_plan_id) ?? null;
        if (!plan) {
          return null;
        }
        if (academicProgramId && plan.academic_program_id !== academicProgramId) {
          return null;
        }
        const detail = catalog.studyPlanCourseDetailById.get(item.id) ?? null;
        const resolvedCycle = detail?.academic_year ?? extractCycleFromStudyPlanCourse(item) ?? null;
        return {
          plan,
          course: item,
          detail,
          resolvedCycle,
        };
      })
      .filter((item): item is NonNullable<typeof item> => Boolean(item));

    const uniquePlans = (items: typeof candidates) => uniqueById(items.map((item) => item.plan));
    const preferCycle = (items: typeof candidates) => {
      if (!cycle) {
        return items;
      }
      const cycleMatches = items.filter((item) => item.resolvedCycle === cycle);
      return cycleMatches.length > 0 ? cycleMatches : items;
    };

    const exactCourseCodeMatches = preferCycle(
      candidates.filter((item) => normalizeCourseCodeValue(item.course.course_code) === normalizedCourseCode),
    );
    if (exactCourseCodeMatches.length > 0) {
      return uniquePlans(exactCourseCodeMatches);
    }

    const exactShortCodeMatches = preferCycle(
      candidates.filter(
        (item) => normalizeCourseCodeValue(item.detail?.short_code) === normalizedCourseCode,
      ),
    );
    if (exactShortCodeMatches.length > 0) {
      return uniquePlans(exactShortCodeMatches);
    }

    if (!cycle) {
      return [];
    }

    const fuzzyMatches = candidates.filter((item) => {
      if (item.resolvedCycle !== cycle) {
        return false;
      }
      const candidateCodes = [item.detail?.short_code, item.course.course_code].filter(
        (candidate): candidate is string => Boolean(candidate),
      );
      return candidateCodes.some((candidate) => courseCodeLikeMatch(candidate, normalizedCourseCode));
    });

    return uniquePlans(fuzzyMatches);
  }

  private resolveStudyPlan(
    value: string,
    academicProgramId: string | null,
    courseCode: string,
    cycle: number | null,
    aliasContext: AliasSourceContext,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    const contextualSourceValue = buildAliasSourceValue('study_plan_code', value, aliasContext);
    const alias =
      resolveAlias(catalog.aliasMap, 'study_plan_code', contextualSourceValue) ??
      resolveAlias(catalog.aliasMap, 'study_plan_code', value);
    if (alias?.target_id) {
      const plan = catalog.studyPlans.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(
        contextualSourceValue || value,
        plan?.id ?? null,
        [plan?.year, plan?.career, plan?.name].filter(Boolean).join(' - ') || alias.target_label || null,
        plan ? 'alias' : 'none',
        { year: plan?.year ?? null },
      );
    }
    const directPlan = catalog.studyPlans.find((item) => item.id === value) ?? null;
    if (directPlan) {
      return toResolution(
        contextualSourceValue || value,
        directPlan.id,
        [directPlan.year, directPlan.career, directPlan.name].filter(Boolean).join(' - ') || null,
        'catalog',
        { year: directPlan.year ?? null },
      );
    }
    const normalized = normalizeStudyPlanCode(value);
    const normalizedName = normalizeLoose(value);
    const candidates = catalog.studyPlans.filter((item) => {
      if (academicProgramId && item.academic_program_id !== academicProgramId) {
        return false;
      }
      const year = normalizeStudyPlanCode(item.year);
      const name = normalizeLoose(item.name);
      return (
        Boolean(year) && (year === normalized || year.endsWith(`-${normalized}`)) ||
        Boolean(normalizedName) && name === normalizedName
      );
    });
    const unique = uniqueById(candidates);
    if (unique.length === 1) {
      const plan = unique[0];
      return toResolution(
        contextualSourceValue || value,
        plan.id,
        [plan.year, plan.career, plan.name].filter(Boolean).join(' - ') || null,
        'catalog',
        { year: plan.year ?? null },
      );
    }

    const inferredPlans = this.findStudyPlansByCourseCode(courseCode, academicProgramId, cycle, catalog);
    if (inferredPlans.length === 1) {
      const plan = inferredPlans[0];
      return toResolution(
        contextualSourceValue || value || courseCode,
        plan.id,
        [plan.year, plan.career, plan.name].filter(Boolean).join(' - ') || null,
        'catalog',
        {
          year: plan.year ?? null,
          inferred_by: 'course_code_like',
          course_code: courseCode || null,
          cycle: cycle ?? null,
        },
      );
    }

    if (unique.length !== 1) {
      issues.push(
        missingCriticalIssue(
          'study_plan_code',
          contextualSourceValue || value,
          unique.length > 1
            ? 'El codigo de plan coincide con mas de un plan de estudios; agrega un mapeo explicito.'
            : inferredPlans.length > 1
              ? 'El codigo del curso coincide con mas de un plan de estudios para ese programa y ciclo; agrega o corrige el mapeo del plan.'
              : 'No se encontro el plan de estudios del archivo.',
        ),
      );
      return toResolution(contextualSourceValue || value, null, null, 'none');
    }
    return toResolution(contextualSourceValue || value, null, null, 'none');
  }

  private resolveStudyPlanCourse(
    courseCode: string,
    courseName: string,
    studyPlanId: string | null,
    cycle: number | null,
    aliasContext: AliasSourceContext,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    const contextualSourceValue = buildAliasSourceValue('course_code', courseCode || courseName, aliasContext);
    if (!studyPlanId) {
      return toResolution(contextualSourceValue || courseCode || courseName, null, null, 'none');
    }
    if (!cycle) {
      return toResolution(contextualSourceValue || courseCode || courseName, null, null, 'none');
    }
    const candidates = catalog.studyPlanCourses
      .filter((item) => item.study_plan_id === studyPlanId)
      .map((item) => {
        const detail = catalog.studyPlanCourseDetailById.get(item.id) ?? null;
        const resolvedCycle = detail?.academic_year ?? extractCycleFromStudyPlanCourse(item) ?? null;
        if (resolvedCycle !== cycle) {
          return null;
        }
        return {
          course: item,
          detail,
          resolvedCycle,
          sourceCourseCode: item.course_code ?? null,
          shortCourseCode: detail?.short_code ?? null,
          resolvedCourseCode: item.course_code ?? detail?.short_code ?? null,
          resolvedCourseName: detail?.name ?? item.course_name ?? null,
        };
      })
      .filter((item): item is NonNullable<typeof item> => Boolean(item));
    const alias = resolveAlias(catalog.aliasMap, 'course_code', contextualSourceValue);
    if (alias?.target_id) {
      const aliased = candidates.find((item) => item.course.id === alias.target_id) ?? null;
      if (aliased) {
        return toResolution(
          contextualSourceValue || courseCode || courseName,
          aliased.course.id,
          [aliased.resolvedCourseCode, aliased.resolvedCourseName].filter(Boolean).join(' - ') || alias.target_label || null,
          'alias',
          {
            course_code: aliased.resolvedCourseCode ?? null,
            course_name: aliased.resolvedCourseName ?? null,
            cycle: aliased.resolvedCycle,
          },
        );
      }
    }
    const normalizedCourseCode = normalizeCourseCodeValue(courseCode);
    const rawCourseCode = asNullableString(courseCode);
    if (rawCourseCode) {
      const directById = candidates.filter((item) => item.course.id === rawCourseCode);
      if (directById.length === 1) {
        return toResolution(
          contextualSourceValue || courseCode || courseName,
          directById[0].course.id,
          [directById[0].resolvedCourseCode, directById[0].resolvedCourseName].filter(Boolean).join(' - ') || null,
          'catalog',
          {
            course_code: directById[0].resolvedCourseCode ?? null,
            course_name: directById[0].resolvedCourseName ?? null,
            cycle: directById[0].resolvedCycle,
          },
        );
      }
    }
    if (normalizedCourseCode) {
      const directByCourseCode = candidates.filter(
        (item) => normalizeCourseCodeValue(item.sourceCourseCode) === normalizedCourseCode,
      );
      if (directByCourseCode.length === 1) {
        return toResolution(
          contextualSourceValue || courseCode || courseName,
          directByCourseCode[0].course.id,
          [directByCourseCode[0].resolvedCourseCode, directByCourseCode[0].resolvedCourseName].filter(Boolean).join(' - ') || null,
          'catalog',
          {
            course_code: directByCourseCode[0].resolvedCourseCode ?? null,
            course_name: directByCourseCode[0].resolvedCourseName ?? null,
            cycle: directByCourseCode[0].resolvedCycle,
          },
        );
      }
      if (directByCourseCode.length > 1) {
        issues.push({
          severity: 'BLOCKING',
          issue_code: 'AMBIGUOUS_STUDY_PLAN_COURSE',
          field_name: 'CODIGO',
          message: `El codigo de curso coincide con mas de un curso del ciclo ${cycle} en el plan de estudios.`,
          meta_json: {
            namespace: 'course_code',
            source_value: contextualSourceValue || courseCode,
          },
        });
        return toResolution(contextualSourceValue || courseCode, null, null, 'none');
      }

      const directByShortCode = candidates.filter(
        (item) => normalizeCourseCodeValue(item.shortCourseCode) === normalizedCourseCode,
      );
      if (directByShortCode.length === 1) {
        return toResolution(
          contextualSourceValue || courseCode || courseName,
          directByShortCode[0].course.id,
          [directByShortCode[0].resolvedCourseCode, directByShortCode[0].resolvedCourseName]
            .filter(Boolean)
            .join(' - ') || null,
          'catalog',
          {
            course_code: directByShortCode[0].resolvedCourseCode ?? null,
            course_name: directByShortCode[0].resolvedCourseName ?? null,
            cycle: directByShortCode[0].resolvedCycle,
          },
        );
      }
      if (directByShortCode.length > 1) {
        issues.push({
          severity: 'BLOCKING',
          issue_code: 'AMBIGUOUS_STUDY_PLAN_COURSE',
          field_name: 'CODIGO',
          message: `El codigo corto del curso coincide con mas de un curso del ciclo ${cycle} en el plan de estudios.`,
          meta_json: {
            namespace: 'course_code',
            source_value: contextualSourceValue || courseCode,
          },
        });
        return toResolution(contextualSourceValue || courseCode, null, null, 'none');
      }

      const likeMatches = candidates.filter((item) =>
        courseCodeLikeMatch(item.sourceCourseCode, normalizedCourseCode) ||
        courseCodeLikeMatch(item.shortCourseCode, normalizedCourseCode),
      );
      if (likeMatches.length === 1) {
        return toResolution(
          contextualSourceValue || courseCode || courseName,
          likeMatches[0].course.id,
          [likeMatches[0].resolvedCourseCode, likeMatches[0].resolvedCourseName]
            .filter(Boolean)
            .join(' - ') || null,
          'heuristic',
          {
            course_code: likeMatches[0].resolvedCourseCode ?? null,
            course_name: likeMatches[0].resolvedCourseName ?? null,
            cycle: likeMatches[0].resolvedCycle,
            matched_by: 'course_code_like',
          },
        );
      }
      if (likeMatches.length > 1) {
        issues.push({
          severity: 'BLOCKING',
          issue_code: 'AMBIGUOUS_STUDY_PLAN_COURSE',
          field_name: 'CODIGO',
          message: `El codigo de curso coincide de forma parcial con mas de un curso del ciclo ${cycle} en el plan de estudios.`,
          meta_json: {
            namespace: 'course_code',
            source_value: contextualSourceValue || courseCode,
          },
        });
        return toResolution(contextualSourceValue || courseCode, null, null, 'none');
      }
    }

    const normalizedCourseName = normalizeCourseName(courseName);
    const fallback = candidates.filter(
      (item) => normalizeCourseName(item.resolvedCourseName) === normalizedCourseName,
    );
    if (fallback.length === 1) {
      return toResolution(
        contextualSourceValue || courseCode || courseName,
        fallback[0].course.id,
        [fallback[0].resolvedCourseCode, fallback[0].resolvedCourseName].filter(Boolean).join(' - ') || null,
        'catalog',
        {
          course_code: fallback[0].resolvedCourseCode ?? null,
          course_name: fallback[0].resolvedCourseName ?? null,
          cycle: fallback[0].resolvedCycle,
        },
      );
    }
    if (fallback.length > 1) {
      issues.push({
        severity: 'BLOCKING',
        issue_code: 'AMBIGUOUS_STUDY_PLAN_COURSE',
        field_name: 'NOMBRE DE CURSO',
        message: `El nombre del curso coincide con mas de un curso del ciclo ${cycle} en el plan de estudios.`,
        meta_json: {
          namespace: 'course_code',
          source_value: contextualSourceValue || courseCode || courseName,
        },
      });
      return toResolution(contextualSourceValue || courseCode || courseName, null, null, 'none');
    }
    issues.push({
      severity: 'BLOCKING',
      issue_code: 'MISSING_STUDY_PLAN_COURSE',
      field_name: 'CODIGO',
      message: `No se encontro el curso dentro del ciclo ${cycle} del plan de estudios.`,
      meta_json: {
        namespace: 'course_code',
        source_value: contextualSourceValue || courseCode || courseName,
      },
    });
    return toResolution(contextualSourceValue || courseCode || courseName, null, null, 'none');
  }

  private resolveTeacher(
    dni: string,
    teacherName: string,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    const normalizedDni = normalizeDni(dni);
    const teacherLabel = [dni, teacherName].filter(Boolean).join(' | ');
    if (!normalizedDni || normalizedDni === 'NUEVO') {
      if (teacherName || dni) {
        issues.push({
          severity: 'WARNING',
          issue_code: 'UNMATCHED_TEACHER',
          field_name: 'DNI',
          message: teacherLabel
            ? `No se pudo asignar docente (${teacherLabel}); la estructura se importara con docente en null.`
            : 'No se pudo asignar docente; la estructura se importara con docente en null.',
        });
      }
      return toResolution(dni || teacherName, null, null, 'none');
    }
    const teacher = catalog.teachers.find((item) => normalizeDni(item.dni) === normalizedDni) ?? null;
    if (!teacher) {
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_TEACHER',
        field_name: 'DNI',
        message: teacherLabel
          ? `El docente ${teacherLabel} no existe en catalogo; la estructura se importara con docente en null.`
          : 'El DNI del docente no existe en catalogo; la estructura se importara con docente en null.',
      });
    }
    return toResolution(
      dni || teacherName,
      teacher?.id ?? null,
      teacher?.full_name ?? teacherName ?? null,
      teacher ? 'catalog' : 'none',
    );
  }

  private resolveCourseModality(value: string, catalog: ImportCatalog, issues: PreviewIssue[]) {
    const alias = resolveAlias(catalog.aliasMap, 'course_modality', value);
    if (alias?.target_id) {
      const modality = catalog.courseModalities.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(value, modality?.id ?? null, modality?.name ?? alias.target_label ?? null, modality ? 'alias' : 'none');
    }
    const normalized = normalizeLoose(value);
    let modality =
      catalog.courseModalities.find((item) => normalizeLoose(item.name) === normalized) ?? null;
    if (!modality && normalized) {
      if (normalized.includes('HIBRIDO') && normalized.includes('VIRTUAL')) {
        modality = catalog.courseModalities.find((item) => normalizeLoose(item.name) === 'HIBRIDO VIRTUAL') ?? null;
      } else if (normalized.includes('HIBRIDO') || normalized.includes('HIBRIDA')) {
        modality = catalog.courseModalities.find((item) => normalizeLoose(item.name) === 'HIBRIDO PRESENCIAL') ?? null;
      } else if (normalized.includes('VIRTUAL') || normalized.includes('DISTANCIA')) {
        modality = catalog.courseModalities.find((item) => normalizeLoose(item.name) === 'VIRTUAL') ?? null;
      } else if (normalized.includes('PRESENCIAL')) {
        modality = catalog.courseModalities.find((item) => normalizeLoose(item.name) === 'PRESENCIAL') ?? null;
      }
    }
    if (!modality && normalized) {
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_COURSE_MODALITY',
        field_name: 'MODALIDAD',
        message: `No se pudo resolver la modalidad del curso (${value}); se importara en null.`,
        meta_json: {
          namespace: 'course_modality',
          source_value: value,
        },
      });
    }
    return toResolution(value, modality?.id ?? null, modality?.name ?? null, modality ? 'heuristic' : 'none');
  }

  private resolveShift(value: string, catalog: ImportCatalog, issues: PreviewIssue[]) {
    const alias = resolveAlias(catalog.aliasMap, 'shift', value);
    if (alias?.target_id) {
      return toResolution(value, alias.target_id, alias.target_label ?? alias.target_id, 'alias');
    }
    const normalized = normalizeShiftValue(value);
    if (!normalized) {
      return toResolution(value, null, null, 'none');
    }
    if (SHIFT_OPTIONS.includes(normalized as (typeof SHIFT_OPTIONS)[number])) {
      return toResolution(value, normalized, normalized, 'heuristic');
    }
    issues.push({
      severity: 'WARNING',
      issue_code: 'UNMATCHED_SHIFT',
      field_name: 'TURNO',
      message: `No se pudo resolver el turno (${value}); se importara en null.`,
      meta_json: {
        namespace: 'shift',
        source_value: value,
      },
    });
    return toResolution(value, null, null, 'none');
  }

  private resolveBuilding(
    value: string,
    campusId: string | null,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ) {
    if (!value) {
      return toResolution(value, null, null, 'none');
    }
    const alias = resolveAlias(catalog.aliasMap, 'building', value);
    if (alias?.target_id) {
      const building = catalog.buildings.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(value, building?.id ?? null, building?.name ?? alias.target_label ?? null, building ? 'alias' : 'none');
    }
    const normalizedCandidates = buildLocationCandidateLabels(value).map((item) => normalizeLoose(item));
    const candidates = catalog.buildings.filter((item) => !campusId || item.campus_id === campusId);
    const building =
      candidates.find((item) => normalizedCandidates.includes(normalizeLoose(item.name))) ??
      null;
    if (!building) {
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_BUILDING',
        field_name: 'PABELLON',
        message: `No se pudo resolver el pabellon (${value}); se importara en null.`,
        meta_json: {
          namespace: 'building',
          source_value: value,
        },
      });
    }
    return toResolution(value, building?.id ?? null, building?.name ?? null, building ? 'catalog' : 'none');
  }

  private resolveClassroom(
    value: string,
    campusId: string | null,
    buildingId: string | null,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
    namespace: 'classroom' | 'laboratory',
  ) {
    if (!value) {
      return toResolution(value, null, null, 'none');
    }
    const alias = resolveAlias(catalog.aliasMap, namespace, value);
    if (alias?.target_id) {
      const classroom = catalog.classrooms.find((item) => item.id === alias.target_id) ?? null;
      return toResolution(value, classroom?.id ?? null, classroom?.name ?? alias.target_label ?? null, classroom ? 'alias' : 'none');
    }
    const classroomMatch = this.findCatalogClassroomMatch(value, catalog, {
      campusId,
      buildingId,
      allowGlobalUniqueFallback: true,
    });
    const classroom = classroomMatch.classroom;
    if (!classroom) {
      issues.push({
        severity: 'WARNING',
        issue_code: namespace === 'laboratory' ? 'UNMATCHED_LABORATORY' : 'UNMATCHED_CLASSROOM',
        field_name: namespace === 'laboratory' ? 'LABORATORIO' : 'AULA',
        message:
          namespace === 'laboratory'
            ? `No se pudo resolver el laboratorio (${value}); se importara en null.`
            : `No se pudo resolver el aula (${value}); se importara en null.`,
        meta_json: {
          namespace,
          source_value: value,
        },
      });
    }
    return toResolution(
      value,
      classroom?.id ?? null,
      classroom?.name ?? null,
      classroom ? classroomMatch.match_source : 'none',
    );
  }

  private findTeacherByNormalizedName(teachers: TeacherEntity[], normalizedTeacherName: string) {
    if (!normalizedTeacherName) {
      return null;
    }

    const exactMatches = teachers.filter((item) =>
      this.teacherLookupLabels(item).some((label) => label === normalizedTeacherName),
    );
    if (exactMatches.length === 1) {
      return exactMatches[0];
    }

    const inclusiveMatches = teachers.filter((item) =>
      this.teacherLookupLabels(item).some(
        (label) => label.includes(normalizedTeacherName) || normalizedTeacherName.includes(label),
      ),
    );
    if (inclusiveMatches.length === 1) {
      return inclusiveMatches[0];
    }

    const tokenMatches = teachers.filter((item) => {
      const labels = this.teacherLookupLabels(item);
      return labels.some((label) => teacherNamesLookEquivalent(label, normalizedTeacherName));
    });
    return tokenMatches.length === 1 ? tokenMatches[0] : null;
  }

  private findTeacherCatalogMatch(
    teachers: TeacherEntity[],
    externalTeacherId: string | null,
    teacherDni: string | null,
    teacherName: string | null,
    teacherUsername: string | null,
    teacherEmail: string | null,
  ) {
    const normalizedExternalId = normalizeLoose(externalTeacherId);
    const normalizedDni = normalizeDni(teacherDni ?? null);
    const normalizedUsername = normalizeLoose(teacherUsername);
    const normalizedEmail = normalizeLoose(teacherEmail);
    const emailLocalPart = normalizedEmail?.includes('@') ? normalizedEmail.split('@')[0] : normalizedEmail;
    const byDirect =
      teachers.find((item) => {
        if (normalizedExternalId) {
          const teacherIdMatches =
            normalizeLoose(item.id) === normalizedExternalId ||
            normalizeLoose(item.user_name) === normalizedExternalId ||
            normalizeLoose(item.institutional_email) === normalizedExternalId ||
            normalizeLoose(item.institutional_email)?.split('@')[0] === normalizedExternalId;
          if (teacherIdMatches) {
            return true;
          }
        }
        if (normalizedDni && normalizeDni(item.dni) === normalizedDni) {
          return true;
        }
        if (normalizedUsername && normalizeLoose(item.user_name) === normalizedUsername) {
          return true;
        }
        if (normalizedEmail) {
          const teacherCatalogEmail = normalizeLoose(item.institutional_email);
          if (
            teacherCatalogEmail === normalizedEmail ||
            teacherCatalogEmail?.split('@')[0] === emailLocalPart
          ) {
            return true;
          }
        }
        return false;
      }) ?? null;
    if (byDirect) {
      return byDirect;
    }

    const normalizedTeacherName = normalizeLoose(teacherName);
    const byName = normalizedTeacherName
      ? this.findTeacherByNormalizedName(teachers, normalizedTeacherName)
      : null;
    if (byName) {
      return byName;
    }

    return emailLocalPart || normalizedUsername
      ? teachers.find((item) => {
          const teacherUser = normalizeLoose(item.user_name);
          const teacherEmailLocalPart = normalizeLoose(item.institutional_email)?.split('@')[0];
          return (
            (normalizedUsername && teacherUser === normalizedUsername) ||
            (emailLocalPart && teacherEmailLocalPart === emailLocalPart)
          );
        }) ?? null
      : null;
  }

  private async findTeacherFromIntranetSearch(
    externalTeacherId: string | null,
    teacherDni: string | null,
    teacherName: string | null,
    teacherUsername: string | null,
    teacherEmail: string | null,
    catalog: ImportCatalog,
  ) {
    const emailLocalPart = normalizeLoose(teacherEmail)?.split('@')[0] ?? null;
    const searchTerms = [
      teacherName,
      normalizeDni(teacherDni ?? null),
      teacherUsername,
      emailLocalPart,
      externalTeacherId,
    ]
      .map((item) => `${item ?? ''}`.replace(/\s+/g, ' ').trim())
      .filter((item, index, all) => item && all.indexOf(item) === index);

    for (const searchTerm of searchTerms) {
      if (searchTerm.length < 3 && !/^\d{8,}$/.test(searchTerm)) {
        continue;
      }
      const remoteTeachers = await this.searchTeachersInIntranet(searchTerm, catalog);
      if (!remoteTeachers.length) {
        continue;
      }
      const matchedRemoteTeacher = this.findTeacherCatalogMatch(
        remoteTeachers,
        externalTeacherId,
        teacherDni,
        teacherName,
        teacherUsername,
        teacherEmail,
      );
      if (!matchedRemoteTeacher) {
        continue;
      }
      const resolvedLocalTeacher = this.findTeacherCatalogMatch(
        catalog.teachers,
        matchedRemoteTeacher.id,
        matchedRemoteTeacher.dni,
        teacherName ?? matchedRemoteTeacher.full_name ?? matchedRemoteTeacher.name,
        matchedRemoteTeacher.user_name,
        matchedRemoteTeacher.institutional_email,
      );
      return resolvedLocalTeacher ?? matchedRemoteTeacher;
    }

    return null;
  }

  private async searchTeachersInIntranet(searchTerm: string, catalog: ImportCatalog) {
    const normalizedSearch = normalizeLoose(searchTerm);
    if (!normalizedSearch) {
      return [];
    }
    const cached = catalog.intranetTeacherSearchCache.get(normalizedSearch);
    if (cached) {
      return cached;
    }

    for (const sourceCode of ['INTRANET', 'DOCENTE']) {
      try {
        const rows = await this.settingsSyncService.fetchSourceRowsByCode(
          sourceCode,
          '/admin/docentes/get',
          {
            start: '0',
            length: '50',
            search: searchTerm,
          },
          { skipProbe: true },
        );
        const teachers = rows
          .map((row) => this.mapTeacherRow(row))
          .filter((item) => Boolean(item.id));
        catalog.intranetTeacherSearchCache.set(normalizedSearch, teachers);
        return teachers;
      } catch {
        continue;
      }
    }

    catalog.intranetTeacherSearchCache.set(normalizedSearch, []);
    return [];
  }

  private mapTeacherRow(row: Record<string, unknown>): TeacherEntity {
    return {
      id: asString(pick(row, 'id')),
      dni: asNullableString(pick(row, 'dni')),
      paternal_surname: asNullableString(pick(row, 'paternalSurName', 'paternalSurname')),
      maternal_surname: asNullableString(pick(row, 'maternalSurName', 'maternalSurname')),
      name: asNullableString(pick(row, 'name')),
      full_name: asNullableString(pick(row, 'fullname', 'fullName')),
      phone_number: asNullableString(pick(row, 'phoneNumber', 'phone')),
      picture: asNullableString(pick(row, 'picture')),
      user_name: asNullableString(pick(row, 'username', 'userName')),
      institutional_email: asNullableString(pick(row, 'institutionalEmail', 'email')),
    };
  }

  private teacherLookupLabels(teacher: TeacherEntity) {
    const labels = [
      teacher.full_name,
      teacher.name,
      [teacher.paternal_surname, teacher.maternal_surname, teacher.name].filter(Boolean).join(' '),
      [teacher.maternal_surname, teacher.paternal_surname, teacher.name].filter(Boolean).join(' '),
      teacher.user_name,
      teacher.institutional_email,
      teacher.institutional_email?.split('@')[0] ?? null,
    ];
    return [...new Set(labels.map((item) => normalizeLoose(item)).filter(Boolean))];
  }

  private teacherDisplayName(teacher: TeacherEntity) {
    return firstNonEmpty([
      teacher.full_name,
      [teacher.paternal_surname, teacher.maternal_surname, teacher.name].filter(Boolean).join(' '),
      teacher.name,
      teacher.user_name,
    ]);
  }

  private resolveSchedule(row: NormalizedImportRow, issues: PreviewIssue[]) {
    const hasScheduleValues =
      Boolean(row.day_raw) ||
      Boolean(row.start_hour_raw) ||
      Boolean(row.start_minute_raw) ||
      Boolean(row.end_hour_raw) ||
      Boolean(row.end_minute_raw);
    if (!hasScheduleValues) {
      return null;
    }
    try {
      const dayOfWeek = parseDayOfWeek(row.day_raw);
      if (!dayOfWeek) {
        throw new Error('Dia invalido.');
      }
      const startTime = buildTimeFromParts(row.start_hour_raw, row.start_minute_raw);
      const endTime = buildTimeFromParts(row.end_hour_raw, row.end_minute_raw);
      const durationMinutes = computeMinutesFromTimes(startTime, endTime);
      return {
        day_of_week: dayOfWeek,
        start_time: startTime,
        end_time: endTime,
        duration_minutes: durationMinutes,
        academic_hours: roundToTwo(durationMinutes / 50),
        signature: `${dayOfWeek}|${startTime}|${endTime}`,
      } as ParsedSchedule;
    } catch (error) {
      issues.push({
        severity: 'WARNING',
        issue_code: 'INVALID_SCHEDULE',
        field_name: 'HORARIO',
        message:
          error instanceof Error
            ? `No se pudo importar el horario: ${error.message}`
            : 'No se pudo importar el horario.',
      });
      return null;
    }
  }

  private scopeFromResolution(resolution: Record<string, unknown> | null) {
    if (!resolution) {
      return null;
    }
    const scope = asRecord(recordValue(resolution, 'scope'));
    if (scope && Object.keys(scope).length > 0) {
      return {
        semester_id: recordString(scope, 'semester_id')!,
        vc_period_id: recordString(scope, 'vc_period_id'),
        campus_id: recordString(scope, 'campus_id')!,
        faculty_id: recordString(scope, 'faculty_id')!,
        academic_program_id: recordString(scope, 'academic_program_id')!,
        study_plan_id: recordString(scope, 'study_plan_id')!,
        cycle: recordNumber(scope, 'cycle'),
        semester_name: recordString(scope, 'semester_name'),
        vc_period_name: recordString(scope, 'vc_period_name'),
        campus_name: recordString(scope, 'campus_name'),
        faculty_name: recordString(scope, 'faculty_name'),
        academic_program_name: recordString(scope, 'academic_program_name'),
        study_plan_name: recordString(scope, 'study_plan_name'),
        study_plan_year: recordString(scope, 'study_plan_year'),
      } satisfies ImportScope;
    }
    const semesterId = recordString(resolution, 'semester_id');
    const vcPeriodId = recordString(resolution, 'vc_period_id');
    const campusId = recordString(resolution, 'campus_id');
    const facultyId = recordString(resolution, 'faculty_id');
    const academicProgramId = recordString(resolution, 'academic_program_id');
    const studyPlanId = recordString(resolution, 'study_plan_id');
    const cycle = recordNumberOrNull(resolution, 'cycle');
    if (!semesterId || !campusId || !facultyId || !academicProgramId || !studyPlanId || !cycle) {
      return null;
    }
    return {
      semester_id: semesterId,
      vc_period_id: vcPeriodId,
      campus_id: campusId,
      faculty_id: facultyId,
      academic_program_id: academicProgramId,
      study_plan_id: studyPlanId,
      cycle,
      semester_name: null,
      vc_period_name: null,
      campus_name: null,
      faculty_name: null,
      academic_program_name: null,
      study_plan_name: null,
      study_plan_year: null,
    } satisfies ImportScope;
  }

  private async loadImportCatalog(): Promise<ImportCatalog> {
    const [
      semesters,
      vcPeriods,
      campuses,
      faculties,
      programs,
      studyPlans,
      studyPlanCourses,
      studyPlanCourseDetails,
      teachers,
      courseModalities,
      studyTypes,
      buildings,
      classrooms,
      campusVcLocations,
      aliasMappings,
    ] = await Promise.all([
      this.semestersRepo.find({
        select: { id: true, name: true },
        order: { name: 'DESC' },
      }),
      this.vcPeriodsRepo.find({
        select: { id: true, text: true, is_active: true },
        where: { is_active: true },
        order: { text: 'DESC' },
      }),
      this.campusesRepo.find({
        select: { id: true, code: true, name: true },
        order: { name: 'ASC' },
      }),
      this.facultiesRepo.find({
        select: { id: true, code: true, name: true, abbreviation: true },
        order: { name: 'ASC' },
      }),
      this.programsRepo.find({
        select: { id: true, code: true, name: true, faculty_id: true, faculty: true },
        order: { name: 'ASC' },
      }),
      this.studyPlansRepo.find({
        select: {
          id: true,
          name: true,
          faculty: true,
          career: true,
          academic_program: true,
          year: true,
          faculty_id: true,
          academic_program_id: true,
        },
        order: { career: 'ASC', year: 'ASC' },
      }),
      this.studyPlanCoursesRepo.find({
        select: {
          id: true,
          study_plan_id: true,
          order: true,
          year_label: true,
          course_code: true,
          course_name: true,
          academic_program: true,
        },
        order: { study_plan_id: 'ASC', order: 'ASC', course_code: 'ASC' },
      }),
      this.studyPlanCourseDetailsRepo.find({
        select: {
          study_plan_course_id: true,
          short_code: true,
          name: true,
          academic_year: true,
        },
      }),
      this.teachersRepo.find({
        select: {
          id: true,
          dni: true,
          name: true,
          full_name: true,
          paternal_surname: true,
          maternal_surname: true,
          user_name: true,
          institutional_email: true,
        },
        order: { full_name: 'ASC' },
      }),
      this.courseModalitiesRepo.find({
        select: { id: true, code: true, name: true, is_active: true },
        where: { is_active: true },
        order: { name: 'ASC' },
      }),
      this.studyTypesRepo.find({
        select: { id: true, code: true, name: true, is_active: true },
        where: { is_active: true },
        order: { name: 'ASC' },
      }),
      this.buildingsRepo.find({
        select: { id: true, campus_id: true, name: true },
        order: { name: 'ASC' },
      }),
      this.classroomsRepo.find({
        select: {
          id: true,
          name: true,
          building_id: true,
          campus_id: true,
          capacity: true,
          code: true,
        },
        order: { name: 'ASC' },
      }),
      this.campusVcLocationMappingsRepo.find({
        select: { id: true, campus_id: true, vc_location_code: true },
        order: { campus_id: 'ASC' },
      }),
      this.importAliasMappingsRepo.find({
        select: {
          id: true,
          namespace: true,
          source_value: true,
          target_id: true,
          target_label: true,
          is_active: true,
          notes: true,
        },
        where: { is_active: true },
        order: { namespace: 'ASC', source_value: 'ASC' },
      }),
    ]);
    const derivedCatalog = buildStudyPlanCatalogForImport(studyPlans, faculties, programs);

    return {
      semesters,
      vcPeriods,
      campuses,
      faculties: derivedCatalog.faculties,
      programs: derivedCatalog.academicPrograms,
      studyPlans: derivedCatalog.studyPlans,
      studyPlanById: new Map(derivedCatalog.studyPlans.map((item) => [item.id, item] as const)),
      studyPlanCourses,
      studyPlanCourseDetails,
      studyPlanCourseDetailById: new Map(
        studyPlanCourseDetails.map((item) => [item.study_plan_course_id, item] as const),
      ),
      teachers,
      courseModalities,
      studyTypes,
      buildings,
      classrooms,
      campusVcLocations,
      defaultStudyTypeId: studyTypes[0]?.id ?? null,
      aliasMap: buildAliasMap(aliasMappings),
      teacherMatchCache: new Map(),
      intranetTeacherSearchCache: new Map(),
    };
  }

  private async requireEntity<T extends ObjectLiteral & { id: string }>(
    repo: Repository<T>,
    id: string,
    label: string,
  ) {
    const entity = await repo.findOne({ where: { id } as any });
    if (!entity) {
      throw new NotFoundException(`No existe ${label} ${id}.`);
    }
    return entity;
  }

  private async requireBatch(batchId: string) {
    const batch = await this.importBatchesRepo.findOne({ where: { id: batchId } });
    if (!batch) {
      throw new NotFoundException(`No existe un batch de importacion ${batchId}.`);
    }
    return batch;
  }

  private buildPlanRuleLogContext(rule: PlanningCyclePlanRuleEntity) {
    return {
      semester_id: rule.semester_id,
      vc_period_id: rule.vc_period_id ?? null,
      campus_id: rule.campus_id ?? null,
      faculty_id: rule.faculty_id ?? null,
      academic_program_id: rule.academic_program_id,
      study_plan_id: rule.study_plan_id,
      cycle: rule.cycle,
    };
  }

  private buildOfferLogContext(offer: PlanningOfferEntity) {
    return {
      offer_id: offer.id,
      semester_id: offer.semester_id,
      vc_period_id: offer.vc_period_id ?? null,
      campus_id: offer.campus_id,
      faculty_id: offer.faculty_id ?? null,
      academic_program_id: offer.academic_program_id ?? null,
      study_plan_id: offer.study_plan_id,
      course_code: offer.course_code ?? null,
      course_name: offer.course_name ?? null,
      cycle: offer.cycle,
    };
  }

  private buildSectionLogContext(section: PlanningSectionEntity, offer: PlanningOfferEntity) {
    return {
      ...this.buildOfferLogContext(offer),
      section_id: section.id,
      section_code: section.code,
    };
  }

  private buildSubsectionLogContext(
    subsection: PlanningSubsectionEntity,
    section: PlanningSectionEntity,
    offer: PlanningOfferEntity,
  ) {
    return {
      ...this.buildSectionLogContext(section, offer),
      subsection_id: subsection.id,
      subsection_code: subsection.code,
    };
  }

  private buildScheduleLogContext(
    schedule: PlanningSubsectionScheduleEntity,
    subsection: PlanningSubsectionEntity,
    section: PlanningSectionEntity,
    offer: PlanningOfferEntity,
  ) {
    return {
      ...this.buildSubsectionLogContext(subsection, section, offer),
      schedule_id: schedule.id,
      day_of_week: schedule.day_of_week,
      start_time: schedule.start_time,
      end_time: schedule.end_time,
    };
  }

  private async logChange(
    entityType: string,
    entityId: string,
    action: 'CREATE' | 'UPDATE' | 'DELETE',
    beforeValue: unknown,
    afterValue: unknown,
    context?: Record<string, unknown>,
    actor?: ImportActor | null,
  ) {
    await this.changeLogsRepo.save(
      this.changeLogsRepo.create(
        this.buildChangeLogEntry(
          entityType,
          entityId,
          action,
          beforeValue,
          afterValue,
          context,
          actor,
        ),
      ),
    );
  }

  private buildChangeLogEntry(
    entityType: string,
    entityId: string,
    action: 'CREATE' | 'UPDATE' | 'DELETE',
    beforeValue: unknown,
    afterValue: unknown,
    context?: Record<string, unknown>,
    actor?: ImportActor | null,
  ): Partial<PlanningChangeLogEntity> {
    return {
      id: newId(),
      entity_type: entityType,
      entity_id: entityId,
      action,
      before_json: toLogJson(beforeValue),
      after_json: toLogJson(afterValue),
      changed_by_user_id: actor?.user_id ?? null,
      changed_by: actor?.display_name || actor?.username || 'SYSTEM',
      changed_from_ip: actor?.ip_address ?? null,
      context_json: context ?? null,
      changed_at: new Date(),
    };
  }

  private async saveChangeLogsBulk(
    entries: Partial<PlanningChangeLogEntity>[],
    manager?: EntityManager,
  ) {
    if (!entries.length) {
      return;
    }
    if (manager) {
      const records = entries.map((entry) => manager.create(PlanningChangeLogEntity, entry));
      await this.insertInChunks(manager, PlanningChangeLogEntity, records);
      return;
    }
    const records = entries.map((entry) => this.changeLogsRepo.create(entry));
    await this.changeLogsRepo.save(records, { chunk: 250 });
  }

  private async insertInChunks<T extends ObjectLiteral>(
    manager: EntityManager,
    target: EntityTarget<T>,
    records: T[],
    chunkSize = 250,
  ) {
    if (!records.length) {
      return;
    }
    for (let index = 0; index < records.length; index += chunkSize) {
      const chunk = records.slice(index, index + chunkSize);
      await manager.insert(target, chunk);
    }
  }

  async syncAkademicSection(sectionId: string, actor?: ImportActor | null) {
    const section = await this.sectionsRepo.findOne({ where: { id: sectionId } });
    if (!section) {
      throw new NotFoundException(`No existe la seccion ${sectionId}.`);
    }
    const offer = await this.offersRepo.findOne({ where: { id: section.planning_offer_id } });
    if (!offer) {
      throw new NotFoundException(`No existe la oferta ${section.planning_offer_id}.`);
    }
    if (offer.source_system !== 'AKADEMIC') {
      throw new BadRequestException('Solo se pueden sincronizar secciones cuyo origen es Akademic.');
    }
    if (!section.source_section_id) {
      throw new BadRequestException('La seccion no tiene source_section_id para sincronizar con Akademic.');
    }

    const sourceTermId = offer.source_term_id ?? offer.semester_id;
    const syncDto: PreviewPlanningAkademicImportDto = {
      semester_id: sourceTermId,
      vc_period_id: offer.vc_period_id ?? undefined,
      campus_id: offer.campus_id ?? undefined,
      faculty_id: offer.faculty_id ?? undefined,
      academic_program_id: offer.academic_program_id ?? undefined,
      study_plan_id: offer.study_plan_id ?? undefined,
      cycle: offer.cycle ?? undefined,
      study_plan_course_id: offer.study_plan_course_id ?? undefined,
      course_code: offer.course_code ?? undefined,
    };

    const [catalog, sourceCourses, detail] = await Promise.all([
      this.loadImportCatalog(),
      this.fetchAkademicCourses(sourceTermId),
      this.fetchAkademicSectionDetail(section.source_section_id),
    ]);
    const sourceCourse =
      sourceCourses.find((item) => item.id === offer.source_course_id) ??
      sourceCourses.find((item) => item.id === recordString(detail ?? {}, 'courseId')) ??
      sourceCourses.find((item) => normalizeLoose(item.code) === normalizeLoose(offer.course_code)) ??
      this.buildFallbackAkademicCourseForSectionSync(offer, detail, catalog);
    if (!sourceCourse) {
      throw new BadRequestException('No se encontro el curso origen en Akademic para esta seccion.');
    }

    const sourceSections = await this.fetchAkademicSections(sourceCourse.id, sourceTermId).catch(() => []);
    const sourceSection =
      sourceSections.find((item: AkademicSectionRow) => item.id === section.source_section_id) ??
      this.buildFallbackAkademicSectionForSectionSync(section, sourceCourse.id, detail);

    if (!sourceSection) {
      const deleted = await this.offersRepo.manager.transaction(async (manager) => {
        return this.deleteAkademicSectionTree(manager, section.id, offer, actor, true);
      });
      const refreshedOffer = await this.offersRepo.findOne({ where: { id: offer.id } });
      if (refreshedOffer) {
        refreshedOffer.last_synced_at = new Date();
        refreshedOffer.updated_at = new Date();
        await this.offersRepo.save(refreshedOffer);
      }
      await this.planningManualService.rebuildOfferConflictsAndStatus(offer.id, actor as any);
      return {
        section_id: section.id,
        external_code: section.external_code,
        course_code: offer.course_code,
        course_name: offer.course_name,
        section_deleted: true,
        summary: {
          section_updated: false,
          subsections_created: 0,
          subsections_updated: 0,
          subsections_deleted: deleted.subsections_deleted,
          schedules_created: 0,
          schedules_updated: 0,
          schedules_deleted: deleted.schedules_deleted,
        },
      };
    }

    const schedules = await this.fetchAkademicSectionSchedules(sourceSection.id);
    const previewRows = await this.buildAkademicPreviewRowsForSection(
      syncDto,
      catalog,
      sourceCourse,
      sourceSection,
      detail,
      schedules,
    );
    const blockingIssues = previewRows.flatMap((row) => row.issues).filter((issue) => issue.severity === 'BLOCKING');
    if (blockingIssues.length > 0) {
      throw new BadRequestException(
        `No se pudo sincronizar la seccion desde Akademic: ${blockingIssues
          .slice(0, 3)
          .map((item) => item.message)
          .join(' | ')}`,
      );
    }

    const sectionSummary = await this.offersRepo.manager.transaction(async (manager) => {
      const currentSection = await manager.findOne(PlanningSectionEntity, { where: { id: section.id } });
      if (!currentSection) {
        throw new NotFoundException(`No existe la seccion ${section.id}.`);
      }
      const currentOffer = await manager.findOne(PlanningOfferEntity, { where: { id: offer.id } });
      if (!currentOffer) {
        throw new NotFoundException(`No existe la oferta ${offer.id}.`);
      }

      const now = new Date();
      const sectionResolutions = previewRows.map((row) => asRecord(row.resolution_json));
      const sourceVcSectionId = firstNonEmpty(
        sectionResolutions.map((row) => recordString(row, 'source_vc_section_id')),
      );
      const nextSection = this.sectionsRepo.create({
        ...currentSection,
        external_code: firstNonEmpty(
          sectionResolutions.map((row) => recordString(row, 'external_section_code')),
        ),
        source_section_id: firstNonEmpty(
          sectionResolutions.map((row) => recordString(row, 'source_section_id')),
        ),
        source_payload_json: attachVcContextMetadata(
          asRecord(recordValue(asRecord(previewRows[0]?.source_json), 'section')) ??
            asRecord(previewRows[0]?.source_json) ??
            null,
          {
            ...(readVcContextMetadata(currentSection.source_payload_json) ?? {}),
            vc_source: sourceVcSectionId ? 'sync_source' : 'fallback_match',
            source_vc_section_id: sourceVcSectionId,
            vc_context_message: sourceVcSectionId
              ? 'Seccion VC preservada desde la sincronizacion.'
              : 'Seccion VC pendiente de resolver por fallback.',
          },
        ),
        teacher_id: firstNonEmpty(sectionResolutions.map((row) => recordString(row, 'teacher_id'))),
        course_modality_id: firstNonEmpty(
          sectionResolutions.map((row) => recordString(row, 'course_modality_id')),
        ),
        projected_vacancies: maxNumber(
          sectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies')),
        ),
        is_cepea: sectionResolutions.some((row) => Boolean(recordBoolean(row, 'is_cepea'))),
        has_subsections:
          new Set(
            sectionResolutions
              .map((row) => recordString(row, 'import_subsection_code'))
              .filter(Boolean),
          ).size > 1,
        default_theoretical_hours: currentOffer.theoretical_hours,
        default_practical_hours: currentOffer.practical_hours,
        default_total_hours: currentOffer.total_hours,
        updated_at: now,
      });

      const sectionChanged =
        nextSection.external_code !== currentSection.external_code ||
        nextSection.source_section_id !== currentSection.source_section_id ||
        nextSection.teacher_id !== currentSection.teacher_id ||
        nextSection.course_modality_id !== currentSection.course_modality_id ||
        Number(nextSection.projected_vacancies) !== Number(currentSection.projected_vacancies) ||
        nextSection.is_cepea !== currentSection.is_cepea ||
        nextSection.has_subsections !== currentSection.has_subsections ||
        Number(nextSection.default_theoretical_hours) !== Number(currentSection.default_theoretical_hours) ||
        Number(nextSection.default_practical_hours) !== Number(currentSection.default_practical_hours) ||
        Number(nextSection.default_total_hours) !== Number(currentSection.default_total_hours) ||
        JSON.stringify(nextSection.source_payload_json ?? null) !==
          JSON.stringify(currentSection.source_payload_json ?? null);

      if (sectionChanged) {
        await manager.save(PlanningSectionEntity, nextSection);
        await this.logChange(
          'planning_section',
          nextSection.id,
          'UPDATE',
          currentSection,
          nextSection,
          {
            ...this.buildSectionLogContext(nextSection, currentOffer),
            reason: 'planning_section_sync_akademic',
          },
          actor,
        );
      }

      const currentSubsections = await manager.find(PlanningSubsectionEntity, {
        where: { planning_section_id: currentSection.id },
      });
      const currentSubsectionsByCode = new Map(currentSubsections.map((item) => [item.code, item] as const));
      const currentSchedules = currentSubsections.length
        ? await manager.find(PlanningSubsectionScheduleEntity, {
            where: { planning_subsection_id: In(currentSubsections.map((item) => item.id)) },
          })
        : [];
      const schedulesBySubsectionId = groupBy(currentSchedules, (item) => item.planning_subsection_id);
      const rowsBySubsection = groupBy(previewRows, (row) => {
        const resolution = asRecord(row.resolution_json);
        return recordString(resolution, 'import_subsection_code') ?? currentSection.code;
      });

      const summary = {
        section_updated: sectionChanged,
        subsections_created: 0,
        subsections_updated: 0,
        subsections_deleted: 0,
        schedules_created: 0,
        schedules_updated: 0,
        schedules_deleted: 0,
      };

      for (const currentSubsection of currentSubsections) {
        if (!rowsBySubsection.has(currentSubsection.code)) {
          const deleted = await this.deleteAkademicSubsectionTree(
            manager,
            currentSubsection,
            nextSection,
            currentOffer,
            actor,
            'planning_section_sync_akademic',
            schedulesBySubsectionId.get(currentSubsection.id) ?? [],
          );
          summary.subsections_deleted += deleted.subsections_deleted;
          summary.schedules_deleted += deleted.schedules_deleted;
        }
      }

      for (const [subsectionCode, subsectionRows] of rowsBySubsection.entries()) {
        const subsectionResolutions = subsectionRows.map((row) => asRecord(row.resolution_json));
        const assignedTheoretical =
          maxNumber(subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_theoretical_hours'))) ?? 0;
        const assignedPractical =
          maxNumber(subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_practical_hours'))) ?? 0;
        const assignedTotal =
          maxNumber(subsectionResolutions.map((row) => recordNumberOrNull(row, 'assigned_total_hours'))) ??
          assignedTheoretical + assignedPractical;
        const schedulePayloads = subsectionResolutions
          .map((row) => asRecord(recordValue(row, 'schedule')))
          .filter((item) => Object.keys(item).length > 0);
        const uniqueScheduleTeacherIds = uniqueIds(schedulePayloads.map((row) => recordString(row, 'teacher_id')));
        const uniqueScheduleBuildingIds = uniqueIds(schedulePayloads.map((row) => recordString(row, 'building_id')));
        const uniqueScheduleClassroomIds = uniqueIds(schedulePayloads.map((row) => recordString(row, 'classroom_id')));
        const subsectionKind = (
          firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'subsection_kind'))) ??
          deriveSubsectionKind(assignedTheoretical, assignedPractical)
        ) as PlanningSubsectionKind;

        const currentSubsection = currentSubsectionsByCode.get(subsectionCode) ?? null;
        const nextSubsection = this.subsectionsRepo.create({
          id: currentSubsection?.id ?? newId(),
          planning_section_id: nextSection.id,
          code: subsectionCode,
          kind: subsectionKind,
          responsible_teacher_id:
            uniqueScheduleTeacherIds.length === 1
              ? uniqueScheduleTeacherIds[0]
              : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'teacher_id'))),
          course_modality_id: firstNonEmpty(
            subsectionResolutions.map((row) => recordString(row, 'course_modality_id')),
          ),
          building_id:
            uniqueScheduleBuildingIds.length === 1
              ? uniqueScheduleBuildingIds[0]
              : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'building_id'))),
          classroom_id:
            uniqueScheduleClassroomIds.length === 1
              ? uniqueScheduleClassroomIds[0]
              : firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'classroom_id'))),
          capacity_snapshot: maxNumber(
            subsectionResolutions.map((row) => recordNumberOrNull(row, 'capacity_snapshot')),
          ),
          shift: firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'shift'))),
          projected_vacancies: maxNumber(
            subsectionResolutions.map((row) => recordNumberOrNull(row, 'projected_vacancies')),
          ),
          course_type: currentOffer.course_type,
          assigned_theoretical_hours: assignedTheoretical,
          assigned_practical_hours: assignedPractical,
          assigned_total_hours: assignedTotal,
          denomination:
            firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'denomination'))) ??
            buildDenomination(
              currentOffer.course_code,
              currentOffer.course_name,
              nextSection.code,
              subsectionCode,
              currentOffer.campus_id,
            ),
          vc_section_id:
            firstNonEmpty(subsectionResolutions.map((row) => recordString(row, 'source_vc_section_id'))) ?? null,
          status: currentSubsection?.status ?? 'DRAFT',
          created_at: currentSubsection?.created_at ?? now,
          updated_at: now,
        });

        if (!currentSubsection) {
          await manager.save(PlanningSubsectionEntity, nextSubsection);
          await this.logChange(
            'planning_subsection',
            nextSubsection.id,
            'CREATE',
            null,
            nextSubsection,
            {
              ...this.buildSubsectionLogContext(nextSubsection, nextSection, currentOffer),
              reason: 'planning_section_sync_akademic',
            },
            actor,
          );
          summary.subsections_created += 1;
        } else {
          const subsectionChanged =
            nextSubsection.kind !== currentSubsection.kind ||
            nextSubsection.responsible_teacher_id !== currentSubsection.responsible_teacher_id ||
            nextSubsection.course_modality_id !== currentSubsection.course_modality_id ||
            nextSubsection.building_id !== currentSubsection.building_id ||
            nextSubsection.classroom_id !== currentSubsection.classroom_id ||
            Number(nextSubsection.capacity_snapshot) !== Number(currentSubsection.capacity_snapshot) ||
            nextSubsection.shift !== currentSubsection.shift ||
            Number(nextSubsection.projected_vacancies) !== Number(currentSubsection.projected_vacancies) ||
            nextSubsection.course_type !== currentSubsection.course_type ||
            Number(nextSubsection.assigned_theoretical_hours) !==
              Number(currentSubsection.assigned_theoretical_hours) ||
            Number(nextSubsection.assigned_practical_hours) !==
              Number(currentSubsection.assigned_practical_hours) ||
            Number(nextSubsection.assigned_total_hours) !==
              Number(currentSubsection.assigned_total_hours) ||
            nextSubsection.denomination !== currentSubsection.denomination ||
            nextSubsection.vc_section_id !== currentSubsection.vc_section_id;

          if (subsectionChanged) {
            await manager.save(PlanningSubsectionEntity, nextSubsection);
            await this.logChange(
              'planning_subsection',
              nextSubsection.id,
              'UPDATE',
              currentSubsection,
              nextSubsection,
              {
                ...this.buildSubsectionLogContext(nextSubsection, nextSection, currentOffer),
                reason: 'planning_section_sync_akademic',
              },
              actor,
            );
            summary.subsections_updated += 1;
          }
        }

        const existingSchedules = schedulesBySubsectionId.get((currentSubsection ?? nextSubsection).id) ?? [];
        const incomingSchedules = dedupeImportedSchedules(schedulePayloads);
        const usedExistingIds = new Set<string>();
        const existingBySourceScheduleId = new Map(
          existingSchedules
            .filter((item) => item.source_schedule_id)
            .map((item) => [item.source_schedule_id as string, item] as const),
        );
        const existingBySignature = new Map<string, PlanningSubsectionScheduleEntity>(
          existingSchedules.map((item) => [
            `${item.day_of_week}|${item.start_time}|${item.end_time}|${item.session_type}`,
            item,
          ] as const),
        );

        for (const selectedSchedule of incomingSchedules) {
          const sourceScheduleId = recordString(selectedSchedule, 'source_schedule_id');
          const signature = [
            recordString(selectedSchedule, 'day_of_week'),
            recordString(selectedSchedule, 'start_time'),
            recordString(selectedSchedule, 'end_time'),
            recordString(selectedSchedule, 'session_type') ?? 'OTHER',
          ].join('|');
          const matchedSchedule =
            (sourceScheduleId ? existingBySourceScheduleId.get(sourceScheduleId) ?? null : null) ??
            existingBySignature.get(signature) ??
            null;

          const scheduleBase = matchedSchedule
            ? { ...matchedSchedule }
            : {
                id: newId(),
                planning_subsection_id: (currentSubsection ?? nextSubsection).id,
                created_at: now,
              };
          const nextSchedule = this.schedulesRepo.create({
            ...scheduleBase,
            planning_subsection_id: (currentSubsection ?? nextSubsection).id,
            day_of_week: recordString(selectedSchedule, 'day_of_week') as (typeof DayOfWeekValues)[number],
            start_time: recordString(selectedSchedule, 'start_time')!,
            end_time: recordString(selectedSchedule, 'end_time')!,
            session_type:
              (recordString(selectedSchedule, 'session_type') as PlanningSessionType | null) ?? 'OTHER',
            source_session_type_code: recordString(selectedSchedule, 'source_session_type_code'),
            teacher_id: recordString(selectedSchedule, 'teacher_id'),
            building_id: recordString(selectedSchedule, 'building_id'),
            classroom_id: recordString(selectedSchedule, 'classroom_id'),
            source_schedule_id: sourceScheduleId,
            source_payload_json:
              asRecord(recordValue(selectedSchedule, 'source_payload_json')) ?? null,
            duration_minutes: recordNumber(selectedSchedule, 'duration_minutes'),
            academic_hours: recordNumber(selectedSchedule, 'academic_hours'),
            updated_at: now,
          });

          if (!matchedSchedule) {
            await manager.save(PlanningSubsectionScheduleEntity, nextSchedule);
            await this.logChange(
              'planning_subsection_schedule',
              nextSchedule.id,
              'CREATE',
              null,
              nextSchedule,
              {
                ...this.buildScheduleLogContext(nextSchedule, currentSubsection ?? nextSubsection, nextSection, currentOffer),
                reason: 'planning_section_sync_akademic',
              },
              actor,
            );
            summary.schedules_created += 1;
            continue;
          }

          usedExistingIds.add(matchedSchedule.id);
          const scheduleChanged =
            nextSchedule.day_of_week !== matchedSchedule.day_of_week ||
            nextSchedule.start_time !== matchedSchedule.start_time ||
            nextSchedule.end_time !== matchedSchedule.end_time ||
            nextSchedule.session_type !== matchedSchedule.session_type ||
            nextSchedule.source_session_type_code !== matchedSchedule.source_session_type_code ||
            nextSchedule.teacher_id !== matchedSchedule.teacher_id ||
            nextSchedule.building_id !== matchedSchedule.building_id ||
            nextSchedule.classroom_id !== matchedSchedule.classroom_id ||
            nextSchedule.source_schedule_id !== matchedSchedule.source_schedule_id ||
            Number(nextSchedule.duration_minutes) !== Number(matchedSchedule.duration_minutes) ||
            Number(nextSchedule.academic_hours) !== Number(matchedSchedule.academic_hours) ||
            JSON.stringify(nextSchedule.source_payload_json ?? null) !==
              JSON.stringify(matchedSchedule.source_payload_json ?? null);

          if (scheduleChanged) {
            await manager.save(PlanningSubsectionScheduleEntity, nextSchedule);
            await this.logChange(
              'planning_subsection_schedule',
              nextSchedule.id,
              'UPDATE',
              matchedSchedule,
              nextSchedule,
              {
                ...this.buildScheduleLogContext(nextSchedule, currentSubsection ?? nextSubsection, nextSection, currentOffer),
                reason: 'planning_section_sync_akademic',
              },
              actor,
            );
            summary.schedules_updated += 1;
          }
        }

        const staleSchedules = existingSchedules.filter((item) => !usedExistingIds.has(item.id)).map((item) => item.id);
        if (staleSchedules.length > 0) {
          await this.cleanupPlanningArtifactsForSchedules(manager, staleSchedules);
          for (const staleScheduleId of staleSchedules) {
            const staleSchedule = existingSchedules.find((item) => item.id === staleScheduleId);
            if (!staleSchedule) {
              continue;
            }
            await this.logChange(
              'planning_subsection_schedule',
              staleSchedule.id,
              'DELETE',
              staleSchedule,
              null,
              {
                ...this.buildScheduleLogContext(staleSchedule, currentSubsection ?? nextSubsection, nextSection, currentOffer),
                reason: 'planning_section_sync_akademic',
              },
              actor,
            );
          }
          await manager.delete(PlanningSubsectionScheduleEntity, { id: In(staleSchedules) });
          summary.schedules_deleted += staleSchedules.length;
        }
      }

      currentOffer.last_synced_at = now;
      if (sourceCourse.id && currentOffer.source_course_id !== sourceCourse.id) {
        currentOffer.source_course_id = sourceCourse.id;
      }
      currentOffer.updated_at = now;
      await manager.save(PlanningOfferEntity, currentOffer);

      return {
        section_id: nextSection.id,
        external_code: nextSection.external_code,
        course_code: currentOffer.course_code,
        course_name: currentOffer.course_name,
        section_deleted: false,
        summary,
      };
    });

    await this.planningManualService.rebuildOfferConflictsAndStatus(offer.id, actor as any);
    await this.planningManualService.recalculateVcMatches(actor as any, { offer_id: offer.id });
    return sectionSummary;
  }

  private async cleanupPlanningArtifactsForSchedules(
    manager: Repository<PlanningOfferEntity>['manager'],
    scheduleIds: string[],
  ) {
    if (!scheduleIds.length) {
      return;
    }
    const placeholders = scheduleIds.map(() => '?').join(', ');
    try {
      await manager.query(
        `DELETE FROM planning_subsection_videoconference_overrides WHERE planning_subsection_schedule_id IN (${placeholders})`,
        scheduleIds,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : `${error ?? ''}`;
      if (!/doesn't exist|does not exist|unknown table/i.test(message)) {
        throw error;
      }
    }
  }

  private async deleteAkademicSubsectionTree(
    manager: Repository<PlanningOfferEntity>['manager'],
    subsection: PlanningSubsectionEntity,
    section: PlanningSectionEntity,
    offer: PlanningOfferEntity,
    actor: ImportActor | null | undefined,
    reason: string,
    knownSchedules?: PlanningSubsectionScheduleEntity[],
  ) {
    const schedules =
      knownSchedules ??
      (await manager.find(PlanningSubsectionScheduleEntity, {
        where: { planning_subsection_id: subsection.id },
      }));
    const scheduleIds = schedules.map((item) => item.id);
    if (scheduleIds.length > 0) {
      await this.cleanupPlanningArtifactsForSchedules(manager, scheduleIds);
      for (const schedule of schedules) {
        await this.logChange(
          'planning_subsection_schedule',
          schedule.id,
          'DELETE',
          schedule,
          null,
          {
            ...this.buildScheduleLogContext(schedule, subsection, section, offer),
            reason,
          },
          actor,
        );
      }
      await manager.delete(PlanningSubsectionScheduleEntity, { id: In(scheduleIds) });
    }

    await this.logChange(
      'planning_subsection',
      subsection.id,
      'DELETE',
      subsection,
      null,
      {
        ...this.buildSubsectionLogContext(subsection, section, offer),
        reason,
      },
      actor,
    );
    await manager.delete(PlanningSubsectionEntity, { id: subsection.id });
    return {
      subsections_deleted: 1,
      schedules_deleted: scheduleIds.length,
    };
  }

  private async deleteAkademicSectionTree(
    manager: Repository<PlanningOfferEntity>['manager'],
    sectionId: string,
    offer: PlanningOfferEntity,
    actor: ImportActor | null | undefined,
    deleteSection: boolean,
  ) {
    const section = await manager.findOne(PlanningSectionEntity, { where: { id: sectionId } });
    if (!section) {
      return { subsections_deleted: 0, schedules_deleted: 0 };
    }
    const subsections = await manager.find(PlanningSubsectionEntity, {
      where: { planning_section_id: section.id },
    });
    const schedules = subsections.length
      ? await manager.find(PlanningSubsectionScheduleEntity, {
          where: { planning_subsection_id: In(subsections.map((item) => item.id)) },
        })
      : [];
    const schedulesBySubsectionId = groupBy(schedules, (item) => item.planning_subsection_id);
    let subsectionsDeleted = 0;
    let schedulesDeleted = 0;
    for (const subsection of subsections) {
      const deleted = await this.deleteAkademicSubsectionTree(
        manager,
        subsection,
        section,
        offer,
        actor,
        'planning_section_sync_akademic',
        schedulesBySubsectionId.get(subsection.id) ?? [],
      );
      subsectionsDeleted += deleted.subsections_deleted;
      schedulesDeleted += deleted.schedules_deleted;
    }
    if (deleteSection) {
      await this.logChange(
        'planning_section',
        section.id,
        'DELETE',
        section,
        null,
        {
          ...this.buildSectionLogContext(section, offer),
          reason: 'planning_section_sync_akademic',
        },
        actor,
      );
      await manager.delete(PlanningSectionEntity, { id: section.id });
    }
    return {
      subsections_deleted: subsectionsDeleted,
      schedules_deleted: schedulesDeleted,
    };
  }

  async repairMissingAkademicSchedulesForSemester(
    semesterId: string,
    actor?: ImportActor | null,
  ) {
    const semester = await this.semestersRepo.findOne({ where: { id: semesterId } });
    if (!semester) {
      throw new NotFoundException(`No existe el semestre ${semesterId}.`);
    }

    const catalog = await this.loadImportCatalog();
    const candidateRows = await this.sectionsRepo
      .createQueryBuilder('section')
      .innerJoin(PlanningOfferEntity, 'offer', 'offer.id = section.planning_offer_id')
      .leftJoin(PlanningSubsectionEntity, 'subsection', 'subsection.planning_section_id = section.id')
      .leftJoin(
        PlanningSubsectionScheduleEntity,
        'schedule',
        'schedule.planning_subsection_id = subsection.id',
      )
      .select('offer.id', 'offer_id')
      .addSelect('section.id', 'section_id')
      .addSelect('section.source_section_id', 'source_section_id')
      .where('offer.semester_id = :semesterId', { semesterId })
      .andWhere('offer.source_system = :sourceSystem', { sourceSystem: 'AKADEMIC' })
      .andWhere('section.source_section_id IS NOT NULL')
      .groupBy('offer.id')
      .addGroupBy('section.id')
      .addGroupBy('section.source_section_id')
      .having('COUNT(schedule.id) = 0')
      .getRawMany<Record<string, unknown>>();

    if (!candidateRows.length) {
      return {
        semester_id: semesterId,
        repaired_sections: 0,
        repaired_subsections: 0,
        created_schedules: 0,
        unresolved_sections: 0,
        skipped_sections: 0,
        examined_sections: 0,
        details: [] as Array<Record<string, unknown>>,
      };
    }

    const offerIds = uniqueIds(candidateRows.map((row) => recordString(row, 'offer_id')));
    const sectionIds = uniqueIds(candidateRows.map((row) => recordString(row, 'section_id')));
    const [offers, sections, subsections] = await Promise.all([
      this.offersRepo.find({ where: { id: In(offerIds) } }),
      this.sectionsRepo.find({ where: { id: In(sectionIds) } }),
      this.subsectionsRepo.find({ where: { planning_section_id: In(sectionIds) } }),
    ]);
    const offerMap = mapById(offers);
    const sectionMap = mapById(sections);
    const subsectionsBySectionId = groupBy(subsections, (item) => item.planning_section_id);

    let repairedSections = 0;
    let repairedSubsections = 0;
    let createdSchedules = 0;
    let unresolvedSections = 0;
    let skippedSections = 0;
    const details: Array<Record<string, unknown>> = [];

    for (const candidate of candidateRows) {
      const offer = offerMap.get(recordString(candidate, 'offer_id') ?? '');
      const section = sectionMap.get(recordString(candidate, 'section_id') ?? '');
      const sourceSectionId = recordString(candidate, 'source_section_id');
      if (!offer || !section || !sourceSectionId) {
        skippedSections += 1;
        continue;
      }

      const existingSubsections = subsectionsBySectionId.get(section.id) ?? [];
      if (!existingSubsections.length) {
        skippedSections += 1;
        details.push({
          section_id: section.id,
          external_code: section.external_code,
          status: 'SKIPPED',
          reason: 'section_without_subsections',
        });
        continue;
      }

      const schedules = await this.fetchAkademicSectionSchedules(sourceSectionId);
      if (!schedules.length) {
        unresolvedSections += 1;
        details.push({
          section_id: section.id,
          external_code: section.external_code,
          source_section_id: sourceSectionId,
          status: 'UNRESOLVED',
          reason: 'source_returned_no_schedules',
        });
        continue;
      }

      const groupedSubsections = this.groupAkademicSubsections(
        section.code,
        schedules,
        offer.course_type,
      );
      let sectionCreatedSchedules = 0;
      let sectionRepairedSubsections = 0;

      for (const group of groupedSubsections) {
        const canReuseSingleExistingSubsection =
          groupedSubsections.length === 1 && existingSubsections.length === 1;
        const matchedSubsection =
          existingSubsections.find((item) => item.code === group.import_subsection_code) ??
          (canReuseSingleExistingSubsection ? existingSubsections[0] : null);
        if (matchedSubsection) {
          const currentScheduleCount = await this.schedulesRepo.count({
            where: { planning_subsection_id: matchedSubsection.id },
          });
          if (currentScheduleCount > 0) {
            continue;
          }
        }

        const rowIssues: PreviewIssue[] = [];
        const resolvedSchedules = dedupeImportedSchedules(
          await Promise.all(
            group.schedules.map(async (schedule) => {
              const teacherResolution = await this.resolveAkademicTeacherMatch(
                schedule.teacher_external_id,
                schedule.teacher_dni,
                schedule.teacher_name,
                schedule.teacher_username,
                schedule.teacher_email,
                catalog,
                rowIssues,
              );
              const locationResolution = this.resolveAkademicScheduleLocation(
                schedule,
                offer.campus_id,
                catalog,
                rowIssues,
              );
              return {
                day_of_week: schedule.day_of_week,
                start_time: schedule.start_time,
                end_time: schedule.end_time,
                duration_minutes: schedule.duration_minutes,
                academic_hours: schedule.academic_hours,
                signature: `${schedule.day_of_week}|${schedule.start_time}|${schedule.end_time}`,
                session_type: schedule.session_type,
                source_session_type_code: schedule.source_session_type_code,
                teacher_id: teacherResolution.target_id,
                building_id: locationResolution.building.target_id,
                classroom_id: locationResolution.classroom.target_id,
                source_schedule_id: schedule.id,
                source_payload_json: schedule.raw,
              } as Record<string, unknown>;
            }),
          ),
        );

        if (!resolvedSchedules.length) {
          continue;
        }

        const now = new Date();
        const uniqueScheduleTeacherIds = uniqueIds(
          resolvedSchedules.map((row) => recordString(row, 'teacher_id')),
        );
        const uniqueScheduleBuildingIds = uniqueIds(
          resolvedSchedules.map((row) => recordString(row, 'building_id')),
        );
        const uniqueScheduleClassroomIds = uniqueIds(
          resolvedSchedules.map((row) => recordString(row, 'classroom_id')),
        );
        const shiftResolution = this.resolveAkademicShift(group.schedules);
        const assignedHours = this.computeAkademicAssignedHours(group.schedules);
        let targetSubsection = matchedSubsection;
        if (!targetSubsection) {
          targetSubsection = await this.subsectionsRepo.save(
            this.subsectionsRepo.create({
              id: newId(),
              planning_section_id: section.id,
              code: group.import_subsection_code,
              kind: this.deriveAkademicSubsectionKindFromSchedules(offer.course_type, group.schedules),
              responsible_teacher_id:
                uniqueScheduleTeacherIds.length === 1 ? uniqueScheduleTeacherIds[0] : null,
              course_modality_id: section.course_modality_id ?? null,
              building_id: uniqueScheduleBuildingIds.length === 1 ? uniqueScheduleBuildingIds[0] : null,
              classroom_id: uniqueScheduleClassroomIds.length === 1 ? uniqueScheduleClassroomIds[0] : null,
              capacity_snapshot: maxNumber(
                resolvedSchedules.map((row) => recordNumberOrNull(row, 'capacity_snapshot')),
              ),
              shift: shiftResolution.target_id ?? null,
              projected_vacancies: section.projected_vacancies ?? null,
              course_type: offer.course_type,
              assigned_theoretical_hours: assignedHours.assigned_theoretical_hours,
              assigned_practical_hours: assignedHours.assigned_practical_hours,
              assigned_total_hours: assignedHours.assigned_total_hours,
              denomination: buildDenomination(
                offer.course_code,
                offer.course_name,
                section.code,
                group.import_subsection_code,
                offer.campus_id,
              ),
              vc_section_id: null,
              status: 'DRAFT',
              created_at: now,
              updated_at: now,
            }),
          );
          await this.logChange(
            'planning_subsection',
            targetSubsection.id,
            'CREATE',
            null,
            targetSubsection,
            {
              ...this.buildSubsectionLogContext(targetSubsection, section, offer),
              reason: 'repair_missing_akademic_schedules',
            },
            actor,
          );
          existingSubsections.push(targetSubsection);
        }

        const nextSubsection = this.subsectionsRepo.create({
          ...targetSubsection,
          kind: this.deriveAkademicSubsectionKindFromSchedules(offer.course_type, group.schedules),
          responsible_teacher_id:
            uniqueScheduleTeacherIds.length === 1
              ? uniqueScheduleTeacherIds[0]
              : targetSubsection.responsible_teacher_id,
          building_id:
            uniqueScheduleBuildingIds.length === 1
              ? uniqueScheduleBuildingIds[0]
              : targetSubsection.building_id,
          classroom_id:
            uniqueScheduleClassroomIds.length === 1
              ? uniqueScheduleClassroomIds[0]
              : targetSubsection.classroom_id,
          shift: shiftResolution.target_id ?? targetSubsection.shift,
          assigned_theoretical_hours: assignedHours.assigned_theoretical_hours,
          assigned_practical_hours: assignedHours.assigned_practical_hours,
          assigned_total_hours: assignedHours.assigned_total_hours,
          updated_at: now,
        });
        const subsectionChanged =
          nextSubsection.kind !== targetSubsection.kind ||
          nextSubsection.responsible_teacher_id !== targetSubsection.responsible_teacher_id ||
          nextSubsection.building_id !== targetSubsection.building_id ||
          nextSubsection.classroom_id !== targetSubsection.classroom_id ||
          nextSubsection.shift !== targetSubsection.shift ||
          Number(nextSubsection.assigned_theoretical_hours) !==
            Number(targetSubsection.assigned_theoretical_hours) ||
          Number(nextSubsection.assigned_practical_hours) !==
            Number(targetSubsection.assigned_practical_hours) ||
          Number(nextSubsection.assigned_total_hours) !==
            Number(targetSubsection.assigned_total_hours);

        if (subsectionChanged) {
          await this.subsectionsRepo.save(nextSubsection);
          await this.logChange(
            'planning_subsection',
            nextSubsection.id,
            'UPDATE',
            targetSubsection,
            nextSubsection,
            {
              ...this.buildSubsectionLogContext(nextSubsection, section, offer),
              reason: 'repair_missing_akademic_schedules',
            },
            actor,
          );
        }

        for (const selectedSchedule of resolvedSchedules) {
          const schedule = this.schedulesRepo.create({
            id: newId(),
            planning_subsection_id: nextSubsection.id,
            day_of_week: recordString(selectedSchedule, 'day_of_week') as (typeof DayOfWeekValues)[number],
            start_time: recordString(selectedSchedule, 'start_time')!,
            end_time: recordString(selectedSchedule, 'end_time')!,
            session_type:
              (recordString(selectedSchedule, 'session_type') as PlanningSessionType | null) ??
              'OTHER',
            source_session_type_code: recordString(selectedSchedule, 'source_session_type_code'),
            teacher_id: recordString(selectedSchedule, 'teacher_id'),
            building_id: recordString(selectedSchedule, 'building_id'),
            classroom_id: recordString(selectedSchedule, 'classroom_id'),
            source_schedule_id: recordString(selectedSchedule, 'source_schedule_id'),
            source_payload_json:
              asRecord(recordValue(selectedSchedule, 'source_payload_json')) ?? null,
            duration_minutes: recordNumber(selectedSchedule, 'duration_minutes'),
            academic_hours: recordNumber(selectedSchedule, 'academic_hours'),
            created_at: now,
            updated_at: now,
          });
          await this.schedulesRepo.save(schedule);
          await this.logChange(
            'planning_subsection_schedule',
            schedule.id,
            'CREATE',
            null,
            schedule,
            {
              ...this.buildScheduleLogContext(schedule, nextSubsection, section, offer),
              reason: 'repair_missing_akademic_schedules',
            },
            actor,
          );
          createdSchedules += 1;
          sectionCreatedSchedules += 1;
        }

        repairedSubsections += 1;
        sectionRepairedSubsections += 1;
      }

      if (sectionCreatedSchedules > 0) {
        repairedSections += 1;
        details.push({
          section_id: section.id,
          external_code: section.external_code,
          source_section_id: sourceSectionId,
          repaired_subsections: sectionRepairedSubsections,
          created_schedules: sectionCreatedSchedules,
          status: 'REPAIRED',
        });
      } else {
        unresolvedSections += 1;
        details.push({
          section_id: section.id,
          external_code: section.external_code,
          source_section_id: sourceSectionId,
          status: 'UNRESOLVED',
          reason: 'normalized_without_target_subsection',
        });
      }
    }

    return {
      semester_id: semesterId,
      repaired_sections: repairedSections,
      repaired_subsections: repairedSubsections,
      created_schedules: createdSchedules,
      unresolved_sections: unresolvedSections,
      skipped_sections: skippedSections,
      examined_sections: candidateRows.length,
      details,
    };
  }

  private async buildWorkspaceComparableRows(filters: WorkspaceExportFilters) {
    const offers = await this.planningManualService.listExpandedOffers(
      filters.semester_id,
      undefined,
      filters.campus_id,
      filters.faculty_id,
      filters.academic_program_id,
      undefined,
      filters.study_plan_id,
      undefined,
    );
    if (!offers.length) {
      return [] as ManualWorkspaceComparableRow[];
    }

    const catalog = await this.loadImportCatalog();
    const studyPlanCourseById = new Map(catalog.studyPlanCourses.map((item) => [item.id, item] as const));
    const studyTypeById = new Map(catalog.studyTypes.map((item) => [item.id, item] as const));
    const teacherById = new Map(catalog.teachers.map((item) => [item.id, item] as const));
    const rows = offers.flatMap((offer: any) =>
      this.buildWorkspaceComparableRowsFromOffer(offer, {
        studyPlanCourseById,
        studyTypeById,
        teacherById,
      }),
    );

    return rows
      .filter((row) => this.matchesWorkspaceComparableFilters(row, filters))
      .sort((left, right) => this.compareWorkspaceComparableRows(left, right));
  }

  private buildWorkspaceComparableRowsFromOffer(
    offer: any,
    lookups: {
      studyPlanCourseById: Map<string, StudyPlanCourseEntity>;
      studyTypeById: Map<string, StudyTypeEntity>;
      teacherById: Map<string, TeacherEntity>;
    },
  ) {
    const sections = Array.isArray(offer?.sections) ? offer.sections : [];
    return sections.flatMap((section: any) =>
      (Array.isArray(section?.subsections) ? section.subsections : []).flatMap((subsection: any) => {
        const schedules = Array.isArray(subsection?.schedules) ? subsection.schedules : [];
        if (!schedules.length) {
          return [this.makeWorkspaceComparableRow(offer, section, subsection, null, lookups)];
        }
        return schedules.map((schedule: any) =>
          this.makeWorkspaceComparableRow(offer, section, subsection, schedule, lookups),
        );
      }),
    );
  }

  private makeWorkspaceComparableRow(
    offer: any,
    section: any,
    subsection: any,
    schedule: any | null,
    lookups: {
      studyPlanCourseById: Map<string, StudyPlanCourseEntity>;
      studyTypeById: Map<string, StudyTypeEntity>;
      teacherById: Map<string, TeacherEntity>;
    },
  ): ManualWorkspaceComparableRow {
    const effectiveTeacher =
      schedule?.teacher ??
      subsection?.responsible_teacher ??
      lookups.teacherById.get(schedule?.teacher_id ?? subsection?.responsible_teacher_id ?? '') ??
      null;
    const effectiveBuilding = schedule?.building ?? subsection?.building ?? null;
    const effectiveClassroom = schedule?.classroom ?? subsection?.classroom ?? null;
    const studyPlan = offer?.study_plan ?? null;
    const studyType = lookups.studyTypeById.get(offer?.study_type_id ?? '') ?? null;
    const studyPlanCourse = lookups.studyPlanCourseById.get(offer?.study_plan_course_id ?? '') ?? null;
    const parsedCourseCode = parseCompositePlanningCourseCode(offer?.course_code);
    const parsedSection = parseAkademicExternalSectionCode(section?.external_code ?? null);
    const modalityName = normalizePlanningModalityLabel(
      parsedSection.modality_token,
      subsection?.modality?.name ?? null,
      subsection?.course_modality_id ?? null,
    );
    const sectionBaseCode = normalizePlanningSectionCode(
      parsedSection.section_code ?? section?.code ?? subsection?.code ?? null,
    );
    const dayName = normalizeDayValue(schedule?.day_of_week ?? null);
    const startParts = splitTimeParts(schedule?.start_time ?? null);
    const endParts = splitTimeParts(schedule?.end_time ?? null);
    const rowLabel = [
      `${parsedCourseCode.course_code ?? offer?.course_code ?? ''}`.trim(),
      `${offer?.course_name ?? ''}`.trim(),
      `${sectionBaseCode ?? ''}`.trim(),
      dayName ? `${dayName} ${schedule?.start_time ?? ''}-${schedule?.end_time ?? ''}` : '',
    ]
      .filter(Boolean)
      .join(' | ');

    return {
      semester_id: offer?.semester_id ?? null,
      semester_name: offer?.semester?.name ?? null,
      study_plan_code: parsedCourseCode.study_plan_code ?? studyPlan?.year ?? studyPlan?.name ?? null,
      rcu: studyPlan?.approve_resolution ?? studyPlan?.creation_resolution ?? null,
      campus_name: normalizePlanningCampusName(offer?.campus?.name ?? null),
      faculty_code: offer?.faculty?.code ?? offer?.faculty?.abbreviation ?? null,
      faculty_name: offer?.faculty?.name ?? null,
      academic_program_code: offer?.academic_program?.code ?? offer?.academic_program_code ?? null,
      academic_program_code_akademic:
        parsedCourseCode.academic_program_code ?? offer?.academic_program?.code ?? offer?.academic_program_code ?? null,
      cycle: numberValue(offer?.cycle),
      section_code: sectionBaseCode,
      subsection_code: sectionBaseCode,
      course_code: parsedCourseCode.course_code ?? emptyToNull(offer?.course_code),
      course_name: emptyToNull(offer?.course_name),
      study_type: studyType?.name ?? null,
      plan_course_type: studyPlanCourse?.is_elective ? 'ELECTIVO' : 'OBLIGATORIO',
      course_modality_name: modalityName,
      theory_hours: safeNumber(numberValue(offer?.theoretical_hours)),
      practical_hours: safeNumber(numberValue(offer?.practical_hours)),
      total_hours: safeNumber(numberValue(offer?.total_hours)),
      credits: safeNumber(numberValue(studyPlanCourse?.credits)),
      projected_vacancies: numberValue(section?.projected_vacancies),
      teacher_dni: emptyToNull(effectiveTeacher?.dni),
      teacher_name: emptyToNull(effectiveTeacher?.full_name ?? effectiveTeacher?.name),
      delivery_modality_id: subsection?.course_modality_id ?? null,
      delivery_modality_name: modalityName,
      shift: emptyToNull(subsection?.shift),
      shift_id: emptyToNull(subsection?.shift),
      assistant_name: null,
      building_name: emptyToNull(effectiveBuilding?.name),
      classroom_name: emptyToNull(effectiveClassroom?.name),
      classroom_capacity: numberValue(effectiveClassroom?.capacity),
      laboratory_name:
        schedule?.session_type === 'LAB' ? emptyToNull(effectiveClassroom?.name ?? subsection?.classroom?.name) : null,
      laboratory_capacity:
        schedule?.session_type === 'LAB' ? numberValue(effectiveClassroom?.capacity) : null,
      second_shift: emptyToNull(subsection?.shift),
      subsection_kind_label: mapSubsectionKindLabel(subsection?.kind),
      day_of_week: dayName,
      day_number: dayName ? dayOfWeekToNumber(dayName) : null,
      start_hour: startParts.hour,
      start_minute: startParts.minute,
      end_hour: endParts.hour,
      end_minute: endParts.minute,
      academic_hours_value:
        safeNumber(numberValue(schedule?.academic_hours)) ??
        safeNumber(numberValue(subsection?.assigned_total_hours)),
      start_time_label: formatTimeLabel(schedule?.start_time ?? null),
      end_time_label: formatTimeLabel(schedule?.end_time ?? null),
      denomination: emptyToNull(subsection?.denomination),
      source_row_id: schedule?.id ?? `subsection:${subsection?.id ?? newId()}`,
      comparison_key: buildPlanningComparisonKey({
        academic_program_code: offer?.academic_program?.code ?? offer?.academic_program_code ?? null,
        cycle: numberValue(offer?.cycle),
        section_code: sectionBaseCode,
        course_code: parsedCourseCode.course_code ?? offer?.course_code ?? null,
        faculty_code: offer?.faculty?.code ?? offer?.faculty?.abbreviation ?? null,
        campus_name: offer?.campus?.name ?? null,
        study_plan_code: parsedCourseCode.study_plan_code ?? studyPlan?.year ?? studyPlan?.name ?? null,
      }),
      row_label:
        rowLabel ||
        `${parsedCourseCode.course_code ?? offer?.course_code ?? ''}`.trim() ||
        `fila ${schedule?.id ?? subsection?.id ?? ''}`,
    };
  }

  private matchesWorkspaceComparableFilters(row: ManualWorkspaceComparableRow, filters: WorkspaceExportFilters) {
    if (filters.delivery_modality_id) {
      const requested = normalizeLoose(filters.delivery_modality_id);
      const current = normalizeLoose(row.delivery_modality_id);
      if (!current || current !== requested) {
        return false;
      }
    }
    if (filters.shift_id) {
      const requested = normalizeLoose(filters.shift_id);
      const current = normalizeLoose(row.shift_id);
      if (!current || current !== requested) {
        return false;
      }
    }
    const query = normalizeLoose(filters.search);
    if (!query) {
      return true;
    }
    const haystack = normalizeLoose(
      [
        row.course_code,
        row.course_name,
        row.academic_program_code,
        row.academic_program_code_akademic,
        row.section_code,
        row.subsection_code,
        row.teacher_name,
        row.classroom_name,
        row.campus_name,
      ]
        .filter(Boolean)
        .join(' '),
    );
    return haystack.includes(query);
  }

  private compareWorkspaceComparableRows(left: ManualWorkspaceComparableRow, right: ManualWorkspaceComparableRow) {
    const courseDelta = `${left.course_code ?? ''} ${left.course_name ?? ''}`.localeCompare(
      `${right.course_code ?? ''} ${right.course_name ?? ''}`,
      'es',
      { sensitivity: 'base' },
    );
    if (courseDelta !== 0) {
      return courseDelta;
    }
    const sectionDelta = `${left.section_code ?? ''}`.localeCompare(`${right.section_code ?? ''}`, 'es', {
      sensitivity: 'base',
    });
    if (sectionDelta !== 0) {
      return sectionDelta;
    }
    const groupDelta = `${left.subsection_code ?? ''}`.localeCompare(`${right.subsection_code ?? ''}`, 'es', {
      sensitivity: 'base',
    });
    if (groupDelta !== 0) {
      return groupDelta;
    }
    const dayDelta = (left.day_number ?? 99) - (right.day_number ?? 99);
    if (dayDelta !== 0) {
      return dayDelta;
    }
    const startDelta = `${left.start_time_label ?? ''}`.localeCompare(`${right.start_time_label ?? ''}`);
    if (startDelta !== 0) {
      return startDelta;
    }
    return `${left.end_time_label ?? ''}`.localeCompare(`${right.end_time_label ?? ''}`);
  }

  private exportWorkbookHeaders() {
    return [
      'SEMESTRE',
      'RCU',
      'COD. PLAN',
      'LOCAL',
      'FACULTAD',
      'COD PROGRAMA',
      'COD PROGRAMA AKDEMIC',
      'CICLO',
      'SECCIÓN',
      'CODIGO CURSO',
      'NOMBRE DE CURSO',
      'TIPO DE ESTUDIOS',
      'TIPO DE CURSO PLAN',
      'MODALIDAD DE CURSO',
      'HORAS TEORÍA',
      'HORAS PRÁCTICA',
      'TOTAL DE HORAS',
      'TOTAL DE CREDITOS',
      'VACANTES PROYECTADAS',
      'DNI',
      'DOCENTE',
      'MODALIDAD',
      'TURNO',
      'JEFE DE PRACTICA',
      'PABELLON',
      'AULA',
      'AFORO AULA',
      'LABORATORIO',
      'AFORO LAB',
      'TURNO',
      'TIPO DE CURSO',
      'DIA',
      'HORA INICIO',
      'MINUTO INICIO',
      'HORA FIN',
      'MINUTO FIN',
      'HORAS ACADEMICAS',
      'HORA INICIO',
      'HORA FIN',
      'CRUCE DOCENTE',
      'DENOMINACIÓN',
      'CRUCE DE SECCIÓN',
    ];
  }

  private exportWorkbookRow(row: ManualWorkspaceComparableRow) {
    return [
      row.semester_name ?? '',
      row.rcu ?? '',
      row.study_plan_code ?? '',
      row.campus_name ?? '',
      row.faculty_code ?? row.faculty_name ?? '',
      row.academic_program_code ?? '',
      row.academic_program_code_akademic ?? row.academic_program_code ?? '',
      row.cycle ?? '',
      row.subsection_code ?? row.section_code ?? '',
      row.course_code ?? '',
      row.course_name ?? '',
      row.study_type ?? '',
      row.plan_course_type ?? '',
      row.course_modality_name ?? '',
      row.theory_hours ?? '',
      row.practical_hours ?? '',
      row.total_hours ?? '',
      row.credits ?? '',
      row.projected_vacancies ?? '',
      row.teacher_dni ?? '',
      row.teacher_name ?? '',
      row.delivery_modality_name ?? '',
      row.shift ?? '',
      row.assistant_name ?? '',
      row.building_name ?? '',
      row.classroom_name ?? '',
      row.classroom_capacity ?? '',
      row.laboratory_name ?? '',
      row.laboratory_capacity ?? '',
      row.second_shift ?? '',
      row.subsection_kind_label ?? '',
      row.day_number ?? '',
      row.start_hour ?? '',
      row.start_minute ?? '',
      row.end_hour ?? '',
      row.end_minute ?? '',
      row.academic_hours_value ?? '',
      row.start_time_label ?? '',
      row.end_time_label ?? '',
      '',
      row.denomination ?? '',
      '',
    ];
  }

  private async parseComparisonExcelRows(fileBuffer: Buffer, requestedSemesterId: string) {
    const semester = await this.requireEntity(this.semestersRepo, requestedSemesterId, 'semester');
    const workbook = XLSX.read(fileBuffer, { type: 'buffer', raw: true });
    const sheetName =
      workbook.SheetNames.find((name) => normalizeLoose(name) === 'PLANIFICACION') ??
      workbook.SheetNames.find((name) => name === IMPORT_SHEET_NAME) ??
      workbook.SheetNames[0] ??
      null;
    const sheet = sheetName ? workbook.Sheets[sheetName] ?? null : null;
    if (!sheet) {
      throw new BadRequestException('No se encontro una hoja valida para comparar.');
    }
    const rawRows = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
      defval: null,
      raw: true,
    });
    const catalog = await this.loadImportCatalog();
    const normalizedRows = rawRows
      .map((row, index) => this.normalizeExcelRow(row, index + 2))
      .filter((row): row is NormalizedImportRow => Boolean(row));
    const previewRows = normalizedRows.map((row) => this.resolvePreviewRow(row, catalog));
    this.assignStructuralCodes(previewRows);

    const warnings: Array<Record<string, unknown>> = [];
    const comparableRows: ManualWorkspaceComparableRow[] = [];
    for (let index = 0; index < previewRows.length; index += 1) {
      const comparable = this.buildComparisonRowFromExcelPreview(
        normalizedRows[index],
        previewRows[index],
        semester,
        warnings,
      );
      if (comparable) {
        comparableRows.push(comparable);
      }
    }
    return { rows: comparableRows, warnings, raw_row_count: rawRows.length, sheet_name: sheetName };
  }

  private buildComparisonRowFromExcelPreview(
    normalized: NormalizedImportRow,
    previewRow: PreviewRow,
    requestedSemester: SemesterEntity,
    warnings: Array<Record<string, unknown>>,
  ) {
    const resolution = asRecord(previewRow.resolution_json);
    const schedule = asRecord(recordValue(resolution, 'schedule'));
    const parsedCourseCode = parseCompositePlanningCourseCode(normalized.course_code_raw);
    const sectionToken = parseSectionToken(normalized.section_raw);
    const semesterToken = normalizePeriodToken(normalized.semester_raw);
    const requestedToken = normalizePeriodToken(requestedSemester.name);
    if (semesterToken && requestedToken && semesterToken !== requestedToken) {
      warnings.push({
        row_number: previewRow.row_number,
        type: 'SEMESTER_MISMATCH',
        message: `La fila pertenece a ${normalized.semester_raw || 'otro semestre'} y se omitio del comparador para ${requestedSemester.name}.`,
      });
      return null;
    }

    const sectionCode =
      recordString(resolution, 'import_section_code') ??
      recordString(resolution, 'section_base_code') ??
      sectionToken.section_code;
    const normalizedSectionCode = normalizePlanningSectionCode(sectionCode);
    const dayName = normalizeDayValue(recordString(schedule, 'day_of_week') ?? normalized.day_raw);
    const startTime = formatImportedTime(
      normalized.start_hour_raw,
      normalized.start_minute_raw,
      recordString(schedule, 'start_time'),
    );
    const endTime = formatImportedTime(
      normalized.end_hour_raw,
      normalized.end_minute_raw,
      recordString(schedule, 'end_time'),
    );
    const comparisonKey = buildPlanningComparisonKey({
      academic_program_code:
        stringifyCell(firstCellValue(normalized.raw_row, 'COD PROGRAMA')) || normalized.academic_program_code_raw,
      cycle: normalized.cycle_raw,
      section_code: normalizedSectionCode,
      course_code: parsedCourseCode.course_code ?? normalized.course_code_raw,
      faculty_code: normalized.faculty_code_raw,
      campus_name: normalized.campus_raw,
      study_plan_code: normalized.study_plan_code_raw || parsedCourseCode.study_plan_code,
    });
    if (!comparisonKey) {
      warnings.push({
        row_number: previewRow.row_number,
        type: 'INCOMPLETE_KEY',
        message: 'La fila no tiene suficiente informacion para construir la clave de comparacion.',
        issues: previewRow.issues.map((item) => item.message),
      });
      return null;
    }

    const modalityResolution = asRecord(recordValue(asRecord(recordValue(resolution, 'mapping_resolution')), 'course_modality'));
    const modalityLabel = normalizePlanningModalityLabel(
      null,
      recordString(modalityResolution, 'target_label') ??
        normalized.delivery_modality_raw ??
        normalized.course_modality_raw,
      recordString(modalityResolution, 'target_id') ?? null,
    );
    const rowLabel = [
      parsedCourseCode.course_code ?? normalized.course_code_raw,
      normalized.course_name_raw,
      normalizedSectionCode,
      dayName ? `${dayName} ${startTime ?? ''}-${endTime ?? ''}` : '',
    ]
      .filter(Boolean)
      .join(' | ');

    const nonBlockingIssues = previewRow.issues.filter((item) => item.severity !== 'BLOCKING');
    if (nonBlockingIssues.length) {
      warnings.push({
        row_number: previewRow.row_number,
        type: 'ROW_WARNING',
        message: nonBlockingIssues.map((item) => item.message).join(' | '),
      });
    }

    return {
      semester_id: requestedSemester.id,
      semester_name: requestedSemester.name,
      study_plan_code: normalized.study_plan_code_raw || parsedCourseCode.study_plan_code || null,
      rcu: null,
      campus_name: normalizePlanningCampusName(normalized.campus_raw || null),
      faculty_code: normalized.faculty_code_raw || null,
      faculty_name: null,
      academic_program_code:
        stringifyCell(firstCellValue(normalized.raw_row, 'COD PROGRAMA')) || normalized.academic_program_code_raw || null,
      academic_program_code_akademic:
        normalized.academic_program_code_akademic_raw || parsedCourseCode.academic_program_code || null,
      cycle: normalized.cycle_raw,
      section_code: normalizedSectionCode,
      subsection_code: normalizedSectionCode,
      course_code: parsedCourseCode.course_code ?? normalized.course_code_raw ?? null,
      course_name: normalized.course_name_raw || null,
      study_type: normalized.study_type_raw || null,
      plan_course_type: normalized.course_requirement_raw || null,
      course_modality_name: normalized.course_modality_raw || null,
      theory_hours: safeNumber(normalized.theory_hours),
      practical_hours: safeNumber(normalized.practical_hours),
      total_hours: safeNumber(normalized.total_hours),
      credits: normalized.credits,
      projected_vacancies: normalized.projected_vacancies,
      teacher_dni: normalized.teacher_dni_raw || null,
      teacher_name: normalized.teacher_name_raw || null,
      delivery_modality_id: recordString(modalityResolution, 'target_id') ?? null,
      delivery_modality_name: modalityLabel ?? null,
      shift: normalized.shift_raw || null,
      shift_id: normalized.shift_raw || null,
      assistant_name: null,
      building_name: normalized.building_raw || null,
      classroom_name: normalized.classroom_raw || null,
      classroom_capacity: null,
      laboratory_name: normalized.laboratory_raw || null,
      laboratory_capacity: null,
      second_shift: normalized.shift_raw || null,
      subsection_kind_label: normalized.course_requirement_raw || null,
      day_of_week: dayName,
      day_number: dayName ? dayOfWeekToNumber(dayName) : null,
      start_hour: numberFromCell(normalized.start_hour_raw),
      start_minute: numberFromCell(normalized.start_minute_raw),
      end_hour: numberFromCell(normalized.end_hour_raw),
      end_minute: numberFromCell(normalized.end_minute_raw),
      academic_hours_value:
        normalized.academic_hours_raw ?? safeNumber(numberValue(recordValue(schedule, 'academic_hours'))),
      start_time_label: startTime,
      end_time_label: endTime,
      denomination: normalized.denomination_raw || null,
      source_row_id: `excel:${previewRow.row_number}`,
      comparison_key: comparisonKey,
      row_label: rowLabel || `fila ${previewRow.row_number}`,
    };
  }

  private compareComparableRows(
    excelRowsInput:
      | ManualWorkspaceComparableRow[]
      | { rows: ManualWorkspaceComparableRow[]; warnings: Array<Record<string, unknown>> },
    systemRows: ManualWorkspaceComparableRow[],
    context: { semester_id: string; semester_name: string | null; file_name: string },
  ): ExcelComparisonResult {
    const excelRows = this.collapseComparableRowsByKey(
      Array.isArray(excelRowsInput) ? excelRowsInput : excelRowsInput.rows,
    );
    const warnings = Array.isArray(excelRowsInput) ? [] : [...excelRowsInput.warnings];
    const systemRowsCollapsed = this.collapseComparableRowsByKey(systemRows);
    const excelByKey = new Map<string, ManualWorkspaceComparableRow>();
    const systemByKey = new Map<string, ManualWorkspaceComparableRow>();

    for (const row of excelRows) {
      if (!row.comparison_key) {
        continue;
      }
      if (excelByKey.has(row.comparison_key)) {
        warnings.push({
          type: 'DUPLICATE_IN_EXCEL',
          key: row.comparison_key,
          message: `Se encontro una fila duplicada en el Excel para ${row.row_label}.`,
        });
        continue;
      }
      excelByKey.set(row.comparison_key, row);
    }
    for (const row of systemRowsCollapsed) {
      if (!row.comparison_key) {
        continue;
      }
      if (systemByKey.has(row.comparison_key)) {
        warnings.push({
          type: 'DUPLICATE_IN_SYSTEM',
          key: row.comparison_key,
          message: `Se encontro una fila duplicada en el sistema para ${row.row_label}.`,
        });
        continue;
      }
      systemByKey.set(row.comparison_key, row);
    }

    const onlyInExcel: ManualWorkspaceComparableRow[] = [];
    const onlyInSystem: ManualWorkspaceComparableRow[] = [];
    const differences: ExcelComparisonDifference[] = [];
    let matchedCount = 0;

    const keys = uniqueIds([...excelByKey.keys(), ...systemByKey.keys()]).sort();
    for (const key of keys) {
      const excelRow = excelByKey.get(key) ?? null;
      const systemRow = systemByKey.get(key) ?? null;
      if (excelRow && !systemRow) {
        onlyInExcel.push(excelRow);
        continue;
      }
      if (!excelRow && systemRow) {
        onlyInSystem.push(systemRow);
        continue;
      }
      if (!excelRow || !systemRow) {
        continue;
      }
      const fields = this.diffComparableRows(excelRow, systemRow);
      if (!fields.length) {
        matchedCount += 1;
        continue;
      }
      differences.push({
        key,
        row_label: excelRow.row_label || systemRow.row_label,
        fields,
      });
    }

    return {
      summary: {
        semester_id: context.semester_id,
        semester_name: context.semester_name,
        file_name: context.file_name,
        excel_rows: excelRows.length,
        system_rows: systemRows.length,
        coinciden: matchedCount,
        solo_en_excel: onlyInExcel.length,
        solo_en_sistema: onlyInSystem.length,
        con_diferencias: differences.length,
        warnings: warnings.length,
      },
      warnings,
      only_in_excel: onlyInExcel,
      only_in_system: onlyInSystem,
      differences,
    };
  }

  private diffComparableRows(excelRow: ManualWorkspaceComparableRow, systemRow: ManualWorkspaceComparableRow) {
    const fields = [
      { field: 'teacher_name', label: 'Docente', excel: excelRow.teacher_name, system: systemRow.teacher_name },
      { field: 'teacher_dni', label: 'DNI', excel: excelRow.teacher_dni, system: systemRow.teacher_dni },
      { field: 'delivery_modality_name', label: 'Modalidad', excel: excelRow.delivery_modality_name, system: systemRow.delivery_modality_name },
      { field: 'shift', label: 'Turno', excel: excelRow.shift, system: systemRow.shift },
      { field: 'building_name', label: 'Pabellon', excel: excelRow.building_name, system: systemRow.building_name },
      { field: 'classroom_name', label: 'Aula', excel: excelRow.classroom_name, system: systemRow.classroom_name },
      { field: 'laboratory_name', label: 'Laboratorio', excel: excelRow.laboratory_name, system: systemRow.laboratory_name },
      { field: 'projected_vacancies', label: 'Vacantes', excel: excelRow.projected_vacancies, system: systemRow.projected_vacancies },
      { field: 'start_time_label', label: 'Hora inicio', excel: excelRow.start_time_label, system: systemRow.start_time_label },
      { field: 'end_time_label', label: 'Hora fin', excel: excelRow.end_time_label, system: systemRow.end_time_label },
      { field: 'academic_hours_value', label: 'Horas academicas', excel: excelRow.academic_hours_value, system: systemRow.academic_hours_value },
      { field: 'denomination', label: 'Denominacion', excel: excelRow.denomination, system: systemRow.denomination },
    ];
    return fields
      .filter((item) => normalizeDiffValue(item.excel) !== normalizeDiffValue(item.system))
      .map((item) => ({
        field: item.field,
        label: item.label,
        excel_value: formatDiffValue(item.excel),
        system_value: formatDiffValue(item.system),
      }));
  }

  private buildComparisonWorkbook(result: ExcelComparisonResult) {
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet([
        {
          semestre: result.summary.semester_name,
          archivo: result.summary.file_name,
          filas_excel: result.summary.excel_rows,
          filas_sistema: result.summary.system_rows,
          coinciden: result.summary.coinciden,
          solo_en_excel: result.summary.solo_en_excel,
          solo_en_sistema: result.summary.solo_en_sistema,
          con_diferencias: result.summary.con_diferencias,
          warnings: result.summary.warnings,
        },
      ]),
      'resumen',
    );
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(result.only_in_excel.map((row) => this.comparisonRowSheetEntry(row))),
      'solo_en_excel',
    );
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(result.only_in_system.map((row) => this.comparisonRowSheetEntry(row))),
      'solo_en_sistema',
    );
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(
        result.differences.flatMap((item) =>
          item.fields.map((field) => ({
            key: item.key,
            fila: item.row_label,
            campo: field.label,
            valor_excel: field.excel_value,
            valor_sistema: field.system_value,
          })),
        ),
      ),
      'con_diferencias',
    );
    return workbook;
  }

  private comparisonRowSheetEntry(row: ManualWorkspaceComparableRow) {
    return {
      semestre: row.semester_name,
      local: row.campus_name,
      facultad: row.faculty_code ?? row.faculty_name,
      cod_programa: row.academic_program_code,
      cod_programa_akademic: row.academic_program_code_akademic,
      cod_plan: row.study_plan_code,
      ciclo: row.cycle,
      curso_codigo: row.course_code,
      curso_nombre: row.course_name,
      seccion: row.section_code,
      grupo: row.subsection_code,
      dia: row.day_of_week,
      hora_inicio: row.start_time_label,
      hora_fin: row.end_time_label,
      docente: row.teacher_name,
      modalidad: row.delivery_modality_name,
      turno: row.shift,
      pabellon: row.building_name,
      aula: row.classroom_name,
      vacantes: row.projected_vacancies,
      denominacion: row.denomination,
    };
  }

  private collapseComparableRowsByKey(rows: ManualWorkspaceComparableRow[]) {
    const grouped = new Map<string, ManualWorkspaceComparableRow[]>();
    for (const row of rows) {
      if (!row.comparison_key) {
        continue;
      }
      const bucket = grouped.get(row.comparison_key) ?? [];
      bucket.push(row);
      grouped.set(row.comparison_key, bucket);
    }
    return [...grouped.values()].map((group) => this.mergeComparableRowGroup(group));
  }

  private mergeComparableRowGroup(group: ManualWorkspaceComparableRow[]) {
    const base = { ...group[0] };
    const mergeText = (selector: (row: ManualWorkspaceComparableRow) => string | null | undefined) => {
      const values = uniqueIds(group.map((row) => `${selector(row) ?? ''}`.trim()).filter(Boolean));
      return values.length ? values.join(' | ') : null;
    };
    const mergeNumber = (selector: (row: ManualWorkspaceComparableRow) => number | null | undefined) => {
      const values = uniqueIds(
        group
          .map((row) => selector(row))
          .filter((value): value is number => value !== null && value !== undefined)
          .map((value) => `${value}`),
      );
      return values.length ? Number(values[0]) : null;
    };

    base.row_label = mergeText((row) => row.row_label) ?? base.row_label;
    base.teacher_name = mergeText((row) => row.teacher_name);
    base.teacher_dni = mergeText((row) => row.teacher_dni);
    base.delivery_modality_name = mergeText((row) => row.delivery_modality_name);
    base.shift = mergeText((row) => row.shift);
    base.building_name = mergeText((row) => row.building_name);
    base.classroom_name = mergeText((row) => row.classroom_name);
    base.laboratory_name = mergeText((row) => row.laboratory_name);
    base.day_of_week = mergeText((row) => row.day_of_week);
    base.start_time_label = mergeText((row) => row.start_time_label);
    base.end_time_label = mergeText((row) => row.end_time_label);
    base.denomination = mergeText((row) => row.denomination);
    base.projected_vacancies = mergeNumber((row) => row.projected_vacancies);
    base.academic_hours_value = mergeNumber((row) => row.academic_hours_value);
    return base;
  }
}

function firstCellValue(row: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(row, key) && row[key] !== undefined) {
      return row[key];
    }
  }
  return undefined;
}

function extractFormattedTime(value: unknown) {
  const raw = stringifyCell(value);
  const match = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) {
    return null;
  }
  return {
    hour: String(Number(match[1])),
    minute: String(Number(match[2])),
  };
}

function splitTimeParts(value: string | null | undefined) {
  const match = `${value ?? ''}`.trim().match(/^(\d{1,2}):(\d{2})/);
  if (!match) {
    return { hour: null, minute: null };
  }
  return {
    hour: Number(match[1]),
    minute: Number(match[2]),
  };
}

function formatTimeLabel(value: string | null | undefined) {
  const match = `${value ?? ''}`.trim().match(/^(\d{1,2}):(\d{2})/);
  if (!match) {
    return null;
  }
  return `${match[1].padStart(2, '0')}:${match[2]}`;
}

function formatImportedTime(hourRaw: string | null | undefined, minuteRaw: string | null | undefined, fallback?: string | null) {
  const hour = numberFromCell(hourRaw);
  const minute = numberFromCell(minuteRaw);
  if (hour !== null) {
    return `${String(hour).padStart(2, '0')}:${String(minute ?? 0).padStart(2, '0')}`;
  }
  return formatTimeLabel(fallback ?? null);
}

function normalizeDayValue(value: string | null | undefined) {
  const parsed = parseDayOfWeek(value);
  return parsed ?? null;
}

function dayOfWeekToNumber(value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  const map: Record<string, number> = {
    LUNES: 1,
    MARTES: 2,
    MIERCOLES: 3,
    JUEVES: 4,
    VIERNES: 5,
    SABADO: 6,
    DOMINGO: 7,
  };
  return map[normalized] ?? null;
}

function mapSubsectionKindLabel(value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  if (normalized === 'THEORY') {
    return 'TEORIA';
  }
  if (normalized === 'PRACTICE') {
    return 'PRACTICA';
  }
  if (normalized === 'LAB') {
    return 'LABORATORIO';
  }
  if (normalized === 'MIXED') {
    return 'MIXTO';
  }
  return `${value ?? ''}`.trim() || null;
}

function buildPlanningComparisonKey(input: {
  academic_program_code: string | null | undefined;
  cycle: number | null | undefined;
  section_code: string | null | undefined;
  course_code: string | null | undefined;
  faculty_code: string | null | undefined;
  campus_name: string | null | undefined;
  study_plan_code: string | null | undefined;
}) {
  const parts = [
    normalizeLoose(input.academic_program_code),
    input.cycle !== null && input.cycle !== undefined ? String(input.cycle) : '',
    normalizeLoose(input.section_code),
    normalizeLoose(input.course_code),
    normalizeLoose(input.faculty_code),
    normalizeLoose(normalizePlanningCampusName(input.campus_name)),
    normalizeLoose(input.study_plan_code),
  ];
  if (parts.some((item) => !item)) {
    return '';
  }
  return parts.join('||');
}

function parseCompositePlanningCourseCode(value: string | null | undefined) {
  const raw = `${value ?? ''}`.trim();
  const parts = raw.split('-').map((item) => item.trim()).filter(Boolean);
  if (parts.length < 3) {
    return {
      academic_program_code: null,
      study_plan_code: null,
      course_code: raw || null,
    };
  }
  return {
    academic_program_code: parts[0] || null,
    study_plan_code: parts[1] || null,
    course_code: parts.slice(2).join('-') || null,
  };
}

function normalizePlanningCampusName(value: string | null | undefined) {
  const raw = `${value ?? ''}`.trim();
  if (!raw) {
    return null;
  }
  return normalizeLoose(raw) === 'SEDE CENTRAL' ? 'PRINCIPAL' : raw;
}

function normalizePlanningSectionCode(value: string | null | undefined) {
  const token = parseSectionToken(value);
  return token.section_code ?? emptyToNull(value);
}

function normalizePlanningModalityLabel(
  modalityToken: string | null | undefined,
  fallbackLabel: string | null | undefined,
  fallbackId: string | null | undefined,
) {
  const normalizedToken = normalizeLoose(modalityToken || fallbackId || '');
  if (normalizedToken === 'V' || normalizedToken === 'CV') {
    return 'Virtual';
  }
  if (normalizedToken === 'P' || normalizedToken === 'CP') {
    return 'Presencial';
  }
  if (normalizedToken === 'HP' || normalizedToken === 'CHP') {
    return 'Hibrido presencial';
  }
  if (normalizedToken === 'HV' || normalizedToken === 'CHV') {
    return 'Hibrido virtual';
  }
  const normalizedLabel = normalizeLoose(fallbackLabel);
  if (normalizedLabel === 'VIRTUAL') {
    return 'Virtual';
  }
  if (normalizedLabel === 'PRESENCIAL') {
    return 'Presencial';
  }
  if (normalizedLabel.includes('HIBRIDO') && normalizedLabel.includes('PRESENCIAL')) {
    return 'Hibrido presencial';
  }
  if (normalizedLabel.includes('HIBRIDO') && normalizedLabel.includes('VIRTUAL')) {
    return 'Hibrido virtual';
  }
  return `${fallbackLabel ?? ''}`.trim() || null;
}

function normalizeDiffValue(value: unknown) {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : '';
  }
  return normalizeLoose(String(value));
}

function formatDiffValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return '';
  }
  return typeof value === 'number' ? String(value) : `${value}`;
}

function sanitizeFileName(value: string | null | undefined) {
  const raw = `${value ?? ''}`.trim();
  if (!raw) {
    return 'export';
  }
  return raw.replace(/[\\/:*?"<>|]+/g, '-').replace(/\s+/g, '_');
}

function buildAliasMap(rows: PlanningImportAliasMappingEntity[]) {
  const aliasMap = new Map<AliasNamespace, Map<string, PlanningImportAliasMappingEntity>>();
  for (const row of rows) {
    const namespace = normalizeAliasNamespace(row.namespace);
    if (!namespace) {
      continue;
    }
    const namespaceMap = aliasMap.get(namespace) ?? new Map<string, PlanningImportAliasMappingEntity>();
    namespaceMap.set(normalizeSourceValue(row.source_value), row);
    aliasMap.set(namespace, namespaceMap);
  }
  return aliasMap;
}

function buildStudyPlanCatalogForImport(
  studyPlans: StudyPlanEntity[],
  faculties: FacultyEntity[],
  academicPrograms: AcademicProgramEntity[],
) {
  if (studyPlans.length === 0) {
    return {
      faculties,
      academicPrograms,
      studyPlans,
    };
  }

  const facultyIdByName = new Map(
    faculties
      .filter((item) => item.name)
      .map((item) => [normalizeCatalogName(item.name), item.id] as const),
  );
  const programsByName = new Map<string, AcademicProgramEntity[]>();
  for (const program of academicPrograms) {
    const key = normalizeCatalogName(program.name);
    if (!key) {
      continue;
    }
    const bucket = programsByName.get(key) ?? [];
    bucket.push(program);
    programsByName.set(key, bucket);
  }

  const normalizedStudyPlans = studyPlans.map((plan) => {
    const facultyName = coalesceCatalogLabel(plan.faculty);
    const careerName = coalesceCatalogLabel(plan.career, plan.academic_program);
    const resolvedFacultyId =
      plan.faculty_id ??
      (facultyName ? facultyIdByName.get(normalizeCatalogName(facultyName)) ?? null : null) ??
      (facultyName ? buildStableCatalogId('faculty', facultyName) : null);
    const resolvedProgramId =
      plan.academic_program_id ??
      resolveCatalogProgramIdForImport(
        programsByName,
        careerName,
        facultyName,
        resolvedFacultyId,
      ) ??
      (careerName ? buildStableCatalogId('program', facultyName, careerName) : null);

    return {
      ...plan,
      faculty: facultyName,
      career: careerName,
      academic_program: careerName,
      faculty_id: resolvedFacultyId,
      academic_program_id: resolvedProgramId,
    } as StudyPlanEntity;
  });

  const facultyMap = new Map<string, FacultyEntity>();
  const academicProgramMap = new Map<string, AcademicProgramEntity>();

  for (const plan of normalizedStudyPlans) {
    if (plan.faculty && plan.faculty_id && !facultyMap.has(plan.faculty_id)) {
      facultyMap.set(plan.faculty_id, {
        id: plan.faculty_id,
        code: null,
        name: plan.faculty,
        abbreviation: null,
        institucional_email: null,
        is_active: true,
        is_valid: true,
      } as FacultyEntity);
    }
    if (plan.career && plan.academic_program_id && !academicProgramMap.has(plan.academic_program_id)) {
      academicProgramMap.set(plan.academic_program_id, {
        id: plan.academic_program_id,
        code: null,
        name: plan.career,
        faculty_id: plan.faculty_id ?? null,
        faculty: plan.faculty ?? null,
        graduate_profile: null,
        general_information: null,
        comments: null,
        decanal_resolution: null,
        rectoral_resolution: null,
        is_active: true,
        is_valid: true,
      } as AcademicProgramEntity);
    }
  }

  return {
    faculties: [...facultyMap.values()].sort((a, b) => compareCatalogLabels(a.name, b.name)),
    academicPrograms: [...academicProgramMap.values()].sort((a, b) =>
      compareCatalogLabels(`${a.faculty ?? ''} ${a.name ?? ''}`, `${b.faculty ?? ''} ${b.name ?? ''}`),
    ),
    studyPlans: normalizedStudyPlans,
  };
}

function resolveCatalogProgramIdForImport(
  programsByName: Map<string, AcademicProgramEntity[]>,
  careerName: string | null,
  facultyName: string | null,
  facultyId: string | null,
) {
  const normalizedCareer = normalizeCatalogName(careerName);
  if (!normalizedCareer) {
    return null;
  }

  const matches = programsByName.get(normalizedCareer) ?? [];
  if (matches.length === 0) {
    return null;
  }
  if (matches.length === 1) {
    return matches[0].id;
  }

  if (facultyId) {
    const byFacultyId = matches.find((item) => item.faculty_id === facultyId);
    if (byFacultyId) {
      return byFacultyId.id;
    }
  }

  const normalizedFaculty = normalizeCatalogName(facultyName);
  if (normalizedFaculty) {
    const byFacultyName = matches.find(
      (item) => normalizeCatalogName(item.faculty) === normalizedFaculty,
    );
    if (byFacultyName) {
      return byFacultyName.id;
    }
  }

  return matches[0].id;
}

function resolveAlias(
  aliasMap: Map<AliasNamespace, Map<string, PlanningImportAliasMappingEntity>>,
  namespace: AliasNamespace,
  value: string | null | undefined,
) {
  const normalized = normalizeSourceValue(value);
  if (!normalized) {
    return null;
  }
  return aliasMap.get(namespace)?.get(normalized) ?? null;
}

function normalizeAliasNamespace(value: string | null | undefined): AliasNamespace | null {
  const normalized = normalizeLoose(value);
  if (
    normalized === 'VC_PERIOD' ||
    normalized === 'CAMPUS' ||
    normalized === 'FACULTY_CODE' ||
    normalized === 'ACADEMIC_PROGRAM_CODE' ||
    normalized === 'STUDY_PLAN_CODE' ||
    normalized === 'COURSE_CODE' ||
    normalized === 'COURSE_MODALITY' ||
    normalized === 'SHIFT' ||
    normalized === 'BUILDING' ||
    normalized === 'CLASSROOM' ||
    normalized === 'LABORATORY'
  ) {
    return normalized.toLowerCase() as AliasNamespace;
  }
  return null;
}

function buildScopeKey(scope: Pick<ImportScope, 'semester_id' | 'vc_period_id' | 'campus_id' | 'faculty_id' | 'academic_program_id' | 'study_plan_id' | 'cycle'>) {
  const rawScopeKey = JSON.stringify([
    scope.semester_id,
    scope.vc_period_id,
    scope.campus_id,
    scope.faculty_id,
    scope.academic_program_id,
    scope.study_plan_id,
    scope.cycle,
  ]);
  // Keep the persisted/indexed key short and deterministic even if the scope grows.
  return createHash('sha256').update(rawScopeKey).digest('hex');
}

function hasExistingData(value: Record<string, unknown> | null | undefined) {
  const existing = asRecord(value);
  return Boolean(recordBoolean(existing, 'has_existing_data'));
}

function toResolution(
  sourceValue: string,
  targetId: string | null,
  targetLabel: string | null,
  matchSource: MappingResolution['match_source'],
  targetExtra?: Record<string, unknown>,
) {
  return {
    source_value: sourceValue,
    target_id: targetId,
    target_label: targetLabel,
    match_source: targetId ? matchSource : 'none',
    target_extra: targetExtra ?? null,
  } as MappingResolution;
}

function missingCriticalIssue(namespace: string, sourceValue: string, message: string): PreviewIssue {
  return {
    severity: 'BLOCKING',
    issue_code: `MISSING_${namespace.toUpperCase()}`,
    field_name: namespace,
    message,
    meta_json: {
      namespace,
      source_value: sourceValue,
    },
  };
}

function shouldSkipExcelRow(row: NormalizedImportRow) {
  const normalizedSemester = normalizeLoose(row.semester_raw);
  if (normalizedSemester === 'VX_PERIODS.TEXT') {
    return true;
  }
  return ![
    row.semester_raw,
    row.study_plan_code_raw,
    row.campus_raw,
    row.academic_program_code_raw,
    row.section_raw,
    row.course_code_raw,
    row.course_name_raw,
  ].some(Boolean);
}

function stringifyCell(value: unknown) {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return String(value);
    }
    return `${value}`.replace(/\.0+$/, '');
  }
  return `${value}`.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
}

function numberFromCell(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  const normalized = `${value}`.replace(',', '.').trim();
  if (!normalized) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return 0;
  }
  return roundToTwo(value);
}

function teacherNamesLookEquivalent(left: string | null | undefined, right: string | null | undefined) {
  const normalizedLeft = normalizeLoose(left);
  const normalizedRight = normalizeLoose(right);
  if (!normalizedLeft || !normalizedRight) {
    return false;
  }
  if (normalizedLeft === normalizedRight) {
    return true;
  }
  const leftTokens = normalizedLeft.split(' ').filter(Boolean);
  const rightTokens = normalizedRight.split(' ').filter(Boolean);
  if (!leftTokens.length || !rightTokens.length) {
    return false;
  }
  const leftSet = new Set(leftTokens);
  const rightSet = new Set(rightTokens);
  const common = leftTokens.filter((token) => rightSet.has(token)).length;
  const required = Math.min(leftSet.size, rightSet.size);
  return common >= Math.max(2, required);
}

function normalizeLoose(value: string | null | undefined) {
  return `${value ?? ''}`
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();
}

function coalesceCatalogLabel(...values: Array<string | null | undefined>) {
  for (const value of values) {
    const normalized = value?.trim().replace(/\s+/g, ' ') ?? '';
    if (normalized) {
      return normalized;
    }
  }
  return null;
}

function normalizeCatalogName(value: string | null | undefined) {
  const normalized = coalesceCatalogLabel(value);
  if (!normalized) {
    return '';
  }
  return normalized.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
}

function compareCatalogLabels(left: string | null | undefined, right: string | null | undefined) {
  return (left ?? '').localeCompare(right ?? '', 'es', { sensitivity: 'base' });
}

function buildStableCatalogId(namespace: string, ...parts: Array<string | null | undefined>) {
  const seed = [namespace, ...parts.map((item) => normalizeCatalogName(item)).filter(Boolean)].join('|');
  const digest = createHash('sha1').update(seed).digest('hex').slice(0, 32);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-${digest.slice(12, 16)}-${digest.slice(16, 20)}-${digest.slice(20, 32)}`;
}

function normalizeSourceValue(value: string | null | undefined) {
  return normalizeLoose(value);
}

function normalizePeriodToken(value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  if (!normalized) {
    return '';
  }
  const compact = normalized.replace(/\s+/g, '');
  const match = compact.match(/\d{4}-\d/);
  return match ? match[0] : compact;
}

function findSemesterByPeriodValue(semesters: SemesterEntity[], value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  const token = normalizePeriodToken(value);
  return (
    semesters.find((item) => normalizeLoose(item.name) === normalized) ??
    semesters.find((item) => normalizePeriodToken(item.name) === token) ??
    null
  );
}

function findVcPeriodByValue(vcPeriods: VcPeriodEntity[], value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  const token = normalizePeriodToken(value);
  return (
    vcPeriods.find((item) => normalizeLoose(item.text) === normalized) ??
    vcPeriods.find((item) => normalizePeriodToken(item.text) === token) ??
    null
  );
}

function normalizeCourseName(value: string | null | undefined) {
  const raw = `${value ?? ''}`.trim();
  if (!raw) {
    return '';
  }
  const parts = raw.split(/\s-\s/);
  return normalizeLoose(parts.length > 1 ? parts.slice(1).join(' - ') : raw);
}

function extractCycleFromStudyPlanCourse(course: Pick<StudyPlanCourseEntity, 'year_label'>) {
  const rawLabel = `${course.year_label ?? ''}`.trim();
  if (!rawLabel) {
    return null;
  }
  const match = rawLabel.match(/(\d+)/);
  return match ? Number(match[1]) : null;
}

function normalizeCourseCodeValue(value: string | null | undefined) {
  return normalizeLoose(value).replace(/[^0-9A-Z]/g, '');
}

function buildLocationCandidateLabels(value: string | null | undefined) {
  const raw = `${value ?? ''}`.replace(/\s+/g, ' ').trim();
  if (!raw) {
    return [];
  }
  const candidates = new Set<string>();
  candidates.add(raw);
  const beforePipe = raw.split('|')[0]?.trim() ?? '';
  if (beforePipe) {
    candidates.add(beforePipe);
  }
  const withoutSuffix = raw.replace(/\s*\|\s*[A-Z0-9-]{1,12}\s*$/i, '').trim();
  if (withoutSuffix) {
    candidates.add(withoutSuffix);
  }
  return [...candidates];
}

function vcCourseCodeMatchesStudyPlanYear(
  vcCourseCode: string | null | undefined,
  normalizedStudyPlanYear: string,
) {
  const normalizedCode = normalizeLoose(vcCourseCode);
  if (!normalizedCode || !normalizedStudyPlanYear) {
    return false;
  }
  return (
    normalizedCode === normalizedStudyPlanYear ||
    normalizedCode.startsWith(`${normalizedStudyPlanYear}-`)
  );
}

function courseCodeLikeMatch(candidateCode: string | null | undefined, normalizedCourseCode: string) {
  const normalizedCandidate = normalizeCourseCodeValue(candidateCode);
  if (!normalizedCandidate || !normalizedCourseCode) {
    return false;
  }
  return (
    normalizedCandidate === normalizedCourseCode ||
    normalizedCandidate.includes(normalizedCourseCode) ||
    normalizedCourseCode.includes(normalizedCandidate)
  );
}

function buildAliasSourceValue(
  namespace: AliasNamespace,
  sourceValue: string | null | undefined,
  context?: AliasSourceContext | null,
) {
  const normalizedSource = normalizeSourceValue(sourceValue);
  if (!normalizedSource) {
    return '';
  }
  const program = normalizeSourceValue(context?.academic_program_code_raw);
  const studyPlan = normalizeSourceValue(context?.study_plan_code_raw);
  const cycle = Number(context?.cycle_raw ?? 0) > 0 ? String(Number(context?.cycle_raw)) : '';

  if (namespace === 'study_plan_code') {
    return program ? `${program} | ${normalizedSource}` : normalizedSource;
  }
  if (namespace === 'course_code') {
    return [program, studyPlan, cycle ? `CICLO ${cycle}` : '', normalizedSource].filter(Boolean).join(' | ');
  }
  return normalizedSource;
}

function buildCourseSourceValueDisplay(
  sourceValue: string | null | undefined,
  courseName: string | null | undefined,
) {
  const normalizedSource = `${sourceValue ?? ''}`.trim();
  const normalizedCourseName = `${courseName ?? ''}`.trim();
  if (!normalizedCourseName) {
    return normalizedSource;
  }
  return [normalizedSource, normalizedCourseName].filter(Boolean).join(' | ');
}

function requiresMappingConfirmation(namespace: string, targetId: string | null, matchSource: string | null) {
  if (!targetId) {
    return false;
  }
  const normalizedNamespace = String(namespace ?? '').trim().toLowerCase();
  const normalizedMatchSource = String(matchSource ?? '').trim().toLowerCase();
  if (['alias', 'catalog', 'manual_value'].includes(normalizedMatchSource)) {
    return false;
  }
  return normalizedMatchSource === 'heuristic' && ['campus', 'faculty_code', 'academic_program_code', 'study_plan_code'].includes(
    normalizedNamespace,
  );
}

function pushDependencyEntry(
  container: Record<string, unknown>,
  key: 'dependent_campuses' | 'dependent_buildings',
  entry: Record<string, unknown>,
) {
  const sourceValue = `${entry.source_value ?? ''}`.trim();
  const targetId = `${entry.target_id ?? ''}`.trim();
  const targetLabel = `${entry.target_label ?? ''}`.trim();
  if (!sourceValue && !targetId && !targetLabel) {
    return;
  }

  const current = Array.isArray(container[key]) ? [...(container[key] as Record<string, unknown>[])] : [];
  if (
    current.some(
      (item) =>
        `${item?.source_value ?? ''}`.trim() === sourceValue &&
        `${item?.target_id ?? ''}`.trim() === targetId,
    )
  ) {
    return;
  }

  current.push(entry);
  container[key] = current;
}

function pushMappingSampleRow(
  container: Record<string, unknown>,
  row: PlanningImportRowEntity,
) {
  const sample = buildMappingSampleRow(row);
  if (!sample) {
    return;
  }

  const current = Array.isArray(container.sample_rows)
    ? [...(container.sample_rows as Record<string, unknown>[])]
    : [];
  if (
    current.some(
      (item) =>
        `${item?.course_code ?? ''}`.trim() === `${sample.course_code ?? ''}`.trim() &&
        `${item?.course_name ?? ''}`.trim() === `${sample.course_name ?? ''}`.trim() &&
        `${item?.section ?? ''}`.trim() === `${sample.section ?? ''}`.trim(),
    )
  ) {
    return;
  }

  current.push(sample);
  container.sample_rows = current.slice(0, 3);
}

function buildMappingSampleRow(row: PlanningImportRowEntity) {
  const normalized = asRecord(row.normalized_json);
  const rowNumber = row.row_number;
  const courseCode = recordString(normalized, 'course_code_raw');
  const courseName = recordString(normalized, 'course_name_raw');
  const section = recordString(normalized, 'section_raw');
  const campus = recordString(normalized, 'campus_raw');
  const academicProgram = recordString(normalized, 'academic_program_code_raw');
  const cycle = recordNumberOrNull(normalized, 'cycle_raw');
  if (!rowNumber && !courseCode && !courseName && !section && !campus && !academicProgram && !cycle) {
    return null;
  }

  return {
    row_number: rowNumber,
    course_code: courseCode,
    course_name: courseName,
    section,
    campus,
    academic_program: academicProgram,
    cycle,
  };
}

function normalizeStudyPlanCode(value: string | null | undefined) {
  return normalizeLoose(value).replace(/\s+/g, '');
}

function normalizeDni(value: string | null | undefined) {
  const normalized = `${value ?? ''}`.trim().toUpperCase();
  if (!normalized) {
    return '';
  }
  if (normalized === 'NUEVO') {
    return 'NUEVO';
  }
  return normalized.replace(/[^0-9A-Z]/g, '');
}

function normalizeShiftValue(value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  if (!normalized) {
    return '';
  }
  if (normalized === 'DURNO') {
    return 'DIURNO';
  }
  if (normalized === 'MANANA' || normalized === 'MAÑANA') {
    return 'MANANA';
  }
  if (normalized === 'NO') {
    return '';
  }
  return normalized;
}

function parseSectionToken(value: string | null | undefined) {
  const raw = normalizeLoose(value);
  const isCepea = raw.includes('CEPEA');
  const withoutCepea = raw.replace(/\bCEPEA\b/g, '').trim();
  const compact = withoutCepea.replace(/\s+/g, '');
  const match = compact.match(/^([A-Z]+)(?:[-_]?(\d+))?$/);
  if (!match) {
    return {
      section_code: null,
      explicit_subsection_code: null,
      is_cepea: isCepea,
    };
  }
  const baseCode = match[1];
  const suffix = match[2] ?? null;
  return {
    section_code: baseCode,
    explicit_subsection_code: suffix ? `${baseCode}${suffix}` : null,
    is_cepea: isCepea,
  };
}

function parseAkademicExternalSectionCode(value: string | null | undefined) {
  const raw = normalizeLoose(value);
  const isCepea = raw.includes('CEPEA');
  const withoutCepea = raw.replace(/\bCEPEA\b/g, '').trim();
  const compact = withoutCepea.replace(/\s+/g, '');
  const match = compact.match(/^([A-Z]+)-([A-Z]{2,})$/);
  if (!match) {
    return {
      section_code: null,
      modality_token: null,
      location_token: null,
      is_cepea: isCepea,
    };
  }
  const prefix = match[1];
  const locationToken = match[2];
  let sectionCode = prefix;
  let modalityToken: string | null = null;
  for (const candidate of ['HV', 'HP', 'V', 'P']) {
    if (prefix.endsWith(candidate) && prefix.length > candidate.length) {
      sectionCode = prefix.slice(0, -candidate.length);
      modalityToken = candidate;
      break;
    }
  }
  return {
    section_code: sectionCode || null,
    modality_token: modalityToken,
    location_token: locationToken,
    is_cepea: isCepea,
  };
}

function isTransientAkademicRequestError(error: unknown) {
  const message =
    error instanceof Error
      ? error.message
      : typeof error === 'string'
        ? error
        : JSON.stringify(error ?? '');
  return /UND_ERR_SOCKET|other side closed|fetch failed|ECONNRESET|socket hang up|ETIMEDOUT|timeout|EAI_AGAIN/i.test(
    message,
  );
}

function waitFor(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractAkademicProgramIdFromSource(
  courseRaw: Record<string, unknown> | null | undefined,
  sectionDetail: Record<string, unknown> | null | undefined,
) {
  return (
    asNullableString(
      pick(
        sectionDetail ?? {},
        'career.id',
        'careerId',
        'programId',
        'program.id',
        'course.career.id',
        'course.careerId',
      ),
    ) ??
    asNullableString(
      pick(
        courseRaw ?? {},
        'career.id',
        'careerId',
        'programId',
        'program.id',
        'detail.career.id',
        'detail.careerId',
      ),
    )
  );
}

function extractAkademicFacultyIdFromSource(
  courseRaw: Record<string, unknown> | null | undefined,
  sectionDetail: Record<string, unknown> | null | undefined,
) {
  return (
    asNullableString(
      pick(
        sectionDetail ?? {},
        'career.facultyId',
        'career.faculty.id',
        'facultyId',
        'faculty.id',
        'course.career.facultyId',
        'course.career.faculty.id',
      ),
    ) ??
    asNullableString(
      pick(
        courseRaw ?? {},
        'career.facultyId',
        'career.faculty.id',
        'facultyId',
        'faculty.id',
        'detail.career.facultyId',
        'detail.career.faculty.id',
      ),
    )
  );
}

function extractAkademicProgramNameFromSource(
  courseRaw: Record<string, unknown> | null | undefined,
  sectionDetail: Record<string, unknown> | null | undefined,
) {
  return (
    asNullableString(
      pick(
        sectionDetail ?? {},
        'career.name',
        'careerName',
        'program.name',
        'programName',
        'course.career.name',
        'course.careerName',
      ),
    ) ??
    asNullableString(
      pick(
        courseRaw ?? {},
        'career.name',
        'careerName',
        'program.name',
        'programName',
        'detail.career.name',
        'detail.careerName',
      ),
    )
  );
}

function extractAkademicFacultyNameFromSource(
  courseRaw: Record<string, unknown> | null | undefined,
  sectionDetail: Record<string, unknown> | null | undefined,
) {
  return (
    asNullableString(
      pick(
        sectionDetail ?? {},
        'career.faculty.name',
        'career.facultyName',
        'faculty.name',
        'facultyName',
        'course.career.faculty.name',
        'course.career.facultyName',
      ),
    ) ??
    asNullableString(
      pick(
        courseRaw ?? {},
        'career.faculty.name',
        'career.facultyName',
        'faculty.name',
        'facultyName',
        'detail.career.faculty.name',
        'detail.career.facultyName',
      ),
    )
  );
}

function buildCatalogNameMatchVariants(value: string | null | undefined) {
  const raw = `${value ?? ''}`.replace(/\s+/g, ' ').trim();
  if (!raw) {
    return [];
  }
  const variants = new Set<string>([normalizeLoose(raw)]);
  const parts = raw.split(/\s-\s/);
  if (parts.length > 1) {
    const head = normalizeLoose(parts[0]);
    if (/^[A-Z0-9/_-]+$/.test(head)) {
      variants.add(normalizeLoose(parts.slice(1).join(' - ')));
    }
  }
  return [...variants].filter(Boolean);
}

function catalogNameMatches(value: string | null | undefined, variants: string[]) {
  if (!variants.length) {
    return false;
  }
  return buildCatalogNameMatchVariants(value).some((item) => variants.includes(item));
}

function readVcContextMetadata(sourcePayload: Record<string, unknown> | null | undefined) {
  const payload = asRecord(sourcePayload);
  const rawContext = payload.__vc_context;
  if (!rawContext || typeof rawContext !== 'object' || Array.isArray(rawContext)) {
    return null;
  }
  return rawContext as Record<string, unknown>;
}

function attachVcContextMetadata(
  sourcePayload: Record<string, unknown> | null | undefined,
  metadata: Record<string, unknown>,
) {
  const payload = asRecord(sourcePayload);
  const current = readVcContextMetadata(payload) ?? {};
  const nextContext = Object.fromEntries(
    Object.entries({
      ...current,
      ...metadata,
    }).filter(([, value]) => value !== undefined),
  );
  return {
    ...payload,
    __vc_context: nextContext,
  };
}

function parseDayOfWeek(value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  if (!normalized) {
    return null;
  }
  const map: Record<string, (typeof DayOfWeekValues)[number]> = {
    '0': 'LUNES',
    '1': 'MARTES',
    '2': 'MIERCOLES',
    '3': 'JUEVES',
    '4': 'VIERNES',
    '5': 'SABADO',
    '6': 'DOMINGO',
    LUNES: 'LUNES',
    MARTES: 'MARTES',
    MIERCOLES: 'MIERCOLES',
    JUEVES: 'JUEVES',
    VIERNES: 'VIERNES',
    SABADO: 'SABADO',
    DOMINGO: 'DOMINGO',
  };
  return map[normalized] ?? null;
}

function buildTimeFromParts(hourValue: string | null | undefined, minuteValue: string | null | undefined) {
  const hour = numberFromCell(hourValue);
  const minute = numberFromCell(minuteValue) ?? 0;
  if (hour === null || minute === null) {
    throw new Error('Hora incompleta.');
  }
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    throw new Error('Hora fuera de rango.');
  }
  return `${String(Math.trunc(hour)).padStart(2, '0')}:${String(Math.trunc(minute)).padStart(2, '0')}:00`;
}

function normalizeAkademicTimeValue(value: string | null | undefined) {
  const normalized = `${value ?? ''}`.trim();
  if (!normalized) {
    return null;
  }

  const meridiemMatch = normalized.match(/^(\d{1,2}):(\d{2})\s*([AP]M)$/i);
  if (meridiemMatch) {
    let hour = Number(meridiemMatch[1]);
    const minute = Number(meridiemMatch[2]);
    const meridiem = meridiemMatch[3].toUpperCase();
    if (hour < 1 || hour > 12 || minute < 0 || minute > 59) {
      return null;
    }
    if (meridiem === 'AM') {
      hour = hour === 12 ? 0 : hour;
    } else {
      hour = hour === 12 ? 12 : hour + 12;
    }
    return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00`;
  }

  const twentyFourHourMatch = normalized.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!twentyFourHourMatch) {
    return null;
  }

  const hour = Number(twentyFourHourMatch[1]);
  const minute = Number(twentyFourHourMatch[2]);
  const second = Number(twentyFourHourMatch[3] ?? '0');
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return null;
  }
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}`;
}

function computeMinutesFromTimes(start: string, end: string) {
  validateAcademicBlockTime(start);
  validateAcademicBlockTime(end);
  const minutes = Math.max(0, toMinutes(end) - toMinutes(start));
  if (minutes <= 0) {
    throw new Error('La hora fin debe ser mayor que la hora inicio.');
  }
  if (minutes % 50 !== 0) {
    throw new Error('El horario debe avanzar en bloques de 50 minutos.');
  }
  return minutes;
}

function validateAcademicBlockTime(value: string) {
  const minutes = toMinutes(value);
  const academicGridStart = 7 * 60 + 40;
  const academicGridEnd = 23 * 60 + 30;
  if (
    minutes < academicGridStart ||
    minutes > academicGridEnd ||
    (minutes - academicGridStart) % 50 !== 0
  ) {
    throw new Error('Las horas deben alinearse a la malla academica de 50 minutos.');
  }
}

function toMinutes(value: string) {
  const [hours = '0', minutes = '0'] = value.split(':');
  return Number(hours) * 60 + Number(minutes);
}

function deriveCourseType(theoreticalHours: number, practicalHours: number) {
  if (theoreticalHours > 0 && practicalHours > 0) {
    return 'TEORICO_PRACTICO';
  }
  if (practicalHours > 0) {
    return 'PRACTICO';
  }
  if (theoreticalHours > 0) {
    return 'TEORICO';
  }
  return 'TEORICO_PRACTICO';
}

function deriveSubsectionKind(
  theoreticalHours: number,
  practicalHours: number,
): PlanningSubsectionKind {
  if (theoreticalHours > 0 && practicalHours > 0) {
    return 'MIXED';
  }
  if (practicalHours > 0) {
    return 'PRACTICE';
  }
  if (theoreticalHours > 0) {
    return 'THEORY';
  }
  return 'MIXED';
}

function roundToTwo(value: number) {
  return Math.round(value * 100) / 100;
}

function buildDenomination(
  courseCode: string | null | undefined,
  courseName: string | null | undefined,
  sectionCode: string | null | undefined,
  subsectionCode: string | null | undefined,
  campusId: string | null | undefined,
) {
  return [courseCode, courseName, sectionCode, subsectionCode, campusId].filter(Boolean).join(' | ');
}

function emptyToNull(value: string | null | undefined) {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function asRecord(value: unknown) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function pick(row: Record<string, unknown>, ...paths: string[]) {
  for (const path of paths) {
    const value = getByPath(row, path);
    if (value !== undefined && value !== null && `${value}`.trim() !== '') {
      return value;
    }
  }
  return null;
}

function getByPath(obj: Record<string, unknown>, path: string): unknown {
  if (!path.includes('.')) {
    return obj[path];
  }
  return path.split('.').reduce<unknown>((acc, key) => {
    if (!acc || typeof acc !== 'object') {
      return undefined;
    }
    return (acc as Record<string, unknown>)[key];
  }, obj);
}

function asString(value: unknown) {
  return `${value ?? ''}`.trim();
}

function asNullableString(value: unknown) {
  const normalized = `${value ?? ''}`.trim();
  return normalized ? normalized : null;
}

function asNullableInt(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
}

function asNullableNumber(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function asStringArray(value: unknown) {
  if (!Array.isArray(value)) {
    return null;
  }
  return value.map((item) => asString(item)).filter((item) => item !== '');
}

function recordValue(value: Record<string, unknown>, key: string) {
  return value[key];
}

function recordString(value: Record<string, unknown>, key: string) {
  const raw = value[key];
  if (raw === null || raw === undefined) {
    return null;
  }
  const normalized = `${raw}`.trim();
  return normalized || null;
}

function recordNumber(value: Record<string, unknown>, key: string) {
  return safeNumber(numberFromCell(value[key]));
}

function recordNumberOrNull(value: Record<string, unknown>, key: string) {
  return numberFromCell(value[key]);
}

function recordBoolean(value: Record<string, unknown>, key: string) {
  const raw = value[key];
  if (typeof raw === 'boolean') {
    return raw;
  }
  if (raw === null || raw === undefined) {
    return false;
  }
  return ['1', 'TRUE', 'SI', 'YES'].includes(normalizeLoose(`${raw}`));
}

function toLogJson(value: unknown) {
  if (value === undefined) {
    return null;
  }
  return JSON.parse(JSON.stringify(value ?? null)) as Record<string, unknown> | null;
}

function mapById<T extends { id: string }>(rows: T[]) {
  return new Map(rows.map((row) => [row.id, row]));
}

function groupBy<T>(rows: T[], keyFactory: (row: T) => string) {
  const grouped = new Map<string, T[]>();
  for (const row of rows) {
    const key = keyFactory(row);
    const bucket = grouped.get(key) ?? [];
    bucket.push(row);
    grouped.set(key, bucket);
  }
  return grouped;
}

function dedupeImportedSchedules(rows: Record<string, unknown>[]) {
  const map = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    const sourceId = recordString(row, 'source_schedule_id');
    const signature =
      recordString(row, 'signature') ??
      [
        recordString(row, 'day_of_week'),
        recordString(row, 'start_time'),
        recordString(row, 'end_time'),
        recordString(row, 'session_type'),
      ]
        .filter(Boolean)
        .join('|');
    const key = sourceId || signature;
    if (!key) {
      continue;
    }
    map.set(key, row);
  }
  return [...map.values()];
}

function buildPreviewScheduleKey(row: Record<string, unknown>) {
  const schedule = asRecord(recordValue(row, 'schedule'));
  if (Object.keys(schedule).length === 0) {
    return '';
  }
  const sourceId = recordString(schedule, 'source_schedule_id');
  const signature =
    recordString(schedule, 'signature') ??
    [
      recordString(schedule, 'day_of_week'),
      recordString(schedule, 'start_time'),
      recordString(schedule, 'end_time'),
      recordString(schedule, 'session_type'),
    ]
      .filter(Boolean)
      .join('|');
  const key = sourceId || signature;
  if (!key) {
    return '';
  }
  return [
    recordString(row, 'study_plan_course_id'),
    recordString(row, 'import_section_code'),
    recordString(row, 'import_subsection_code'),
    key,
  ]
    .filter(Boolean)
    .join('::');
}

function buildIncomingScopeSnapshot(rows: Record<string, unknown>[]): ExistingScopeSnapshot {
  const offerEntries = new Map<string, SnapshotEntry>();
  const sectionEntries = new Map<string, SnapshotEntry>();
  const subsectionEntries = new Map<string, SnapshotEntry>();
  const scheduleEntries = new Map<string, SnapshotEntry>();

  for (const row of rows) {
    const offerKey = buildIncomingOfferKey(row);
    if (offerKey) {
      offerEntries.set(offerKey, {
        key: offerKey,
        label: [
          recordString(row, 'course_code'),
          recordString(row, 'course_name'),
          offerKey,
        ]
          .filter(Boolean)
          .join(' | '),
      });
    }
    const sectionKey = buildIncomingSectionKey(row);
    if (sectionKey) {
      sectionEntries.set(sectionKey, {
        key: sectionKey,
        label: [
          recordString(row, 'external_section_code') ?? recordString(row, 'import_section_code'),
          recordString(row, 'course_code'),
        ]
          .filter(Boolean)
          .join(' | '),
      });
    }
    const subsectionKey = buildIncomingSubsectionKey(row);
    if (subsectionKey) {
      subsectionEntries.set(subsectionKey, {
        key: subsectionKey,
        label: [
          recordString(row, 'import_section_code'),
          recordString(row, 'import_subsection_code'),
          recordString(row, 'course_code'),
        ]
          .filter(Boolean)
          .join(' | '),
      });
    }
    const scheduleKey = buildPreviewScheduleKey(row);
    if (scheduleKey) {
      scheduleEntries.set(scheduleKey, {
        key: scheduleKey,
        label: buildIncomingScheduleLabel(row),
      });
    }
  }

  return {
    offer_keys: [...offerEntries.keys()],
    section_keys: [...sectionEntries.keys()],
    subsection_keys: [...subsectionEntries.keys()],
    schedule_keys: [...scheduleEntries.keys()],
    offer_entries: [...offerEntries.values()],
    section_entries: [...sectionEntries.values()],
    subsection_entries: [...subsectionEntries.values()],
    schedule_entries: [...scheduleEntries.values()],
  };
}

function buildIncomingOfferKey(row: Record<string, unknown>) {
  return recordString(row, 'study_plan_course_id') ?? '';
}

function buildIncomingSectionKey(row: Record<string, unknown>) {
  const parts = [
    recordString(row, 'study_plan_course_id'),
    recordString(row, 'import_section_code'),
  ].filter(Boolean);
  return parts.length === 2 ? parts.join('::') : '';
}

function buildIncomingSubsectionKey(row: Record<string, unknown>) {
  const parts = [
    recordString(row, 'study_plan_course_id'),
    recordString(row, 'import_section_code'),
    recordString(row, 'import_subsection_code'),
  ].filter(Boolean);
  return parts.length === 3 ? parts.join('::') : '';
}

function buildExistingSectionKey(row: Record<string, unknown>) {
  const parts = [
    recordString(row, 'study_plan_course_id'),
    recordString(row, 'section_code'),
  ].filter(Boolean);
  return parts.length === 2 ? parts.join('::') : '';
}

function buildExistingSubsectionKey(row: Record<string, unknown>) {
  const parts = [
    recordString(row, 'study_plan_course_id'),
    recordString(row, 'section_code'),
    recordString(row, 'subsection_code'),
  ].filter(Boolean);
  return parts.length === 3 ? parts.join('::') : '';
}

function buildExistingScheduleKey(row: Record<string, unknown>) {
  const sourceId = recordString(row, 'source_schedule_id');
  const signature = sourceId || [
    recordString(row, 'day_of_week'),
    shortTimeValue(recordString(row, 'start_time')),
    shortTimeValue(recordString(row, 'end_time')),
    recordString(row, 'session_type'),
  ]
    .filter(Boolean)
    .join('|');

  if (!signature) {
    return '';
  }

  const parts = [
    recordString(row, 'study_plan_course_id'),
    recordString(row, 'section_code'),
    recordString(row, 'subsection_code'),
    signature,
  ].filter(Boolean);
  return parts.length === 4 ? parts.join('::') : '';
}

function emptyScopeChangeMap() {
  return {
    offers_created: 0,
    offers_replaced: 0,
    offers_deleted: 0,
    sections_created: 0,
    sections_updated: 0,
    sections_deleted: 0,
    groups_created: 0,
    groups_updated: 0,
    groups_deleted: 0,
    schedules_created: 0,
    schedules_updated: 0,
    schedules_deleted: 0,
  };
}

function buildScopeChangeMap(
  existingSummary: Record<string, unknown>,
  incomingSnapshot: ExistingScopeSnapshot,
  hasIncomingData: boolean,
) {
  const snapshot = asRecord(recordValue(existingSummary, 'change_map_snapshot'));
  const existingSnapshot: ExistingScopeSnapshot = {
    offer_keys: toStringArray(recordValue(snapshot, 'offer_keys')),
    section_keys: toStringArray(recordValue(snapshot, 'section_keys')),
    subsection_keys: toStringArray(recordValue(snapshot, 'subsection_keys')),
    schedule_keys: toStringArray(recordValue(snapshot, 'schedule_keys')),
    offer_entries: toSnapshotEntries(recordValue(snapshot, 'offer_entries')),
    section_entries: toSnapshotEntries(recordValue(snapshot, 'section_entries')),
    subsection_entries: toSnapshotEntries(recordValue(snapshot, 'subsection_entries')),
    schedule_entries: toSnapshotEntries(recordValue(snapshot, 'schedule_entries')),
  };
  const offerDiff = compareEntityEntrySets(existingSnapshot.offer_entries, incomingSnapshot.offer_entries);
  const sectionDiff = compareEntityEntrySets(existingSnapshot.section_entries, incomingSnapshot.section_entries);
  const subsectionDiff = compareEntityEntrySets(existingSnapshot.subsection_entries, incomingSnapshot.subsection_entries);
  const scheduleDiff = compareEntityEntrySets(existingSnapshot.schedule_entries, incomingSnapshot.schedule_entries);

  return {
    offers_created: offerDiff.created,
    offers_replaced: hasIncomingData ? offerDiff.updated : 0,
    offers_deleted: offerDiff.deleted,
    sections_created: sectionDiff.created,
    sections_updated: sectionDiff.updated,
    sections_deleted: sectionDiff.deleted,
    groups_created: subsectionDiff.created,
    groups_updated: subsectionDiff.updated,
    groups_deleted: subsectionDiff.deleted,
    schedules_created: scheduleDiff.created,
    schedules_updated: scheduleDiff.updated,
    schedules_deleted: scheduleDiff.deleted,
    offers_created_items: offerDiff.created_labels,
    offers_replaced_items: hasIncomingData ? offerDiff.updated_labels : [],
    offers_deleted_items: offerDiff.deleted_labels,
    sections_created_items: sectionDiff.created_labels,
    sections_updated_items: sectionDiff.updated_labels,
    sections_deleted_items: sectionDiff.deleted_labels,
    groups_created_items: subsectionDiff.created_labels,
    groups_updated_items: subsectionDiff.updated_labels,
    groups_deleted_items: subsectionDiff.deleted_labels,
    schedules_created_items: scheduleDiff.created_labels,
    schedules_updated_items: scheduleDiff.updated_labels,
    schedules_deleted_items: scheduleDiff.deleted_labels,
  };
}

function compareEntityKeySets(existingKeys: string[], incomingKeys: string[]) {
  const existing = new Set(existingKeys.filter(Boolean));
  const incoming = new Set(incomingKeys.filter(Boolean));

  let updated = 0;
  let created = 0;
  let deleted = 0;

  for (const key of incoming) {
    if (existing.has(key)) {
      updated += 1;
    } else {
      created += 1;
    }
  }

  for (const key of existing) {
    if (!incoming.has(key)) {
      deleted += 1;
    }
  }

  return { created, updated, deleted };
}

function compareEntityEntrySets(
  existingEntries: SnapshotEntry[] | undefined,
  incomingEntries: SnapshotEntry[] | undefined,
) {
  const existing = new Map((existingEntries ?? []).filter((item) => item?.key).map((item) => [item.key, item.label] as const));
  const incoming = new Map((incomingEntries ?? []).filter((item) => item?.key).map((item) => [item.key, item.label] as const));

  let updated = 0;
  let created = 0;
  let deleted = 0;
  const createdLabels: string[] = [];
  const updatedLabels: string[] = [];
  const deletedLabels: string[] = [];

  for (const [key, label] of incoming.entries()) {
    if (existing.has(key)) {
      updated += 1;
      if (updatedLabels.length < 5) {
        updatedLabels.push(label || key);
      }
    } else {
      created += 1;
      if (createdLabels.length < 5) {
        createdLabels.push(label || key);
      }
    }
  }

  for (const [key, label] of existing.entries()) {
    if (!incoming.has(key)) {
      deleted += 1;
      if (deletedLabels.length < 5) {
        deletedLabels.push(label || key);
      }
    }
  }

  return {
    created,
    updated,
    deleted,
    created_labels: createdLabels,
    updated_labels: updatedLabels,
    deleted_labels: deletedLabels,
  };
}

function mergeScopeChangeMaps(
  current: ReturnType<typeof emptyScopeChangeMap>,
  next: Record<string, unknown>,
) {
  return {
    offers_created: current.offers_created + numberValue(next.offers_created),
    offers_replaced: current.offers_replaced + numberValue(next.offers_replaced),
    offers_deleted: current.offers_deleted + numberValue(next.offers_deleted),
    sections_created: current.sections_created + numberValue(next.sections_created),
    sections_updated: current.sections_updated + numberValue(next.sections_updated),
    sections_deleted: current.sections_deleted + numberValue(next.sections_deleted),
    groups_created: current.groups_created + numberValue(next.groups_created),
    groups_updated: current.groups_updated + numberValue(next.groups_updated),
    groups_deleted: current.groups_deleted + numberValue(next.groups_deleted),
    schedules_created: current.schedules_created + numberValue(next.schedules_created),
    schedules_updated: current.schedules_updated + numberValue(next.schedules_updated),
    schedules_deleted: current.schedules_deleted + numberValue(next.schedules_deleted),
  };
}

function toStringArray(value: unknown) {
  return Array.isArray(value)
    ? value
        .map((item) => String(item ?? '').trim())
        .filter(Boolean)
    : [];
}

function toSnapshotEntries(value: unknown) {
  return Array.isArray(value)
    ? value
        .map((item) => asRecord(item))
        .map((item) => ({
          key: recordString(item, 'key') ?? '',
          label: recordString(item, 'label') ?? '',
        }))
        .filter((item) => item.key)
    : [];
}

function uniqueSnapshotEntries(entries: Array<SnapshotEntry | null | undefined>) {
  const map = new Map<string, SnapshotEntry>();
  for (const entry of entries) {
    if (!entry?.key) {
      continue;
    }
    map.set(entry.key, {
      key: entry.key,
      label: entry.label || entry.key,
    });
  }
  return [...map.values()];
}

function shortTimeValue(value: string | null | undefined) {
  const normalized = String(value ?? '').trim();
  return normalized ? normalized.slice(0, 5) : '';
}

function buildIncomingScheduleLabel(row: Record<string, unknown>) {
  return [
    recordString(row, 'external_section_code') ?? recordString(row, 'import_section_code'),
    recordString(row, 'import_subsection_code'),
    recordString(row, 'day_of_week'),
    [shortTimeValue(recordString(row, 'start_time')), shortTimeValue(recordString(row, 'end_time'))]
      .filter(Boolean)
      .join('-'),
    recordString(row, 'session_type'),
  ]
    .filter(Boolean)
    .join(' | ');
}

function buildExistingScheduleLabel(row: Record<string, unknown>) {
  return [
    recordString(row, 'section_code'),
    recordString(row, 'subsection_code'),
    recordString(row, 'day_of_week'),
    [shortTimeValue(recordString(row, 'start_time')), shortTimeValue(recordString(row, 'end_time'))]
      .filter(Boolean)
      .join('-'),
    recordString(row, 'session_type'),
  ]
    .filter(Boolean)
    .join(' | ');
}

function deduplicateRows(rows: Array<Record<string, unknown>>) {
  const map = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    const id = `${row.id ?? ''}`.trim();
    if (!id) {
      continue;
    }
    map.set(id, row);
  }
  return [...map.values()];
}

function uniqueById<T extends { id: string }>(rows: T[]) {
  return [...new Map(rows.map((row) => [row.id, row])).values()];
}

function uniqueIds(ids: Array<string | null | undefined>) {
  return [...new Set(ids.filter((item): item is string => Boolean(item)))];
}

function firstNonEmpty(values: Array<string | null | undefined>) {
  for (const value of values) {
    if (value) {
      return value;
    }
  }
  return null;
}

function maxNumber(values: Array<number | null | undefined>) {
  const valid = values.filter((value): value is number => value !== null && value !== undefined && Number.isFinite(value));
  if (valid.length === 0) {
    return null;
  }
  return Math.max(...valid);
}

function hashRowKey(parts: Array<string | null | undefined>) {
  return createHash('sha1').update(parts.filter(Boolean).join('::')).digest('hex');
}

function numberValue(value: unknown, fallback = 0) {
  const parsed = Number(value);
  if (Number.isFinite(parsed)) {
    return parsed;
  }
  return Number(fallback) || 0;
}

async function runWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<R>,
) {
  if (items.length === 0) {
    return [] as R[];
  }

  const safeConcurrency = Math.max(1, Math.min(concurrency, items.length));
  const results = new Array<R>(items.length);
  let nextIndex = 0;

  const runners = Array.from({ length: safeConcurrency }, async () => {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  });

  await Promise.all(runners);
  return results;
}
