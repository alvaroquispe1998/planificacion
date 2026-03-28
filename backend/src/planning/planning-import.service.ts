import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { createHash } from 'crypto';
import { In, Repository } from 'typeorm';
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
  CreatePlanningImportAliasDto,
  PreviewPlanningAkademicImportDto,
  UpdatePlanningImportAliasDto,
  UpdatePlanningImportScopeDecisionsDto,
} from './dto/planning.dto';
import {
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcFacultyEntity,
  VcPeriodEntity,
} from '../videoconference/videoconference.entity';

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
};

type NormalizedImportRow = {
  row_number: number;
  semester_raw: string;
  study_plan_code_raw: string;
  campus_raw: string;
  faculty_code_raw: string;
  academic_program_code_raw: string;
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
  teacher_name: string | null;
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

const IMPORT_SHEET_NAME = 'Hoja1';
const AKADEMIC_IMPORT_SOURCE_NAME = 'Akademic';
const ISSUE_PREVIEW_LIMIT = 200;
const SHIFT_OPTIONS = ['DIURNO', 'MANANA', 'TARDE', 'NOCHE', 'NOCTURNO', 'TARDE/NOCHE'] as const;

@Injectable()
export class PlanningImportService {
  constructor(
    private readonly settingsSyncService: SettingsSyncService,
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
        const created = await this.importRowsForScope(batchId, scope, scopeRows, catalog, actor);
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

    const courses = (await this.fetchAkademicCourses(dto.semester_id)).filter((course) => {
      if (!requestedCourseCode) {
        return true;
      }
      return courseCodeLikeMatch(course.code, normalizeCourseCodeValue(requestedCourseCode));
    });

    const totalCourses = courses.length;
    const byCourse = await runWithConcurrency(courses, 4, async (course, index) => {
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
      const sectionRows = await runWithConcurrency(sections, 3, async (section) => {
        const [detail, schedules] = await Promise.all([
          this.fetchAkademicSectionDetail(section.id),
          this.fetchAkademicSectionSchedules(section.id),
        ]);
        return this.buildAkademicPreviewRowsForSection(dto, catalog, enrichedCourse, section, detail, schedules);
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

  private async fetchAkademicCourses(termId: string): Promise<AkademicCourseRow[]> {
    const rows = await this.settingsSyncService.fetchSourceRowsByCode(
      'DOCENTE',
      '/admin/secciones-profesores/cursos/get',
      {
        draw: '1',
        tid: termId,
        acid: '0',
        acaprog: 'null',
        ayid: 'null',
        onlyWithSections: 'true',
        onlyWithoutCoordinator: 'false',
        start: '0',
        length: '2500',
        'search[value]': '',
        'search[regex]': 'false',
        keyName: '',
        curriculumId: 'null',
        _: `${Date.now()}`,
      },
      { skipProbe: true },
    );
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
      const rows = await this.settingsSyncService.fetchSourceRowsByCode(
        'DOCENTE',
        '/admin/cursos/get',
        {
          start: '0',
          length: '10',
          search: course.code,
          tid: termId,
        },
        { skipProbe: true },
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
    const rows = await this.settingsSyncService.fetchSourceRowsByCode(
      'DOCENTE',
      `/admin/secciones-profesores/cursos/${encodeURIComponent(courseId)}/secciones/get`,
      {
        tid: termId,
        start: '0',
        length: '500',
      },
      { skipProbe: true },
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
    const payload = await this.settingsSyncService.fetchSourcePayloadByCode(
      'DOCENTE',
      '/admin/secciones-profesores/cursos/0/secciones/seccion/get',
      { id: sectionId },
      { skipProbe: true },
    );
    return asRecord(payload);
  }

  private async fetchAkademicSectionSchedules(sectionId: string): Promise<AkademicScheduleRow[]> {
    const rows = await this.settingsSyncService.fetchSourceRowsByCode(
      'DOCENTE',
      `/admin/secciones-profesores/cursos/0/secciones/${encodeURIComponent(sectionId)}/horarios/get`,
      undefined,
      { skipProbe: true },
    );

    return deduplicateRows(rows)
      .map((row) => this.normalizeAkademicScheduleRow(row, sectionId))
      .filter((item): item is AkademicScheduleRow => Boolean(item));
  }

  private buildAkademicPreviewRowsForSection(
    dto: PreviewPlanningAkademicImportDto,
    catalog: ImportCatalog,
    course: AkademicCourseRow,
    section: AkademicSectionRow,
    detail: Record<string, unknown> | null,
    schedules: AkademicScheduleRow[],
  ): PreviewRow[] {
    const issues: PreviewIssue[] = [];
    const parsedSection = parseAkademicExternalSectionCode(section.external_code);
    if (!parsedSection.section_code) {
      return [
        {
          row_number: 0,
          source_json: {
            course: course.raw,
            section: section.raw,
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
    const sectionTeacherNames = [
      ...section.teacher_names,
      ...(Array.isArray(detail?.teacherSections)
        ? detail.teacherSections
            .map((item) => asNullableString(pick(asRecord(item) ?? {}, 'fullName')))
            .filter((item): item is string => Boolean(item))
        : []),
    ];

    const semester = catalog.semesters.find((item) => item.id === dto.semester_id) ?? null;
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
      const summaryTeacherResolution = this.resolveAkademicTeacherMatch(
        sectionTeacherIds[0] ?? null,
        sectionTeacherNames[0] ?? null,
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
          ? this.resolveAkademicTeacherMatch(
              schedule.teacher_external_id,
              schedule.teacher_name ?? sectionTeacherNames[0] ?? null,
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
            section: section.raw,
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
            section_base_code: parsedSection.section_code,
            explicit_subsection_code: group.import_subsection_code,
            import_section_code: parsedSection.section_code,
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
            projected_vacancies: section.vacancies,
            capacity_snapshot: schedule?.capacity ?? section.vacancies ?? null,
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
    if (!startTime || !endTime || !weekDay) {
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
      teacher_name:
        asNullableString(pick(teacherUser ?? {}, 'fullName')) ??
        asNullableString(pick(teacherNode ?? {}, 'fullName')),
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
      const normalized = normalizeLoose(item.name || item.code);
      if (normalizedToken === 'V') {
        return normalized.includes('VIRTUAL');
      }
      if (normalizedToken === 'P') {
        return normalized.includes('PRESENCIAL');
      }
      return normalized === normalizeLoose(value);
    }) ?? null;
    if (!modality && value) {
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_COURSE_MODALITY',
        field_name: 'MODALIDAD',
        message: 'No se pudo resolver la modalidad enviada por Akademic.',
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

  private resolveAkademicTeacherMatch(
    externalTeacherId: string | null,
    teacherName: string | null,
    catalog: ImportCatalog,
    issues: PreviewIssue[],
  ): MappingResolution {
    const byId = externalTeacherId
      ? catalog.teachers.find((item) => item.id === externalTeacherId) ?? null
      : null;
    if (byId) {
      return toResolution(
        teacherName ?? byId.full_name ?? byId.name ?? externalTeacherId ?? '',
        byId.id,
        byId.full_name ?? byId.name ?? null,
        'catalog',
      );
    }
    const normalizedTeacherName = normalizeLoose(teacherName);
    const byName = normalizedTeacherName
      ? catalog.teachers.find((item) =>
          normalizeLoose(item.full_name || item.name).includes(normalizedTeacherName),
        ) ?? null
      : null;
    if (byName) {
      return toResolution(
        teacherName ?? byName.full_name ?? byName.name ?? byName.id,
        byName.id,
        byName.full_name ?? byName.name ?? null,
        'heuristic',
      );
    }
    if (teacherName || externalTeacherId) {
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_TEACHER',
        field_name: 'DOCENTE',
        message: 'No se pudo resolver el docente de Akademic; se importara en null.',
      });
    }
    return toResolution(teacherName ?? externalTeacherId ?? '', null, null, 'none');
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
      issues.push({
        severity: 'WARNING',
        issue_code: 'UNMATCHED_CLASSROOM',
        field_name: 'AULA',
        message: 'No se pudo resolver el aula de Akademic; se importara en null.',
        meta_json: {
          namespace: 'classroom',
          source_value: schedule.classroom_description,
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

  private async importRowsForScope(
    batchId: string,
    scope: ImportScope,
    rows: PlanningImportRowEntity[],
    catalog: ImportCatalog,
    actor?: ImportActor | null,
  ) {
    const rowsByOffer = groupBy(rows, (row) => {
      const resolution = asRecord(row.resolution_json);
      return `${recordString(resolution, 'study_plan_course_id')}::${recordString(resolution, 'offer_course_code')}`;
    });
    const vcContext = await this.resolveVcContextForScope(scope);
    const vcCourseIdByOfferKey = new Map<string, string | null>();
    for (const [offerKey, offerRows] of rowsByOffer.entries()) {
      const representative = offerRows.map((row) => asRecord(row.resolution_json)).find(Boolean);
      if (!representative) {
        vcCourseIdByOfferKey.set(offerKey, null);
        continue;
      }
      const vcCourseId = await this.resolveVcCourseIdForImport({
        course_code: recordString(representative, 'offer_course_code'),
        course_name: recordString(representative, 'offer_course_name'),
        study_plan_year: scope.study_plan_year,
        vc_academic_program_id: vcContext.vcAcademicProgramId,
      });
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

    await this.offersRepo.manager.transaction(async (manager) => {
      const planRule = manager.create(PlanningCyclePlanRuleEntity, {
        id: newId(),
        semester_id: scope.semester_id,
        vc_period_id: scope.vc_period_id,
        campus_id: scope.campus_id,
        academic_program_id: scope.academic_program_id,
        faculty_id: scope.faculty_id,
        career_name: scope.academic_program_name ?? scope.study_plan_name ?? null,
        cycle: scope.cycle,
        study_plan_id: scope.study_plan_id,
        vc_faculty_id: vcContext.vcFacultyId,
        vc_academic_program_id: vcContext.vcAcademicProgramId,
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
      await manager.save(PlanningCyclePlanRuleEntity, planRule);
      await this.logChange(
        'planning_cycle_plan_rule',
        planRule.id,
        'CREATE',
        null,
        planRule,
        this.buildPlanRuleLogContext(planRule),
        actor,
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
        const sourceSystem =
          (firstNonEmpty(
            offerResolutions.map((row) => recordString(row, 'source_system')),
          ) as PlanningSourceSystem | null) ?? 'EXCEL';
        const firstOfferSource = asRecord(offerRows[0]?.source_json);
        const offer = manager.create(PlanningOfferEntity, {
          id: newId(),
          semester_id: scope.semester_id,
          vc_period_id: scope.vc_period_id,
          campus_id: scope.campus_id,
          faculty_id: scope.faculty_id,
          academic_program_id: scope.academic_program_id,
          study_plan_id: scope.study_plan_id,
          cycle: scope.cycle,
          study_plan_course_id: recordString(representative, 'study_plan_course_id')!,
          vc_faculty_id: vcContext.vcFacultyId,
          vc_academic_program_id: vcContext.vcAcademicProgramId,
          vc_course_id: vcCourseIdByOfferKey.get(offerKey) ?? null,
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
          source_payload_json:
            asRecord(recordValue(firstOfferSource, 'course')) ??
            firstOfferSource ??
            null,
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
        await manager.save(PlanningOfferEntity, offer);
        await this.logChange(
          'planning_offer',
          offer.id,
          'CREATE',
          null,
          offer,
          this.buildOfferLogContext(offer),
          actor,
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
          const section = manager.create(PlanningSectionEntity, {
            id: newId(),
            planning_offer_id: offer.id,
            code: sectionCode,
            external_code: firstNonEmpty(
              sectionResolutions.map((row) => recordString(row, 'external_section_code')),
            ),
            source_section_id: firstNonEmpty(
              sectionResolutions.map((row) => recordString(row, 'source_section_id')),
            ),
            source_payload_json:
              asRecord(recordValue(asRecord(sectionRows[0]?.source_json), 'section')) ??
              asRecord(sectionRows[0]?.source_json) ??
              null,
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
          await manager.save(PlanningSectionEntity, section);
          await this.logChange(
            'planning_section',
            section.id,
            'CREATE',
            null,
            section,
            this.buildSectionLogContext(section, offer),
            actor,
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
            const subsection = manager.create(PlanningSubsectionEntity, {
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
              vc_section_id: null,
              status: 'DRAFT',
              created_at: now,
              updated_at: now,
            });
            await manager.save(PlanningSubsectionEntity, subsection);
            await this.logChange(
              'planning_subsection',
              subsection.id,
              'CREATE',
              null,
              subsection,
              this.buildSubsectionLogContext(subsection, section, offer),
              actor,
            );
            result.subsections += 1;

            const uniqueSchedules = dedupeImportedSchedules(schedulePayloads);
            for (const selectedSchedule of uniqueSchedules) {
              const schedule = manager.create(PlanningSubsectionScheduleEntity, {
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
              await manager.save(PlanningSubsectionScheduleEntity, schedule);
              await this.logChange(
                'planning_subsection_schedule',
                schedule.id,
                'CREATE',
                null,
                schedule,
                this.buildScheduleLogContext(schedule, subsection, section, offer),
                actor,
              );
              result.schedules += 1;
            }
          }
        }
      }

      await manager.update(
        PlanningImportRowEntity,
        { batch_id: batchId, id: In(rows.map((row) => row.id)) },
        { imported: true, updated_at: new Date() },
      );
    });

    return result;
  }

  private async resolveVcContextForScope(scope: ImportScope) {
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

    return {
      vcFacultyId: vcFaculty?.id ?? null,
      vcAcademicProgramId: vcAcademicProgram?.id ?? null,
    };
  }

  private matchVcFaculty(scope: ImportScope, vcFaculties: VcFacultyEntity[]) {
    if (scope.faculty_id) {
      const byId = vcFaculties.find((item) => item.id === scope.faculty_id) ?? null;
      if (byId) {
        return byId;
      }
    }

    const normalizedFacultyName = normalizeLoose(scope.faculty_name);
    if (!normalizedFacultyName) {
      return null;
    }

    const matches = vcFaculties.filter(
      (item) => normalizeLoose(item.name) === normalizedFacultyName,
    );
    return matches.length === 1 ? matches[0] : null;
  }

  private matchVcAcademicProgram(
    scope: ImportScope,
    vcFacultyId: string | null,
    vcPrograms: VcAcademicProgramEntity[],
  ) {
    if (scope.academic_program_id) {
      const byId = vcPrograms.find((item) => item.id === scope.academic_program_id) ?? null;
      if (byId && (!vcFacultyId || byId.faculty_id === vcFacultyId)) {
        return byId;
      }
    }

    const normalizedProgramName = normalizeLoose(scope.academic_program_name);
    if (!normalizedProgramName) {
      return null;
    }

    const nameMatches = vcPrograms.filter(
      (item) => normalizeLoose(item.name) === normalizedProgramName,
    );
    const scopedMatches = vcFacultyId
      ? nameMatches.filter((item) => item.faculty_id === vcFacultyId)
      : nameMatches;

    return scopedMatches.length === 1 ? scopedMatches[0] : null;
  }

  private async resolveVcCourseIdForImport(input: {
    course_name: string | null | undefined;
    course_code: string | null | undefined;
    study_plan_year: string | null | undefined;
    vc_academic_program_id: string | null;
  }) {
    const normalizedCourseName = normalizeCourseName(input.course_name);
    if (!normalizedCourseName || !input.vc_academic_program_id) {
      return null;
    }

    const candidates = await this.vcCoursesRepo.find({
      where: { program_id: input.vc_academic_program_id },
      order: { name: 'ASC' },
    });
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
    for (const schedule of schedules) {
      const subsection = subsectionMap.get(schedule.planning_subsection_id);
      const section = subsection ? sectionMap.get(subsection.planning_section_id) : null;
      const offer = section ? offerMap.get(section.planning_offer_id) : null;
      if (!subsection || !section || !offer) {
        continue;
      }
      await this.logChange(
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
      );
    }
    for (const subsection of subsections) {
      const section = sectionMap.get(subsection.planning_section_id);
      const offer = section ? offerMap.get(section.planning_offer_id) : null;
      if (!section || !offer) {
        continue;
      }
      await this.logChange(
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
      );
    }
    for (const section of sections) {
      const offer = offerMap.get(section.planning_offer_id);
      if (!offer) {
        continue;
      }
      await this.logChange(
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
      );
    }
    for (const offer of offers) {
      await this.logChange(
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
      );
    }
    for (const rule of rules) {
      await this.logChange(
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
      );
    }

    return true;
  }

  private async buildScopeDecisionEntities(
    batchId: string,
    previewRows: PreviewRow[],
    now: Date,
    onProgress?: (current: number, total: number) => Promise<void> | void,
  ) {
    const rowsByScope = groupBy(
      previewRows.filter((row) => row.can_import && row.scope_key),
      (row) => row.scope_key!,
    );
    const entities: PlanningImportScopeDecisionEntity[] = [];
    const totalScopes = rowsByScope.size;
    let processedScopes = 0;

    for (const [scopeKey, rows] of rowsByScope.entries()) {
      const scope = this.scopeFromResolution(asRecord(rows[0].resolution_json));
      if (!scope) {
        continue;
      }
      const existing = await this.findExistingScopeSummary(scope);
      entities.push(
        this.importScopeDecisionsRepo.create({
          id: newId(),
          batch_id: batchId,
          scope_key: scopeKey,
          scope_json: scope as unknown as Record<string, unknown>,
          existing_summary_json: existing,
          decision: hasExistingData(existing) ? 'PENDING' : 'REPLACE_SCOPE',
          notes: null,
          created_at: now,
          updated_at: now,
        }),
      );
      processedScopes += 1;
      if (onProgress && (processedScopes === totalScopes || processedScopes % 8 === 0)) {
        await onProgress(processedScopes, totalScopes);
      }
    }

    return entities;
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
    const planRules = await this.planRulesRepo.find({
      where: scopeWhere,
      order: { created_at: 'DESC' },
    });
    const offers = await this.offersRepo.find({
      where: scopeWhere,
      select: ['id'],
    });
    const offerIds = offers.map((item) => item.id);
    const sections = offerIds.length
      ? await this.sectionsRepo.find({
          where: { planning_offer_id: In(offerIds) },
          select: ['id'],
        })
      : [];
    const sectionIds = sections.map((item) => item.id);
    const subsections = sectionIds.length
      ? await this.subsectionsRepo.find({
          where: { planning_section_id: In(sectionIds) },
          select: ['id'],
        })
      : [];
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = subsectionIds.length
      ? await this.schedulesRepo.find({
          where: { planning_subsection_id: In(subsectionIds) },
          select: ['id'],
        })
      : [];

    return {
      has_existing_data:
        planRules.length > 0 ||
        offers.length > 0 ||
        sections.length > 0 ||
        subsections.length > 0 ||
        schedules.length > 0,
      plan_rule_id: planRules[0]?.id ?? null,
      plan_rule_count: planRules.length,
      offer_count: offers.length,
      section_count: sections.length,
      subsection_count: subsections.length,
      schedule_count: schedules.length,
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
      const offerCount = new Set(
        resolutionRows.map((row) => recordString(row, 'study_plan_course_id')).filter(Boolean),
      ).size;
      const sectionCount = new Set(
        resolutionRows
          .map((row) => `${recordString(row, 'study_plan_course_id')}::${recordString(row, 'import_section_code')}`)
          .filter((value) => !value.includes('null')),
      ).size;
      const subsectionCount = new Set(
        resolutionRows
          .map(
            (row) =>
              `${recordString(row, 'study_plan_course_id')}::${recordString(row, 'import_section_code')}::${recordString(row, 'import_subsection_code')}`,
          )
          .filter((value) => !value.includes('null')),
      ).size;
      const scheduleCount = new Set(
        resolutionRows
          .map((row) => buildPreviewScheduleKey(row))
          .filter(Boolean),
      ).size;
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

    const rowPreview = rows
      .filter((row) => {
        const rowIssues = issuesByRowId.get(row.id) ?? [];
        return !row.can_import || rowIssues.length > 0;
      })
      .slice(0, ISSUE_PREVIEW_LIMIT)
      .map((row) => {
        const normalized = asRecord(row.normalized_json);
        const resolution = asRecord(row.resolution_json);
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
    const normalized: NormalizedImportRow = {
      row_number: rowNumber,
      semester_raw: stringifyCell(row['SEMESTRE ']),
      study_plan_code_raw: stringifyCell(row['CODIGO DE  PLAN']),
      campus_raw: stringifyCell(row.LOCAL),
      faculty_code_raw: stringifyCell(row['Cod Facultad:']),
      academic_program_code_raw: stringifyCell(row['PROGRAMA ACADÉMICO']),
      cycle_raw: numberFromCell(row.CICLO),
      section_raw: stringifyCell(row['SECCIÓN']),
      course_code_raw: stringifyCell(row['CÓDIGO']),
      course_name_raw: stringifyCell(row['NOMBRE DE CURSO']),
      study_type_raw: stringifyCell(row['TIPO DE ESTUDIOS']),
      course_requirement_raw: stringifyCell(row['TIPO DE CURSO']),
      course_modality_raw: stringifyCell(row['MODALIDAD DE CURSO']),
      delivery_modality_raw: stringifyCell(row['MODALIDAD \r\n(Presenc./VIRTUAL/HIBRIDO)']),
      theory_hours: safeNumber(numberFromCell(row['HORAS TEORÍA'])),
      practical_hours: safeNumber(numberFromCell(row['HORAS PRÁCTICA'])),
      total_hours: safeNumber(numberFromCell(row['TOTAL DE HORAS'])),
      credits: numberFromCell(row['TOTAL DE CREDITOS']),
      projected_vacancies: numberFromCell(row['NÚMERO VACANTES PROYECTADAS 2025-2']),
      teacher_dni_raw: stringifyCell(row.DNI),
      teacher_name_raw: stringifyCell(row['APELLIDOS Y NOMBRES']),
      shift_raw:
        stringifyCell(row['TURNO\r\n(DIURNO/\r\nMAÑANA/ TARDE/NOCHE)']) ||
        stringifyCell(row.TURNO),
      building_raw: stringifyCell(row.PABELLON),
      classroom_raw: stringifyCell(row.AULA),
      laboratory_raw: stringifyCell(row.LABORATORIO),
      day_raw: stringifyCell(row.DIA),
      start_hour_raw: stringifyCell(row['HORA INICIO']),
      start_minute_raw: stringifyCell(row['MINUTO INICIO']),
      end_hour_raw: stringifyCell(row['HORA FIN']),
      end_minute_raw: stringifyCell(row['MINUTO FIN']),
      academic_hours_raw: numberFromCell(row['HORAS\r\nACADEM.']),
      denomination_raw: stringifyCell(row['DENOMINACIÓN']),
      raw_row: row,
    };

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
    if (!normalizedDni || normalizedDni === 'NUEVO') {
      if (teacherName || dni) {
        issues.push({
          severity: 'WARNING',
          issue_code: 'UNMATCHED_TEACHER',
          field_name: 'DNI',
          message: 'No se pudo asignar docente; la estructura se importara con docente en null.',
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
        message: 'El DNI del docente no existe en catalogo; la estructura se importara con docente en null.',
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
        message: 'No se pudo resolver la modalidad del curso; se importara en null.',
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
      message: 'No se pudo resolver el turno; se importara en null.',
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
        message: 'No se pudo resolver el pabellon; se importara en null.',
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
            ? 'No se pudo resolver el laboratorio; se importara en null.'
            : 'No se pudo resolver el aula; se importara en null.',
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
      this.semestersRepo.find({ order: { name: 'DESC' } }),
      this.vcPeriodsRepo.find({ where: { is_active: true }, order: { text: 'DESC' } }),
      this.campusesRepo.find({ order: { name: 'ASC' } }),
      this.facultiesRepo.find({ order: { name: 'ASC' } }),
      this.programsRepo.find({ order: { name: 'ASC' } }),
      this.studyPlansRepo.find({ order: { career: 'ASC', year: 'ASC' } }),
      this.studyPlanCoursesRepo.find({ order: { study_plan_id: 'ASC', order: 'ASC', course_code: 'ASC' } }),
      this.studyPlanCourseDetailsRepo.find(),
      this.teachersRepo.find({ order: { full_name: 'ASC' } }),
      this.courseModalitiesRepo.find({ where: { is_active: true }, order: { name: 'ASC' } }),
      this.studyTypesRepo.find({ where: { is_active: true }, order: { name: 'ASC' } }),
      this.buildingsRepo.find({ order: { name: 'ASC' } }),
      this.classroomsRepo.find({ order: { name: 'ASC' } }),
      this.campusVcLocationMappingsRepo.find({ order: { campus_id: 'ASC' } }),
      this.importAliasMappingsRepo.find({ where: { is_active: true }, order: { namespace: 'ASC', source_value: 'ASC' } }),
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
    };
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
      this.changeLogsRepo.create({
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
      }),
    );
  }
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
  const sectionCode = prefix.length > 1 ? prefix.slice(0, -1) : prefix;
  const modalityToken = prefix.length > 1 ? prefix.slice(-1) : null;
  return {
    section_code: sectionCode || null,
    modality_token: modalityToken,
    location_token: locationToken,
    is_cepea: isCepea,
  };
}

function parseDayOfWeek(value: string | null | undefined) {
  const normalized = normalizeLoose(value);
  if (!normalized) {
    return null;
  }
  const map: Record<string, (typeof DayOfWeekValues)[number]> = {
    '1': 'LUNES',
    '2': 'MARTES',
    '3': 'MIERCOLES',
    '4': 'JUEVES',
    '5': 'VIERNES',
    '6': 'SABADO',
    '7': 'DOMINGO',
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
