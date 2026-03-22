import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'crypto';
import { In, Repository } from 'typeorm';
import { newId } from '../common';
import { ZoomUserEntity } from '../entities/audit.entities';
import {
  AcademicProgramCampusEntity,
  AcademicProgramEntity,
  BuildingEntity,
  CampusEntity,
  ClassroomSectionScheduleEntity,
  ClassroomEntity,
  ClassroomTypeEntity,
  CourseEntity,
  CourseSectionEntity,
  DayOfWeekValues,
  ExternalSessionEntity,
  ExternalSessionStatusValues,
  ExternalSourceEntity,
  FacultyEntity,
  SectionEntity,
  SemesterEntity,
  StudyPlanCourseDetailEntity,
  StudyPlanCourseEntity,
  StudyPlanEntity,
  SyncJobEntity,
  SyncJobModeValues,
  SyncJobStatusValues,
  SyncLogEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcFacultyEntity,
  VcPeriodEntity,
  VcSectionEntity,
} from '../videoconference/videoconference.entity';
import { RunSettingsSyncDto, UpsertSourceSessionDto } from './dto/settings-sync.dto';

type SourceCode = 'MATRICULA' | 'DOCENTE' | 'INTRANET' | 'AULAVIRTUAL';
type ResourceCode =
  | 'semesters'
  | 'campuses'
  | 'academic_programs'
  | 'sections'
  | 'courses'
  | 'classroom_types'
  | 'zoom_users'
  | 'faculties'
  | 'study_plans'
  | 'teachers'
  | 'buildings'
  | 'classrooms'
  | 'classroom_section_schedules'
  | 'academic_program_campuses'
  | 'academic_program_campuses'
  | 'course_sections'
  | 'vc_periods'
  | 'vc_faculties'
  | 'vc_academic_programs'
  | 'vc_courses'
  | 'vc_sections';

type SourceProbeResult = {
  ok: boolean;
  source: ExternalSourceEntity;
  session: ExternalSessionEntity | null;
  reason?: string;
  cookie?: string;
  status_code?: number;
  final_url?: string;
};

type AulaVirtualAutoLoginConfig = {
  loginUrl: string;
  username: string;
  password: string;
  usernameField: string;
  passwordField: string;
  returnUrlField: string;
  returnUrlValue: string | null;
  userAgent: string;
};

const SOURCE_DEFINITIONS: Array<{
  code: SourceCode;
  name: string;
  base_url: string;
  login_url: string | null;
  validate_path: string;
}> = [
    {
      code: 'MATRICULA',
      name: 'Matricula',
      base_url: 'https://matricula.autonomadeica.edu.pe',
      login_url: null,
      validate_path: '/admin/campus/get?start=0&length=1',
    },
    {
      code: 'DOCENTE',
      name: 'Docente',
      base_url: 'https://docente.autonomadeica.edu.pe',
      login_url: null,
      validate_path: '/admin/especialidades/get?start=0&length=1',
    },
    {
      code: 'INTRANET',
      name: 'Intranet',
      base_url: 'https://intranet.autonomadeica.edu.pe',
      login_url: null,
      validate_path: '/admin/docentes/get?start=0&length=1',
    },
    {
      code: 'AULAVIRTUAL',
      name: 'Aula Virtual',
      base_url: 'https://aulavirtual2.autonomadeica.edu.pe',
      login_url: 'https://aulavirtual2.autonomadeica.edu.pe/account/login?ReturnUrl=%2F',
      validate_path: '/facultades/get',
    },
  ];

const RESOURCE_DEFINITIONS: Array<{
  code: ResourceCode;
  label: string;
  source: SourceCode;
  module_code: string;
  module_label: string;
  module_description: string;
  module_order: number;
  resource_order: number;
}> = [
    {
      code: 'semesters',
      label: 'Semestres',
      source: 'MATRICULA',
      module_code: 'CALENDAR',
      module_label: 'Calendario Academico',
      module_description: 'Estructura temporal del periodo academico.',
      module_order: 10,
      resource_order: 10,
    },
    {
      code: 'campuses',
      label: 'Sedes / Campus',
      source: 'MATRICULA',
      module_code: 'ACADEMIC_STRUCTURE',
      module_label: 'Estructura Academica',
      module_description: 'Facultades, programas, sedes, planes y secciones base.',
      module_order: 20,
      resource_order: 10,
    },
    {
      code: 'faculties',
      label: 'Facultades',
      source: 'MATRICULA',
      module_code: 'ACADEMIC_STRUCTURE',
      module_label: 'Estructura Academica',
      module_description: 'Facultades, programas, sedes, planes y secciones base.',
      module_order: 20,
      resource_order: 20,
    },
    {
      code: 'academic_programs',
      label: 'Programas Academicos',
      source: 'MATRICULA',
      module_code: 'ACADEMIC_STRUCTURE',
      module_label: 'Estructura Academica',
      module_description: 'Facultades, programas, sedes, planes y secciones base.',
      module_order: 20,
      resource_order: 30,
    },
    {
      code: 'academic_program_campuses',
      label: 'Relacion Programa-Campus',
      source: 'MATRICULA',
      module_code: 'ACADEMIC_STRUCTURE',
      module_label: 'Estructura Academica',
      module_description: 'Facultades, programas, sedes, planes y secciones base.',
      module_order: 20,
      resource_order: 40,
    },
    {
      code: 'study_plans',
      label: 'Planes de Estudio',
      source: 'DOCENTE',
      module_code: 'ACADEMIC_STRUCTURE',
      module_label: 'Estructura Academica',
      module_description: 'Facultades, programas, sedes, planes y secciones base.',
      module_order: 20,
      resource_order: 50,
    },
    {
      code: 'sections',
      label: 'Secciones',
      source: 'MATRICULA',
      module_code: 'ACADEMIC_STRUCTURE',
      module_label: 'Estructura Academica',
      module_description: 'Facultades, programas, sedes, planes y secciones base.',
      module_order: 20,
      resource_order: 60,
    },
    {
      code: 'courses',
      label: 'Cursos',
      source: 'AULAVIRTUAL',
      module_code: 'ACADEMIC_CATALOG',
      module_label: 'Catalogo Academico',
      module_description: 'Catalogos curriculares para planificacion.',
      module_order: 30,
      resource_order: 10,
    },
    {
      code: 'course_sections',
      label: 'Secciones por Curso',
      source: 'AULAVIRTUAL',
      module_code: 'ACADEMIC_CATALOG',
      module_label: 'Catalogo Academico',
      module_description: 'Catalogos curriculares para planificacion.',
      module_order: 30,
      resource_order: 20,
    },
    {
      code: 'teachers',
      label: 'Docentes',
      source: 'INTRANET',
      module_code: 'ACADEMIC_STAFF',
      module_label: 'Personal Academico',
      module_description: 'Personas y cuentas necesarias para el dictado.',
      module_order: 40,
      resource_order: 10,
    },
    {
      code: 'zoom_users',
      label: 'Usuarios Zoom',
      source: 'AULAVIRTUAL',
      module_code: 'ACADEMIC_STAFF',
      module_label: 'Personal Academico',
      module_description: 'Personas y cuentas necesarias para el dictado.',
      module_order: 40,
      resource_order: 20,
    },
    {
      code: 'classroom_types',
      label: 'Tipos de Aula',
      source: 'MATRICULA',
      module_code: 'INFRASTRUCTURE',
      module_label: 'Infraestructura',
      module_description: 'Ambientes fisicos y tipologias.',
      module_order: 50,
      resource_order: 10,
    },
    {
      code: 'buildings',
      label: 'Pabellones',
      source: 'MATRICULA',
      module_code: 'INFRASTRUCTURE',
      module_label: 'Infraestructura',
      module_description: 'Ambientes fisicos y tipologias.',
      module_order: 50,
      resource_order: 20,
    },
    {
      code: 'classrooms',
      label: 'Aulas',
      source: 'MATRICULA',
      module_code: 'INFRASTRUCTURE',
      module_label: 'Infraestructura',
      module_description: 'Ambientes fisicos y tipologias.',
      module_order: 50,
      resource_order: 30,
    },
    {
      code: 'classroom_section_schedules',
      label: 'Horarios por Aula',
      source: 'INTRANET',
      module_code: 'INFRASTRUCTURE',
      module_label: 'Infraestructura',
      module_description: 'Horarios por aula vinculados a secciones de curso.',
      module_order: 50,
      resource_order: 40,
    },
    {
      code: 'vc_periods',
      label: 'VC: Periodos',
      source: 'AULAVIRTUAL',
      module_code: 'VIDEOCONFERENCE',
      module_label: 'Videoconferencia',
      module_description: 'Recursos para videoconferencias.',
      module_order: 60,
      resource_order: 10,
    },
    {
      code: 'vc_faculties',
      label: 'VC: Facultades',
      source: 'AULAVIRTUAL',
      module_code: 'VIDEOCONFERENCE',
      module_label: 'Videoconferencia',
      module_description: 'Recursos para videoconferencias.',
      module_order: 60,
      resource_order: 20,
    },
    {
      code: 'vc_academic_programs',
      label: 'VC: Programas',
      source: 'AULAVIRTUAL',
      module_code: 'VIDEOCONFERENCE',
      module_label: 'Videoconferencia',
      module_description: 'Recursos para videoconferencias.',
      module_order: 60,
      resource_order: 30,
    },
    {
      code: 'vc_courses',
      label: 'VC: Cursos',
      source: 'AULAVIRTUAL',
      module_code: 'VIDEOCONFERENCE',
      module_label: 'Videoconferencia',
      module_description: 'Recursos para videoconferencias.',
      module_order: 60,
      resource_order: 40,
    },
    {
      code: 'vc_sections',
      label: 'VC: Secciones',
      source: 'AULAVIRTUAL',
      module_code: 'VIDEOCONFERENCE',
      module_label: 'Videoconferencia',
      module_description: 'Recursos para videoconferencias.',
      module_order: 60,
      resource_order: 50,
    },
  ];

const RESOURCE_CODES = RESOURCE_DEFINITIONS.map((item) => item.code);
const RESOURCE_BY_CODE = new Map(RESOURCE_DEFINITIONS.map((item) => [item.code, item]));

@Injectable()
export class SettingsSyncService {
  private readonly allowInsecureTls: boolean;
  private readonly sourceAutoLoginInFlight = new Map<SourceCode, Promise<SourceProbeResult>>();

  constructor(
    private readonly configService: ConfigService,
    @InjectRepository(SemesterEntity)
    private readonly semestersRepo: Repository<SemesterEntity>,
    @InjectRepository(CampusEntity)
    private readonly campusesRepo: Repository<CampusEntity>,
    @InjectRepository(FacultyEntity)
    private readonly facultiesRepo: Repository<FacultyEntity>,
    @InjectRepository(AcademicProgramEntity)
    private readonly programsRepo: Repository<AcademicProgramEntity>,
    @InjectRepository(SectionEntity)
    private readonly sectionsRepo: Repository<SectionEntity>,
    @InjectRepository(CourseEntity)
    private readonly coursesRepo: Repository<CourseEntity>,
    @InjectRepository(ClassroomTypeEntity)
    private readonly classroomTypesRepo: Repository<ClassroomTypeEntity>,
    @InjectRepository(ZoomUserEntity)
    private readonly zoomUsersRepo: Repository<ZoomUserEntity>,
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
    @InjectRepository(ClassroomSectionScheduleEntity)
    private readonly classroomSectionSchedulesRepo: Repository<ClassroomSectionScheduleEntity>,
    @InjectRepository(AcademicProgramCampusEntity)
    private readonly programCampusesRepo: Repository<AcademicProgramCampusEntity>,
    @InjectRepository(CourseSectionEntity)
    private readonly courseSectionsRepo: Repository<CourseSectionEntity>,
    @InjectRepository(ExternalSourceEntity)
    private readonly sourcesRepo: Repository<ExternalSourceEntity>,
    @InjectRepository(ExternalSessionEntity)
    private readonly sessionsRepo: Repository<ExternalSessionEntity>,
    @InjectRepository(SyncJobEntity)
    private readonly jobsRepo: Repository<SyncJobEntity>,
    @InjectRepository(SyncLogEntity)
    private readonly logsRepo: Repository<SyncLogEntity>,
    @InjectRepository(VcPeriodEntity)
    private readonly vcPeriodsRepo: Repository<VcPeriodEntity>,
    @InjectRepository(VcFacultyEntity)
    private readonly vcFacultiesRepo: Repository<VcFacultyEntity>,
    @InjectRepository(VcAcademicProgramEntity)
    private readonly vcProgramsRepo: Repository<VcAcademicProgramEntity>,
    @InjectRepository(VcCourseEntity)
    private readonly vcCoursesRepo: Repository<VcCourseEntity>,
    @InjectRepository(VcSectionEntity)
    private readonly vcSectionsRepo: Repository<VcSectionEntity>,
  ) {
    this.allowInsecureTls =
      this.configService.get<string>('SYNC_TLS_ALLOW_INSECURE', 'true') === 'true';
    if (this.allowInsecureTls) {
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
    }
  }

  async listResources() {
    return RESOURCE_DEFINITIONS.map((resource) => ({
      code: resource.code,
      label: resource.label,
      source: resource.source,
      module_code: resource.module_code,
      module_label: resource.module_label,
      module_description: resource.module_description,
      module_order: resource.module_order,
      resource_order: resource.resource_order,
    }));
  }

  async listSources(probe = false) {
    await this.ensureDefaultSources();
    const sources = await this.sourcesRepo.find({ order: { code: 'ASC' } });
    const sessions = await this.sessionsRepo.find();
    const bySourceId = new Map(sessions.map((item) => [item.source_id, item]));

    if (probe) {
      await Promise.allSettled(sources.map((source) => this.probeSourceSession(source.code)));
    }

    const refreshedSessions = await this.sessionsRepo.find();
    const refreshedMap = new Map(refreshedSessions.map((item) => [item.source_id, item]));
    return sources.map((source) => {
      const session = refreshedMap.get(source.id) ?? bySourceId.get(source.id) ?? null;
      const sourceError = this.normalizeSourceError(session?.error_last ?? null);
      const status: string = session?.status ?? (session ? 'ACTIVE' : 'MISSING');

      return {
        id: source.id,
        code: source.code,
        name: source.name,
        base_url: source.base_url,
        login_url: source.login_url,
        is_active: source.is_active,
        session_status: status,
        last_validated_at: session?.last_validated_at ?? null,
        error_last: sourceError,
        needs_renewal: !session || status !== 'ACTIVE',
      };
    });
  }

  async getSourceSessionCookie(code: string) {
    await this.ensureDefaultSources();
    const source = await this.findSourceOrFail(code);
    const session = await this.sessionsRepo.findOne({ where: { source_id: source.id } });
    if (!session) {
      return {
        source_code: source.code,
        cookie_text: '',
        has_cookie: false,
        updated_at: null,
      };
    }

    return {
      source_code: source.code,
      cookie_text: this.decryptCookieJar(session.cookie_jar_encrypted),
      has_cookie: true,
      updated_at: session.updated_at,
    };
  }

  async upsertSourceSession(code: string, dto: UpsertSourceSessionDto) {
    await this.ensureDefaultSources();
    const source = await this.findSourceOrFail(code);
    const cookie = this.normalizeCookieText(dto.cookie_text);
    if (!cookie) {
      throw new BadRequestException('cookie_text no contiene un valor de cookie util.');
    }

    const encrypted = this.encryptCookieJar(cookie);
    const now = new Date();
    const existing = await this.sessionsRepo.findOne({ where: { source_id: source.id } });

    const session = existing
      ? this.sessionsRepo.create({
        ...existing,
        cookie_jar_encrypted: encrypted,
        status: 'ACTIVE',
        expires_at: null,
        error_last: null,
        updated_at: now,
      })
      : this.sessionsRepo.create({
        id: newId(),
        source_id: source.id,
        cookie_jar_encrypted: encrypted,
        status: 'ACTIVE',
        last_validated_at: null,
        expires_at: null,
        error_last: null,
        created_at: now,
        updated_at: now,
      });

    await this.sessionsRepo.save(session);
    return {
      source_code: source.code,
      status: session.status,
      cookie_text: cookie,
      updated_at: session.updated_at,
    };
  }

  async validateSourceSession(code: string) {
    const normalized = code.trim().toUpperCase();
    const def = SOURCE_DEFINITIONS.find((s) => s.code === normalized);
    if (!def) {
      return { source_code: normalized, ok: false, reason: 'Fuente no configurada.' };
    }

    await this.ensureDefaultSources();
    const result = await this.probeSourceSession(normalized as SourceCode);
    return {
      source_code: normalized,
      ok: result.ok,
      reason: result.reason ?? null,
      status_code: result.status_code ?? null,
      final_url: result.final_url ?? null,
    };
  }

  async validateAllSourceSessions() {
    const results = [];
    for (const source of SOURCE_DEFINITIONS) {
      results.push(await this.validateSourceSession(source.code));
    }
    return results;
  }

  async runSync(dto: RunSettingsSyncDto) {
    await this.ensureDefaultSources();
    const requestedResources = dto.resources?.length
      ? dto.resources.map((item) => item.trim())
      : RESOURCE_CODES;
    const unknownResources = requestedResources.filter(
      (item) => !RESOURCE_CODES.includes(item as ResourceCode),
    );

    if (unknownResources.length > 0) {
      throw new BadRequestException(`resources invalidos: ${unknownResources.join(', ')}`);
    }

    const resources = requestedResources as ResourceCode[];
    const sourceMap = new Map(
      (await this.sourcesRepo.find({ where: { is_active: true } })).map((item) => [item.code, item] as const),
    );
    const jobs: Array<{ job: SyncJobEntity; resource: ResourceCode; sourceCode: SourceCode }> = [];

    for (const resource of resources) {
      const resourceDefinition = RESOURCE_BY_CODE.get(resource);
      if (!resourceDefinition) {
        throw new BadRequestException(`Recurso no configurado: ${resource}`);
      }
      const source = sourceMap.get(resourceDefinition.source);
      if (!source) {
        throw new BadRequestException(`Fuente no configurada para el recurso ${resource}.`);
      }
      const job = await this.createJob(source.id, resource, dto.mode ?? 'FULL', {
        mode: dto.mode ?? 'FULL',
        resources: dto.resources ?? null,
        campus_ids: dto.campus_ids ?? null,
        course_ids: dto.course_ids ?? null,
        classroom_ids: dto.classroom_ids ?? null,
        semester_id: dto.semester_id ?? null,
        schedule_start: dto.schedule_start ?? null,
        schedule_end: dto.schedule_end ?? null,
      });
      jobs.push({
        job,
        resource,
        sourceCode: resourceDefinition.source,
      });
    }

    this.scheduleSyncExecution(jobs, dto);

    return {
      started_at: new Date().toISOString(),
      total_resources: resources.length,
      queued: jobs.length,
      accepted: true,
      completed: 0,
      failed: 0,
      results: jobs.map(({ job, resource, sourceCode }) => ({
        resource,
        source: sourceCode,
        status: 'PENDING',
        job_id: job.id,
      })),
    };
  }

  private scheduleSyncExecution(
    jobs: Array<{ job: SyncJobEntity; resource: ResourceCode; sourceCode: SourceCode }>,
    dto: RunSettingsSyncDto,
  ) {
    setTimeout(() => {
      void this.executeQueuedSync(jobs, dto);
    }, 0);
  }

  private async executeQueuedSync(
    jobs: Array<{ job: SyncJobEntity; resource: ResourceCode; sourceCode: SourceCode }>,
    dto: RunSettingsSyncDto,
  ) {
    const sourceProbeCache = new Map<SourceCode, SourceProbeResult>();

    for (const { job, resource, sourceCode } of jobs) {
      try {
        const sourceResult = sourceProbeCache.has(sourceCode)
          ? (sourceProbeCache.get(sourceCode) as SourceProbeResult)
          : await this.probeSourceSession(sourceCode);
        sourceProbeCache.set(sourceCode, sourceResult);

        if (!sourceResult.ok || !sourceResult.cookie) {
          await this.failJob(job, sourceResult.reason ?? 'Sesion no valida.');
          continue;
        }

        await this.updateJobStatus(job, 'RUNNING');
        await this.log(job.id, 'INFO', `Iniciando sync de ${resource}`, {
          source: sourceCode,
          mode: dto.mode ?? 'FULL',
        });

        const rows = await this.fetchResourceRows(resource, sourceResult.source, sourceResult.cookie, dto);
        const persisted = await this.persistResourceRows(resource, rows);
        await this.log(job.id, 'INFO', `Sync de ${resource} completado`, persisted);
        await this.completeJob(job);
      } catch (error) {
        const message = this.toErrorMessage(error);
        await this.failJob(job, message);
        await this.log(job.id, 'ERROR', `Error en sync de ${resource}`, { error: message });
      }
    }
  }

  async listJobs(limit = 20) {
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(100, Number(limit))) : 20;
    const jobs = await this.jobsRepo.find({
      order: { created_at: 'DESC' },
      take: safeLimit,
    });
    const jobIds = jobs.map((job) => job.id);
    const logs =
      jobIds.length > 0
        ? await this.logsRepo.find({
          where: { job_id: In(jobIds) },
          order: { created_at: 'DESC' },
        })
        : [];

    const logsByJob = new Map<string, SyncLogEntity[]>();
    for (const log of logs) {
      if (!logsByJob.has(log.job_id)) {
        logsByJob.set(log.job_id, []);
      }
      if ((logsByJob.get(log.job_id) as SyncLogEntity[]).length < 10) {
        (logsByJob.get(log.job_id) as SyncLogEntity[]).push(log);
      }
    }

    return jobs.map((job) => ({
      ...job,
      logs: logsByJob.get(job.id) ?? [],
    }));
  }

  private async fetchResourceRows(
    resource: ResourceCode,
    source: ExternalSourceEntity,
    cookie: string,
    dto: RunSettingsSyncDto,
  ) {
    if (resource === 'semesters') {
      return this.fetchRows(source, cookie, '/admin/periodos/get', { start: '0', length: '500' });
    }
    if (resource === 'campuses') {
      return this.fetchRows(source, cookie, '/admin/campus/get');
    }
    if (resource === 'academic_programs') {
      return this.fetchRows(source, cookie, '/admin/carreras/get');
    }
    if (resource === 'sections') {
      return this.fetchRows(source, cookie, '/admin/codigos-de-seccion/get', { length: '500' });
    }
    if (resource === 'courses') {
      const programIds = await this.resolveProgramIds();
      const rows: Record<string, unknown>[] = [];
      for (const careerId of programIds) {
        const partial = await this.fetchRows(source, cookie, '/cursos/get', { careerId, length: '500' });
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            career_id: careerId,
          })),
        );
      }
      return rows;
    }
    if (resource === 'classroom_types') {
      return this.fetchRows(source, cookie, '/admin/aulas/categorias/get', { length: '500' });
    }
    if (resource === 'zoom_users') {
      return this.fetchRows(source, cookie, '/web/conference/aulas/listar', { length: '500' });
    }
    if (resource === 'faculties') {
      return this.fetchRows(source, cookie, '/admin/facultades/get');
    }
    if (resource === 'study_plans') {
      return this.fetchStudyPlanSyncRows(source, cookie);
    }
    if (resource === 'teachers') {
      return this.fetchRows(source, cookie, '/admin/docentes/get', { length: '1000' });
    }
    if (resource === 'classrooms') {
      return this.fetchRows(source, cookie, '/admin/aulas/get', { length: '1000' });
    }
    if (resource === 'classroom_section_schedules') {
      const classroomIds = await this.resolveClassroomIds(dto.classroom_ids);
      const scheduleWindow = await this.resolveScheduleWindow(dto);
      const rows: Record<string, unknown>[] = [];
      for (const classroomId of classroomIds) {
        const partial = await this.fetchRows(
          source,
          cookie,
          `/admin/horarioaulas/${classroomId}/get`,
          {
            start: scheduleWindow.start,
            end: scheduleWindow.end,
            _: `${Date.now()}`,
          },
        );
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            __classroom_id: classroomId,
          })),
        );
      }
      return rows;
    }
    if (resource === 'buildings') {
      const campusIds = await this.resolveCampusIds(dto.campus_ids);
      const rows: Record<string, unknown>[] = [];
      for (const campusId of campusIds) {
        const partial = await this.fetchRows(source, cookie, '/admin/campus/pabellones/get', {
          length: '500',
          campus: campusId,
        });
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            __campus_id: campusId,
          })),
        );
      }
      return rows;
    }
    if (resource === 'academic_program_campuses') {
      const campusIds = await this.resolveCampusIds(dto.campus_ids);
      const rows: Record<string, unknown>[] = [];
      for (const campusId of campusIds) {
        const partial = await this.fetchRows(source, cookie, '/admin/campus/carreras/get', {
          length: '500',
          campus: campusId,
        });
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            __campus_id: campusId,
          })),
        );
      }
      return rows;
    }
    if (resource === 'course_sections') {
      const courseIds = await this.resolveCourseIds(dto.course_ids);
      const rows: Record<string, unknown>[] = [];
      for (const courseId of courseIds) {
        const partial = await this.fetchRows(source, cookie, '/secciones/get', {
          courseId,
        });
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            __course_id: courseId,
          })),
        );
      }
      return rows;
    }

    // --- NUEVOS RECURSOS VC ---
    if (resource === 'vc_periods') {
      // https://aulavirtual2.autonomadeica.edu.pe/periodos-academicos/get
      return this.fetchRows(source, cookie, '/periodos-academicos/get');
    }

    if (resource === 'vc_faculties') {
      // https://aulavirtual2.autonomadeica.edu.pe/facultades/get
      return this.fetchRows(source, cookie, '/facultades/get');
    }

    if (resource === 'vc_academic_programs') {
      // https://aulavirtual2.autonomadeica.edu.pe/carreras/get?facultyId=...
      // Iteramos sobre TODAS las facultades que tengamos, o resolvemos IDs
      const facultyIds = await this.resolveVcFacultyIds();
      const rows: Record<string, unknown>[] = [];
      for (const facultyId of facultyIds) {
        const partial = await this.fetchRows(source, cookie, '/carreras/get', { facultyId });
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            __faculty_id: facultyId,
          })),
        );
      }
      return rows;
    }

    if (resource === 'vc_courses') {
      // https://aulavirtual2.autonomadeica.edu.pe/cursos/get?careerId=...
      const programIds = await this.resolveVcProgramIds();
      const rows: Record<string, unknown>[] = [];
      for (const careerId of programIds) {
        const partial = await this.fetchRows(source, cookie, '/cursos/get', { careerId, length: '1000' });
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            __program_id: careerId,
          })),
        );
      }
      return rows;
    }

    if (resource === 'vc_sections') {
      // https://aulavirtual2.autonomadeica.edu.pe/secciones/get?courseId=...
      const courseIds = await this.resolveVcCourseIds();
      const rows: Record<string, unknown>[] = [];
      // Lógica por lotes o 1 a 1. Si son muchos cursos, demora.
      for (const courseId of courseIds) {
        const partial = await this.fetchRows(source, cookie, '/secciones/get', { courseId });
        rows.push(
          ...partial.map((item) => ({
            ...(item as Record<string, unknown>),
            __course_id: courseId,
          })),
        );
      }
      return rows;
    }

    return [];
  }

  private async persistResourceRows(resource: ResourceCode, rows: Record<string, unknown>[]) {
    if (resource === 'semesters') {
      return this.upsertById(
        this.semestersRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asNullableString(pick(row, 'name', 'text', 'period')),
          state: asNullableString(pick(row, 'state', 'status', 'estado')),
          start_date: asDateOnly(pick(row, 'startDate', 'start_date', 'start')),
          end_date: asDateOnly(pick(row, 'endDate', 'end_date', 'end')),
          class_start_date: asDateOnly(
            pick(row, 'classStartDate', 'class_start_date', 'classStart', 'startDate', 'start_date'),
          ),
          class_end_date: asDateOnly(
            pick(row, 'classEndDate', 'class_end_date', 'classEnd', 'endDate', 'end_date'),
          ),
        })),
      );
    }

    if (resource === 'campuses') {
      return this.upsertById(
        this.campusesRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          code: asNullableString(pick(row, 'code')),
          name: asNullableString(pick(row, 'name')),
          address: asNullableString(pick(row, 'address')),
          is_valid: asBoolean(pick(row, 'isValid', 'is_valid', 'is_valided', 'status'), true),
          is_principal: asBoolean(pick(row, 'isPrincipal', 'is_principal', 'principal'), false),
          district: asNullableString(pick(row, 'district')),
          province: asNullableString(pick(row, 'province')),
          department: asNullableString(pick(row, 'department')),
        })),
      );
    }

    if (resource === 'faculties') {
      return this.upsertById(
        this.facultiesRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          code: asNullableString(pick(row, 'code')),
          name: asNullableString(pick(row, 'name')),
          abbreviation: asNullableString(pick(row, 'abbreviation')),
          institucional_email: asNullableString(
            pick(row, 'institucional_email', 'institutionalEmail', 'email'),
          ),
          is_active: asBoolean(pick(row, 'is_active', 'isActive', 'status'), true),
          is_valid: asBoolean(pick(row, 'is_valid', 'isValid'), true),
        })),
      );
    }

    if (resource === 'academic_programs') {
      return this.upsertById(
        this.programsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          code: asNullableString(pick(row, 'code')),
          name: asNullableString(pick(row, 'name')),
          faculty_id: asNullableString(pick(row, 'faculty_id', 'facultyId')),
          faculty: asNullableString(pick(row, 'faculty', 'facultyName')),
          graduate_profile: asNullableString(pick(row, 'graduate_profile', 'graduateProfile')),
          general_information: asNullableString(pick(row, 'general_information', 'generalInformation')),
          comments: asNullableString(pick(row, 'comments')),
          decanal_resolution: asNullableString(pick(row, 'decanal_resolution', 'decanalResolution')),
          rectoral_resolution: asNullableString(pick(row, 'rectoral_resolution', 'rectoralResolution')),
        })),
      );
    }

    if (resource === 'sections') {
      return this.upsertById(
        this.sectionsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          description: asNullableString(pick(row, 'description', 'text', 'name')),
        })),
      );
    }

    if (resource === 'courses') {
      return this.upsertById(
        this.coursesRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          code: asNullableString(pick(row, 'code', 'courseCode')),
          name: asNullableString(pick(row, 'name', 'courseName')),
          area_career: asNullableString(pick(row, 'area_career', 'areaCareer')),
          cycle: asNullableString(pick(row, 'cycle')),
          program: asNullableString(pick(row, 'program')),
          type: asNullableString(pick(row, 'type')),
          has_syllabus: asBoolean(pick(row, 'hasSyllabus', 'has_syllabus'), false),
          can_edit: asBoolean(pick(row, 'canEdit', 'can_edit'), false),
          career_id: asNullableString(pick(row, 'career_id', 'careerId', 'programId')),
          area: asNullableString(pick(row, 'area')),
          academic_year: asNullableInt(pick(row, 'academic_year', 'academicYear', 'year')),
        })),
      );
    }

    if (resource === 'classroom_types') {
      return this.upsertById(
        this.classroomTypesRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asNullableString(pick(row, 'name')),
        })),
      );
    }

    if (resource === 'zoom_users') {
      return this.upsertById(
        this.zoomUsersRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asString(pick(row, 'name')),
          email: asString(pick(row, 'email')),
        })),
      );
    }

    if (resource === 'study_plans') {
      return this.persistStudyPlanSyncRows(rows);
    }

    if (resource === 'teachers') {
      return this.upsertById(
        this.teachersRepo,
        rows.map((row) => ({
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
        })),
      );
    }

    if (resource === 'buildings') {
      return this.upsertById(
        this.buildingsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          campus_id: asNullableString(pick(row, 'campus_id', 'campusId', '__campus_id', 'campus.id')),
          name: asNullableString(pick(row, 'name')),
        })),
      );
    }

    if (resource === 'classrooms') {
      return this.upsertById(
        this.classroomsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asNullableString(pick(row, 'name')),
          building_id: asNullableString(pick(row, 'building_id', 'buildingId', 'building.id')),
          campus_id: asNullableString(pick(row, 'campus_id', 'campusId', 'campus.id')),
          type_id: asNullableString(pick(row, 'type_id', 'typeId', 'type.id')),
          faculty_id: asNullableString(pick(row, 'faculty_id', 'facultyId')),
          capacity: asNullableInt(pick(row, 'capacity')),
          status: asBoolean(pick(row, 'status', 'isActive'), true),
          ip_address: asNullableString(pick(row, 'iPAddress', 'ip_address', 'ipAddress')),
          code: asNullableString(pick(row, 'code')),
          floor: asNullableString(pick(row, 'floor')),
          number: asNullableInt(pick(row, 'number')),
        })),
      );
    }

    if (resource === 'classroom_section_schedules') {
      return this.persistClassroomSectionSchedules(rows);
    }

    if (resource === 'academic_program_campuses') {
      return this.upsertById(
        this.programCampusesRepo,
        rows.map((row) => {
          const academicProgramId = asString(
            pick(row, 'academic_program_id', 'academicProgramId', 'careerId', 'id'),
          );
          const campusId = asString(pick(row, 'campus_id', 'campusId', '__campus_id'));
          return {
            id: stableId(academicProgramId, campusId),
            academic_program_id: academicProgramId,
            campus_id: campusId,
            is_active: asBoolean(pick(row, 'is_active', 'isActive', 'status'), true),
          };
        }),
      );
    }

    if (resource === 'course_sections') {
      return this.upsertById(
        this.courseSectionsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          course_id: asNullableString(pick(row, 'course_id', 'courseId', '__course_id')),
          section_id: asNullableString(pick(row, 'section_id', 'sectionId')),
          text: asNullableString(pick(row, 'text', 'description', 'name')),
          teacher_id: asNullableString(pick(row, 'teacher_id', 'teacherId')),
          semester_id: asNullableString(pick(row, 'semester_id', 'semested_id', 'semesterId')),
        })),
      );
    }

    // --- PERSIST VC RESOURCES ---
    if (resource === 'vc_periods') {
      return this.upsertById(
        this.vcPeriodsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          text: asNullableString(pick(row, 'text', 'name')),
          selected: asBoolean(pick(row, 'selected'), false),
          is_active: true,
        })),
      );
    }

    if (resource === 'vc_faculties') {
      return this.upsertById(
        this.vcFacultiesRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asString(pick(row, 'text', 'name')),
        })),
      );
    }

    if (resource === 'vc_academic_programs') {
      // facultyId viene de fetchRows injected param (__faculty_id) o respuesta directa
      // La API no devuelve facultyId en el objeto, pero nosotros lo inyectamos en fetchRows
      return this.upsertById(
        this.vcProgramsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asString(pick(row, 'text', 'name')),
          faculty_id: asString(pick(row, '__faculty_id', 'facultyId')),
        })),
      );
    }

    if (resource === 'vc_courses') {
      return this.upsertById(
        this.vcCoursesRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          code: asNullableString(pick(row, 'code')),
          name: asString(pick(row, 'text', 'name')),
          program_id: asString(pick(row, '__program_id', 'careerId', 'programId')),
        })),
      );
    }

    if (resource === 'vc_sections') {
      return this.upsertById(
        this.vcSectionsRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asString(pick(row, 'text', 'name')),
          course_id: asString(pick(row, '__course_id', 'courseId')),
          teachers_json: (row.teachers as Record<string, unknown>[]) ?? [],
        })),
      );
    }

    return { received: 0, processed: 0, created: 0, updated: 0, skipped: 0, deduplicated: 0 };
  }

  private async fetchStudyPlanSyncRows(
    source: ExternalSourceEntity,
    cookie: string,
  ): Promise<Record<string, unknown>[]> {
    const plans = await this.fetchRows(source, cookie, '/admin/plan-estudios/get', { length: '1000' });
    const rows: Record<string, unknown>[] = [];
    const validPlans = plans
      .map((plan) => ({
        study_plan_id: asString(pick(plan, 'id')),
        plan,
      }))
      .filter((item) => item.study_plan_id !== '');

    console.log(`[SYNC study_plans] Plans received: ${validPlans.length}`);

    for (const { plan } of validPlans) {
      rows.push({
        ...plan,
        __kind: 'study_plan',
      });
    }

    const planCourses = await runWithConcurrency(validPlans, 4, async ({ study_plan_id, plan }) => {
      const courses = await this.fetchRows(
        source,
        cookie,
        `/admin/plan-estudios/${study_plan_id}/cursos/get`,
      );

      return { study_plan_id, plan, courses };
    });

    const detailTargets: Array<{ study_plan_id: string; study_plan_course_id: string }> = [];
    for (const { study_plan_id, courses } of planCourses) {
      for (const course of courses) {
        const studyPlanCourseId = asString(pick(course, 'id'));
        if (!studyPlanCourseId) {
          continue;
        }

        rows.push({
          ...course,
          __kind: 'study_plan_course',
          __study_plan_id: study_plan_id,
        });

        detailTargets.push({
          study_plan_id,
          study_plan_course_id: studyPlanCourseId,
        });
      }
    }

    console.log(`[SYNC study_plans] Course detail requests queued: ${detailTargets.length}`);

    let completedDetails = 0;
    const detailRows = await runWithConcurrency(detailTargets, 8, async (target) => {
      const detail = await this.fetchPayload(
        source,
        cookie,
        '/admin/plan-estudios/get-plan-estudio-curso',
        { academicYerCourseId: target.study_plan_course_id },
      );

      completedDetails += 1;
      if (completedDetails % 50 === 0 || completedDetails === detailTargets.length) {
        console.log(
          `[SYNC study_plans] Detail progress: ${completedDetails}/${detailTargets.length}`,
        );
      }

      if (!detail || typeof detail !== 'object' || Array.isArray(detail)) {
        return null;
      }

      return {
        ...(detail as Record<string, unknown>),
        __kind: 'study_plan_course_detail',
        __study_plan_id: target.study_plan_id,
        __study_plan_course_id: target.study_plan_course_id,
      };
    });

    for (const detailRow of detailRows) {
      if (detailRow) {
        rows.push(detailRow as Record<string, unknown>);
      }
    }

    return rows;
  }

  private async persistStudyPlanSyncRows(rows: Record<string, unknown>[]) {
    const planRows = rows.filter((row) => row.__kind === 'study_plan');
    const courseRows = rows.filter((row) => row.__kind === 'study_plan_course');
    const detailRows = rows.filter((row) => row.__kind === 'study_plan_course_detail');

    const faculties = await this.facultiesRepo.find();
    const programs = await this.programsRepo.find();
    const facultyIdByName = new Map(
      faculties
        .filter((faculty) => faculty.name)
        .map((faculty) => [normalizeLookupKey(faculty.name), faculty.id]),
    );
    const programsByName = new Map<string, AcademicProgramEntity[]>();
    for (const program of programs) {
      const normalizedProgramName = normalizeLookupKey(program.name);
      if (!normalizedProgramName) {
        continue;
      }
      const bucket = programsByName.get(normalizedProgramName) ?? [];
      bucket.push(program);
      programsByName.set(normalizedProgramName, bucket);
    }

    const planStats = await this.upsertById(
      this.studyPlansRepo,
      planRows.map((row) => {
        const faculty = asNullableString(pick(row, 'faculty'));
        const career = asNullableString(pick(row, 'career'));
        const academicProgram = asNullableString(pick(row, 'academicProgram', 'academic_program'));
        const year = asNullableString(pick(row, 'year'));
        const normalizedFaculty = normalizeLookupKey(faculty);
        const normalizedCareer = normalizeLookupKey(career ?? academicProgram);
        const resolvedFacultyId =
          asNullableString(pick(row, 'facultyId', 'faculty_id')) ??
          (faculty ? facultyIdByName.get(normalizedFaculty) ?? null : null);
        const resolvedProgramId = resolveProgramIdFromCatalog(
          programsByName,
          career ?? academicProgram,
          resolvedFacultyId,
          faculty,
        );
        return {
          id: asString(pick(row, 'id')),
          name: buildStudyPlanDisplayName(career, year),
          faculty,
          career,
          academic_program: academicProgram ?? career,
          year,
          creation_resolution: asNullableString(
            pick(row, 'creationresolution', 'creationResolution'),
          ),
          approve_resolution: asNullableString(pick(row, 'aproberesolution', 'approveResolution')),
          faculty_id: resolvedFacultyId,
          academic_program_id: resolvedProgramId,
          is_active: asBoolean(pick(row, 'isActive', 'is_active'), true),
          is_new: asBoolean(pick(row, 'isNew', 'is_new'), false),
          is_unique: asBoolean(pick(row, 'isUnique', 'is_unique'), false),
        };
      }),
    );

    const courseStats = await this.upsertById(
      this.studyPlanCoursesRepo,
      courseRows.map((row) => ({
        id: asString(pick(row, 'id')),
        study_plan_id: asString(pick(row, '__study_plan_id', 'study_plan_id')),
        order: asNullableInt(pick(row, 'order')),
        year_credits: asNullableString(pick(row, 'yearCredits', 'year_credits')),
        year_label: asNullableString(pick(row, 'year', 'yearLabel', 'year_label')),
        course_code: asNullableString(pick(row, 'code', 'courseCode')),
        course_name: asNullableString(pick(row, 'course', 'courseName', 'name')),
        academic_program: asNullableString(pick(row, 'academicProgram', 'academic_program')),
        credits: asNullableNumber(pick(row, 'credits')),
        required_credits: asNullableNumber(pick(row, 'requiredCredits', 'required_credits')),
        is_elective: asBoolean(pick(row, 'isElective', 'is_elective'), false),
        requisites: asNullableString(pick(row, 'requisites')),
        certificates: asJsonArray(pick(row, 'certificates')),
        requisites_ids: asStringArray(pick(row, 'requisitesIds', 'requisites_ids')),
        optional_requisites_ids: asStringArray(
          pick(row, 'optionalRequisitesIds', 'optional_requisites_ids'),
        ),
        requisite1: asNullableString(pick(row, 'requisite1')),
        requisite2: asNullableString(pick(row, 'requisite2')),
        requisite3: asNullableString(pick(row, 'requisite3')),
        count: asNullableInt(pick(row, 'count')),
        competencie_id: asNullableString(pick(row, 'competencieId', 'competencie_id')),
        competencie: asNullableString(pick(row, 'competencie')),
        is_exonerable: asBoolean(pick(row, 'isExonerable', 'is_exonerable'), false),
      })),
    );

    const detailStats = await this.upsertByKey(
      this.studyPlanCourseDetailsRepo,
      detailRows.map((row) => ({
        study_plan_course_id: asString(
          pick(row, '__study_plan_course_id', 'study_plan_course_id', 'id'),
        ),
        short_code: asNullableString(pick(row, 'shortCode', 'short_code')),
        name: asNullableString(pick(row, 'name')),
        practical_hours: asNullableInt(pick(row, 'practicalHours', 'practical_hours')),
        theoretical_hours: asNullableInt(pick(row, 'theoreticalHours', 'theoretical_hours')),
        virtual_hours: asNullableInt(pick(row, 'virtualHours', 'virtual_hours')),
        seminar_hours: asNullableInt(pick(row, 'seminarHours', 'seminar_hours')),
        credits: asNullableNumber(pick(row, 'credits')),
        required_credits: asNullableNumber(pick(row, 'requiredCredits', 'required_credits')),
        academic_year: asNullableInt(pick(row, 'academicYear', 'academic_year')),
        course_type_id: asNullableString(pick(row, 'courseTypeId', 'course_type_id')),
        is_elective: asBoolean(pick(row, 'isElective', 'is_elective'), false),
        area_id: asNullableString(pick(row, 'areaId', 'area_id')),
        course_component_id: asNullableString(
          pick(row, 'courseComponentId', 'course_component_id'),
        ),
        academic_program_id: asNullableString(
          pick(row, 'academicProgramId', 'academic_program_id'),
        ),
        any_course_term: asBoolean(pick(row, 'anyCourseTerm', 'any_course_term'), false),
      })),
      'study_plan_course_id',
    );

    const syncedPlanIds = uniqueIds(planRows.map((row) => asString(pick(row, 'id'))));
    let deletedCourses = 0;
    let deletedDetails = 0;

    for (const studyPlanId of syncedPlanIds) {
      const currentCourseIds = uniqueIds(
        courseRows
          .filter((row) => asString(pick(row, '__study_plan_id', 'study_plan_id')) === studyPlanId)
          .map((row) => asString(pick(row, 'id'))),
      );
      const currentDetailIds = new Set(
        detailRows
          .filter((row) => asString(pick(row, '__study_plan_id', 'study_plan_id')) === studyPlanId)
          .map((row) => asString(pick(row, '__study_plan_course_id', 'study_plan_course_id', 'id'))),
      );

      const existingCourses = await this.studyPlanCoursesRepo.find({
        where: { study_plan_id: studyPlanId },
      });
      const staleCourseIds = existingCourses
        .map((course) => course.id)
        .filter((courseId) => !currentCourseIds.includes(courseId));

      if (staleCourseIds.length > 0) {
        const deletedCourseDetails = await this.studyPlanCourseDetailsRepo.delete({
          study_plan_course_id: In(staleCourseIds),
        });
        const deletedPlanCourses = await this.studyPlanCoursesRepo.delete({ id: In(staleCourseIds) });
        deletedCourses += deletedPlanCourses.affected ?? 0;
        deletedDetails += deletedCourseDetails.affected ?? 0;
      }

      if (currentCourseIds.length > 0) {
        const existingDetails = await this.studyPlanCourseDetailsRepo.find({
          where: { study_plan_course_id: In(currentCourseIds) },
        });
        const staleDetailIds = existingDetails
          .map((detail) => detail.study_plan_course_id)
          .filter((detailId) => !currentDetailIds.has(detailId));

        if (staleDetailIds.length > 0) {
          const deleted = await this.studyPlanCourseDetailsRepo.delete({
            study_plan_course_id: In(staleDetailIds),
          });
          deletedDetails += deleted.affected ?? 0;
        }
      }
    }

    return {
      received: rows.length,
      processed: planStats.processed + courseStats.processed + detailStats.processed,
      created: planStats.created + courseStats.created + detailStats.created,
      updated: planStats.updated + courseStats.updated + detailStats.updated,
      skipped: planStats.skipped + courseStats.skipped + detailStats.skipped,
      deduplicated:
        planStats.deduplicated + courseStats.deduplicated + detailStats.deduplicated,
      plans: planStats,
      study_plan_courses: courseStats,
      study_plan_course_details: detailStats,
      deleted_courses: deletedCourses,
      deleted_details: deletedDetails,
    };
  }

  private async fetchPayload(
    source: ExternalSourceEntity,
    cookie: string,
    path: string,
    query?: Record<string, string>,
  ) {
    const response = await this.requestSource(source, cookie, path, query);
    const responseError = this.normalizeErrorText(response.error);
    if (response.expired) {
      await this.markSessionStatus(source.id, 'EXPIRED', responseError ?? 'Sesion vencida');
      throw new BadRequestException(`Sesion vencida para fuente ${source.code}. Renueva cookie.`);
    }
    if (!response.ok) {
      throw new BadRequestException(
        `Error consultando ${source.code}: ${response.status} ${responseError ?? 'sin detalle'}`,
      );
    }
    return response.payload;
  }

  private async fetchRows(
    source: ExternalSourceEntity,
    cookie: string,
    path: string,
    query?: Record<string, string>,
  ) {
    const payload = await this.fetchPayload(source, cookie, path, query);
    return extractRows(payload);
  }

  private async probeSourceSession(code: string): Promise<SourceProbeResult> {
    const source = await this.findSourceOrFail(code);
    const session = await this.sessionsRepo.findOne({ where: { source_id: source.id } });
    if (!session) {
      const autoLoginResult = await this.tryAutoLoginForSource(
        source,
        null,
        'No existe cookie/sesion configurada.',
      );
      if (autoLoginResult) {
        return autoLoginResult;
      }
      return { ok: false, source, session: null, reason: 'No existe cookie/sesion configurada.' };
    }

    const cookie = this.decryptCookieJar(session.cookie_jar_encrypted);
    const validatePath = this.getSourceDefinition(source.code).validate_path;
    const response = await this.requestSource(
      source,
      cookie,
      validatePath,
      undefined,
      this.getValidateRequestTimeoutMs(),
    );
    const responseError = this.normalizeErrorText(response.error);

    if (response.expired) {
      const autoLoginResult = await this.tryAutoLoginForSource(
        source,
        session,
        responseError ?? 'Sesion vencida',
      );
      if (autoLoginResult?.ok) {
        return autoLoginResult;
      }
      await this.markSessionStatus(source.id, 'EXPIRED', responseError ?? 'Sesion vencida');
      return {
        ok: false,
        source,
        session,
        reason: autoLoginResult?.reason ?? responseError ?? 'Sesion vencida',
        status_code: response.status,
        final_url: response.final_url,
      };
    }

    if (!response.ok) {
      await this.markSessionStatus(source.id, 'ERROR', responseError ?? 'Error validando sesion');
      return {
        ok: false,
        source,
        session,
        reason: responseError ?? `HTTP ${response.status}`,
        status_code: response.status,
        final_url: response.final_url,
      };
    }

    const updatedExpiresAt = response.expires_at ?? session.expires_at ?? null;
    await this.sessionsRepo.update(
      { id: session.id },
      {
        status: 'ACTIVE',
        last_validated_at: new Date(),
        expires_at: updatedExpiresAt,
        error_last: null,
        updated_at: new Date(),
      },
    );

    return {
      ok: true,
      source,
      session,
      cookie,
      status_code: response.status,
      final_url: response.final_url,
    };
  }

  private async tryAutoLoginForSource(
    source: ExternalSourceEntity,
    session: ExternalSessionEntity | null,
    reason: string,
  ) {
    if (source.code !== 'AULAVIRTUAL') {
      return null;
    }

    const config = this.getAulaVirtualAutoLoginConfig(source);
    if (!config) {
      return null;
    }

    return this.runAutoLoginOnce('AULAVIRTUAL', () =>
      this.loginAulaVirtualAndPersistSession(source, session, config, reason),
    );
  }

  private async runAutoLoginOnce(code: SourceCode, worker: () => Promise<SourceProbeResult>) {
    const existing = this.sourceAutoLoginInFlight.get(code);
    if (existing) {
      return existing;
    }

    const current = worker().finally(() => {
      this.sourceAutoLoginInFlight.delete(code);
    });
    this.sourceAutoLoginInFlight.set(code, current);
    return current;
  }

  private getAulaVirtualAutoLoginConfig(
    source: ExternalSourceEntity,
  ): AulaVirtualAutoLoginConfig | null {
    const username = this.configService.get<string>('AULA_VIRTUAL_USERNAME', '').trim();
    const password = this.configService.get<string>('AULA_VIRTUAL_PASSWORD', '').trim();
    const enabledDefault = username && password ? 'true' : 'false';
    const enabled =
      this.configService.get<string>('AULA_VIRTUAL_AUTO_LOGIN_ENABLED', enabledDefault) === 'true';

    if (!enabled || !username || !password) {
      return null;
    }

    const loginUrl =
      this.configService.get<string>('AULA_VIRTUAL_LOGIN_URL', '').trim() ||
      source.login_url ||
      this.getSourceDefinition(source.code).login_url ||
      '';
    if (!loginUrl) {
      return null;
    }

    const parsedLoginUrl = new URL(loginUrl);
    const returnUrlValue =
      this.configService.get<string>('AULA_VIRTUAL_LOGIN_RETURN_URL_VALUE', '').trim() ||
      parsedLoginUrl.searchParams.get('ReturnUrl') ||
      '/';

    return {
      loginUrl,
      username,
      password,
      usernameField: this.configService
        .get<string>('AULA_VIRTUAL_LOGIN_USERNAME_FIELD', 'username')
        .trim(),
      passwordField: this.configService
        .get<string>('AULA_VIRTUAL_LOGIN_PASSWORD_FIELD', 'password')
        .trim(),
      returnUrlField: this.configService
        .get<string>('AULA_VIRTUAL_LOGIN_RETURN_URL_FIELD', 'ReturnUrl')
        .trim(),
      returnUrlValue,
      userAgent: this.configService
        .get<string>('AULA_VIRTUAL_LOGIN_USER_AGENT', 'Mozilla/5.0 (UAI Sync)')
        .trim(),
    };
  }

  private async loginAulaVirtualAndPersistSession(
    source: ExternalSourceEntity,
    existingSession: ExternalSessionEntity | null,
    config: AulaVirtualAutoLoginConfig,
    reason: string,
  ): Promise<SourceProbeResult> {
    try {
      const loginPageResponse = await fetch(config.loginUrl, {
        method: 'GET',
        headers: {
          accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'user-agent': config.userAgent,
        },
        redirect: 'follow',
      });

      const loginPageHtml = await loginPageResponse.text();
      const loginPageCookies = getSetCookieValues(loginPageResponse.headers);
      let cookieHeader = mergeCookieHeaders(
        buildCookieHeaderFromSetCookies(loginPageCookies),
      );

      const form = new URLSearchParams();
      const hiddenFields = extractHiddenInputFields(loginPageHtml);
      for (const [field, value] of Object.entries(hiddenFields)) {
        form.set(field, value);
      }
      form.set(config.usernameField, config.username);
      form.set(config.passwordField, config.password);
      if (config.returnUrlField && config.returnUrlValue && !form.has(config.returnUrlField)) {
        form.set(config.returnUrlField, config.returnUrlValue);
      }

      const loginResponse = await fetch(config.loginUrl, {
        method: 'POST',
        headers: {
          accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'content-type': 'application/x-www-form-urlencoded',
          cookie: cookieHeader,
          origin: new URL(config.loginUrl).origin,
          referer: config.loginUrl,
          'user-agent': config.userAgent,
        },
        body: form.toString(),
        redirect: 'manual',
      });

      const loginCookies = getSetCookieValues(loginResponse.headers);
      cookieHeader = mergeCookieHeaders(
        cookieHeader,
        buildCookieHeaderFromSetCookies(loginCookies),
      );

      const redirectLocation = loginResponse.headers.get('location');
      const shouldFollowRedirect =
        isRedirectStatus(loginResponse.status) && Boolean(redirectLocation);

      if (shouldFollowRedirect && redirectLocation) {
        const redirectedUrl = new URL(redirectLocation, config.loginUrl).toString();
        const followResponse = await fetch(redirectedUrl, {
          method: 'GET',
          headers: {
            accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            cookie: cookieHeader,
            referer: config.loginUrl,
            'user-agent': config.userAgent,
          },
          redirect: 'follow',
        });
        cookieHeader = mergeCookieHeaders(
          cookieHeader,
          buildCookieHeaderFromSetCookies(getSetCookieValues(followResponse.headers)),
        );
      }

      const lauxAuth = getCookieValue(cookieHeader, 'LAUXAUTH');
      if (!lauxAuth) {
        await this.markSessionStatus(
          source.id,
          'ERROR',
          'No se obtuvo LAUXAUTH al renovar la sesion de Aula Virtual.',
        );
        return {
          ok: false,
          source,
          session: existingSession,
          reason: 'No se obtuvo LAUXAUTH al renovar la sesion de Aula Virtual.',
        };
      }

      const validatePath = this.getSourceDefinition(source.code).validate_path;
      const validateResponse = await this.requestSource(
        source,
        cookieHeader,
        validatePath,
        undefined,
        this.getValidateRequestTimeoutMs(),
      );
      const validateError = this.normalizeErrorText(validateResponse.error);
      if (validateResponse.expired || !validateResponse.ok) {
        const errorText =
          validateError ?? 'Login automatico ejecutado, pero la sesion sigue sin validar.';
        await this.markSessionStatus(source.id, 'ERROR', errorText);
        return {
          ok: false,
          source,
          session: existingSession,
          reason: errorText,
          status_code: validateResponse.status,
          final_url: validateResponse.final_url,
        };
      }

      const expiresAt =
        validateResponse.expires_at ??
        extractEarliestExpiry([...loginPageCookies, ...loginCookies]) ??
        null;
      const persistedSession = await this.saveSourceSessionCookie(
        source,
        existingSession,
        cookieHeader,
        expiresAt,
      );

      return {
        ok: true,
        source,
        session: persistedSession,
        cookie: cookieHeader,
        reason,
        status_code: validateResponse.status,
        final_url: validateResponse.final_url,
      };
    } catch (error) {
      const errorText = `No se pudo renovar Aula Virtual automaticamente: ${this.toErrorMessage(error)}`;
      await this.markSessionStatus(source.id, 'ERROR', errorText);
      return {
        ok: false,
        source,
        session: existingSession,
        reason: errorText,
      };
    }
  }

  private async saveSourceSessionCookie(
    source: ExternalSourceEntity,
    existingSession: ExternalSessionEntity | null,
    cookie: string,
    expiresAt: Date | null,
  ) {
    const encrypted = this.encryptCookieJar(this.normalizeCookieText(cookie));
    const now = new Date();
    const session = existingSession
      ? this.sessionsRepo.create({
        ...existingSession,
        cookie_jar_encrypted: encrypted,
        status: 'ACTIVE',
        last_validated_at: now,
        expires_at: expiresAt,
        error_last: null,
        updated_at: now,
      })
      : this.sessionsRepo.create({
        id: newId(),
        source_id: source.id,
        cookie_jar_encrypted: encrypted,
        status: 'ACTIVE',
        last_validated_at: now,
        expires_at: expiresAt,
        error_last: null,
        created_at: now,
        updated_at: now,
      });

    return this.sessionsRepo.save(session);
  }

  private async requestSource(
    source: ExternalSourceEntity,
    cookie: string,
    path: string,
    query?: Record<string, string>,
    timeoutOverrideMs?: number,
  ) {
    const url = new URL(path, source.base_url);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        url.searchParams.set(key, value);
      }
    }

    const timeoutMs = timeoutOverrideMs ?? this.getSourceRequestTimeoutMs();
    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url.toString(), {
        method: 'GET',
        headers: {
          accept: 'application/json, text/plain, */*',
          cookie,
          'x-requested-with': 'XMLHttpRequest',
          'user-agent': 'Mozilla/5.0 (UAI Sync)',
        },
        redirect: 'follow',
        signal: controller.signal,
      });

      const contentType = response.headers.get('content-type') ?? '';
      const bodyText = await response.text();
      const parsed = parseMaybeJson(bodyText);
      const redirectedToLogin =
        response.redirected && /login|signin|account\/login/i.test(response.url);
      const unauthorizedStatus = response.status === 401 || response.status === 403;
      const htmlLooksLikeLogin =
        contentType.includes('text/html') && /login|iniciar sesi[oó]n|password/i.test(bodyText);
      const explicitSessionError =
        typeof parsed === 'object' &&
        parsed !== null &&
        /session|unauthorized|forbidden/i.test(JSON.stringify(parsed));
      const expired = unauthorizedStatus || redirectedToLogin || htmlLooksLikeLogin || explicitSessionError;

      const setCookie = (response.headers as any).getSetCookie?.() as string[] | undefined;
      const expiresAt = extractEarliestExpiry(setCookie ?? []);

      return {
        ok: response.ok && !expired,
        expired,
        status: response.status,
        payload: parsed,
        error: expired ? 'Sesion vencida o redirigida a login.' : !response.ok ? bodyText.slice(0, 300) : null,
        final_url: response.url,
        expires_at: expiresAt,
      };
    } catch (error) {
      const isTimeout =
        error instanceof Error &&
        (error.name === 'AbortError' || /aborted|abort|timeout/i.test(error.message));
      const rawMessage = this.toErrorMessage(error);
      const isTlsError =
        /certificate|ssl|tls|unable to verify|self signed|CERT_/i.test(rawMessage);
      const detailedError = isTlsError
        ? `Error TLS/certificado hacia ${source.code}. Activa SYNC_TLS_ALLOW_INSECURE=true en entorno local.`
        : rawMessage;
      return {
        ok: false,
        expired: false,
        status: 0,
        payload: null,
        error: isTimeout
          ? `Timeout consultando ${source.code} (${timeoutMs}ms).`
          : detailedError,
        final_url: url.toString(),
        expires_at: null,
      };
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  private getSourceRequestTimeoutMs() {
    const raw = Number(this.configService.get<string>('SYNC_REQUEST_TIMEOUT_MS', '12000'));
    if (!Number.isFinite(raw)) {
      return 12000;
    }
    return Math.max(3000, Math.min(60000, Math.trunc(raw)));
  }

  private getValidateRequestTimeoutMs() {
    const raw = Number(this.configService.get<string>('SYNC_VALIDATE_TIMEOUT_MS', '5000'));
    if (!Number.isFinite(raw)) {
      return 5000;
    }
    return Math.max(3000, Math.min(30000, Math.trunc(raw)));
  }

  private normalizeErrorText(error: string | null | undefined) {
    if (!error) {
      return null;
    }
    const trimmed = error.trim();
    return trimmed ? trimmed : null;
  }

  private normalizeSourceError(error: string | null) {
    const normalized = this.normalizeErrorText(error);
    if (!normalized) {
      return null;
    }
    if (/^fetch failed$/i.test(normalized)) {
      return 'No se pudo conectar a la fuente. Valida la cookie de esa fuente.';
    }
    return normalized;
  }

  private async persistClassroomSectionSchedules(rows: Record<string, unknown>[]) {
    const received = rows.length;
    if (received === 0) {
      return { received, processed: 0, created: 0, updated: 0, skipped: 0, deduplicated: 0 };
    }

    let skippedInvalidShape = 0;
    const parsedRows: ClassroomSectionScheduleEntity[] = [];
    for (const row of rows) {
      const classroomId = asNullableString(pick(row, '__classroom_id', 'classroom_id'));
      const sourceSectionId = asNullableString(pick(row, 'id', 'section_id', 'sectionId'));
      const start = parseDateTimeParts(asNullableString(pick(row, 'start')));
      const end = parseDateTimeParts(asNullableString(pick(row, 'end')));
      if (!classroomId || !sourceSectionId || !start || !end) {
        skippedInvalidShape += 1;
        continue;
      }
      const dayOfWeek = mapDayOfWeek(start);
      if (!dayOfWeek) {
        skippedInvalidShape += 1;
        continue;
      }

      const title = asNullableString(pick(row, 'title'));
      const parsedTitle = parseScheduleTitle(title);
      const startTime = toTimeValue(start);
      const endTime = toTimeValue(end);
      parsedRows.push({
        id: stableId(classroomId, `${sourceSectionId}|${dayOfWeek}|${startTime}|${endTime}`),
        classroom_id: classroomId,
        source_section_id: sourceSectionId,
        course_section_id: null,
        section_name: parsedTitle.section_name,
        course_code: parsedTitle.course_code,
        day_of_week: dayOfWeek,
        start_time: startTime,
        end_time: endTime,
        title,
        description: asNullableString(pick(row, 'description')),
        all_day: asBoolean(pick(row, 'allDay', 'all_day'), false),
      });
    }

    const deduplicatedMap = new Map<string, ClassroomSectionScheduleEntity>();
    for (const row of parsedRows) {
      deduplicatedMap.set(row.id, row);
    }
    const deduplicatedRows = [...deduplicatedMap.values()];
    const deduplicated = parsedRows.length - deduplicatedRows.length;
    let skipped = skippedInvalidShape;

    const sourceSectionIds = [...new Set(deduplicatedRows.map((row) => row.source_section_id))];
    let mappedById = 0;
    if (sourceSectionIds.length > 0) {
      const existingCourseSections = await this.courseSectionsRepo.find({
        where: { id: In(sourceSectionIds) },
        select: { id: true },
      });
      const existingVcSections = await this.vcSectionsRepo.find({
        where: { id: In(sourceSectionIds) },
        select: { id: true },
      });
      const existingSections = await this.sectionsRepo.find({
        where: { id: In(sourceSectionIds) },
        select: { id: true },
      });
      const validSectionIds = new Set([
        ...existingCourseSections.map((row) => row.id),
        ...existingVcSections.map((row) => row.id),
        ...existingSections.map((row) => row.id),
      ]);
      for (const row of deduplicatedRows) {
        if (validSectionIds.has(row.source_section_id)) {
          row.course_section_id = row.source_section_id;
          mappedById += 1;
        }
      }
    }

    let mappedByTitle = 0;
    const rowsToMapByTitle = deduplicatedRows.filter(
      (row) => !row.course_section_id && row.course_code && row.section_name,
    );
    if (rowsToMapByTitle.length > 0) {
      const vcCourses = await this.vcCoursesRepo.find({
        select: { id: true, code: true },
      });
      const codeToCourseIds = new Map<string, string[]>();
      for (const course of vcCourses) {
        const normalizedCode = normalizeCourseCode(course.code);
        if (!normalizedCode) {
          continue;
        }
        if (!codeToCourseIds.has(normalizedCode)) {
          codeToCourseIds.set(normalizedCode, []);
        }
        (codeToCourseIds.get(normalizedCode) as string[]).push(course.id);
      }

      const candidateCourseIds = [
        ...new Set(
          rowsToMapByTitle.flatMap((row) => codeToCourseIds.get(normalizeCourseCode(row.course_code)) ?? []),
        ),
      ];
      if (candidateCourseIds.length > 0) {
        const vcSections = await this.vcSectionsRepo.find({
          where: { course_id: In(candidateCourseIds) },
          select: { id: true, course_id: true, name: true },
        });
        const sectionByCourseAndName = new Map<string, string>();
        for (const section of vcSections) {
          const key = `${section.course_id}::${normalizeSectionName(section.name)}`;
          if (!sectionByCourseAndName.has(key)) {
            sectionByCourseAndName.set(key, section.id);
          }
        }

        for (const row of rowsToMapByTitle) {
          const normalizedCode = normalizeCourseCode(row.course_code);
          const normalizedSectionName = normalizeSectionName(row.section_name);
          const relatedCourseIds = codeToCourseIds.get(normalizedCode) ?? [];
          for (const courseId of relatedCourseIds) {
            const mappedSectionId = sectionByCourseAndName.get(
              `${courseId}::${normalizedSectionName}`,
            );
            if (mappedSectionId) {
              row.course_section_id = mappedSectionId;
              mappedByTitle += 1;
              break;
            }
          }
        }
      }
    }

    const unresolvedSectionRefs = deduplicatedRows.filter((row) => !row.course_section_id).length;
    const ids = deduplicatedRows.map((row) => row.id);
    const existing = await this.classroomSectionSchedulesRepo.find({
      where: { id: In(ids) },
      select: { id: true },
    });
    const existingIds = new Set(existing.map((row) => row.id));
    await this.classroomSectionSchedulesRepo.save(deduplicatedRows);

    const updated = deduplicatedRows.filter((row) => existingIds.has(row.id)).length;
    const created = deduplicatedRows.length - updated;
    return {
      received,
      processed: deduplicatedRows.length,
      created,
      updated,
      skipped,
      deduplicated,
      mapped_by_id: mappedById,
      mapped_by_title: mappedByTitle,
      unresolved_section_refs: unresolvedSectionRefs,
      skipped_invalid_shape: skippedInvalidShape,
    };
  }

  private async upsertById(
    repo: Repository<any>,
    rows: Array<Record<string, unknown>>,
  ): Promise<{
    received: number;
    processed: number;
    created: number;
    updated: number;
    skipped: number;
    deduplicated: number;
  }> {
    const received = rows.length;
    const deduplicatedRows = deduplicateRows(rows);
    const validRows = deduplicatedRows.filter((row) => typeof row.id === 'string' && row.id.trim() !== '');
    const skipped = received - validRows.length;
    if (validRows.length === 0) {
      return { received, processed: 0, created: 0, updated: 0, skipped, deduplicated: received };
    }

    const ids = validRows.map((row) => row.id as string);
    const existing = await repo.find({ where: { id: In(ids) } });
    const existingIds = new Set(existing.map((row: { id: string }) => row.id));
    await repo.save(validRows);

    const updated = validRows.filter((row) => existingIds.has(row.id as string)).length;
    const created = validRows.length - updated;
    return {
      received,
      processed: validRows.length,
      created,
      updated,
      skipped,
      deduplicated: received - deduplicatedRows.length,
    };
  }

  private async upsertByKey(
    repo: Repository<any>,
    rows: Array<Record<string, unknown>>,
    key: string,
  ): Promise<{
    received: number;
    processed: number;
    created: number;
    updated: number;
    skipped: number;
    deduplicated: number;
  }> {
    const received = rows.length;
    const deduplicatedMap = new Map<string, Record<string, unknown>>();
    for (const row of rows) {
      const keyValue = asString(row[key]);
      if (!keyValue) {
        continue;
      }
      deduplicatedMap.set(keyValue, row);
    }

    const deduplicatedRows = [...deduplicatedMap.values()];
    const validRows = deduplicatedRows.filter((row) => typeof row[key] === 'string' && asString(row[key]) !== '');
    const skipped = received - validRows.length;
    if (validRows.length === 0) {
      return { received, processed: 0, created: 0, updated: 0, skipped, deduplicated: received };
    }

    const ids = validRows.map((row) => asString(row[key]));
    const existing = await repo.find({ where: { [key]: In(ids) } });
    const existingIds = new Set(existing.map((row: Record<string, unknown>) => asString(row[key])));
    await repo.save(validRows);

    const updated = validRows.filter((row) => existingIds.has(asString(row[key]))).length;
    const created = validRows.length - updated;
    return {
      received,
      processed: validRows.length,
      created,
      updated,
      skipped,
      deduplicated: received - deduplicatedRows.length,
    };
  }

  private async createJob(
    sourceId: string,
    resource: ResourceCode,
    mode: (typeof SyncJobModeValues)[number],
    params: Record<string, unknown>,
  ) {
    const now = new Date();
    const job = this.jobsRepo.create({
      id: newId(),
      source_id: sourceId,
      resource,
      mode,
      status: 'PENDING',
      params_json: params,
      started_at: null,
      finished_at: null,
      created_by: null,
      created_at: now,
    });
    return this.jobsRepo.save(job);
  }

  private async updateJobStatus(
    job: SyncJobEntity,
    status: (typeof SyncJobStatusValues)[number],
  ) {
    if (status === 'RUNNING') {
      await this.jobsRepo.update({ id: job.id }, { status, started_at: new Date() });
      return;
    }
    await this.jobsRepo.update({ id: job.id }, { status });
  }

  private async completeJob(job: SyncJobEntity) {
    await this.jobsRepo.update(
      { id: job.id },
      { status: 'DONE', finished_at: new Date(), started_at: job.started_at ?? new Date() },
    );
  }

  private async failJob(job: SyncJobEntity, message: string) {
    await this.jobsRepo.update({ id: job.id }, { status: 'FAILED', finished_at: new Date() });
    await this.log(job.id, 'ERROR', message, null);
  }

  private async log(
    jobId: string,
    level: 'INFO' | 'WARN' | 'ERROR',
    message: string,
    meta: Record<string, unknown> | null,
  ) {
    await this.logsRepo.save(
      this.logsRepo.create({
        id: newId(),
        job_id: jobId,
        level,
        message,
        meta_json: meta,
        created_at: new Date(),
      }),
    );
  }

  private normalizeCookieText(raw: string) {
    const text = raw.trim();
    if (!text) {
      return '';
    }
    if (/^cookie:/i.test(text)) {
      return text.replace(/^cookie:/i, '').trim();
    }
    const lines = text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    if (lines.some((line) => /^set-cookie:/i.test(line))) {
      const pieces: string[] = [];
      for (const line of lines) {
        const value = line.replace(/^set-cookie:/i, '').trim();
        const firstPiece = value.split(';')[0]?.trim() ?? '';
        if (firstPiece.includes('=')) {
          pieces.push(firstPiece);
        }
      }
      return [...new Set(pieces)].join('; ');
    }

    return text;
  }

  private encryptCookieJar(raw: string) {
    const secret = this.configService.get<string>('SYNC_COOKIE_SECRET', 'change-me-in-env');
    const key = createHash('sha256').update(secret).digest();
    const iv = randomBytes(12);
    const cipher = createCipheriv('aes-256-gcm', key, iv);
    const encrypted = Buffer.concat([cipher.update(raw, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag();
    return Buffer.from(
      JSON.stringify({
        iv: iv.toString('base64'),
        tag: tag.toString('base64'),
        data: encrypted.toString('base64'),
      }),
      'utf8',
    ).toString('base64');
  }

  private decryptCookieJar(value: string) {
    try {
      const secret = this.configService.get<string>('SYNC_COOKIE_SECRET', 'change-me-in-env');
      const key = createHash('sha256').update(secret).digest();
      const parsed = JSON.parse(Buffer.from(value, 'base64').toString('utf8')) as {
        iv: string;
        tag: string;
        data: string;
      };
      const decipher = createDecipheriv(
        'aes-256-gcm',
        key,
        Buffer.from(parsed.iv, 'base64'),
      );
      decipher.setAuthTag(Buffer.from(parsed.tag, 'base64'));
      const decoded = Buffer.concat([
        decipher.update(Buffer.from(parsed.data, 'base64')),
        decipher.final(),
      ]);
      return decoded.toString('utf8');
    } catch {
      return value;
    }
  }

  private async ensureDefaultSources() {
    const now = new Date();
    const existing = await this.sourcesRepo.find();
    const byCode = new Map(existing.map((item) => [item.code, item]));
    const toSave: ExternalSourceEntity[] = [];

    for (const source of SOURCE_DEFINITIONS) {
      const found = byCode.get(source.code);
      if (found) {
        toSave.push(
          this.sourcesRepo.create({
            ...found,
            name: source.name,
            base_url: source.base_url,
            login_url: source.login_url,
            is_active: true,
            updated_at: now,
          }),
        );
        continue;
      }

      toSave.push(
        this.sourcesRepo.create({
          id: newId(),
          code: source.code,
          name: source.name,
          base_url: source.base_url,
          login_url: source.login_url,
          is_active: true,
          created_at: now,
          updated_at: now,
        }),
      );
    }

    if (toSave.length > 0) {
      await this.sourcesRepo.save(toSave);
    }
  }

  private getSourceDefinition(code: string) {
    const found = SOURCE_DEFINITIONS.find((item) => item.code === code);
    if (!found) {
      throw new NotFoundException(`Fuente ${code} no configurada.`);
    }
    return found;
  }

  private async findSourceOrFail(code: string) {
    const normalized = code.trim().toUpperCase();
    const found = await this.sourcesRepo.findOne({ where: { code: normalized } });
    if (!found) {
      throw new NotFoundException(`Fuente ${normalized} no encontrada.`);
    }
    return found;
  }

  private async resolveCampusIds(campusIds?: string[]) {
    if (campusIds?.length) {
      return campusIds;
    }
    const rows = await this.campusesRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveCourseIds(courseIds?: string[]) {
    if (courseIds?.length) {
      return courseIds;
    }
    const rows = await this.coursesRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveClassroomIds(classroomIds?: string[]) {
    if (classroomIds?.length) {
      return classroomIds;
    }
    const rows = await this.classroomsRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveScheduleWindow(dto: RunSettingsSyncDto) {
    const explicitStart = normalizeScheduleBoundary(dto.schedule_start, '00:00:00');
    const explicitEnd = normalizeScheduleBoundary(dto.schedule_end, '00:00:00');
    if (explicitStart && explicitEnd) {
      return { start: explicitStart, end: explicitEnd };
    }

    if (explicitStart) {
      const startDate = parseDateTimeParts(explicitStart);
      if (startDate) {
        return {
          start: explicitStart,
          end: formatDateTimeForSource(addDays(startDate, 7), '00:00:00'),
        };
      }
    }

    if (dto.semester_id) {
      const semester = await this.semestersRepo.findOne({
        where: { id: dto.semester_id },
        select: { class_start_date: true, start_date: true },
      });
      const semesterStart = normalizeScheduleBoundary(
        semester?.class_start_date ?? semester?.start_date ?? null,
        '00:00:00',
      );
      if (semesterStart) {
        const semesterStartDate = parseDateTimeParts(semesterStart);
        if (semesterStartDate) {
          return {
            start: semesterStart,
            end: formatDateTimeForSource(addDays(semesterStartDate, 7), '00:00:00'),
          };
        }
      }
    }

    const monday = startOfWeekMonday(new Date());
    return {
      start: formatDateTimeForSource(monday, '00:00:00'),
      end: formatDateTimeForSource(addDays(monday, 7), '00:00:00'),
    };
  }

  private async resolveFacultyIds() {
    const rows = await this.facultiesRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveProgramIds() {
    const rows = await this.programsRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveVcFacultyIds() {
    const rows = await this.vcFacultiesRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveVcProgramIds() {
    const rows = await this.vcProgramsRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveVcCourseIds() {
    const rows = await this.vcCoursesRepo.find({ select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async resolveVcCourseIdsByProgram(programId: string) {
    const rows = await this.vcCoursesRepo.find({ where: { program_id: programId }, select: { id: true } });
    return rows.map((row) => row.id);
  }

  private async markSessionStatus(
    sourceId: string,
    status: (typeof ExternalSessionStatusValues)[number],
    error: string | null,
  ) {
    const session = await this.sessionsRepo.findOne({ where: { source_id: sourceId } });
    if (!session) {
      return;
    }
    await this.sessionsRepo.update(
      { id: session.id },
      {
        status,
        error_last: error,
        last_validated_at: new Date(),
        updated_at: new Date(),
      },
    );
  }

  private toErrorMessage(error: unknown) {
    if (error && typeof error === 'object') {
      const asRecord = error as Record<string, unknown>;
      const message =
        typeof asRecord.message === 'string' ? asRecord.message : null;
      const cause = asRecord.cause as Record<string, unknown> | undefined;
      const causeCode = cause && typeof cause.code === 'string' ? cause.code : null;
      const causeMessage = cause && typeof cause.message === 'string' ? cause.message : null;

      if (message && causeCode && causeMessage) {
        return `${message} (${causeCode}: ${causeMessage})`;
      }
      if (message && causeMessage) {
        return `${message} (${causeMessage})`;
      }
      if (message) {
        return message;
      }
    }
    return String(error);
  }
}

function parseMaybeJson(text: string): unknown {
  const trimmed = text.trim();
  if (!trimmed) {
    return null;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return trimmed;
  }
}

function extractRows(payload: unknown): Record<string, unknown>[] {
  if (Array.isArray(payload)) {
    return payload.filter((item) => typeof item === 'object') as Record<string, unknown>[];
  }
  if (!payload || typeof payload !== 'object') {
    return [];
  }
  const asRecord = payload as Record<string, unknown>;
  if (Array.isArray(asRecord.data)) {
    return asRecord.data.filter((item) => typeof item === 'object') as Record<string, unknown>[];
  }
  if (asRecord.data && typeof asRecord.data === 'object') {
    const nested = asRecord.data as Record<string, unknown>;
    if (Array.isArray(nested.data)) {
      return nested.data.filter((item) => typeof item === 'object') as Record<string, unknown>[];
    }
  }
  if (Array.isArray(asRecord.rows)) {
    return asRecord.rows.filter((item) => typeof item === 'object') as Record<string, unknown>[];
  }
  return [];
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

function asBoolean(value: unknown, fallback: boolean) {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value > 0;
  }
  const normalized = `${value}`.trim().toLowerCase();
  if (['1', 'true', 'yes', 'si', 'activo', 'active'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'inactivo', 'inactive'].includes(normalized)) {
    return false;
  }
  return fallback;
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
  return value
    .map((item) => asString(item))
    .filter((item) => item !== '');
}

function asJsonArray(value: unknown) {
  if (!Array.isArray(value)) {
    return null;
  }
  return value;
}

function buildStudyPlanDisplayName(career: string | null, year: string | null) {
  const parts = [career, year].filter((item) => item && item.trim() !== '');
  return parts.length > 0 ? parts.join(' - ') : null;
}

function normalizeLookupKey(value: string | null | undefined) {
  return `${value ?? ''}`
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();
}

function resolveProgramIdFromCatalog(
  programsByName: Map<string, AcademicProgramEntity[]>,
  programName: string | null,
  facultyId: string | null,
  facultyName: string | null,
) {
  const normalizedProgramName = normalizeLookupKey(programName);
  if (!normalizedProgramName) {
    return null;
  }

  const matches = programsByName.get(normalizedProgramName) ?? [];
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

  const normalizedFacultyName = normalizeLookupKey(facultyName);
  if (normalizedFacultyName) {
    const byFacultyName = matches.find(
      (item) => normalizeLookupKey(item.faculty) === normalizedFacultyName,
    );
    if (byFacultyName) {
      return byFacultyName.id;
    }
  }

  return matches[0].id;
}

function asDateOnly(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const raw = `${value}`.trim();
  if (!raw) return null;

  // Handle DD/MM/YYYY format (from external APIs)
  const ddmmyyyy = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (ddmmyyyy) {
    const day = ddmmyyyy[1].padStart(2, '0');
    const month = ddmmyyyy[2].padStart(2, '0');
    const year = ddmmyyyy[3];
    return `${year}-${month}-${day}`;
  }

  // Handle YYYY-MM-DD (already correct format)
  const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) {
    return `${iso[1]}-${iso[2]}-${iso[3]}`;
  }

  // Fallback: try native parsing
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString().slice(0, 10);
}

type DateTimeParts = {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
};

function parseDateTimeParts(value: string | null) {
  if (!value) {
    return null;
  }
  const raw = value.trim();
  if (!raw) {
    return null;
  }

  const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?/);
  if (iso) {
    return {
      year: Number(iso[1]),
      month: Number(iso[2]),
      day: Number(iso[3]),
      hour: Number(iso[4] ?? '0'),
      minute: Number(iso[5] ?? '0'),
      second: Number(iso[6] ?? '0'),
    };
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return {
    year: parsed.getUTCFullYear(),
    month: parsed.getUTCMonth() + 1,
    day: parsed.getUTCDate(),
    hour: parsed.getUTCHours(),
    minute: parsed.getUTCMinutes(),
    second: parsed.getUTCSeconds(),
  };
}

function toTimeValue(parts: DateTimeParts) {
  const hour = `${parts.hour}`.padStart(2, '0');
  const minute = `${parts.minute}`.padStart(2, '0');
  const second = `${parts.second}`.padStart(2, '0');
  return `${hour}:${minute}:${second}`;
}

function mapDayOfWeek(parts: DateTimeParts): (typeof DayOfWeekValues)[number] | null {
  const dayIndex = new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay();
  const dayMap: Record<number, (typeof DayOfWeekValues)[number]> = {
    0: 'DOMINGO',
    1: 'LUNES',
    2: 'MARTES',
    3: 'MIERCOLES',
    4: 'JUEVES',
    5: 'VIERNES',
    6: 'SABADO',
  };
  return dayMap[dayIndex] ?? null;
}

function normalizeScheduleBoundary(value: string | null | undefined, fallbackTime: string) {
  if (!value) {
    return null;
  }
  const raw = value.trim();
  if (!raw) {
    return null;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return `${raw}T${fallbackTime}`;
  }

  const parsed = parseDateTimeParts(raw);
  if (!parsed) {
    return null;
  }

  const year = `${parsed.year}`.padStart(4, '0');
  const month = `${parsed.month}`.padStart(2, '0');
  const day = `${parsed.day}`.padStart(2, '0');
  return `${year}-${month}-${day}T${toTimeValue(parsed)}`;
}

function addDays(dateInput: Date | DateTimeParts, days: number) {
  const baseDate =
    dateInput instanceof Date
      ? new Date(dateInput.getTime())
      : new Date(Date.UTC(dateInput.year, dateInput.month - 1, dateInput.day));
  baseDate.setUTCDate(baseDate.getUTCDate() + days);
  return baseDate;
}

function startOfWeekMonday(value: Date) {
  const date = new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()));
  const day = date.getUTCDay();
  const offset = day === 0 ? -6 : 1 - day;
  date.setUTCDate(date.getUTCDate() + offset);
  return date;
}

function formatDateTimeForSource(value: Date, time: string) {
  const year = `${value.getUTCFullYear()}`.padStart(4, '0');
  const month = `${value.getUTCMonth() + 1}`.padStart(2, '0');
  const day = `${value.getUTCDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}T${time}`;
}

function normalizeCourseCode(value: string | null | undefined) {
  return `${value ?? ''}`.trim().toUpperCase();
}

function normalizeSectionName(value: string | null | undefined) {
  return `${value ?? ''}`
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s*-\s*/g, '-')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();
}

function parseScheduleTitle(title: string | null) {
  if (!title) {
    return { course_code: null, section_name: null };
  }

  const sectionMatch = title.match(/\(([^()]+)\)\s*$/);
  const sectionName = sectionMatch ? sectionMatch[1].trim() : null;

  const explicitCodeMatch = title.match(/^([^-]+)-([^-]+)-([^-]+)-/);
  const fullCourseCode = explicitCodeMatch
    ? `${explicitCodeMatch[1]}-${explicitCodeMatch[2]}-${explicitCodeMatch[3]}`.trim()
    : null;
  const fallbackCodeMatch = title.match(/\b[A-Z]{1,6}\d{2,8}[A-Z0-9]*\b/);
  const courseCode = fullCourseCode ?? fallbackCodeMatch?.[0]?.trim() ?? null;

  return {
    course_code: courseCode,
    section_name: sectionName,
  };
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

function uniqueIds(ids: Array<string | null | undefined>) {
  return [...new Set(ids.filter((item): item is string => Boolean(item)))];
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

function stableId(partA: string, partB: string) {
  const hash = createHash('sha1').update(`${partA}::${partB}`).digest('hex');
  return hash.slice(0, 36);
}

function getSetCookieValues(headers: Headers) {
  const values = (headers as Headers & { getSetCookie?: () => string[] }).getSetCookie?.();
  if (Array.isArray(values) && values.length > 0) {
    return values;
  }

  const single = headers.get('set-cookie');
  return single ? [single] : [];
}

function buildCookieHeaderFromSetCookies(cookies: string[]) {
  const pieces = cookies
    .map((item) => item.split(';')[0]?.trim() ?? '')
    .filter((item) => item.includes('='));
  return mergeCookieHeaders(...pieces);
}

function mergeCookieHeaders(...headers: Array<string | null | undefined>) {
  const map = new Map<string, string>();
  for (const header of headers) {
    const normalized = `${header ?? ''}`.trim();
    if (!normalized) {
      continue;
    }

    for (const part of normalized.split(';')) {
      const piece = part.trim();
      if (!piece || !piece.includes('=')) {
        continue;
      }
      const separatorIndex = piece.indexOf('=');
      const name = piece.slice(0, separatorIndex).trim();
      const value = piece.slice(separatorIndex + 1).trim();
      if (!name) {
        continue;
      }
      map.set(name, value);
    }
  }

  return [...map.entries()].map(([name, value]) => `${name}=${value}`).join('; ');
}

function getCookieValue(cookieHeader: string, cookieName: string) {
  for (const part of cookieHeader.split(';')) {
    const piece = part.trim();
    if (!piece || !piece.includes('=')) {
      continue;
    }
    const separatorIndex = piece.indexOf('=');
    const name = piece.slice(0, separatorIndex).trim();
    const value = piece.slice(separatorIndex + 1).trim();
    if (name === cookieName) {
      return value;
    }
  }
  return null;
}

function extractHiddenInputFields(html: string) {
  const fields: Record<string, string> = {};
  const inputRegex = /<input\b[^>]*>/gi;
  const matches = html.match(inputRegex) ?? [];

  for (const tag of matches) {
    const attributes = parseHtmlAttributes(tag);
    const type = `${attributes.type ?? ''}`.trim().toLowerCase();
    const name = `${attributes.name ?? ''}`.trim();
    if (type !== 'hidden' || !name) {
      continue;
    }
    fields[name] = decodeHtmlEntityValue(attributes.value ?? '');
  }

  return fields;
}

function parseHtmlAttributes(tag: string) {
  const attributes: Record<string, string> = {};
  const attributeRegex = /([^\s=/>]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/g;
  let match: RegExpExecArray | null = attributeRegex.exec(tag);
  while (match) {
    const name = match[1];
    const value = match[2] ?? match[3] ?? match[4] ?? '';
    attributes[name] = value;
    match = attributeRegex.exec(tag);
  }
  return attributes;
}

function decodeHtmlEntityValue(value: string) {
  return value
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>');
}

function isRedirectStatus(status: number) {
  return status === 301 || status === 302 || status === 303 || status === 307 || status === 308;
}

function extractEarliestExpiry(cookies: string[]) {
  if (!cookies.length) {
    return null;
  }

  const candidates: Date[] = [];
  for (const item of cookies) {
    const maxAgeMatch = item.match(/max-age=(\d+)/i);
    if (maxAgeMatch) {
      const seconds = Number(maxAgeMatch[1]);
      if (Number.isFinite(seconds)) {
        candidates.push(new Date(Date.now() + seconds * 1000));
      }
    }

    const expiresMatch = item.match(/expires=([^;]+)/i);
    if (expiresMatch) {
      const parsed = new Date(expiresMatch[1]);
      if (!Number.isNaN(parsed.getTime())) {
        candidates.push(parsed);
      }
    }
  }

  if (!candidates.length) {
    return null;
  }
  candidates.sort((a, b) => a.getTime() - b.getTime());
  return candidates[0];
}
