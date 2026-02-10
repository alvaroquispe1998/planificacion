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
  ClassroomEntity,
  ClassroomTypeEntity,
  CourseEntity,
  CourseSectionEntity,
  ExternalSessionEntity,
  ExternalSessionStatusValues,
  ExternalSourceEntity,
  FacultyEntity,
  SectionEntity,
  SemesterEntity,
  StudyPlanEntity,
  SyncJobEntity,
  SyncJobModeValues,
  SyncJobStatusValues,
  SyncLogEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
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
  | 'academic_program_campuses'
  | 'course_sections';

type SourceProbeResult = {
  ok: boolean;
  source: ExternalSourceEntity;
  session: ExternalSessionEntity | null;
  reason?: string;
  cookie?: string;
  status_code?: number;
  final_url?: string;
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
      validate_path: '/web/conference/aulas/listar?start=0&length=1',
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
      source: 'INTRANET',
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
      source: 'MATRICULA',
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
  ];

const RESOURCE_CODES = RESOURCE_DEFINITIONS.map((item) => item.code);
const RESOURCE_BY_CODE = new Map(RESOURCE_DEFINITIONS.map((item) => [item.code, item]));

@Injectable()
export class SettingsSyncService {
  private readonly allowInsecureTls: boolean;

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
    @InjectRepository(TeacherEntity)
    private readonly teachersRepo: Repository<TeacherEntity>,
    @InjectRepository(BuildingEntity)
    private readonly buildingsRepo: Repository<BuildingEntity>,
    @InjectRepository(ClassroomEntity)
    private readonly classroomsRepo: Repository<ClassroomEntity>,
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

    const source = await this.sourcesRepo.findOne({ where: { code: normalized } });
    if (!source) {
      return { source_code: normalized, ok: false, reason: 'Fuente no encontrada en BD.' };
    }

    const session = await this.sessionsRepo.findOne({ where: { source_id: source.id } });
    if (!session) {
      return { source_code: normalized, ok: false, reason: 'No hay cookie guardada.' };
    }

    const cookie = this.decryptCookieJar(session.cookie_jar_encrypted);
    const url = new URL(def.validate_path, def.base_url);
    if (!url.searchParams.has('length')) {
      url.searchParams.set('length', '1');
    }
    const fetchUrl = url.toString();

    console.log(`[VALIDATE ${normalized}] URL: ${fetchUrl}`);
    console.log(`[VALIDATE ${normalized}] Cookie (first 80): ${cookie.substring(0, 80)}...`);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);

    try {
      const res = await fetch(fetchUrl, {
        method: 'GET',
        headers: {
          accept: 'application/json, text/plain, */*',
          cookie,
          'x-requested-with': 'XMLHttpRequest',
        },
        redirect: 'follow',
        signal: controller.signal,
      });

      const text = await res.text();
      const contentType = res.headers.get('content-type') ?? '';

      console.log(`[VALIDATE ${normalized}] Status: ${res.status} | Content-Type: ${contentType}`);
      console.log(`[VALIDATE ${normalized}] Redirected: ${res.redirected} | Final URL: ${res.url}`);
      console.log(`[VALIDATE ${normalized}] Body preview: ${text.substring(0, 200)}`);

      // If redirected to login or 401/403 → invalid
      if (res.status === 401 || res.status === 403) {
        return { source_code: normalized, ok: false, reason: `No autorizado (${res.status}).` };
      }
      if (res.redirected && /login|signin/i.test(res.url)) {
        return { source_code: normalized, ok: false, reason: 'Redirigido a login. Cookie expirada.' };
      }

      // Try parse JSON
      try {
        const json = JSON.parse(text);
        const hasData = json && (Array.isArray(json.data) || json.data !== undefined || json.recordsTotal !== undefined);
        if (hasData) {
          await this.sessionsRepo.update({ id: session.id }, {
            status: 'ACTIVE',
            last_validated_at: new Date(),
            error_last: null,
            updated_at: new Date(),
          });
          return { source_code: normalized, ok: true, reason: null };
        }
        return { source_code: normalized, ok: false, reason: `JSON sin campo "data". Keys: ${Object.keys(json).join(', ')}` };
      } catch {
        // Not JSON - maybe HTML login page
        const bodyPreview = text.substring(0, 120).replace(/\s+/g, ' ').trim();
        return {
          source_code: normalized,
          ok: false,
          reason: `HTTP ${res.status} - No es JSON (${contentType}). Preview: ${bodyPreview}`,
        };
      }
    } catch (err) {
      const msg = err instanceof Error && err.name === 'AbortError'
        ? 'Timeout (5s).' : (err instanceof Error ? err.message : 'Error de conexión.');
      console.log(`[VALIDATE ${normalized}] Error: ${msg}`);
      return { source_code: normalized, ok: false, reason: msg };
    } finally {
      clearTimeout(timer);
    }
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
    const sourceProbeCache = new Map<SourceCode, SourceProbeResult>();
    const results: Array<Record<string, unknown>> = [];

    for (const resource of resources) {
      const resourceDefinition = RESOURCE_BY_CODE.get(resource);
      if (!resourceDefinition) {
        throw new BadRequestException(`Recurso no configurado: ${resource}`);
      }
      const sourceCode = resourceDefinition.source;
      const sourceResult = sourceProbeCache.has(sourceCode)
        ? (sourceProbeCache.get(sourceCode) as SourceProbeResult)
        : await this.probeSourceSession(sourceCode);
      sourceProbeCache.set(sourceCode, sourceResult);

      const job = await this.createJob(sourceResult.source.id, resource, dto.mode ?? 'FULL', {
        mode: dto.mode ?? 'FULL',
        resources: dto.resources ?? null,
        campus_ids: dto.campus_ids ?? null,
        course_ids: dto.course_ids ?? null,
        semester_id: dto.semester_id ?? null,
      });

      if (!sourceResult.ok || !sourceResult.cookie) {
        await this.failJob(job, sourceResult.reason ?? 'Sesion no valida.');
        results.push({
          resource,
          source: sourceCode,
          status: 'FAILED',
          reason: sourceResult.reason ?? 'Sesion no valida.',
          job_id: job.id,
        });
        continue;
      }

      try {
        await this.updateJobStatus(job, 'RUNNING');
        await this.log(job.id, 'INFO', `Iniciando sync de ${resource}`, {
          source: sourceCode,
          mode: dto.mode ?? 'FULL',
        });

        const rows = await this.fetchResourceRows(resource, sourceResult.source, sourceResult.cookie, dto);
        const persisted = await this.persistResourceRows(resource, rows);
        await this.log(job.id, 'INFO', `Sync de ${resource} completado`, persisted);
        await this.completeJob(job);

        results.push({
          resource,
          source: sourceCode,
          status: 'DONE',
          ...persisted,
          job_id: job.id,
        });
      } catch (error) {
        const message = this.toErrorMessage(error);
        await this.failJob(job, message);
        await this.log(job.id, 'ERROR', `Error en sync de ${resource}`, { error: message });
        results.push({
          resource,
          source: sourceCode,
          status: 'FAILED',
          reason: message,
          job_id: job.id,
        });
      }
    }

    return {
      started_at: new Date().toISOString(),
      total_resources: resources.length,
      completed: results.filter((item) => item.status === 'DONE').length,
      failed: results.filter((item) => item.status === 'FAILED').length,
      results,
    };
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
      return this.fetchRows(source, cookie, '/admin/carreras/get', { length: '500' });
    }
    if (resource === 'sections') {
      return this.fetchRows(source, cookie, '/admin/codigos-de-seccion/get', { length: '500' });
    }
    if (resource === 'courses') {
      return this.fetchRows(source, cookie, '/admin/cursos/get', { length: '500' });
    }
    if (resource === 'classroom_types') {
      return this.fetchRows(source, cookie, '/admin/aulas/categorias/get', { length: '500' });
    }
    if (resource === 'zoom_users') {
      return this.fetchRows(source, cookie, '/web/conference/aulas/listar', { length: '500' });
    }
    if (resource === 'faculties') {
      return this.fetchRows(source, cookie, '/admin/facultades/get', { length: '500' });
    }
    if (resource === 'study_plans') {
      return this.fetchRows(source, cookie, '/admin/planes-de-estudios/get', { length: '1000' });
    }
    if (resource === 'teachers') {
      return this.fetchRows(source, cookie, '/admin/docentes/get', { length: '1000' });
    }
    if (resource === 'classrooms') {
      return this.fetchRows(source, cookie, '/admin/aulas/get', { length: '1000' });
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
      return this.upsertById(
        this.studyPlansRepo,
        rows.map((row) => ({
          id: asString(pick(row, 'id')),
          name: asNullableString(pick(row, 'name')),
          curriculum: asNullableString(pick(row, 'curriculum')),
          curriculum_code: asNullableString(pick(row, 'curriculum_code', 'curriculumCode', 'code')),
          faculty_id: asNullableString(pick(row, 'faculty_id', 'facultyId')),
          academic_program_id: asNullableString(
            pick(row, 'academic_program_id', 'academicProgramId', 'careerId'),
          ),
          is_active: asBoolean(pick(row, 'is_active', 'isActive', 'status'), true),
        })),
      );
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

    return { received: 0, processed: 0, created: 0, updated: 0, skipped: 0, deduplicated: 0 };
  }

  private async fetchRows(
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
    return extractRows(response.payload);
  }

  private async probeSourceSession(code: string): Promise<SourceProbeResult> {
    const source = await this.findSourceOrFail(code);
    const session = await this.sessionsRepo.findOne({ where: { source_id: source.id } });
    if (!session) {
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
      await this.markSessionStatus(source.id, 'EXPIRED', responseError ?? 'Sesion vencida');
      return {
        ok: false,
        source,
        session,
        reason: responseError ?? 'Sesion vencida',
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

function stableId(partA: string, partB: string) {
  const hash = createHash('sha1').update(`${partA}::${partB}`).digest('hex');
  return hash.slice(0, 36);
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
