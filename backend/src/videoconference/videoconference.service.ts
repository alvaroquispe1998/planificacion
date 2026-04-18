import { BadRequestException, Injectable, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository, SelectQueryBuilder } from 'typeorm';
import { newId } from '../common';
import { ZoomUserEntity } from '../entities/audit.entities';
import {
    AcademicProgramEntity,
    CampusEntity,
    ExternalSourceEntity,
    FacultyEntity,
    SemesterEntity,
    TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
    CourseModalityEntity,
    PlanningOfferEntity,
    PlanningSectionEntity,
    PlanningSubsectionEntity,
    PlanningSubsectionScheduleEntity,
} from '../entities/planning.entities';
import { SettingsSyncService } from '../settings/settings-sync.service';
import {
    AssignmentPreviewVideoconferenceDto,
    CreateHostRuleDto,
    CreateZoomGroupDto,
    FilterOptionsDto,
    GenerateVideoconferenceDto,
    PreviewVideoconferenceDto,
    UpdateHostRuleDto,
    UpdateZoomGroupDto,
    UpsertVideoconferenceOverrideDto,
    UpdateZoomPoolDto,
} from './videoconference.dto';
import {
    PlanningSubsectionVideoconferenceEntity,
    PlanningSubsectionVideoconferenceOverrideEntity,
    PlanningSubsectionScheduleVcInheritanceEntity,
    PlanningSubsectionVideoconferenceLinkModeValues,
    VcAcademicProgramEntity,
    VcCourseEntity,
    VcFacultyEntity,
    VcSectionEntity,
    VcScheduleHostRuleEntity,
    VideoconferenceZoomPoolUserEntity,
    ZoomGroupEntity,
    ZoomGroupUserEntity,
} from './videoconference.entity';
import {
    ZoomAccountService,
    ZoomAccountUserSummary,
    ZoomMeetingSummary,
} from './zoom-account.service';

const ACTIVE_CONFERENCE_STATUSES = ['CREATING', 'CREATED_UNMATCHED', 'MATCHED'] as const;
const ZOOM_MATCH_ATTEMPTS = 5;
const ZOOM_MATCH_DELAY_MS = 3000;
// Minimum delay between successive Aula Virtual POST requests during bulk generation.
// Prevents overwhelming the remote server and avoids session-level throttling.
const AULA_VIRTUAL_THROTTLE_MS = 500;
// Max retries for transient Aula Virtual errors (5xx / gateway) during bulk generation.
const AULA_VIRTUAL_MAX_RETRIES = 3;
const AULA_VIRTUAL_RETRY_BASE_DELAY_MS = 5_000;
const MEETING_MARGIN_MINUTES = 10;
const DEFAULT_REMOTE_MEETING_DURATION_MINUTES = 60;
// For live Zoom meetings the API returns the *scheduled* duration, which may be
// shorter than how long the meeting actually runs. Use a 12-hour ceiling so that
// a same-day live meeting (e.g. started at 15:10) is still detected as a conflict
// even if its scheduled duration ends before the window being evaluated.
const LIVE_MEETING_FALLBACK_DURATION_MINUTES = 720;
const AULA_VIRTUAL_SOURCE_CODE = 'AULAVIRTUAL';
const DAY_TO_AULA_VIRTUAL_NUMBER: Record<string, number> = {
    LUNES: 2,
    MARTES: 3,
    MIERCOLES: 4,
    JUEVES: 5,
    VIERNES: 6,
    SABADO: 7,
    DOMINGO: 1,
};
const DAY_TO_JS_NUMBER: Record<string, number> = {
    LUNES: 1,
    MARTES: 2,
    MIERCOLES: 3,
    JUEVES: 4,
    VIERNES: 5,
    SABADO: 6,
    DOMINGO: 0,
};
const LEGACY_AUTO_INHERITANCE_NOTE = 'Agrupamiento automatico sucesivo por docente (Back-end).';
const DEFAULT_ZOOM_GROUP_REGULAR_CODE = 'REGULAR';
const DEFAULT_ZOOM_GROUP_HIBRIDO_CODE = 'HIBRIDO';

type ScheduleContextRow = {
    offer_id: string;
    cycle: number | null;
    section_id: string;
    section_code: string;
    section_external_code: string | null;
    section_is_cepea: boolean;
    section_projected_vacancies: number | null;
    subsection_id: string;
    subsection_code: string;
    subsection_projected_vacancies: number | null;
    schedule_id: string;
    semester_id: string | null;
    campus_id: string | null;
    campus_name: string | null;
    faculty_id: string | null;
    faculty_name: string | null;
    program_id: string | null;
    program_name: string | null;
    course_id: string;
    course_code: string | null;
    course_name: string | null;
    vc_period_id: string | null;
    vc_faculty_id: string | null;
    vc_academic_program_id: string | null;
    vc_course_id: string | null;
    vc_section_id: string | null;
    vc_section_name: string | null;
    vc_faculty_name: string | null;
    vc_academic_program_name: string | null;
    vc_course_name: string | null;
    vc_source: string | null;
    vc_context_message: string | null;
    offer_source_payload_json: Record<string, unknown> | null;
    section_source_payload_json: Record<string, unknown> | null;
    responsible_teacher_id: string | null;
    responsible_teacher_name: string | null;
    responsible_teacher_full_name: string | null;
    responsible_teacher_dni: string | null;
    section_teacher_id: string | null;
    section_teacher_name: string | null;
    section_teacher_full_name: string | null;
    section_teacher_dni: string | null;
    subsection_modality_code: string | null;
    subsection_modality_name: string | null;
    section_modality_code: string | null;
    section_modality_name: string | null;
    day_of_week: string;
    start_time: string;
    end_time: string;
    duration_minutes: number;
};

type ResolvedTeacher = {
    id: string | null;
    name: string | null;
    dni: string | null;
};

type ResolvedModality = {
    code: string | null;
    name: string | null;
};

type FilterOptionKey = keyof Pick<
    FilterOptionsDto,
    'semesterId' | 'campusIds' | 'facultyIds' | 'programIds' | 'courseIds' | 'modality' | 'modalities' | 'days'
>;

type FilterCatalogOption = {
    id: string;
    label: string;
};

type FilterOptionRow = {
    semester_id: string | null;
    semester_name: string | null;
    cycle: number | null;
    campus_id: string | null;
    campus_name: string | null;
    faculty_id: string | null;
    faculty_name: string | null;
    program_id: string | null;
    program_name: string | null;
    course_id: string | null;
    course_code: string | null;
    course_name: string | null;
    vc_section_name: string | null;
    section_external_code: string | null;
    section_is_cepea: boolean;
    subsection_modality_code: string | null;
    subsection_modality_name: string | null;
    section_modality_code: string | null;
    section_modality_name: string | null;
    day_of_week: string | null;
};

type ZoomPoolUser = {
    pool_id: string;
    zoom_user_id: string;
    sort_order: number;
    is_active: boolean;
    name: string | null;
    email: string | null;
};

type ZoomPoolLicenseSnapshot = {
    ok: boolean;
    error: string | null;
    byEmail: Map<string, ZoomAccountUserSummary>;
    byId: Map<string, ZoomAccountUserSummary>;
};

type PreviewZoomPoolUser = ZoomPoolUser & {
    license_status: 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN';
    license_label: string;
    is_licensed: boolean | null;
};

type ExpandedOccurrence = {
    occurrence_key: string;
    row: ScheduleContextRow;
    base_conference_date: string;
    effective_conference_date: string;
    effective_start_time: string;
    effective_end_time: string;
    scheduled_start: Date;
    scheduled_end: Date;
    occurrence_type: 'BASE' | 'RESCHEDULED' | 'SKIPPED';
    override_id: string | null;
    override_reason_code: string | null;
    override_notes: string | null;
    inheritance: {
        is_inherited: boolean;
        mapping_id: string | null;
        parent_schedule_id: string | null;
        parent_occurrence_key: string | null;
        parent_label: string | null;
        family_owner_schedule_id: string;
    };
};

type AulaVirtualRequestContext = {
    baseUrl: string;
    cookie: string;
};

type AulaVirtualPayload = {
    courseCode: string;
    courseName: string;
    section: string;
    dni: string;
    teacher: string;
    day: string;
    startTime: string;
    endTime: string;
    termId: string;
    facultyId: string;
    careerId: string;
    courseId: string;
    name: string;
    sectionId: string;
    start: string;
    end: string;
    minutes: string;
    'daysOfWeek[0]': string;
    credentialId: string;
};

type GenerateResultItem = {
    schedule_id: string;
    occurrence_key: string | null;
    conference_date: string | null;
    status:
    | 'MATCHED'
    | 'CREATED_UNMATCHED'
    | 'BLOCKED_EXISTING'
    | 'NO_AVAILABLE_ZOOM_USER'
    | 'VALIDATION_ERROR'
    | 'ERROR';
    message: string;
    record_id: string | null;
    zoom_user_id: string | null;
    zoom_user_email: string | null;
    zoom_meeting_id: string | null;
    link_mode: (typeof PlanningSubsectionVideoconferenceLinkModeValues)[number];
    owner_videoconference_id: string | null;
    inheritance: ExpandedOccurrence['inheritance'];
};

type ReconcileResult = {
    success: boolean;
    matched: boolean;
    message: string;
    result: GenerateResultItem;
};

type ZoomMatchResult = ZoomMeetingSummary & {
    attempts: number;
};

type StoredOccurrenceOverride = {
    id: string;
    schedule_id: string;
    conference_date: string;
    action: 'KEEP' | 'SKIP' | 'RESCHEDULE';
    override_date: string | null;
    override_start_time: string | null;
    override_end_time: string | null;
    reason_code: string | null;
    notes: string | null;
};

type ScheduleInheritanceMapping = {
    id: string | null;
    parent_schedule_id: string;
    child_schedule_id: string;
    notes: string | null;
    is_active: boolean;
    created_at: Date;
    updated_at: Date;
};

type ScheduleInheritanceInfo = {
    mapping: ScheduleInheritanceMapping | null;
    parent_schedule_id: string | null;
    family_owner_schedule_id: string;
    is_inherited: boolean;
    parent_label: string | null;
};

type InheritanceIndex = {
    byChild: Map<string, ScheduleInheritanceMapping>;
    childrenByParent: Map<string, ScheduleInheritanceMapping[]>;
};

type ContinuousBlockInfo = {
    owner_schedule_id: string;
    family_schedule_ids: string[];
    grouped_subsection_codes: string[];
    grouped_subsection_labels: string[];
};

type InheritanceCandidateBlock = {
    owner_schedule_id: string;
    schedule_ids: string[];
    semester_id: string | null;
    faculty_id: string | null;
    cycle: number | null;
    day_of_week: string;
    start_time: string;
    end_time: string;
    teacher_key: string;
    teacher_name: string | null;
    faculty_name: string | null;
    campus_id: string | null;
    campus_name: string | null;
    program_id: string | null;
    program_name: string | null;
    course_id: string;
    course_label: string;
    vc_section_name: string | null;
    section_id: string;
    section_label: string;
    section_projected_vacancies: number | null;
    subsection_label: string;
};

type InheritanceBlockPair = {
    parent_schedule_id: string;
    child_schedule_id: string;
};

type AssignmentPreviewMode = 'BASE' | 'OCCURRENCE';

type AssignmentPreviewStatus =
    | 'ASSIGNED_LICENSED'
    | 'ASSIGNED_RISK'
    | 'INHERITED'
    | 'BLOCKED_EXISTING'
    | 'NO_AVAILABLE_ZOOM_USER'
    | 'VALIDATION_ERROR';

type AssignmentPreviewItem = {
    id: string;
    mode: AssignmentPreviewMode;
    occurrence_key: string | null;
    schedule_id: string;
    conference_date: string | null;
    day_of_week: string;
    day_label: string;
    start_time: string;
    end_time: string;
    preview_status: AssignmentPreviewStatus;
    message: string;
    zoom_user_id: string | null;
    zoom_user_email: string | null;
    zoom_user_name: string | null;
    license_status: 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN' | null;
    license_label: string | null;
    is_licensed: boolean | null;
    depends_on_unverified_license: boolean;
    consumes_capacity: boolean;
    inheritance: ExpandedOccurrence['inheritance'];
    owner_occurrence_key: string | null;
};

type AssignmentPreviewDaySummary = {
    day_of_week: string;
    day_label: string;
    required_hosts: number;
};

type AssignmentPreviewSummary = {
    requested_rows: number;
    assigned_rows: number;
    hosts_used: number;
    verified_hosts_used: number;
    risk_hosts_used: number;
    no_available_zoom_user: number;
    validation_errors: number;
    blocked_existing: number;
    virtual_hosts_needed: number;
    licenses_required_global: number;
    additional_licenses_needed: number | null;
    licenses_by_day: AssignmentPreviewDaySummary[];
};

type PreferredHostSelection = {
    schedule_id: string;
    conference_date: string | null;
    zoom_user_id: string;
};

type ExistingConferenceLookup = Map<string, PlanningSubsectionVideoconferenceEntity>;

type SimulatedReservation = {
    scheduled_start: Date;
    scheduled_end: Date;
};

@Injectable()
export class VideoconferenceService implements OnModuleInit {
    private zoomGroupBootstrapPromise: Promise<void> | null = null;

    constructor(
        @InjectRepository(PlanningOfferEntity)
        private readonly offersRepo: Repository<PlanningOfferEntity>,
        @InjectRepository(VcFacultyEntity)
        private readonly vcFacultiesRepo: Repository<VcFacultyEntity>,
        @InjectRepository(VcAcademicProgramEntity)
        private readonly vcAcademicProgramsRepo: Repository<VcAcademicProgramEntity>,
        @InjectRepository(VcCourseEntity)
        private readonly vcCoursesRepo: Repository<VcCourseEntity>,
        @InjectRepository(VcSectionEntity)
        private readonly vcSectionsRepo: Repository<VcSectionEntity>,
        @InjectRepository(PlanningSubsectionScheduleEntity)
        private readonly schedulesRepo: Repository<PlanningSubsectionScheduleEntity>,
        @InjectRepository(VideoconferenceZoomPoolUserEntity)
        private readonly zoomPoolRepo: Repository<VideoconferenceZoomPoolUserEntity>,
        @InjectRepository(ZoomGroupEntity)
        private readonly zoomGroupsRepo: Repository<ZoomGroupEntity>,
        @InjectRepository(ZoomGroupUserEntity)
        private readonly zoomGroupUsersRepo: Repository<ZoomGroupUserEntity>,
        @InjectRepository(VcScheduleHostRuleEntity)
        private readonly hostRulesRepo: Repository<VcScheduleHostRuleEntity>,
        @InjectRepository(PlanningSubsectionScheduleVcInheritanceEntity)
        private readonly inheritanceRepo: Repository<PlanningSubsectionScheduleVcInheritanceEntity>,
        @InjectRepository(PlanningSubsectionVideoconferenceEntity)
        private readonly planningVideoconferencesRepo: Repository<PlanningSubsectionVideoconferenceEntity>,
        @InjectRepository(PlanningSubsectionVideoconferenceOverrideEntity)
        private readonly planningVideoconferenceOverridesRepo: Repository<PlanningSubsectionVideoconferenceOverrideEntity>,
        @InjectRepository(ZoomUserEntity)
        private readonly zoomUsersRepo: Repository<ZoomUserEntity>,
        @InjectRepository(ExternalSourceEntity)
        private readonly sourcesRepo: Repository<ExternalSourceEntity>,
        private readonly settingsSyncService: SettingsSyncService,
        private readonly zoomAccountService: ZoomAccountService,
    ) { }

    async onModuleInit() {
        try {
            await this.ensureZoomGroupsBootstrap();
        } catch (err) {
            console.error('[VideoconferenceService] onModuleInit bootstrap failed (non-fatal):', err);
        }
    }

    async getCampuses() {
        const rows = await this.offersRepo
            .createQueryBuilder('offer')
            .innerJoin(CampusEntity, 'campus', 'campus.id = offer.campus_id')
            .select('campus.id', 'id')
            .addSelect('campus.code', 'code')
            .addSelect('campus.name', 'name')
            .where('offer.campus_id IS NOT NULL')
            .distinct(true)
            .orderBy('campus.name', 'ASC')
            .getRawMany<Record<string, unknown>>();

        return rows.map((row) => ({
            id: readString(row.id),
            code: readNullableString(row.code),
            name: readNullableString(row.name) ?? '(Sin sede)',
        }));
    }

    async getFaculties() {
        const rows = await this.offersRepo
            .createQueryBuilder('offer')
            .innerJoin(FacultyEntity, 'faculty', 'faculty.id = offer.faculty_id')
            .select('faculty.id', 'id')
            .addSelect('faculty.code', 'code')
            .addSelect('faculty.name', 'name')
            .where('offer.faculty_id IS NOT NULL')
            .distinct(true)
            .orderBy('faculty.name', 'ASC')
            .getRawMany<Record<string, unknown>>();

        return rows.map((row) => ({
            id: readString(row.id),
            code: readNullableString(row.code),
            name: readNullableString(row.name) ?? '(Sin facultad)',
        }));
    }

    async getPrograms(facultyIds: string[]) {
        const normalized = normalizeIdArray(facultyIds);
        if (!normalized.length) {
            return [];
        }

        const rows = await this.offersRepo
            .createQueryBuilder('offer')
            .innerJoin(AcademicProgramEntity, 'program', 'program.id = offer.academic_program_id')
            .select('program.id', 'id')
            .addSelect('program.code', 'code')
            .addSelect('program.name', 'name')
            .addSelect('program.faculty_id', 'faculty_id')
            .where('offer.academic_program_id IS NOT NULL')
            .andWhere('offer.faculty_id IN (:...facultyIds)', { facultyIds: normalized })
            .distinct(true)
            .orderBy('program.name', 'ASC')
            .getRawMany<Record<string, unknown>>();

        return rows.map((row) => ({
            id: readString(row.id),
            code: readNullableString(row.code),
            name: readNullableString(row.name) ?? '(Sin programa)',
            faculty_id: readNullableString(row.faculty_id),
        }));
    }

    async getCourses(programIds: string[]) {
        const normalized = normalizeIdArray(programIds);
        if (!normalized.length) {
            return [];
        }

        const rows = await this.offersRepo
            .createQueryBuilder('offer')
            .select('offer.study_plan_course_id', 'id')
            .addSelect('offer.course_code', 'code')
            .addSelect('offer.course_name', 'name')
            .addSelect('offer.academic_program_id', 'program_id')
            .where('offer.academic_program_id IN (:...programIds)', { programIds: normalized })
            .andWhere('offer.study_plan_course_id IS NOT NULL')
            .distinct(true)
            .orderBy('offer.course_name', 'ASC')
            .addOrderBy('offer.course_code', 'ASC')
            .getRawMany<Record<string, unknown>>();

        return rows.map((row) => ({
            id: readString(row.id),
            code: readNullableString(row.code),
            name: readNullableString(row.name) ?? '(Sin curso)',
            program_id: readNullableString(row.program_id),
        }));
    }

    async getFilterOptions(filters: FilterOptionsDto) {
        const periodFilters = this.pickCatalogFilters(filters, []);
        const campusFilters = this.pickCatalogFilters(filters, ['semesterId']);
        const facultyFilters = this.pickCatalogFilters(filters, ['semesterId', 'campusIds']);
        const programFilters = this.pickCatalogFilters(filters, ['semesterId', 'campusIds', 'facultyIds']);
        const courseFilters = this.pickCatalogFilters(filters, ['semesterId', 'campusIds', 'facultyIds', 'programIds']);
        const modalityFilters = this.pickCatalogFilters(filters, [
            'semesterId',
            'campusIds',
            'facultyIds',
            'programIds',
            'courseIds',
        ]);
        const dayFilters = this.pickCatalogFilters(filters, [
            'semesterId',
            'campusIds',
            'facultyIds',
            'programIds',
            'courseIds',
            'modalities',
        ]);

        const [periodRows, campusRows, facultyRows, programRows, courseRows, modalityRows, dayRows] = await Promise.all([
            this.getFilterOptionRows(periodFilters),
            this.getFilterOptionRows(campusFilters),
            this.getFilterOptionRows(facultyFilters),
            this.getFilterOptionRows(programFilters),
            this.getFilterOptionRows(courseFilters),
            this.getFilterOptionRows(modalityFilters),
            this.getFilterOptionRows(dayFilters),
        ]);

        return {
            periods: this.buildPeriodOptions(periodRows),
            campuses: this.buildNamedOptions(campusRows, 'campus_id', 'campus_name', '(Sin sede)'),
            faculties: this.buildNamedOptions(facultyRows, 'faculty_id', 'faculty_name', '(Sin facultad)'),
            programs: this.buildNamedOptions(programRows, 'program_id', 'program_name', '(Sin programa)'),
            courses: this.buildCourseOptions(courseRows),
            modalities: this.buildModalityOptions(modalityRows),
            days: this.buildDayOptions(dayRows),
        };
    }

    async getInheritanceCatalog(filters: {
        semesterId: string;
        campusId?: string;
        facultyId: string;
        programId?: string;
    }) {
        const semesterId = String(filters.semesterId ?? '').trim();
        const campusId = String(filters.campusId ?? '').trim();
        const facultyId = String(filters.facultyId ?? '').trim();
        const programId = String(filters.programId ?? '').trim();
        if (!semesterId || !facultyId) {
            throw new BadRequestException('Periodo y facultad son requeridos.');
        }

        const rows = await this.getScheduleRows({
            semesterId,
            campusIds: campusId ? [campusId] : undefined,
            facultyIds: [facultyId],
            programIds: programId ? [programId] : undefined,
        });
        const inheritances = await this.listInheritedMappings(rows.map((row) => row.schedule_id));
        const inheritanceMap = new Map(inheritances.map((item) => [item.child_schedule_id, item] as const));

        return {
            schedules: rows.map((row) => {
                const inheritance = inheritanceMap.get(row.schedule_id) ?? null;
                return {
                    schedule_id: row.schedule_id,
                    campus_id: row.campus_id,
                    program_id: row.program_id,
                    subsection_id: row.subsection_id,
                    section_id: row.section_id,
                    section_code: row.section_code,
                    course_id: row.course_id,
                    course_code: row.course_code,
                    course_name: row.course_name,
                    course_label: buildCourseLabel(row.course_code, row.course_name),
                    section_label: buildSectionLabel(row),
                    subsection_label: buildGroupLabel(row),
                    day_of_week: row.day_of_week,
                    day_label: displayDay(row.day_of_week),
                    start_time: row.start_time,
                    end_time: row.end_time,
                    schedule_label: buildScheduleLabel(row),
                    vacancy_label: buildVacancyLabel(row),
                    section_projected_vacancies: row.section_projected_vacancies,
                    subsection_projected_vacancies: row.subsection_projected_vacancies,
                    teacher_name: resolveTeacher(row).name,
                    is_child_inherited: Boolean(inheritance?.is_active),
                    inherited_from_schedule_id: inheritance?.parent_schedule_id ?? null,
                };
            }),
        };
    }

    async listVideoconferenceInheritances() {
        const mappings = await this.inheritanceRepo.find({
            order: { updated_at: 'DESC', created_at: 'DESC' },
        });
        return this.serializeInheritanceMappings(mappings);
    }

    async createVideoconferenceInheritance(payload: {
        parentScheduleId: string;
        childScheduleId: string;
        notes?: string;
        isActive?: boolean;
    }) {
        const normalized = await this.validateInheritanceDefinition(
            String(payload.parentScheduleId ?? ''),
            String(payload.childScheduleId ?? ''),
        );
        const blockPairs = await this.resolveInheritanceBlockPairs(normalized.parent, normalized.child);
        const childScheduleIds = Array.from(new Set(blockPairs.map((item) => item.child_schedule_id)));
        const existing = await this.inheritanceRepo.find({
            where: childScheduleIds.map((childScheduleId) => ({ child_schedule_id: childScheduleId, is_active: true })),
        });
        if (existing.length) {
            throw new BadRequestException(
                'Al menos un horario hijo del bloque ya tiene una herencia activa.',
            );
        }

        const now = new Date();
        const entities = blockPairs.map((pair) =>
            this.inheritanceRepo.create({
                id: newId(),
                parent_schedule_id: pair.parent_schedule_id,
                child_schedule_id: pair.child_schedule_id,
                notes: emptyTextToNull(payload.notes),
                is_active: payload.isActive ?? true,
                created_at: now,
                updated_at: now,
            }),
        );
        await this.inheritanceRepo.save(entities);
        return (await this.serializeInheritanceMappings([entities[0]]))[0] ?? null;
    }

    async updateVideoconferenceInheritance(
        id: string,
        payload: {
            parentScheduleId?: string;
            childScheduleId?: string;
            notes?: string;
            isActive?: boolean;
        },
    ) {
        const entity = await this.inheritanceRepo.findOne({ where: { id: String(id ?? '').trim() } });
        if (!entity) {
            throw new BadRequestException('No se encontro el mapeo de herencia.');
        }

        const nextParentId = String(payload.parentScheduleId ?? entity.parent_schedule_id);
        const nextChildId = String(payload.childScheduleId ?? entity.child_schedule_id);
        const normalized = await this.validateInheritanceDefinition(nextParentId, nextChildId, entity.id);

        entity.parent_schedule_id = normalized.parent.schedule_id;
        entity.child_schedule_id = normalized.child.schedule_id;
        if (payload.notes !== undefined) {
            entity.notes = emptyTextToNull(payload.notes);
        }
        if (payload.isActive !== undefined) {
            entity.is_active = Boolean(payload.isActive);
        }
        entity.updated_at = new Date();
        await this.inheritanceRepo.save(entity);
        return (await this.serializeInheritanceMappings([entity]))[0] ?? null;
    }

    async previewInheritanceCandidates(payload: { semesterId: string; facultyId: string }) {
        const semesterId = String(payload.semesterId ?? '').trim();
        const facultyId = String(payload.facultyId ?? '').trim();
        if (!semesterId || !facultyId) {
            throw new BadRequestException('semesterId y facultyId son requeridos.');
        }

        const rows = await this.getScheduleRows({
            semesterId,
            facultyIds: [facultyId],
        });
        const activeMappings = (await this.listInheritedMappings(rows.map((row) => row.schedule_id)))
            .filter((item) => item.is_active);
        const blockedScheduleIds = new Set(
            activeMappings.flatMap((item) => [item.parent_schedule_id, item.child_schedule_id]),
        );
        const blocks = this.buildInheritanceCandidateBlocks(rows, blockedScheduleIds);

        const groups = new Map<string, InheritanceCandidateBlock[]>();
        for (const block of blocks) {
            const key = [
                block.semester_id ?? '',
                block.faculty_id ?? '',
                block.day_of_week,
                compactTime(block.start_time),
                compactTime(block.end_time),
                block.teacher_key,
            ].join('|');
            const current = groups.get(key) ?? [];
            current.push(block);
            groups.set(key, current);
        }

        const items = [];
        for (const groupRows of groups.values()) {
            if (groupRows.length < 2) {
                continue;
            }

            const distinctSections = new Set(groupRows.map((row) => row.section_id));
            if (distinctSections.size < 2) {
                continue;
            }

            const ranked = [...groupRows].sort((left, right) => {
                const leftVacancies = left.section_projected_vacancies ?? 0;
                const rightVacancies = right.section_projected_vacancies ?? 0;
                const comparisons = [
                    rightVacancies - leftVacancies,
                    (left.campus_name ?? '').localeCompare(right.campus_name ?? ''),
                    (left.program_name ?? '').localeCompare(right.program_name ?? ''),
                    left.course_label.localeCompare(right.course_label),
                    left.section_label.localeCompare(right.section_label),
                ];
                return comparisons.find((value) => value !== 0) || 0;
            });

            const parent = ranked[0];
            const children = ranked
                .filter((row) => row.section_id !== parent.section_id)
                .map((child) => ({
                    schedule_id: child.owner_schedule_id,
                    campus_id: child.campus_id,
                    campus_name: child.campus_name,
                    program_id: child.program_id,
                    program_name: child.program_name,
                    course_id: child.course_id,
                    course_label: child.course_label,
                    vc_section_name: child.vc_section_name,
                    section_id: child.section_id,
                    section_label: child.section_label,
                    section_projected_vacancies: child.section_projected_vacancies,
                    subsection_label: child.subsection_label,
                    schedule_label: `${displayDay(child.day_of_week)} ${compactTime(child.start_time)}-${compactTime(child.end_time)}`,
                }));

            if (!children.length) {
                continue;
            }

            items.push({
                id: `${parent.owner_schedule_id}::${children.map((item) => item.schedule_id).join('|')}`,
                teacher_name: parent.teacher_name,
                cycle: parent.cycle,
                day_of_week: parent.day_of_week,
                day_label: displayDay(parent.day_of_week),
                start_time: compactTime(parent.start_time),
                end_time: compactTime(parent.end_time),
                faculty_name: parent.faculty_name,
                parent: {
                    schedule_id: parent.owner_schedule_id,
                    campus_id: parent.campus_id,
                    campus_name: parent.campus_name,
                    program_id: parent.program_id,
                    program_name: parent.program_name,
                    course_id: parent.course_id,
                    course_label: parent.course_label,
                    vc_section_name: parent.vc_section_name,
                    section_id: parent.section_id,
                    section_label: parent.section_label,
                    section_projected_vacancies: parent.section_projected_vacancies,
                    subsection_label: parent.subsection_label,
                    schedule_label: `${displayDay(parent.day_of_week)} ${compactTime(parent.start_time)}-${compactTime(parent.end_time)}`,
                },
                children,
            });
        }

        return {
            success: true,
            count: items.length,
            items,
        };
    }

    private buildInheritanceCandidateBlocks(rows: ScheduleContextRow[], blockedScheduleIds: Set<string>) {
        const groups = new Map<string, ScheduleContextRow[]>();
        for (const row of rows) {
            if (blockedScheduleIds.has(row.schedule_id)) {
                continue;
            }
            const teacher = resolveTeacher(row);
            const teacherKey = String(teacher.id ?? teacher.name ?? '').trim().toUpperCase();
            if (!teacherKey || teacherKey === 'POR ASIGNAR') {
                continue;
            }
            const key = [
                row.semester_id ?? '',
                row.faculty_id ?? '',
                row.course_id,
                row.section_id,
                row.day_of_week,
                row.campus_id ?? '',
                row.program_id ?? '',
                teacherKey,
            ].join('|');
            const current = groups.get(key) ?? [];
            current.push(row);
            groups.set(key, current);
        }

        const blocks: InheritanceCandidateBlock[] = [];
        for (const groupRows of groups.values()) {
            const ordered = [...groupRows].sort((left, right) =>
                compactTime(left.start_time).localeCompare(compactTime(right.start_time)),
            );
            let currentBlockRows: ScheduleContextRow[] = [ordered[0]];
            for (let index = 1; index < ordered.length; index += 1) {
                const previous = currentBlockRows[currentBlockRows.length - 1];
                const row = ordered[index];
                if (compactTime(previous.end_time) === compactTime(row.start_time)) {
                    currentBlockRows.push(row);
                    continue;
                }
                blocks.push(this.buildCandidateBlock(currentBlockRows));
                currentBlockRows = [row];
            }
            blocks.push(this.buildCandidateBlock(currentBlockRows));
        }

        return blocks;
    }

    private buildCandidateBlock(rows: ScheduleContextRow[]): InheritanceCandidateBlock {
        const ordered = [...rows].sort((left, right) =>
            compactTime(left.start_time).localeCompare(compactTime(right.start_time)),
        );
        const owner = ordered[0];
        const teacher = resolveTeacher(owner);
        const teacherKey = String(teacher.id ?? teacher.name ?? '').trim().toUpperCase();
        const subsectionLabels = ordered.map((row) => buildGroupLabel(row)).filter(Boolean);
        return {
            owner_schedule_id: owner.schedule_id,
            schedule_ids: ordered.map((row) => row.schedule_id),
            semester_id: owner.semester_id,
            faculty_id: owner.faculty_id,
            cycle: owner.cycle,
            day_of_week: owner.day_of_week,
            start_time: ordered[0].start_time,
            end_time: ordered[ordered.length - 1].end_time,
            teacher_key: teacherKey,
            teacher_name: teacher.name,
            faculty_name: owner.faculty_name,
            campus_id: owner.campus_id,
            campus_name: owner.campus_name,
            program_id: owner.program_id,
            program_name: owner.program_name,
            course_id: owner.course_id,
            course_label: buildCourseLabel(owner.course_code, owner.course_name),
            vc_section_name: owner.vc_section_name,
            section_id: owner.section_id,
            section_label: buildSectionLabel(owner),
            section_projected_vacancies: owner.section_projected_vacancies,
            subsection_label: subsectionLabels.join(' + ') || buildGroupLabel(owner),
        };
    }

    async cleanupLegacyInheritances(filters?: { semesterId?: string; facultyId?: string }) {
        const legacyMappings = await this.inheritanceRepo.find({
            where: { notes: LEGACY_AUTO_INHERITANCE_NOTE },
        });
        if (!legacyMappings.length) {
            return { success: true, count: 0 };
        }

        const semesterId = String(filters?.semesterId ?? '').trim();
        const facultyId = String(filters?.facultyId ?? '').trim();
        let candidateMappings = legacyMappings;

        if (semesterId || facultyId) {
            const serialized = await this.serializeInheritanceMappings(legacyMappings);
            const scheduleIds = serialized.map((item) => item.parent_schedule_id);
            const rows = await this.getScheduleRows(undefined, scheduleIds);
            const rowMap = new Map(rows.map((row) => [row.schedule_id, row] as const));
            candidateMappings = legacyMappings.filter((mapping) => {
                const row = rowMap.get(mapping.parent_schedule_id);
                if (!row) {
                    return false;
                }
                if (semesterId && row.semester_id !== semesterId) {
                    return false;
                }
                if (facultyId && row.faculty_id !== facultyId) {
                    return false;
                }
                return true;
            });
        }

        const ids = candidateMappings.map((item) => item.id).filter(Boolean);
        if (!ids.length) {
            return { success: true, count: 0 };
        }

        await this.inheritanceRepo.delete({ id: In(ids) });
        return { success: true, count: ids.length };
    }

    async autoDetectInheritances(semesterId: string) {
        const normalizedId = String(semesterId ?? '').trim();
        if (!normalizedId) throw new BadRequestException('semesterId es requerido.');

        // Se carga TODO el semestre sin filtrar por facultad, programa ni sede.
        const rows = await this.getScheduleRows({ semesterId: normalizedId });

        // Cargar herencias existentes para poder saltarlas
        const inheritances = await this.listInheritedMappings(rows.map((row) => row.schedule_id));
        const childScheduleIds = new Set(inheritances.map((item) => item.child_schedule_id));

        const groups = new Map<string, ScheduleContextRow[]>();
        for (const item of rows) {
            const hasTeacher = resolveTeacher(item).name && resolveTeacher(item).name !== 'Por asignar';
            if (!hasTeacher || childScheduleIds.has(item.schedule_id)) continue;
            const teacherName = resolveTeacher(item).name;
            // Se asume continuidad por curso, seccion, dia y docente
            const key = `${item.course_id}|${item.section_id}|${item.day_of_week}|${teacherName}`;
            const curr = groups.get(key) ?? [];
            curr.push(item);
            groups.set(key, curr);
        }

        const toCreate: PlanningSubsectionScheduleVcInheritanceEntity[] = [];
        const parseMinutes = (time: string) => {
            const [h, m] = compactTime(time).split(':').map(Number);
            return (h || 0) * 60 + (m || 0);
        };

        for (const group of groups.values()) {
            if (group.length < 2) continue;
            group.sort((a, b) => compactTime(a.start_time).localeCompare(compactTime(b.start_time)));
            for (let i = 0; i < group.length - 1; i++) {
                const parent = group[i];
                const child = group[i + 1];
                const pTime = parseMinutes(parent.end_time);
                const cTime = parseMinutes(child.start_time);
                const diffMs = cTime - pTime;
                // Si la diferencia entre termino e inicio es de 0 a 20 minutos
                if (diffMs >= 0 && diffMs <= 20) {
                    const entity = new PlanningSubsectionScheduleVcInheritanceEntity();
                    entity.id = newId();
                    entity.parent_schedule_id = parent.schedule_id;
                    entity.child_schedule_id = child.schedule_id;
                    entity.notes = LEGACY_AUTO_INHERITANCE_NOTE;
                    entity.is_active = true;
                    entity.created_at = new Date();
                    entity.updated_at = new Date();
                    toCreate.push(entity);
                    // Como ya se emparejo este hijo, agregar al set para no tomarlo como padre si sobran mas
                    childScheduleIds.add(child.schedule_id);
                }
            }
        }

        if (toCreate.length > 0) {
            await this.inheritanceRepo.save(toCreate);
        }
        return { success: true, count: toCreate.length };
    }

    async deleteVideoconferenceInheritance(id: string) {
        const normalizedId = String(id ?? '').trim();
        if (!normalizedId) {
            throw new BadRequestException('id es requerido.');
        }
        await this.inheritanceRepo.delete({ id: normalizedId });
        return { success: true, id: normalizedId };
    }

    async preview(filters: PreviewVideoconferenceDto) {
        const hostRuleMap = await this.getActiveHostRuleMap();
        const includeAll = filters.includeAll === true;
        const includeSplit = filters.includeSplit === true;
        const expandGroups = filters.expandGroups === true;

        let requestedRows = await this.getScheduleRows(filters);
        // includeAll bypasses split filtering (used by the Cursos Especiales admin page)
        if (!includeAll) {
            if (includeSplit) {
                // Cursos Especiales view: show only rows that have a host rule configured
                requestedRows = requestedRows.filter((row) => hostRuleMap.has(row.schedule_id));
            } else {
                // Normal VC view: exclude only rows explicitly marked as skip_zoom
                // (rows with host_rules are included — their host_rule data is attached to the result)
                requestedRows = requestedRows.filter((row) => {
                    const rule = hostRuleMap.get(row.schedule_id);
                    return !rule?.skip_zoom;
                });
            }
        }

        // expandGroups: return each schedule as its own independent row (no continuous-block grouping).
        // Used by the Cursos Especiales "Por horario" mode.
        if (expandGroups) {
            const noInheritance: ScheduleInheritanceInfo = {
                mapping: null,
                parent_schedule_id: null,
                family_owner_schedule_id: '',
                is_inherited: false,
                parent_label: null,
            };
            return requestedRows.map((row) => ({
                ...this.serializeBasePreviewRow(
                    row,
                    { ...noInheritance, family_owner_schedule_id: row.schedule_id },
                    [row.schedule_id],
                    [buildGroupLabel(row)],
                ),
                host_rule: hostRuleMap.get(row.schedule_id) ?? null,
            }));
        }

        const requestedScheduleIds = requestedRows.map((row) => row.schedule_id);
        const requestedScheduleIdSet = new Set<string>(requestedScheduleIds);
        const requestedInheritanceIndex = await this.buildOperationalInheritanceIndex(requestedRows);
        const expandedScheduleIds = new Set<string>(requestedScheduleIds);
        for (const scheduleId of requestedScheduleIds) {
            const inherited = requestedInheritanceIndex.byChild.get(scheduleId);
            if (inherited?.parent_schedule_id) {
                expandedScheduleIds.add(inherited.parent_schedule_id);
            }
            const childMappings = requestedInheritanceIndex.childrenByParent.get(scheduleId) ?? [];
            for (const item of childMappings) {
                expandedScheduleIds.add(item.child_schedule_id);
            }
        }
        const allRows = await this.getScheduleRows(undefined, Array.from(expandedScheduleIds));
        const inheritanceIndex = await this.buildOperationalInheritanceIndex(allRows);
        const hasStartDate = Boolean(filters.startDate?.trim());
        const hasEndDate = Boolean(filters.endDate?.trim());
        if (hasStartDate !== hasEndDate) {
            throw new BadRequestException('Debe enviar startDate y endDate juntos.');
        }

        if (!hasStartDate || !hasEndDate) {
            const continuousBlockMap = this.buildContinuousBlockMap(allRows, inheritanceIndex);
            const mappedBaseRows = [];
            for (const row of allRows) {
                if (!requestedScheduleIdSet.has(row.schedule_id)) continue;
                const inheritance = this.resolveScheduleInheritance(row.schedule_id, inheritanceIndex, allRows);
                if (inheritance.is_inherited) continue; // Omitir hijos de la vista

                const familyRows = allRows.filter((r) =>
                    this.resolveScheduleInheritance(r.schedule_id, inheritanceIndex, allRows).family_owner_schedule_id === row.schedule_id
                );
                const maxEndTime = familyRows.reduce(
                    (max, r) => (compactTime(r.end_time) > compactTime(max) ? r.end_time : max),
                    row.end_time,
                );
                const blockInfo = continuousBlockMap.get(row.schedule_id) ?? null;
                const clonedRow = {
                    ...row,
                    end_time: maxEndTime,
                    duration_minutes: calculateDurationMinutes(row.start_time, maxEndTime),
                    subsection_code:
                        blockInfo?.grouped_subsection_codes.length
                            ? blockInfo.grouped_subsection_codes.join(' + ')
                            : row.subsection_code,
                };
                mappedBaseRows.push(
                    {
                        ...this.serializeBasePreviewRow(
                            clonedRow,
                            inheritance,
                            blockInfo?.family_schedule_ids ?? [row.schedule_id],
                            blockInfo?.grouped_subsection_labels ?? [buildGroupLabel(row)],
                        ),
                        host_rule: hostRuleMap.get(row.schedule_id) ?? null,
                    },
                );
            }
            return mappedBaseRows;
        }

        const startDate = normalizeIsoDate(filters.startDate ?? '');
        const endDate = normalizeIsoDate(filters.endDate ?? '');
        if (startDate > endDate) {
            throw new BadRequestException('startDate no puede ser mayor que endDate.');
        }
        const occurrences = await this.resolveOccurrences(allRows, startDate, endDate, 'America/Lima', inheritanceIndex);
        const continuousBlockMap = this.buildContinuousBlockMap(allRows, inheritanceIndex);

        const mappedOccurrences = [];
        for (const occ of occurrences) {
            if (!requestedScheduleIdSet.has(occ.row.schedule_id)) continue;
            if (occ.inheritance.is_inherited) continue; // Omitir hijos de la vista

            const familyOccurrences = occurrences.filter((r) =>
                r.inheritance.family_owner_schedule_id === occ.inheritance.family_owner_schedule_id &&
                r.base_conference_date === occ.base_conference_date
            );
            const maxEndTime = familyOccurrences.reduce(
                (max, r) => (compactTime(r.effective_end_time) > compactTime(max) ? r.effective_end_time : max),
                occ.effective_end_time,
            );
            const blockInfo = continuousBlockMap.get(occ.row.schedule_id) ?? null;
            const clonedOcc = {
                ...occ,
                effective_end_time: maxEndTime,
                scheduled_end: familyOccurrences.reduce(
                    (max, r) => (r.scheduled_end > max ? r.scheduled_end : max),
                    occ.scheduled_end,
                ),
                row: {
                    ...occ.row,
                    duration_minutes: calculateDurationMinutes(occ.effective_start_time, maxEndTime),
                    subsection_code:
                        blockInfo?.grouped_subsection_codes.length
                            ? blockInfo.grouped_subsection_codes.join(' + ')
                            : occ.row.subsection_code,
                },
            };
            mappedOccurrences.push(
                {
                    ...this.serializeOccurrencePreviewRow(
                        clonedOcc,
                        blockInfo?.family_schedule_ids ?? [occ.row.schedule_id],
                        blockInfo?.grouped_subsection_labels ?? [buildGroupLabel(occ.row)],
                    ),
                    host_rule: hostRuleMap.get(occ.row.schedule_id) ?? null,
                },
            );
        }
        return mappedOccurrences;
    }

    /**
     * Given a list of occurrenceKeys (schedule_id::conference_date) OR scheduleIds + date range
     * checks which ones already have an active (non-ERROR) videoconference record.
     *
     * Mode A – occurrence keys: used after "Ver ocurrencias" when each row has a specific date.
     * Mode B – schedule IDs + date range: used on base-schedule view to count existing
     *          conferences per schedule within the given range.
     */
    async checkExisting(payload: {
        occurrenceKeys?: string[];
        scheduleIds?: string[];
        startDate?: string;
        endDate?: string;
    }): Promise<{
        existing: Array<{
            occurrence_key: string;
            schedule_id: string;
            conference_date: string;
            status: string;
            zoom_meeting_id: string | null;
            zoom_user_email: string | null;
            record_id: string;
        }>;
    }> {
        const keys = normalizeIdArray(payload.occurrenceKeys);
        const scheduleIds = normalizeIdArray(payload.scheduleIds);

        // ── Mode A: occurrence keys ─────────────────────────────────────────
        if (keys.length) {
            const pairs = keys
                .map((key) => {
                    const sep = key.lastIndexOf('::');
                    if (sep < 0) return null;
                    const date = key.slice(sep + 2);
                    if (!date) return null; // skip keys without a real date
                    return { schedule_id: key.slice(0, sep), conference_date: date, occurrence_key: key };
                })
                .filter((item): item is NonNullable<typeof item> => item !== null);

            if (!pairs.length) {
                return { existing: [] };
            }

            const uniqueScheduleIds = [...new Set(pairs.map((p) => p.schedule_id))];
            const conferenceDates = [...new Set(pairs.map((p) => p.conference_date))];

            const rows = await this.planningVideoconferencesRepo
                .createQueryBuilder('vc')
                .where('vc.planning_subsection_schedule_id IN (:...scheduleIds)', { scheduleIds: uniqueScheduleIds })
                .andWhere('vc.conference_date IN (:...conferenceDates)', { conferenceDates })
                .andWhere("vc.status != 'ERROR'")
                .select([
                    'vc.id',
                    'vc.planning_subsection_schedule_id',
                    'vc.conference_date',
                    'vc.status',
                    'vc.zoom_meeting_id',
                    'vc.zoom_user_email',
                ])
                .getMany();

            const rowMap = new Map(rows.map((r) => [`${r.planning_subsection_schedule_id}::${r.conference_date}`, r]));

            const existing = pairs
                .map((p) => {
                    const record = rowMap.get(`${p.schedule_id}::${p.conference_date}`);
                    if (!record) return null;
                    return {
                        occurrence_key: p.occurrence_key,
                        schedule_id: p.schedule_id,
                        conference_date: p.conference_date,
                        status: record.status,
                        zoom_meeting_id: record.zoom_meeting_id,
                        zoom_user_email: record.zoom_user_email,
                        record_id: record.id,
                    };
                })
                .filter((item): item is NonNullable<typeof item> => item !== null);

            return { existing };
        }

        // ── Mode B: schedule IDs + date range ──────────────────────────────
        if (!scheduleIds.length) {
            return { existing: [] };
        }

        const startDate = payload.startDate?.trim();
        const endDate = payload.endDate?.trim();
        if (!startDate || !endDate) {
            return { existing: [] };
        }

        const rows = await this.planningVideoconferencesRepo
            .createQueryBuilder('vc')
            .where('vc.planning_subsection_schedule_id IN (:...scheduleIds)', { scheduleIds })
            .andWhere('vc.conference_date >= :startDate', { startDate })
            .andWhere('vc.conference_date <= :endDate', { endDate })
            .andWhere("vc.status != 'ERROR'")
            .select([
                'vc.id',
                'vc.planning_subsection_schedule_id',
                'vc.conference_date',
                'vc.status',
                'vc.zoom_meeting_id',
                'vc.zoom_user_email',
            ])
            .orderBy('vc.conference_date', 'ASC')
            .getMany();

        const existing = rows.map((r) => ({
            occurrence_key: `${r.planning_subsection_schedule_id}::${r.conference_date}`,
            schedule_id: r.planning_subsection_schedule_id,
            conference_date: r.conference_date,
            status: r.status,
            zoom_meeting_id: r.zoom_meeting_id,
            zoom_user_email: r.zoom_user_email,
            record_id: r.id,
        }));

        return { existing };
    }

    async assignmentPreview(payload: AssignmentPreviewVideoconferenceDto) {
        const zoomGroupId = String(payload.zoomGroupId ?? '').trim();
        if (!zoomGroupId) {
            throw new BadRequestException('zoomGroupId es requerido.');
        }
        const scheduleIds = normalizeIdArray(payload.scheduleIds);
        const occurrenceKeys = normalizeIdArray(payload.occurrenceKeys);
        const useCurrentFilters = Boolean(payload.selectAllVisible);
        if (!scheduleIds.length && !occurrenceKeys.length && !useCurrentFilters) {
            throw new BadRequestException('Debe enviar al menos un scheduleId u occurrenceKey.');
        }

        const hasStartDate = Boolean(payload.startDate?.trim());
        const hasEndDate = Boolean(payload.endDate?.trim());
        if (hasStartDate !== hasEndDate) {
            throw new BadRequestException('Debe enviar startDate y endDate juntos.');
        }

        const zoomConfig = await this.zoomAccountService.requireConfiguredConfig();
        const poolValidation = await this.getActiveZoomPoolUsers(zoomGroupId, true);
        if (!poolValidation.users.length) {
            throw new BadRequestException(
                `No hay usuarios Zoom activos en el grupo ${poolValidation.group.name}.`,
            );
        }
        const licenseSnapshot = await this.loadZoomPoolLicenseSnapshot();
        const zoomUsers: PreviewZoomPoolUser[] = poolValidation.users.map((item) => ({
            ...item,
            ...this.resolveZoomLicenseMetadata(item.email, licenseSnapshot, item.zoom_user_id),
        }));

        const requestedScheduleIds = scheduleIds.length
            ? scheduleIds
            : uniqueIds(occurrenceKeys.map((item) => parseOccurrenceKey(item).schedule_id).filter(Boolean));
        const selectedFilters = this.extractAssignmentPreviewFilters(payload);
        const expanded = useCurrentFilters
            ? await this.buildExpandedScheduleContextFromFilters(selectedFilters)
            : await this.buildExpandedScheduleContext(requestedScheduleIds);

        if (!hasStartDate || !hasEndDate) {
            const result = await this.simulateBaseAssignmentPreview(
                useCurrentFilters
                    ? new Set(expanded.requestedRows.map((row) => row.schedule_id))
                    : new Set(requestedScheduleIds),
                expanded.rows,
                expanded.inheritanceIndex,
                zoomUsers,
                zoomConfig.maxConcurrent,
                licenseSnapshot,
            );
            return {
                mode: 'BASE' as const,
                ...result,
                zoom_group: {
                    id: poolValidation.group.id,
                    name: poolValidation.group.name,
                    code: poolValidation.group.code,
                },
                pool_warnings: poolValidation.warnings,
                license_sync_ok: licenseSnapshot.ok,
                license_sync_error: licenseSnapshot.error,
            };
        }

        const startDate = normalizeIsoDate(payload.startDate ?? '');
        const endDate = normalizeIsoDate(payload.endDate ?? '');
        if (startDate > endDate) {
            throw new BadRequestException('startDate no puede ser mayor que endDate.');
        }

        const occurrences = await this.resolveOccurrences(
            expanded.rows,
            startDate,
            endDate,
            zoomConfig.timezone,
            expanded.inheritanceIndex,
        );
        const result = await this.simulateOccurrenceAssignmentPreview({
            selectedOccurrenceKeys: useCurrentFilters ? new Set<string>() : new Set(occurrenceKeys),
            requestedScheduleIds: useCurrentFilters
                ? new Set(expanded.requestedRows.map((row) => row.schedule_id))
                : new Set(requestedScheduleIds),
            occurrences,
            zoomUsers,
            maxConcurrent: zoomConfig.maxConcurrent,
            licenseSnapshot,
        });
        return {
            mode: 'OCCURRENCE' as const,
            ...result,
            zoom_group: {
                id: poolValidation.group.id,
                name: poolValidation.group.name,
                code: poolValidation.group.code,
            },
            pool_warnings: poolValidation.warnings,
            license_sync_ok: licenseSnapshot.ok,
            license_sync_error: licenseSnapshot.error,
        };
    }

    private async buildExpandedScheduleContext(requestedScheduleIds: string[]) {
        const requestedRows = await this.getScheduleRows(undefined, requestedScheduleIds);
        return this.buildExpandedScheduleContextFromRequestedRows(requestedRows);
    }

    private async buildExpandedScheduleContextFromFilters(filters?: FilterOptionsDto) {
        const requestedRows = await this.getScheduleRows(filters);
        return this.buildExpandedScheduleContextFromRequestedRows(requestedRows);
    }

    private async buildExpandedScheduleContextFromRequestedRows(requestedRows: ScheduleContextRow[]) {
        const requestedScheduleIds = uniqueIds(requestedRows.map((row) => row.schedule_id));
        const scopedRows = await this.getRowsForRequestedScope(requestedRows);
        const requestedInheritanceIndex = await this.buildOperationalInheritanceIndex(scopedRows);
        const expandedScheduleIds = new Set<string>(requestedScheduleIds);
        for (const scheduleId of requestedScheduleIds) {
            const inherited = requestedInheritanceIndex.byChild.get(scheduleId);
            if (inherited?.parent_schedule_id) {
                expandedScheduleIds.add(inherited.parent_schedule_id);
            }
            const childMappings = requestedInheritanceIndex.childrenByParent.get(scheduleId) ?? [];
            for (const item of childMappings) {
                expandedScheduleIds.add(item.child_schedule_id);
            }
        }
        const rows = scopedRows.filter((row) => expandedScheduleIds.has(row.schedule_id));
        const inheritanceIndex = await this.buildOperationalInheritanceIndex(rows);
        return {
            requestedRows,
            rows,
            inheritanceIndex,
        };
    }

    private async getRowsForRequestedScope(requestedRows: ScheduleContextRow[]) {
        if (!requestedRows.length) {
            return [] as ScheduleContextRow[];
        }

        const scopes = new Map<string, { semesterId: string | undefined; facultyId: string | undefined }>();
        for (const row of requestedRows) {
            const semesterId = row.semester_id ?? undefined;
            const facultyId = row.faculty_id ?? undefined;
            const key = `${semesterId ?? ''}|${facultyId ?? ''}`;
            if (!scopes.has(key)) {
                scopes.set(key, { semesterId, facultyId });
            }
        }

        const rowsById = new Map<string, ScheduleContextRow>();
        for (const scope of scopes.values()) {
            const scopeRows = await this.getScheduleRows({
                semesterId: scope.semesterId,
                facultyIds: scope.facultyId ? [scope.facultyId] : undefined,
            });
            for (const row of scopeRows) {
                rowsById.set(row.schedule_id, row);
            }
        }

        for (const row of requestedRows) {
            rowsById.set(row.schedule_id, row);
        }
        return Array.from(rowsById.values());
    }

    private extractAssignmentPreviewFilters(payload: AssignmentPreviewVideoconferenceDto): FilterOptionsDto | undefined {
        const filters: FilterOptionsDto = {};
        if (payload.semesterId?.trim()) {
            filters.semesterId = payload.semesterId.trim();
        }
        if (payload.campusIds?.length) {
            filters.campusIds = normalizeIdArray(payload.campusIds);
        }
        if (payload.facultyIds?.length) {
            filters.facultyIds = normalizeIdArray(payload.facultyIds);
        }
        if (payload.programIds?.length) {
            filters.programIds = normalizeIdArray(payload.programIds);
        }
        if (payload.courseIds?.length) {
            filters.courseIds = normalizeIdArray(payload.courseIds);
        }
        const normalizedModalities = normalizeModalityFilterInput(payload.modalities ?? payload.modality);
        if (normalizedModalities.length) {
            filters.modalities = normalizedModalities;
        }
        if (payload.days?.length) {
            filters.days = normalizeIdArray(payload.days);
        }
        return Object.keys(filters).length ? filters : undefined;
    }

    private normalizePreferredHosts(input?: Array<{ scheduleId?: string; conferenceDate?: string; zoomUserId?: string }>) {
        const items = Array.isArray(input) ? input : [];
        return items
            .map((item) => ({
                schedule_id: String(item?.scheduleId ?? '').trim(),
                conference_date: item?.conferenceDate ? normalizeIsoDate(item.conferenceDate) : null,
                zoom_user_id: String(item?.zoomUserId ?? '').trim(),
            }))
            .filter((item) => item.schedule_id && item.zoom_user_id) as PreferredHostSelection[];
    }

    private findPreferredZoomUserId(
        ownerOccurrence: ExpandedOccurrence,
        preferredHosts: PreferredHostSelection[],
    ) {
        const byExact = preferredHosts.find(
            (item) =>
                item.schedule_id === ownerOccurrence.row.schedule_id &&
                item.conference_date === ownerOccurrence.effective_conference_date,
        );
        if (byExact?.zoom_user_id) {
            return byExact.zoom_user_id;
        }

        const bySchedule = preferredHosts.find(
            (item) => item.schedule_id === ownerOccurrence.row.schedule_id && !item.conference_date,
        );
        return bySchedule?.zoom_user_id ?? null;
    }

    private async simulateBaseAssignmentPreview(
        selectedScheduleIds: Set<string>,
        rows: ScheduleContextRow[],
        inheritanceIndex: InheritanceIndex,
        zoomUsers: PreviewZoomPoolUser[],
        maxConcurrent: number,
        licenseSnapshot: ZoomPoolLicenseSnapshot,
    ) {
        const inheritanceByScheduleId = new Map(
            rows.map((row) => [row.schedule_id, this.resolveScheduleInheritance(row.schedule_id, inheritanceIndex, rows)] as const),
        );
        const selectedRows = rows.filter((row) => selectedScheduleIds.has(row.schedule_id));
        const selectedOwnerIds = new Set(
            selectedRows.map((row) => inheritanceByScheduleId.get(row.schedule_id)?.family_owner_schedule_id ?? row.schedule_id),
        );
        const rowsByOwnerId = new Map<string, ScheduleContextRow[]>();
        const selectedRowsByOwnerId = new Map<string, ScheduleContextRow[]>();
        for (const row of rows) {
            const ownerId = inheritanceByScheduleId.get(row.schedule_id)?.family_owner_schedule_id ?? row.schedule_id;
            if (!selectedOwnerIds.has(ownerId)) {
                continue;
            }
            const currentRows = rowsByOwnerId.get(ownerId) ?? [];
            currentRows.push(row);
            rowsByOwnerId.set(ownerId, currentRows);
            if (selectedScheduleIds.has(row.schedule_id)) {
                const currentSelected = selectedRowsByOwnerId.get(ownerId) ?? [];
                currentSelected.push(row);
                selectedRowsByOwnerId.set(ownerId, currentSelected);
            }
        }

        // Pre-seed simulatedReservations with already-scheduled DB meetings for all pool users.
        // IMPORTANT: We must convert to reference-week dates (same fictional week used by
        // buildWeeklyScheduleDateTime) using day_of_week + start_time/end_time, NOT real
        // scheduled_start/scheduled_end dates, because the weekly simulation compares everything
        // against reference dates (Jan 2026) and real UTC dates would never overlap.
        const simulatedReservations = new Map<string, SimulatedReservation[]>();
        const poolUserIds = zoomUsers.map((u) => u.zoom_user_id).filter(Boolean);
        if (poolUserIds.length) {
            const existingConferences = await this.planningVideoconferencesRepo.find({
                where: {
                    zoom_user_id: In(poolUserIds),
                    status: In([...ACTIVE_CONFERENCE_STATUSES]),
                },
                select: ['zoom_user_id', 'day_of_week', 'start_time', 'end_time', 'zoom_meeting_id'],
            });
            const dbMeetingIds = new Set<string>();
            for (const conf of existingConferences) {
                if (!conf.zoom_user_id) {
                    continue;
                }
                if (conf.zoom_meeting_id) {
                    dbMeetingIds.add(conf.zoom_meeting_id);
                }
                const refStart = buildWeeklyScheduleDateTime(conf.day_of_week, conf.start_time);
                const refEnd = buildWeeklyScheduleDateTime(conf.day_of_week, conf.end_time);
                const current = simulatedReservations.get(conf.zoom_user_id) ?? [];
                current.push({ scheduled_start: refStart, scheduled_end: refEnd });
                simulatedReservations.set(conf.zoom_user_id, current);
            }

            // Also pre-seed from live/upcoming Zoom meetings that are NOT already in our DB.
            // This catches meetings created directly in Zoom outside the system.
            for (const zoomUser of zoomUsers) {
                if (!zoomUser.email) {
                    continue;
                }
                try {
                    const remoteMeetings = await this.zoomAccountService.listUserMeetingsByTypes(
                        zoomUser.email,
                        ['live', 'upcoming'],
                    );
                    for (const meeting of remoteMeetings) {
                        if (dbMeetingIds.has(meeting.id)) {
                            // Already counted via the DB record
                            continue;
                        }
                        const slot = zoomMeetingToReferenceWeekSlot(meeting);
                        if (!slot) {
                            continue;
                        }
                        const current = simulatedReservations.get(zoomUser.zoom_user_id) ?? [];
                        current.push({ scheduled_start: slot.start, scheduled_end: slot.end });
                        simulatedReservations.set(zoomUser.zoom_user_id, current);
                    }
                } catch {
                    // Zoom API unavailable — continue without remote meetings for this user
                }
            }
        }

        const virtualReservations = new Map<string, SimulatedReservation[]>();
        const items: AssignmentPreviewItem[] = [];
        let virtualHostCounter = 0;
        const ownerRows = sortBaseRowsForSimulation(
            Array.from(selectedOwnerIds)
                .map((ownerId) => rows.find((row) => row.schedule_id === ownerId) ?? null)
                .filter((row): row is ScheduleContextRow => Boolean(row)),
        );

        for (const ownerRow of ownerRows) {
            const ownerId = ownerRow.schedule_id;
            const selectedFamilyRows = sortBaseRowsForSimulation(selectedRowsByOwnerId.get(ownerId) ?? []);
            if (!selectedFamilyRows.length) {
                continue;
            }
            const fullFamilyRows = sortBaseRowsForSimulation(rowsByOwnerId.get(ownerId) ?? []);
            if (!fullFamilyRows.length) {
                continue;
            }

            const maxEndTime = fullFamilyRows.reduce(
                (max, r) => (compactTime(r.end_time) > compactTime(max) ? r.end_time : max),
                ownerRow.end_time,
            );
            const validationError = this.validateScheduleForGeneration(ownerRow);
            let host: PreviewZoomPoolUser | null = null;
            let ownerStatus: AssignmentPreviewStatus = 'NO_AVAILABLE_ZOOM_USER';
            let ownerMessage =
                'No se asigno host Zoom para este horario base. Aunque haya varios usuarios en el grupo, todos pueden estar ocupados en ese bloque horario o superar la concurrencia maxima.';
            if (validationError) {
                ownerStatus = 'VALIDATION_ERROR';
                ownerMessage = validationError;
            } else {
                const slotStart = buildWeeklyScheduleDateTime(ownerRow.day_of_week, ownerRow.start_time);
                const slotEnd = buildWeeklyScheduleDateTime(ownerRow.day_of_week, maxEndTime);
                host = this.findAvailableZoomUserForBaseSlot(
                    slotStart,
                    slotEnd,
                    zoomUsers,
                    maxConcurrent,
                    simulatedReservations,
                );
                if (host) {
                    ownerStatus = host.is_licensed === true ? 'ASSIGNED_LICENSED' : 'ASSIGNED_RISK';
                    ownerMessage =
                        host.is_licensed === true
                            ? 'Host Zoom sugerido. Puede repetirse en varias filas si los horarios no se traslapan; el sistema prioriza reutilizar hosts antes de consumir otro.'
                            : 'Host Zoom sugerido, pero depende de licencia basica o no verificada. Puede repetirse en varias filas si los horarios no se traslapan.';
                    const currentReservations = simulatedReservations.get(host.zoom_user_id) ?? [];
                    currentReservations.push({
                        scheduled_start: slotStart,
                        scheduled_end: slotEnd,
                    });
                    simulatedReservations.set(host.zoom_user_id, currentReservations);
                }
            }

            if (ownerStatus === 'NO_AVAILABLE_ZOOM_USER') {
                const slotStart = buildWeeklyScheduleDateTime(ownerRow.day_of_week, ownerRow.start_time);
                const slotEnd = buildWeeklyScheduleDateTime(ownerRow.day_of_week, maxEndTime);
                assignReservationToVirtualHost(slotStart, slotEnd, maxConcurrent, virtualReservations, () => {
                    virtualHostCounter += 1;
                    return `virtual:${virtualHostCounter}`;
                });
            }

            for (const row of selectedFamilyRows) {
                const inheritance = inheritanceByScheduleId.get(row.schedule_id) ?? null;
                if (row.schedule_id === ownerId) {
                    items.push(
                        this.buildAssignmentPreviewItem({
                            id: `schedule:${row.schedule_id}`,
                            mode: 'BASE',
                            occurrenceKey: `schedule:${row.schedule_id}`,
                            scheduleId: row.schedule_id,
                            conferenceDate: null,
                            row: { ...row, end_time: maxEndTime },
                            previewStatus: ownerStatus,
                            message: ownerMessage,
                            host,
                            consumesCapacity: ownerStatus === 'ASSIGNED_LICENSED' || ownerStatus === 'ASSIGNED_RISK',
                            inheritance: buildDefaultInheritance(row.schedule_id),
                            ownerOccurrenceKey: null,
                        }),
                    );
                    continue;
                }
                // Se omiten los hijos de la vista (según el rediseño) ya que el padre absorbe el bloque completo.
                continue;
            }
        }

        const orderedItems = sortAssignmentPreviewItems(items);
        return {
            summary: this.buildAssignmentPreviewSummary(orderedItems, licenseSnapshot.ok, virtualReservations.size),
            items: orderedItems,
        };
    }

    private async simulateOccurrenceAssignmentPreview(input: {
        selectedOccurrenceKeys: Set<string>;
        requestedScheduleIds: Set<string>;
        occurrences: ExpandedOccurrence[];
        zoomUsers: PreviewZoomPoolUser[];
        maxConcurrent: number;
        licenseSnapshot: ZoomPoolLicenseSnapshot;
    }) {
        const selectedOccurrences = input.selectedOccurrenceKeys.size
            ? input.occurrences.filter((item) => input.selectedOccurrenceKeys.has(item.occurrence_key))
            : input.occurrences.filter(
                (item) =>
                    input.requestedScheduleIds.has(item.row.schedule_id) && item.occurrence_type !== 'SKIPPED',
            );
        const selectedOccurrenceKeys = new Set(selectedOccurrences.map((item) => item.occurrence_key));
        const familyKeys = Array.from(
            new Set(
                selectedOccurrences.map((item) =>
                    buildOccurrenceKey(item.inheritance.family_owner_schedule_id, item.base_conference_date),
                ),
            ),
        );
        const familyOccurrences = familyKeys.flatMap((familyKey) =>
            input.occurrences.filter(
                (item) =>
                    buildOccurrenceKey(item.inheritance.family_owner_schedule_id, item.base_conference_date) === familyKey &&
                    item.occurrence_type !== 'SKIPPED',
            ),
        );
        const existingLookup = await this.loadExistingConferenceLookup(familyOccurrences);
        const remoteMeetingsCache = new Map<string, ZoomMeetingSummary[] | null>();
        const simulatedReservations = new Map<string, SimulatedReservation[]>();
        const virtualReservations = new Map<string, SimulatedReservation[]>();
        const items: AssignmentPreviewItem[] = [];
        let virtualHostCounter = 0;

        for (const familyKey of familyKeys) {
            const familyItems = familyOccurrences.filter(
                (item) =>
                    buildOccurrenceKey(item.inheritance.family_owner_schedule_id, item.base_conference_date) === familyKey,
            );
            const ownerOccurrence = familyItems.find((item) => !item.inheritance.is_inherited) ?? null;
            if (!ownerOccurrence) {
                continue;
            }

            const maxEndTime = familyItems.reduce(
                (max, occ) => (compactTime(occ.effective_end_time) > compactTime(max) ? occ.effective_end_time : max),
                ownerOccurrence.effective_end_time,
            );
            const maxScheduledEnd = familyItems.reduce(
                (max, occ) => (occ.scheduled_end > max ? occ.scheduled_end : max),
                ownerOccurrence.scheduled_end,
            );

            ownerOccurrence.effective_end_time = maxEndTime;
            ownerOccurrence.scheduled_end = maxScheduledEnd;

            const ownerResult = await this.previewOwnerOccurrenceAssignment(
                ownerOccurrence,
                input.zoomUsers,
                input.maxConcurrent,
                existingLookup,
                remoteMeetingsCache,
                simulatedReservations,
                input.licenseSnapshot,
            );

            if (ownerResult.preview_status === 'NO_AVAILABLE_ZOOM_USER') {
                assignReservationToVirtualHost(
                    ownerOccurrence.scheduled_start,
                    ownerOccurrence.scheduled_end,
                    input.maxConcurrent,
                    virtualReservations,
                    () => {
                        virtualHostCounter += 1;
                        return `virtual:${virtualHostCounter}`;
                    },
                );
            }

            for (const occurrence of familyItems.filter((item) => selectedOccurrenceKeys.has(item.occurrence_key))) {
                if (!occurrence.inheritance.is_inherited) {
                    items.push(ownerResult);
                    continue;
                }
                // Se omiten los hijos de la vista (según el rediseño) ya que el padre absorbe el bloque completo.
                continue;
            }
        }

        const orderedItems = sortAssignmentPreviewItems(items);
        return {
            summary: this.buildAssignmentPreviewSummary(
                orderedItems,
                input.licenseSnapshot.ok,
                virtualReservations.size,
            ),
            items: orderedItems,
        };
    }

    private async previewOwnerOccurrenceAssignment(
        occurrence: ExpandedOccurrence,
        zoomUsers: PreviewZoomPoolUser[],
        maxConcurrent: number,
        existingLookup: ExistingConferenceLookup,
        remoteMeetingsCache: Map<string, ZoomMeetingSummary[] | null>,
        simulatedReservations: Map<string, SimulatedReservation[]>,
        licenseSnapshot: ZoomPoolLicenseSnapshot,
    ) {
        const validationError = this.validateScheduleForGeneration(occurrence.row);
        if (validationError) {
            return this.buildAssignmentPreviewItem({
                id: occurrence.occurrence_key,
                mode: 'OCCURRENCE',
                occurrenceKey: occurrence.occurrence_key,
                scheduleId: occurrence.row.schedule_id,
                conferenceDate: occurrence.effective_conference_date,
                row: occurrence.row,
                dayOfWeek: dayCodeForDate(occurrence.effective_conference_date),
                startTime: occurrence.effective_start_time,
                endTime: occurrence.effective_end_time,
                previewStatus: 'VALIDATION_ERROR',
                message: validationError,
                host: null,
                consumesCapacity: false,
                inheritance: occurrence.inheritance,
                ownerOccurrenceKey: null,
            });
        }

        const existing =
            existingLookup.get(`${occurrence.row.schedule_id}::${occurrence.effective_conference_date}`) ?? null;
        if (existing) {
            const existingHost = this.resolvePreviewZoomUser(
                existing.zoom_user_id,
                existing.zoom_user_email,
                existing.zoom_user_name,
                zoomUsers,
                licenseSnapshot,
            );
            return this.buildAssignmentPreviewItem({
                id: occurrence.occurrence_key,
                mode: 'OCCURRENCE',
                occurrenceKey: occurrence.occurrence_key,
                scheduleId: occurrence.row.schedule_id,
                conferenceDate: occurrence.effective_conference_date,
                row: occurrence.row,
                dayOfWeek: dayCodeForDate(occurrence.effective_conference_date),
                startTime: occurrence.effective_start_time,
                endTime: occurrence.effective_end_time,
                previewStatus: 'BLOCKED_EXISTING',
                message: 'Ya existe una videoconferencia registrada para esta ocurrencia.',
                host: existingHost,
                consumesCapacity: false,
                inheritance: occurrence.inheritance,
                ownerOccurrenceKey: null,
            });
        }

        const selectedZoomUser = await this.findAvailableZoomUser(
            occurrence.scheduled_start,
            occurrence.scheduled_end,
            zoomUsers,
            maxConcurrent,
            remoteMeetingsCache,
            simulatedReservations,
        );
        if (!selectedZoomUser) {
            return this.buildAssignmentPreviewItem({
                id: occurrence.occurrence_key,
                mode: 'OCCURRENCE',
                occurrenceKey: occurrence.occurrence_key,
                scheduleId: occurrence.row.schedule_id,
                conferenceDate: occurrence.effective_conference_date,
                row: occurrence.row,
                dayOfWeek: dayCodeForDate(occurrence.effective_conference_date),
                startTime: occurrence.effective_start_time,
                endTime: occurrence.effective_end_time,
                previewStatus: 'NO_AVAILABLE_ZOOM_USER',
                message: 'No se asigno host Zoom para esta ocurrencia. Aunque haya varios usuarios en el grupo, todos pueden estar ocupados en ese bloque horario o superar la concurrencia maxima.',
                host: null,
                consumesCapacity: false,
                inheritance: occurrence.inheritance,
                ownerOccurrenceKey: null,
            });
        }

        const currentReservations = simulatedReservations.get(selectedZoomUser.zoom_user_id) ?? [];
        currentReservations.push({
            scheduled_start: occurrence.scheduled_start,
            scheduled_end: occurrence.scheduled_end,
        });
        simulatedReservations.set(selectedZoomUser.zoom_user_id, currentReservations);
        return this.buildAssignmentPreviewItem({
            id: occurrence.occurrence_key,
            mode: 'OCCURRENCE',
            occurrenceKey: occurrence.occurrence_key,
            scheduleId: occurrence.row.schedule_id,
            conferenceDate: occurrence.effective_conference_date,
            row: occurrence.row,
            dayOfWeek: dayCodeForDate(occurrence.effective_conference_date),
            startTime: occurrence.effective_start_time,
            endTime: occurrence.effective_end_time,
            previewStatus: selectedZoomUser.is_licensed === true ? 'ASSIGNED_LICENSED' : 'ASSIGNED_RISK',
            message:
                selectedZoomUser.is_licensed === true
                    ? 'Host Zoom sugerido para esta ocurrencia. Puede repetirse en otras filas si no hay traslape de horarios.'
                    : 'Host Zoom sugerido, pero depende de licencia basica o no verificada. Puede repetirse en otras filas si no hay traslape de horarios.',
            host: selectedZoomUser,
            consumesCapacity: true,
            inheritance: occurrence.inheritance,
            ownerOccurrenceKey: null,
        });
    }

    private buildAssignmentPreviewItem(input: {
        id: string;
        mode: AssignmentPreviewMode;
        occurrenceKey: string | null;
        scheduleId: string;
        conferenceDate: string | null;
        row: ScheduleContextRow;
        dayOfWeek?: string;
        startTime?: string;
        endTime?: string;
        previewStatus: AssignmentPreviewStatus;
        message: string;
        host: PreviewZoomPoolUser | null;
        consumesCapacity: boolean;
        inheritance: ExpandedOccurrence['inheritance'];
        ownerOccurrenceKey: string | null;
    }): AssignmentPreviewItem {
        return {
            id: input.id,
            mode: input.mode,
            occurrence_key: input.occurrenceKey,
            schedule_id: input.scheduleId,
            conference_date: input.conferenceDate,
            day_of_week: input.dayOfWeek ?? input.row.day_of_week,
            day_label: displayDay(input.dayOfWeek ?? input.row.day_of_week),
            start_time: compactTime(input.startTime ?? input.row.start_time),
            end_time: compactTime(input.endTime ?? input.row.end_time),
            preview_status: input.previewStatus,
            message: input.message,
            zoom_user_id: input.host?.zoom_user_id ?? null,
            zoom_user_email: input.host?.email ?? null,
            zoom_user_name: input.host?.name ?? null,
            license_status: input.host?.license_status ?? null,
            license_label: input.host?.license_label ?? null,
            is_licensed: input.host?.is_licensed ?? null,
            depends_on_unverified_license: input.host ? input.host.is_licensed !== true : false,
            consumes_capacity: input.consumesCapacity,
            inheritance: input.inheritance,
            owner_occurrence_key: input.ownerOccurrenceKey,
        };
    }

    private buildAssignmentPreviewSummary(
        items: AssignmentPreviewItem[],
        licenseSyncOk: boolean,
        virtualHostsNeeded = 0,
    ): AssignmentPreviewSummary {
        const assignedStatuses = new Set<AssignmentPreviewStatus>([
            'ASSIGNED_LICENSED',
            'ASSIGNED_RISK',
            'INHERITED',
            'BLOCKED_EXISTING',
        ]);
        const usedHosts = new Map<string, AssignmentPreviewItem>();
        const hostsByDay = new Map<string, Map<string, AssignmentPreviewItem>>();
        for (const item of items) {
            if (!assignedStatuses.has(item.preview_status)) {
                continue;
            }
            const hostKey = buildAssignmentHostKey(item);
            if (!hostKey) {
                continue;
            }
            usedHosts.set(hostKey, item);
            const dayHosts = hostsByDay.get(item.day_of_week) ?? new Map<string, AssignmentPreviewItem>();
            dayHosts.set(hostKey, item);
            hostsByDay.set(item.day_of_week, dayHosts);
        }

        const verifiedHostsUsed = [...usedHosts.values()].filter((item) => item.is_licensed === true).length;
        const riskHostsUsed = [...usedHosts.values()].filter((item) => item.is_licensed !== true).length;

        return {
            requested_rows: items.length,
            assigned_rows: items.filter((item) => assignedStatuses.has(item.preview_status)).length,
            hosts_used: usedHosts.size,
            verified_hosts_used: verifiedHostsUsed,
            risk_hosts_used: riskHostsUsed,
            no_available_zoom_user: items.filter((item) => item.preview_status === 'NO_AVAILABLE_ZOOM_USER').length,
            validation_errors: items.filter((item) => item.preview_status === 'VALIDATION_ERROR').length,
            blocked_existing: items.filter((item) => item.preview_status === 'BLOCKED_EXISTING').length,
            virtual_hosts_needed: virtualHostsNeeded,
            licenses_required_global: usedHosts.size + virtualHostsNeeded,
            additional_licenses_needed: licenseSyncOk ? riskHostsUsed + virtualHostsNeeded : null,
            licenses_by_day: [...hostsByDay.entries()]
                .map(([dayCode, dayHosts]) => ({
                    day_of_week: dayCode,
                    day_label: displayDay(dayCode),
                    required_hosts: dayHosts.size,
                }))
                .sort((left, right) => getDaySortOrder(left.day_of_week) - getDaySortOrder(right.day_of_week)),
        };
    }

    private resolvePreviewZoomUser(
        zoomUserId: string | null | undefined,
        email: string | null | undefined,
        name: string | null | undefined,
        zoomUsers: PreviewZoomPoolUser[],
        licenseSnapshot: ZoomPoolLicenseSnapshot,
    ) {
        const foundById = `${zoomUserId ?? ''}`.trim()
            ? zoomUsers.find((item) => item.zoom_user_id === `${zoomUserId ?? ''}`.trim()) ?? null
            : null;
        if (foundById) {
            return foundById;
        }

        const foundByEmail = `${email ?? ''}`.trim().toLowerCase()
            ? zoomUsers.find((item) => `${item.email ?? ''}`.trim().toLowerCase() === `${email ?? ''}`.trim().toLowerCase()) ?? null
            : null;
        if (foundByEmail) {
            return foundByEmail;
        }

        const fallbackLicense = this.resolveZoomLicenseMetadata(email, licenseSnapshot, zoomUserId);
        if (!zoomUserId && !email && !name) {
            return null;
        }
        return {
            pool_id: '',
            zoom_user_id: `${zoomUserId ?? ''}`.trim(),
            sort_order: 999999,
            is_active: false,
            name: emptyTextToNull(name),
            email: emptyTextToNull(email),
            ...fallbackLicense,
        };
    }

    private async loadExistingConferenceLookup(occurrences: ExpandedOccurrence[]): Promise<ExistingConferenceLookup> {
        const scheduleIds = uniqueIds(occurrences.map((item) => item.row.schedule_id));
        const conferenceDates = uniqueIds(occurrences.map((item) => item.effective_conference_date));
        if (!scheduleIds.length || !conferenceDates.length) {
            return new Map();
        }

        const rows = await this.planningVideoconferencesRepo
            .createQueryBuilder('conference')
            .where('conference.planning_subsection_schedule_id IN (:...scheduleIds)', { scheduleIds })
            .andWhere('conference.conference_date IN (:...conferenceDates)', { conferenceDates })
            .getMany();

        return new Map(
            rows.map((item) => [`${item.planning_subsection_schedule_id}::${item.conference_date}`, item] as const),
        );
    }

    async listZoomGroups() {
        await this.ensureZoomGroupsBootstrap();
        const groups = await this.zoomGroupsRepo.find({
            order: { name: 'ASC', code: 'ASC' },
        });
        if (!groups.length) {
            return [];
        }
        const rows = await this.zoomGroupUsersRepo
            .createQueryBuilder('group_user')
            .select('group_user.group_id', 'group_id')
            .addSelect('COUNT(*)', 'members_count')
            .addSelect('SUM(CASE WHEN group_user.is_active = 1 THEN 1 ELSE 0 END)', 'active_members_count')
            .where('group_user.group_id IN (:...groupIds)', { groupIds: groups.map((item) => item.id) })
            .groupBy('group_user.group_id')
            .getRawMany<Record<string, unknown>>();
        const countByGroup = new Map(
            rows.map((row) => [
                readNullableString(row.group_id) ?? '',
                {
                    members_count: readNumber(row.members_count),
                    active_members_count: readNumber(row.active_members_count),
                },
            ]),
        );
        return groups.map((group) => ({
            id: group.id,
            name: group.name,
            code: group.code,
            is_active: group.is_active,
            is_default:
                group.code === DEFAULT_ZOOM_GROUP_REGULAR_CODE ||
                group.code === DEFAULT_ZOOM_GROUP_HIBRIDO_CODE,
            members_count: countByGroup.get(group.id)?.members_count ?? 0,
            active_members_count: countByGroup.get(group.id)?.active_members_count ?? 0,
            created_at: group.created_at,
            updated_at: group.updated_at,
        }));
    }

    async listActiveZoomGroups() {
        const groups = await this.listZoomGroups();
        return groups
            .filter((item) => item.is_active)
            .map((item) => ({
                id: item.id,
                name: item.name,
                code: item.code,
                is_active: item.is_active,
                members_count: item.members_count ?? 0,
                active_members_count: item.active_members_count ?? 0,
            }));
    }

    async createZoomGroup(dto: CreateZoomGroupDto) {
        await this.ensureZoomGroupsBootstrap();
        const name = String(dto.name ?? '').trim();
        if (!name) {
            throw new BadRequestException('name es requerido.');
        }
        const code = this.normalizeZoomGroupCode(dto.code, name);
        const existing = await this.zoomGroupsRepo.findOne({ where: { code } });
        if (existing) {
            throw new BadRequestException(`Ya existe un grupo Zoom con codigo ${code}.`);
        }
        const now = new Date();
        const entity = this.zoomGroupsRepo.create({
            id: newId(),
            name,
            code,
            is_active: dto.is_active ?? true,
            created_at: now,
            updated_at: now,
        });
        await this.zoomGroupsRepo.save(entity);
        return {
            id: entity.id,
            name: entity.name,
            code: entity.code,
            is_active: entity.is_active,
            created_at: entity.created_at,
            updated_at: entity.updated_at,
        };
    }

    async updateZoomGroup(id: string, dto: UpdateZoomGroupDto) {
        await this.ensureZoomGroupsBootstrap();
        const group = await this.zoomGroupsRepo.findOne({ where: { id: String(id ?? '').trim() } });
        if (!group) {
            throw new BadRequestException('No se encontro el grupo Zoom.');
        }
        const nextName = dto.name !== undefined ? String(dto.name ?? '').trim() : group.name;
        if (!nextName) {
            throw new BadRequestException('name no puede quedar vacio.');
        }
        const nextCode =
            dto.code !== undefined || dto.name !== undefined
                ? this.normalizeZoomGroupCode(dto.code, nextName)
                : group.code;
        const isDefaultGroup =
            group.code === DEFAULT_ZOOM_GROUP_REGULAR_CODE ||
            group.code === DEFAULT_ZOOM_GROUP_HIBRIDO_CODE;
        if (isDefaultGroup && nextCode !== group.code) {
            throw new BadRequestException('No se puede cambiar el codigo de un grupo Zoom por defecto.');
        }
        const duplicated = await this.zoomGroupsRepo.findOne({ where: { code: nextCode } });
        if (duplicated && duplicated.id !== group.id) {
            throw new BadRequestException(`Ya existe un grupo Zoom con codigo ${nextCode}.`);
        }

        group.name = nextName;
        group.code = nextCode;
        if (dto.is_active !== undefined) {
            group.is_active = Boolean(dto.is_active);
        }
        group.updated_at = new Date();
        await this.zoomGroupsRepo.save(group);
        return {
            id: group.id,
            name: group.name,
            code: group.code,
            is_active: group.is_active,
            created_at: group.created_at,
            updated_at: group.updated_at,
        };
    }

    async deleteZoomGroup(id: string) {
        await this.ensureZoomGroupsBootstrap();
        const group = await this.zoomGroupsRepo.findOne({ where: { id: String(id ?? '').trim() } });
        if (!group) {
            throw new BadRequestException('No se encontro el grupo Zoom.');
        }
        if (
            group.code === DEFAULT_ZOOM_GROUP_REGULAR_CODE ||
            group.code === DEFAULT_ZOOM_GROUP_HIBRIDO_CODE
        ) {
            throw new BadRequestException('No se puede eliminar un grupo Zoom por defecto.');
        }
        await this.zoomGroupUsersRepo.delete({ group_id: group.id });
        await this.zoomGroupsRepo.delete({ id: group.id });
        return { success: true, id: group.id };
    }

    // ─── Host Rules (Cursos Especiales) ──────────────────────────────────────

    async listHostRules() {
        const rows = await this.hostRulesRepo
            .createQueryBuilder('rule')
            .leftJoin(ZoomUserEntity, 'zoom_user', 'zoom_user.id = rule.zoom_user_id')
            .leftJoin(ZoomGroupEntity, 'zoom_group', 'zoom_group.id = rule.zoom_group_id')
            .innerJoin(PlanningSubsectionScheduleEntity, 'sched', 'sched.id = rule.schedule_id')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = sched.planning_subsection_id')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'offer', 'offer.id = sec.planning_offer_id')
            .select('rule.id', 'id')
            .addSelect('rule.schedule_id', 'schedule_id')
            .addSelect('rule.zoom_group_id', 'zoom_group_id')
            .addSelect('zoom_group.name', 'zoom_group_name')
            .addSelect('rule.zoom_user_id', 'zoom_user_id')
            .addSelect('zoom_user.email', 'zoom_user_email')
            .addSelect('zoom_user.name', 'zoom_user_name')
            .addSelect('rule.notes', 'notes')
            .addSelect('rule.is_active', 'is_active')
            .addSelect('rule.lock_host', 'lock_host')
            .addSelect('rule.skip_zoom', 'skip_zoom')
            .addSelect('rule.created_at', 'created_at')
            .addSelect('rule.updated_at', 'updated_at')
            .addSelect('sec.id', 'section_id')
            .addSelect('sec.code', 'section_code')
            .addSelect('offer.vc_course_id', 'course_id')
            .addSelect('offer.course_code', 'course_code')
            .addSelect('offer.course_name', 'course_name')
            .orderBy('offer.course_code', 'ASC')
            .addOrderBy('sec.code', 'ASC')
            .getRawMany<Record<string, unknown>>();
        return rows.map((r) => ({
            id: readString(r.id),
            schedule_id: readString(r.schedule_id),
            zoom_group_id: readNullableString(r.zoom_group_id),
            zoom_group_name: readNullableString(r.zoom_group_name),
            zoom_user_id: readNullableString(r.zoom_user_id),
            zoom_user_email: readNullableString(r.zoom_user_email),
            zoom_user_name: readNullableString(r.zoom_user_name),
            notes: readNullableString(r.notes),
            is_active: Boolean(r.is_active),
            lock_host: r.lock_host === true || r.lock_host === 1,
            skip_zoom: r.skip_zoom === true || r.skip_zoom === 1,
            created_at: r.created_at as Date,
            updated_at: r.updated_at as Date,
            section_id: readString(r.section_id),
            section_code: readString(r.section_code),
            course_id: readNullableString(r.course_id),
            course_label: [readNullableString(r.course_code), readNullableString(r.course_name)].filter(Boolean).join(' - ') || null,
        }));
    }

    async createHostRule(dto: CreateHostRuleDto) {
        const scheduleId = String(dto.scheduleId ?? '').trim();
        const skipZoom = dto.skipZoom === true;
        const zoomGroupId = String(dto.zoomGroupId ?? '').trim() || null;
        const zoomUserId = String(dto.zoomUserId ?? '').trim() || null;
        if (!scheduleId) {
            throw new BadRequestException('scheduleId es requerido.');
        }
        const existing = await this.hostRulesRepo.findOne({ where: { schedule_id: scheduleId } });
        if (existing) {
            throw new BadRequestException(
                `Ya existe una regla de host para el schedule ${scheduleId}. Use PATCH para actualizarla.`,
            );
        }
        const now = new Date();
        const rule = this.hostRulesRepo.create({
            id: newId(),
            schedule_id: scheduleId,
            zoom_group_id: skipZoom ? null : zoomGroupId,
            zoom_user_id: skipZoom ? null : zoomUserId,
            notes: dto.notes?.trim() || null,
            is_active: true,
            lock_host: dto.lockHost === true,
            skip_zoom: skipZoom,
            created_at: now,
            updated_at: now,
        });
        await this.hostRulesRepo.save(rule);
        return { id: rule.id, schedule_id: rule.schedule_id };
    }

    async updateHostRule(id: string, dto: UpdateHostRuleDto) {
        const rule = await this.hostRulesRepo.findOne({ where: { id: String(id ?? '').trim() } });
        if (!rule) {
            throw new BadRequestException('No se encontro la regla de host.');
        }
        if (dto.zoomGroupId !== undefined) {
            rule.zoom_group_id = String(dto.zoomGroupId ?? '').trim() || null;
        }
        if (dto.zoomUserId !== undefined) {
            rule.zoom_user_id = String(dto.zoomUserId ?? '').trim() || null;
        }
        if (dto.notes !== undefined) {
            rule.notes = dto.notes?.trim() || null;
        }
        if (dto.isActive !== undefined) {
            rule.is_active = Boolean(dto.isActive);
        }
        if (dto.lockHost !== undefined) {
            rule.lock_host = Boolean(dto.lockHost);
        }
        if (dto.skipZoom !== undefined) {
            rule.skip_zoom = Boolean(dto.skipZoom);
            if (rule.skip_zoom) {
                rule.zoom_group_id = null;
                rule.zoom_user_id = null;
            }
        }
        rule.updated_at = new Date();
        await this.hostRulesRepo.save(rule);
        return { id: rule.id, schedule_id: rule.schedule_id };
    }

    async deleteHostRule(id: string) {
        const rule = await this.hostRulesRepo.findOne({ where: { id: String(id ?? '').trim() } });
        if (!rule) {
            throw new BadRequestException('No se encontro la regla de host.');
        }
        await this.hostRulesRepo.delete({ id: rule.id });
        return { success: true, id: rule.id };
    }

    async getActiveHostRuleMap(): Promise<Map<string, { rule_id: string; zoom_user_id: string | null; zoom_user_email: string | null; zoom_user_name: string | null; zoom_group_id: string | null; zoom_group_name: string | null; lock_host: boolean; skip_zoom: boolean }>> {
        const rows = await this.hostRulesRepo
            .createQueryBuilder('rule')
            .leftJoin(ZoomUserEntity, 'zoom_user', 'zoom_user.id = rule.zoom_user_id')
            .leftJoin(ZoomGroupEntity, 'zoom_group', 'zoom_group.id = rule.zoom_group_id')
            .select('rule.id', 'id')
            .addSelect('rule.schedule_id', 'schedule_id')
            .addSelect('rule.zoom_group_id', 'zoom_group_id')
            .addSelect('zoom_group.name', 'zoom_group_name')
            .addSelect('rule.zoom_user_id', 'zoom_user_id')
            .addSelect('zoom_user.email', 'zoom_user_email')
            .addSelect('zoom_user.name', 'zoom_user_name')
            .addSelect('rule.lock_host', 'lock_host')
            .addSelect('rule.skip_zoom', 'skip_zoom')
            .where('rule.is_active = true')
            .getRawMany<Record<string, unknown>>();
        const map = new Map<string, { rule_id: string; zoom_user_id: string | null; zoom_user_email: string | null; zoom_user_name: string | null; zoom_group_id: string | null; zoom_group_name: string | null; lock_host: boolean; skip_zoom: boolean }>();
        for (const r of rows) {
            map.set(readString(r.schedule_id), {
                rule_id: readString(r.id),
                zoom_user_id: readNullableString(r.zoom_user_id),
                zoom_user_email: readNullableString(r.zoom_user_email),
                zoom_user_name: readNullableString(r.zoom_user_name),
                zoom_group_id: readNullableString(r.zoom_group_id),
                zoom_group_name: readNullableString(r.zoom_group_name),
                lock_host: r.lock_host === true || r.lock_host === 1,
                skip_zoom: r.skip_zoom === true || r.skip_zoom === 1,
            });
        }
        return map;
    }

    async getZoomGroupPool(groupId: string) {
        await this.ensureZoomGroupsBootstrap();
        const group = await this.getZoomGroupOrFail(groupId);
        const poolRows = await this.zoomGroupUsersRepo
            .createQueryBuilder('group_user')
            .leftJoin(ZoomUserEntity, 'zoom_user', 'zoom_user.id = group_user.zoom_user_id')
            .select('group_user.id', 'pool_id')
            .addSelect('group_user.zoom_user_id', 'zoom_user_id')
            .addSelect('group_user.sort_order', 'sort_order')
            .addSelect('group_user.is_active', 'is_active')
            .addSelect('zoom_user.name', 'name')
            .addSelect('zoom_user.email', 'email')
            .where('group_user.group_id = :groupId', { groupId: group.id })
            .orderBy('group_user.sort_order', 'ASC')
            .addOrderBy('zoom_user.name', 'ASC')
            .getRawMany<Record<string, unknown>>();

        const users = await this.zoomUsersRepo.find({
            order: { name: 'ASC', email: 'ASC' },
        });
        const licenseSnapshot = await this.loadZoomPoolLicenseSnapshot();
        const selectedIds = new Set(poolRows.map((row) => readString(row.zoom_user_id)));

        return {
            group: {
                id: group.id,
                name: group.name,
                code: group.code,
                is_active: group.is_active,
            },
            items: poolRows.map((row) => ({
                id: readString(row.pool_id),
                zoom_user_id: readString(row.zoom_user_id),
                sort_order: readNumber(row.sort_order),
                is_active: Boolean(row.is_active),
                name: readNullableString(row.name),
                email: readNullableString(row.email),
                ...this.resolveZoomLicenseMetadata(readNullableString(row.email), licenseSnapshot, readNullableString(row.zoom_user_id)),
            })),
            users: users.map((user) => ({
                id: user.id,
                name: user.name,
                email: user.email,
                in_pool: selectedIds.has(user.id),
                ...this.resolveZoomLicenseMetadata(user.email, licenseSnapshot, user.id),
            })),
            license_sync_ok: licenseSnapshot.ok,
            license_sync_error: licenseSnapshot.error,
        };
    }

    async replaceZoomGroupPool(groupId: string, dto: UpdateZoomPoolDto) {
        await this.ensureZoomGroupsBootstrap();
        const group = await this.getZoomGroupOrFail(groupId);
        const normalizedItems = dto.items.map((item) => ({
            zoom_user_id: item.zoom_user_id.trim(),
            sort_order: item.sort_order,
            is_active: item.is_active,
        }));
        const uniqueIds = new Set<string>();
        for (const item of normalizedItems) {
            if (!item.zoom_user_id) {
                throw new BadRequestException('Cada item del pool requiere zoom_user_id.');
            }
            if (uniqueIds.has(item.zoom_user_id)) {
                throw new BadRequestException(`zoom_user_id duplicado en pool: ${item.zoom_user_id}`);
            }
            uniqueIds.add(item.zoom_user_id);
        }

        if (normalizedItems.length) {
            const users = await this.zoomUsersRepo.find({
                where: { id: In(normalizedItems.map((item) => item.zoom_user_id)) },
            });
            const foundIds = new Set(users.map((item) => item.id));
            const missing = normalizedItems
                .map((item) => item.zoom_user_id)
                .filter((id) => !foundIds.has(id));
            if (missing.length) {
                throw new BadRequestException(
                    `No existen los siguientes usuarios Zoom: ${missing.join(', ')}`,
                );
            }
        }

        await this.zoomGroupUsersRepo.delete({ group_id: group.id });
        if (normalizedItems.length) {
            const now = new Date();
            await this.zoomGroupUsersRepo.save(
                normalizedItems.map((item) =>
                    this.zoomGroupUsersRepo.create({
                        id: newId(),
                        group_id: group.id,
                        zoom_user_id: item.zoom_user_id,
                        sort_order: item.sort_order,
                        is_active: item.is_active,
                        created_at: now,
                        updated_at: now,
                    }),
                ),
            );
        }

        return this.getZoomGroupPool(group.id);
    }

    async getZoomPool() {
        await this.ensureZoomGroupsBootstrap();
        const defaultGroup = await this.getDefaultRegularZoomGroup();
        const response = await this.getZoomGroupPool(defaultGroup.id);
        return {
            items: response.items,
            users: response.users,
            license_sync_ok: response.license_sync_ok,
            license_sync_error: response.license_sync_error,
        };
    }

    async replaceZoomPool(dto: UpdateZoomPoolDto) {
        await this.ensureZoomGroupsBootstrap();
        const defaultGroup = await this.getDefaultRegularZoomGroup();
        const response = await this.replaceZoomGroupPool(defaultGroup.id, dto);
        return {
            items: response.items,
            users: response.users,
            license_sync_ok: response.license_sync_ok,
            license_sync_error: response.license_sync_error,
        };
    }

    private async ensureZoomGroupsBootstrap() {
        if (!this.zoomGroupBootstrapPromise) {
            this.zoomGroupBootstrapPromise = this.runZoomGroupsBootstrap();
        }
        await this.zoomGroupBootstrapPromise;
    }

    private async runZoomGroupsBootstrap() {
        const now = new Date();
        const existing = await this.zoomGroupsRepo.find();
        const byCode = new Map(existing.map((item) => [item.code, item] as const));

        const ensureGroup = async (code: string, name: string) => {
            const found = byCode.get(code);
            if (found) {
                return found;
            }
            const created = this.zoomGroupsRepo.create({
                id: newId(),
                name,
                code,
                is_active: true,
                created_at: now,
                updated_at: now,
            });
            await this.zoomGroupsRepo.save(created);
            byCode.set(code, created);
            return created;
        };

        const regularGroup = await ensureGroup(DEFAULT_ZOOM_GROUP_REGULAR_CODE, 'Grupo Regular');
        await ensureGroup(DEFAULT_ZOOM_GROUP_HIBRIDO_CODE, 'Grupo Hibrido');

        const regularMembersCount = await this.zoomGroupUsersRepo.count({
            where: { group_id: regularGroup.id },
        });
        if (regularMembersCount > 0) {
            return;
        }

        const legacyPoolRows = await this.zoomPoolRepo.find({
            order: { sort_order: 'ASC', updated_at: 'DESC', created_at: 'DESC' },
        });
        if (!legacyPoolRows.length) {
            return;
        }
        await this.zoomGroupUsersRepo.save(
            legacyPoolRows.map((row, index) =>
                this.zoomGroupUsersRepo.create({
                    id: newId(),
                    group_id: regularGroup.id,
                    zoom_user_id: row.zoom_user_id,
                    sort_order: Number(row.sort_order ?? index + 1) || index + 1,
                    is_active: Boolean(row.is_active),
                    created_at: now,
                    updated_at: now,
                }),
            ),
        );
    }

    private normalizeZoomGroupCode(inputCode: string | null | undefined, fallbackName?: string | null) {
        const raw = `${inputCode ?? ''}`.trim() || `${fallbackName ?? ''}`.trim();
        if (!raw) {
            throw new BadRequestException('code es requerido para el grupo Zoom.');
        }
        const normalized = raw
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .toUpperCase()
            .replace(/[^A-Z0-9]+/g, '_')
            .replace(/^_+|_+$/g, '')
            .slice(0, 40);
        if (!normalized) {
            throw new BadRequestException('code de grupo Zoom invalido.');
        }
        return normalized;
    }

    private async getZoomGroupOrFail(id: string) {
        const normalizedId = String(id ?? '').trim();
        if (!normalizedId) {
            throw new BadRequestException('zoomGroupId es requerido.');
        }
        const group = await this.zoomGroupsRepo.findOne({ where: { id: normalizedId } });
        if (!group) {
            throw new BadRequestException('No se encontro el grupo Zoom seleccionado.');
        }
        return group;
    }

    private async getDefaultRegularZoomGroup() {
        const group = await this.zoomGroupsRepo.findOne({
            where: { code: DEFAULT_ZOOM_GROUP_REGULAR_CODE },
        });
        if (!group) {
            throw new BadRequestException('No existe el grupo Zoom REGULAR.');
        }
        return group;
    }

    async upsertOverride(dto: UpsertVideoconferenceOverrideDto) {
        const scheduleId = dto.scheduleId.trim();
        if (!scheduleId) {
            throw new BadRequestException('scheduleId es requerido.');
        }
        const inheritance = await this.inheritanceRepo.findOne({
            where: { child_schedule_id: scheduleId, is_active: true },
        });
        if (inheritance?.parent_schedule_id) {
            throw new BadRequestException(
                `Este horario hereda Zoom del horario padre ${inheritance.parent_schedule_id}. El override debe configurarse en el padre.`,
            );
        }
        const conferenceDate = normalizeIsoDate(dto.conferenceDate);
        const action = String(dto.action ?? '').trim().toUpperCase() as StoredOccurrenceOverride['action'];
        if (!['KEEP', 'SKIP', 'RESCHEDULE'].includes(action)) {
            throw new BadRequestException('action debe ser KEEP, SKIP o RESCHEDULE.');
        }

        const schedule = await this.schedulesRepo.findOne({ where: { id: scheduleId } });
        if (!schedule) {
            throw new BadRequestException('No se encontro el horario base para el override.');
        }

        let overrideDate: string | null = null;
        let overrideStartTime: string | null = null;
        let overrideEndTime: string | null = null;
        if (action === 'RESCHEDULE') {
            overrideDate = normalizeIsoDate(dto.overrideDate ?? '');
            overrideStartTime = normalizeTimeValue(dto.overrideStartTime ?? '');
            overrideEndTime = normalizeTimeValue(dto.overrideEndTime ?? '');
            if (!overrideDate || !overrideStartTime || !overrideEndTime) {
                throw new BadRequestException('La reprogramacion requiere fecha, hora inicio y hora fin.');
            }
            if (overrideStartTime >= overrideEndTime) {
                throw new BadRequestException('La hora inicio debe ser menor que la hora fin.');
            }
        }

        const now = new Date();
        const current = await this.planningVideoconferenceOverridesRepo.findOne({
            where: {
                planning_subsection_schedule_id: scheduleId,
                conference_date: conferenceDate,
            },
        });
        const entity = current
            ? this.planningVideoconferenceOverridesRepo.merge(current, {
                action,
                override_date: overrideDate,
                override_start_time: overrideStartTime,
                override_end_time: overrideEndTime,
                reason_code: normalizeOverrideReason(dto.reasonCode),
                notes: emptyTextToNull(dto.notes),
                updated_at: now,
            })
            : this.planningVideoconferenceOverridesRepo.create({
                id: newId(),
                planning_subsection_schedule_id: scheduleId,
                conference_date: conferenceDate,
                action,
                override_date: overrideDate,
                override_start_time: overrideStartTime,
                override_end_time: overrideEndTime,
                reason_code: normalizeOverrideReason(dto.reasonCode),
                notes: emptyTextToNull(dto.notes),
                created_at: now,
                updated_at: now,
            });

        const saved = await this.planningVideoconferenceOverridesRepo.save(entity);
        return this.serializeOverride(saved);
    }

    async deleteOverride(scheduleIdRaw: string, conferenceDateRaw: string) {
        const scheduleId = String(scheduleIdRaw ?? '').trim();
        const conferenceDate = normalizeIsoDate(String(conferenceDateRaw ?? '').trim());
        if (!scheduleId) {
            throw new BadRequestException('scheduleId es requerido.');
        }
        const inheritance = await this.inheritanceRepo.findOne({
            where: { child_schedule_id: scheduleId, is_active: true },
        });
        if (inheritance?.parent_schedule_id) {
            throw new BadRequestException(
                `Este horario hereda Zoom del horario padre ${inheritance.parent_schedule_id}. El override debe administrarse en el padre.`,
            );
        }

        await this.planningVideoconferenceOverridesRepo.delete({
            planning_subsection_schedule_id: scheduleId,
            conference_date: conferenceDate,
        });

        return {
            success: true,
            schedule_id: scheduleId,
            conference_date: conferenceDate,
        };
    }

    async generate(payload: GenerateVideoconferenceDto) {
        const zoomGroupId = String(payload.zoomGroupId ?? '').trim();
        if (!zoomGroupId) {
            throw new BadRequestException('zoomGroupId es requerido.');
        }
        const scheduleIds = normalizeIdArray(payload.scheduleIds);
        const occurrenceKeys = normalizeIdArray(payload.occurrenceKeys);
        if (!scheduleIds.length && !occurrenceKeys.length) {
            throw new BadRequestException('Debe enviar al menos un scheduleId u occurrenceKey.');
        }

        const startDate = normalizeIsoDate(payload.startDate);
        const endDate = normalizeIsoDate(payload.endDate);
        if (startDate > endDate) {
            throw new BadRequestException('startDate no puede ser mayor que endDate.');
        }

        const requestedScheduleIds = scheduleIds.length
            ? scheduleIds
            : Array.from(new Set(occurrenceKeys.map((item) => parseOccurrenceKey(item).schedule_id).filter(Boolean)));
        const expanded = await this.buildExpandedScheduleContext(requestedScheduleIds);
        const requestedRows = expanded.requestedRows;
        const missingScheduleIds = requestedScheduleIds.filter(
            (scheduleId) => !requestedRows.some((row) => row.schedule_id === scheduleId),
        );
        const rows = expanded.rows;
        const rowMap = new Map(rows.map((row) => [row.schedule_id, row] as const));
        const inheritanceIndex = await this.buildOperationalInheritanceIndex(Array.from(rowMap.values()));
        const preferredHosts = this.normalizePreferredHosts(payload.preferredHosts);

        const aulaVirtualContext = await this.getAulaVirtualRequestContext();
        const zoomConfig = await this.zoomAccountService.requireConfiguredConfig();
        const poolValidation = await this.getActiveZoomPoolUsers(
            zoomGroupId,
            Boolean(payload.allowPoolWarnings),
        );
        const zoomUsers = poolValidation.users;
        if (!zoomUsers.length) {
            throw new BadRequestException(
                `No hay usuarios Zoom activos en el grupo ${poolValidation.group.name}.`,
            );
        }

        const resolvedOccurrences = await this.resolveOccurrences(
            Array.from(rowMap.values()),
            startDate,
            endDate,
            zoomConfig.timezone,
            inheritanceIndex,
        );
        const selectedOccurrences = occurrenceKeys.length
            ? resolvedOccurrences.filter((item) => occurrenceKeys.includes(item.occurrence_key))
            : resolvedOccurrences.filter(
                (item) =>
                    requestedScheduleIds.includes(item.row.schedule_id) && item.occurrence_type !== 'SKIPPED',
            );
        const familyKeys = Array.from(
            new Set(
                selectedOccurrences.map((item) =>
                    buildOccurrenceKey(item.inheritance.family_owner_schedule_id, item.base_conference_date),
                ),
            ),
        );
        const familyOccurrences = familyKeys.flatMap((familyKey) =>
            resolvedOccurrences.filter(
                (item) =>
                    buildOccurrenceKey(item.inheritance.family_owner_schedule_id, item.base_conference_date) === familyKey &&
                    item.occurrence_type !== 'SKIPPED',
            ),
        );
        const remoteMeetingsCache = new Map<string, ZoomMeetingSummary[] | null>();
        const results: GenerateResultItem[] = [];

        for (const missingScheduleId of missingScheduleIds) {
            results.push({
                schedule_id: missingScheduleId,
                occurrence_key: null,
                conference_date: null,
                status: 'ERROR',
                message: 'No se encontro el horario seleccionado en planificacion.',
                record_id: null,
                zoom_user_id: null,
                zoom_user_email: null,
                zoom_meeting_id: null,
                link_mode: 'OWNED',
                owner_videoconference_id: null,
                inheritance: buildDefaultInheritance(missingScheduleId),
            });
        }

        for (const familyKey of familyKeys) {
            const familyItems = familyOccurrences.filter(
                (item) =>
                    buildOccurrenceKey(item.inheritance.family_owner_schedule_id, item.base_conference_date) === familyKey,
            );
            const ownerOccurrence =
                familyItems.find((item) => !item.inheritance.is_inherited) ?? null;
            if (!ownerOccurrence) {
                continue;
            }

            // Merge the full continuous block end time into the owner occurrence,
            // exactly as the assignment preview does, so that the payload sent to
            // Aula Virtual reflects the combined block duration and not only the
            // first schedule's end time.
            const maxEndTime = familyItems.reduce(
                (max, occ) => (compactTime(occ.effective_end_time) > compactTime(max) ? occ.effective_end_time : max),
                ownerOccurrence.effective_end_time,
            );
            const maxScheduledEnd = familyItems.reduce(
                (max, occ) => (occ.scheduled_end > max ? occ.scheduled_end : max),
                ownerOccurrence.scheduled_end,
            );
            ownerOccurrence.effective_end_time = maxEndTime;
            ownerOccurrence.scheduled_end = maxScheduledEnd;

            const ownerResult = await this.generateOccurrence(
                ownerOccurrence,
                aulaVirtualContext,
                zoomConfig.maxConcurrent,
                zoomUsers,
                remoteMeetingsCache,
                this.findPreferredZoomUserId(ownerOccurrence, preferredHosts),
                poolValidation.group.name,
            );
            results.push(ownerResult);

            // Brief pause after each successful creation attempt to respect Aula Virtual
            // and Zoom rate limits during bulk operations. Skipped when blocked/validation/error.
            if (ownerResult.status === 'MATCHED' || ownerResult.status === 'CREATED_UNMATCHED') {
                await new Promise((resolve) => setTimeout(resolve, AULA_VIRTUAL_THROTTLE_MS));
            }

            const ownerRecord =
                ownerResult.record_id
                    ? await this.planningVideoconferencesRepo.findOne({ where: { id: ownerResult.record_id } })
                    : null;
            for (const childOccurrence of familyItems.filter((item) => item.inheritance.is_inherited)) {
                const inheritedResult = await this.upsertInheritedOccurrence(childOccurrence, ownerRecord, ownerResult);
                results.push(inheritedResult);
            }
        }

        const summary = {
            requestedSchedules: requestedScheduleIds.length,
            requestedOccurrences: familyOccurrences.length,
            matched: results.filter((item) => item.status === 'MATCHED').length,
            createdUnmatched: results.filter((item) => item.status === 'CREATED_UNMATCHED').length,
            blockedExisting: results.filter((item) => item.status === 'BLOCKED_EXISTING').length,
            noAvailableZoomUser: results.filter((item) => item.status === 'NO_AVAILABLE_ZOOM_USER').length,
            validationErrors: results.filter((item) => item.status === 'VALIDATION_ERROR').length,
            errors: results.filter((item) => item.status === 'ERROR').length,
        };

        return {
            success: true,
            message: buildGenerationMessage(summary, poolValidation.warning_count),
            summary,
            results,
            pool_warnings: poolValidation.warnings,
        };
    }

    async reconcile(id: string): Promise<ReconcileResult> {
        const recordId = String(id ?? '').trim();
        if (!recordId) {
            throw new BadRequestException('id es requerido.');
        }

        const record = await this.planningVideoconferencesRepo.findOne({
            where: { id: recordId },
        });
        if (!record) {
            throw new BadRequestException('No se encontro la videoconferencia a conciliar.');
        }

        if (record.link_mode === 'INHERITED' && record.owner_videoconference_id) {
            const owner = await this.planningVideoconferencesRepo.findOne({
                where: { id: record.owner_videoconference_id },
            });
            if (owner) {
                return {
                    success: true,
                    matched: owner.status === 'MATCHED',
                    message: 'Esta videoconferencia es heredada. La conciliacion se gestiona desde el horario padre.',
                    result: this.buildGenerationResultFromRecord(
                        record,
                        'Esta videoconferencia es heredada. La conciliacion se gestiona desde el horario padre.',
                    ),
                };
            }
        }

        if (record.zoom_meeting_id && record.status === 'MATCHED') {
            return {
                success: true,
                matched: true,
                message: 'La videoconferencia ya estaba conciliada con Zoom.',
                result: this.buildGenerationResultFromRecord(
                    record,
                    'La videoconferencia ya estaba conciliada con Zoom.',
                ),
            };
        }

        if (!record.zoom_user_email?.trim()) {
            throw new BadRequestException('La videoconferencia no tiene un usuario Zoom asociado.');
        }
        if (!record.topic?.trim()) {
            throw new BadRequestException('La videoconferencia no tiene topic para intentar la conciliacion.');
        }
        if (!record.scheduled_start) {
            throw new BadRequestException('La videoconferencia no tiene scheduled_start para intentar la conciliacion.');
        }

        const durationMinutes =
            calculateDurationMinutes(compactTime(record.start_time), compactTime(record.end_time));
        const matched = await this.matchZoomMeeting(
            record.zoom_user_email,
            record.topic,
            record.scheduled_start,
            durationMinutes,
        );

        if (matched) {
            record.zoom_meeting_id = matched.id;
            record.join_url = matched.join_url;
            record.start_url = matched.start_url;
            record.status = 'MATCHED';
            record.match_attempts = (record.match_attempts ?? 0) + matched.attempts;
            record.matched_at = new Date();
            record.error_message = null;
            record.updated_at = new Date();
            const saved = await this.planningVideoconferencesRepo.save(record);
            return {
                success: true,
                matched: true,
                message: 'Conciliacion completada. La reunion ya quedo enlazada con Zoom.',
                result: this.buildGenerationResultFromRecord(
                    saved,
                    'Conciliacion completada. La reunion ya quedo enlazada con Zoom.',
                ),
            };
        }

        record.status = record.zoom_meeting_id ? 'MATCHED' : 'CREATED_UNMATCHED';
        record.match_attempts = (record.match_attempts ?? 0) + ZOOM_MATCH_ATTEMPTS;
        record.updated_at = new Date();
        const saved = await this.planningVideoconferencesRepo.save(record);
        return {
            success: false,
            matched: false,
            message:
                'No se encontro una coincidencia unica en Zoom con el topic, hora y duracion esperados.',
            result: this.buildGenerationResultFromRecord(
                saved,
                'No se encontro una coincidencia unica en Zoom con el topic, hora y duracion esperados.',
            ),
        };
    }

    private async generateOccurrence(
        occurrence: ExpandedOccurrence,
        aulaVirtualContext: AulaVirtualRequestContext,
        maxConcurrent: number,
        zoomUsers: ZoomPoolUser[],
        remoteMeetingsCache: Map<string, ZoomMeetingSummary[] | null>,
        preferredZoomUserId: string | null,
        zoomGroupName: string,
    ): Promise<GenerateResultItem> {
        const validationError = this.validateScheduleForGeneration(occurrence.row);
        if (validationError) {
            return {
                schedule_id: occurrence.row.schedule_id,
                occurrence_key: occurrence.occurrence_key,
                conference_date: occurrence.effective_conference_date,
                status: 'VALIDATION_ERROR',
                message: validationError,
                record_id: null,
                zoom_user_id: null,
                zoom_user_email: null,
                zoom_meeting_id: null,
                link_mode: 'OWNED',
                owner_videoconference_id: null,
                inheritance: occurrence.inheritance,
            };
        }

        const existing = await this.planningVideoconferencesRepo.findOne({
            where: {
                planning_subsection_schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.effective_conference_date,
            },
        });
        if (existing) {
            return {
                schedule_id: occurrence.row.schedule_id,
                occurrence_key: occurrence.occurrence_key,
                conference_date: occurrence.effective_conference_date,
                status: 'BLOCKED_EXISTING',
                message: 'Ya existe un registro para este horario en la fecha indicada.',
                record_id: existing.id,
                zoom_user_id: existing.zoom_user_id,
                zoom_user_email: existing.zoom_user_email,
                zoom_meeting_id: existing.zoom_meeting_id,
                link_mode: existing.link_mode ?? 'OWNED',
                owner_videoconference_id: existing.owner_videoconference_id,
                inheritance: occurrence.inheritance,
            };
        }

        let selectedZoomUser: ZoomPoolUser | null = null;
        if (preferredZoomUserId) {
            selectedZoomUser = zoomUsers.find((item) => item.zoom_user_id === preferredZoomUserId) ?? null;
            if (!selectedZoomUser) {
                return {
                    schedule_id: occurrence.row.schedule_id,
                    occurrence_key: occurrence.occurrence_key,
                    conference_date: occurrence.effective_conference_date,
                    status: 'ERROR',
                    message: `El host sugerido (${preferredZoomUserId}) no pertenece al grupo Zoom "${zoomGroupName}" o esta inactivo.`,
                    record_id: null,
                    zoom_user_id: null,
                    zoom_user_email: null,
                    zoom_meeting_id: null,
                    link_mode: 'OWNED',
                    owner_videoconference_id: null,
                    inheritance: occurrence.inheritance,
                };
            }
        } else {
            selectedZoomUser = await this.findAvailableZoomUser(
                occurrence.scheduled_start,
                occurrence.scheduled_end,
                zoomUsers,
                maxConcurrent,
                remoteMeetingsCache,
            );
        }
        if (!selectedZoomUser) {
            return {
                schedule_id: occurrence.row.schedule_id,
                occurrence_key: occurrence.occurrence_key,
                conference_date: occurrence.effective_conference_date,
                status: 'NO_AVAILABLE_ZOOM_USER',
                message:
                    'No se encontro un usuario Zoom disponible para el horario con el margen definido.',
                record_id: null,
                zoom_user_id: null,
                zoom_user_email: null,
                zoom_meeting_id: null,
                link_mode: 'OWNED',
                owner_videoconference_id: null,
                inheritance: occurrence.inheritance,
            };
        }

        const teacher = resolveTeacher(occurrence.row);
        const topic = buildMeetingTopic(occurrence, teacher);
        const aulaVirtualPayload = this.buildAulaVirtualPayload(
            occurrence,
            selectedZoomUser.zoom_user_id,
            teacher,
            topic,
        );

        const now = new Date();
        let record = this.planningVideoconferencesRepo.create({
            id: newId(),
            planning_offer_id: occurrence.row.offer_id,
            planning_section_id: occurrence.row.section_id,
            planning_subsection_id: occurrence.row.subsection_id,
            planning_subsection_schedule_id: occurrence.row.schedule_id,
            conference_date: occurrence.effective_conference_date,
            day_of_week: dayCodeForDate(occurrence.effective_conference_date),
            start_time: compactTime(occurrence.effective_start_time),
            end_time: compactTime(occurrence.effective_end_time),
            scheduled_start: occurrence.scheduled_start,
            scheduled_end: occurrence.scheduled_end,
            zoom_user_id: selectedZoomUser.zoom_user_id,
            zoom_user_email: selectedZoomUser.email,
            zoom_user_name: selectedZoomUser.name,
            zoom_meeting_id: null,
            topic,
            aula_virtual_name: aulaVirtualPayload.name,
            join_url: null,
            start_url: null,
            link_mode: 'OWNED',
            owner_videoconference_id: null,
            inheritance_mapping_id: null,
            status: 'CREATING',
            match_attempts: 0,
            matched_at: null,
            error_message: null,
            payload_json: {
                ...aulaVirtualPayload,
                occurrence_key: occurrence.occurrence_key,
                occurrence_type: occurrence.occurrence_type,
                base_conference_date: occurrence.base_conference_date,
                effective_conference_date: occurrence.effective_conference_date,
                override_id: occurrence.override_id,
                override_reason_code: occurrence.override_reason_code,
                override_notes: occurrence.override_notes,
            },
            response_json: null,
            created_at: now,
            updated_at: now,
        });
        record = await this.planningVideoconferencesRepo.save(record);

        try {
            const aulaVirtualResponse = await this.postAulaVirtualConference(
                aulaVirtualContext,
                aulaVirtualPayload,
            );
            record.response_json = aulaVirtualResponse;
            record.status = 'CREATED_UNMATCHED';
            record.error_message = null;
            record.updated_at = new Date();
            await this.planningVideoconferencesRepo.save(record);
            // Zoom meeting ID matching is deferred — use "Sincronizar datos Zoom" in Auditoría.
            return {
                schedule_id: occurrence.row.schedule_id,
                occurrence_key: occurrence.occurrence_key,
                conference_date: occurrence.effective_conference_date,
                status: 'CREATED_UNMATCHED',
                message: 'Videoconferencia creada en Aula Virtual. Sincroniza el ID Zoom desde Auditoría.',
                record_id: record.id,
                zoom_user_id: record.zoom_user_id,
                zoom_user_email: record.zoom_user_email,
                zoom_meeting_id: null,
                link_mode: 'OWNED',
                owner_videoconference_id: null,
                inheritance: occurrence.inheritance,
            };
        } catch (error) {
            record.status = 'ERROR';
            record.error_message = toErrorMessage(error);
            record.response_json = {
                error: toErrorMessage(error),
            };
            record.updated_at = new Date();
            await this.planningVideoconferencesRepo.save(record);
            return {
                schedule_id: occurrence.row.schedule_id,
                occurrence_key: occurrence.occurrence_key,
                conference_date: occurrence.effective_conference_date,
                status: 'ERROR',
                message: toErrorMessage(error),
                record_id: record.id,
                zoom_user_id: record.zoom_user_id,
                zoom_user_email: record.zoom_user_email,
                zoom_meeting_id: record.zoom_meeting_id,
                link_mode: 'OWNED',
                owner_videoconference_id: null,
                inheritance: occurrence.inheritance,
            };
        }
    }

    private async upsertInheritedOccurrence(
        occurrence: ExpandedOccurrence,
        ownerRecord: PlanningSubsectionVideoconferenceEntity | null,
        ownerResult: GenerateResultItem,
    ): Promise<GenerateResultItem> {
        const now = new Date();
        const existing = await this.planningVideoconferencesRepo.findOne({
            where: {
                planning_subsection_schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.effective_conference_date,
            },
        });

        const status =
            ownerRecord?.status === 'ERROR'
                ? 'ERROR'
                : ownerRecord?.status ?? normalizeRecordStatusFromResult(ownerResult.status);
        const errorMessage =
            status === 'ERROR'
                ? ownerRecord?.error_message || 'No se pudo heredar porque el horario padre fallo.'
                : null;
        const joinUrl = ownerRecord?.join_url ?? null;
        const startUrl = ownerRecord?.start_url ?? null;
        const zoomMeetingId = ownerRecord?.zoom_meeting_id ?? null;
        const zoomUserId = ownerRecord?.zoom_user_id ?? ownerResult.zoom_user_id;
        const zoomUserEmail = ownerRecord?.zoom_user_email ?? ownerResult.zoom_user_email;
        const zoomUserName = ownerRecord?.zoom_user_name ?? null;
        const topic = ownerRecord?.topic ?? null;
        const aulaVirtualName = ownerRecord?.aula_virtual_name ?? null;
        const ownerId = ownerRecord?.id ?? ownerResult.record_id ?? null;
        const entity = existing
            ? this.planningVideoconferencesRepo.merge(existing, {
                planning_offer_id: occurrence.row.offer_id,
                planning_section_id: occurrence.row.section_id,
                planning_subsection_id: occurrence.row.subsection_id,
                scheduled_start: occurrence.scheduled_start,
                scheduled_end: occurrence.scheduled_end,
                day_of_week: dayCodeForDate(occurrence.effective_conference_date),
                start_time: compactTime(occurrence.effective_start_time),
                end_time: compactTime(occurrence.effective_end_time),
                zoom_user_id: zoomUserId ?? null,
                zoom_user_email: zoomUserEmail ?? null,
                zoom_user_name: zoomUserName,
                zoom_meeting_id: zoomMeetingId,
                topic,
                aula_virtual_name: aulaVirtualName,
                join_url: joinUrl,
                start_url: startUrl,
                link_mode: 'INHERITED',
                owner_videoconference_id: ownerId,
                inheritance_mapping_id: occurrence.inheritance.mapping_id,
                status,
                matched_at: ownerRecord?.matched_at ?? null,
                match_attempts: ownerRecord?.match_attempts ?? 0,
                error_message: errorMessage,
                payload_json: {
                    inherited_from_schedule_id: occurrence.inheritance.parent_schedule_id,
                    inherited_from_occurrence_key: occurrence.inheritance.parent_occurrence_key,
                    inherited_from_record_id: ownerId,
                    owner_status: ownerRecord?.status ?? ownerResult.status,
                    inheritance: occurrence.inheritance,
                },
                response_json: ownerRecord?.response_json ?? null,
                audit_sync_status: ownerRecord?.audit_sync_status ?? 'PENDING',
                audit_synced_at: ownerRecord?.audit_synced_at ?? null,
                audit_sync_error: ownerRecord?.audit_sync_error ?? null,
                updated_at: now,
            })
            : this.planningVideoconferencesRepo.create({
                id: newId(),
                planning_offer_id: occurrence.row.offer_id,
                planning_section_id: occurrence.row.section_id,
                planning_subsection_id: occurrence.row.subsection_id,
                planning_subsection_schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.effective_conference_date,
                day_of_week: dayCodeForDate(occurrence.effective_conference_date),
                start_time: compactTime(occurrence.effective_start_time),
                end_time: compactTime(occurrence.effective_end_time),
                scheduled_start: occurrence.scheduled_start,
                scheduled_end: occurrence.scheduled_end,
                zoom_user_id: zoomUserId ?? null,
                zoom_user_email: zoomUserEmail ?? null,
                zoom_user_name: zoomUserName,
                zoom_meeting_id: zoomMeetingId,
                topic,
                aula_virtual_name: aulaVirtualName,
                join_url: joinUrl,
                start_url: startUrl,
                link_mode: 'INHERITED',
                owner_videoconference_id: ownerId,
                inheritance_mapping_id: occurrence.inheritance.mapping_id,
                status,
                match_attempts: ownerRecord?.match_attempts ?? 0,
                matched_at: ownerRecord?.matched_at ?? null,
                error_message: errorMessage,
                payload_json: {
                    inherited_from_schedule_id: occurrence.inheritance.parent_schedule_id,
                    inherited_from_occurrence_key: occurrence.inheritance.parent_occurrence_key,
                    inherited_from_record_id: ownerId,
                    owner_status: ownerRecord?.status ?? ownerResult.status,
                    inheritance: occurrence.inheritance,
                },
                response_json: ownerRecord?.response_json ?? null,
                audit_sync_status: ownerRecord?.audit_sync_status ?? 'PENDING',
                audit_synced_at: ownerRecord?.audit_synced_at ?? null,
                audit_sync_error: ownerRecord?.audit_sync_error ?? null,
                created_at: now,
                updated_at: now,
            });

        const saved = await this.planningVideoconferencesRepo.save(entity);
        return {
            schedule_id: occurrence.row.schedule_id,
            occurrence_key: occurrence.occurrence_key,
            conference_date: occurrence.effective_conference_date,
            status: normalizeGenerationResultStatus(saved.status),
            message:
                saved.status === 'ERROR'
                    ? 'No se pudo heredar porque la videoconferencia del horario padre fallo.'
                    : saved.status === 'CREATED_UNMATCHED'
                        ? 'Hereda la videoconferencia del horario padre, pero el match con Zoom sigue pendiente.'
                        : 'Hereda la videoconferencia del horario padre.',
            record_id: saved.id,
            zoom_user_id: saved.zoom_user_id,
            zoom_user_email: saved.zoom_user_email,
            zoom_meeting_id: saved.zoom_meeting_id,
            link_mode: 'INHERITED',
            owner_videoconference_id: saved.owner_videoconference_id,
            inheritance: occurrence.inheritance,
        };
    }

    private buildGenerationResultFromRecord(
        record: PlanningSubsectionVideoconferenceEntity,
        message: string,
    ): GenerateResultItem {
        const payloadInheritance = readRecordInheritance(record.payload_json);
        return {
            schedule_id: record.planning_subsection_schedule_id,
            occurrence_key: buildOccurrenceKey(
                record.planning_subsection_schedule_id,
                record.conference_date,
            ),
            conference_date: record.conference_date,
            status: normalizeGenerationResultStatus(record.status),
            message,
            record_id: record.id,
            zoom_user_id: record.zoom_user_id,
            zoom_user_email: record.zoom_user_email,
            zoom_meeting_id: record.zoom_meeting_id,
            link_mode: record.link_mode ?? 'OWNED',
            owner_videoconference_id: record.owner_videoconference_id,
            inheritance:
                payloadInheritance ??
                buildDefaultInheritance(record.planning_subsection_schedule_id),
        };
    }

    private async getScheduleRows(filters?: FilterOptionsDto, scheduleIds?: string[]) {
        const qb = this.createScheduleBaseQuery()
            .select('offer.id', 'offer_id')
            .addSelect('offer.cycle', 'cycle')
            .addSelect('section.id', 'section_id')
            .addSelect('section.code', 'section_code')
            .addSelect('section.external_code', 'section_external_code')
            .addSelect('section.is_cepea', 'section_is_cepea')
            .addSelect('section.projected_vacancies', 'section_projected_vacancies')
            .addSelect('subsection.id', 'subsection_id')
            .addSelect('subsection.code', 'subsection_code')
            .addSelect('subsection.projected_vacancies', 'subsection_projected_vacancies')
            .addSelect('schedule.id', 'schedule_id')
            .addSelect('offer.campus_id', 'campus_id')
            .addSelect('offer.semester_id', 'semester_id')
            .addSelect('semester.name', 'semester_name')
            .addSelect('campus.name', 'campus_name')
            .addSelect('offer.faculty_id', 'faculty_id')
            .addSelect('faculty.name', 'faculty_name')
            .addSelect('offer.academic_program_id', 'program_id')
            .addSelect('program.name', 'program_name')
            .addSelect('offer.study_plan_course_id', 'course_id')
            .addSelect('offer.course_code', 'course_code')
            .addSelect('offer.course_name', 'course_name')
            .addSelect('offer.source_payload_json', 'offer_source_payload_json')
            .addSelect('offer.vc_period_id', 'vc_period_id')
            .addSelect('offer.vc_faculty_id', 'vc_faculty_id')
            .addSelect('offer.vc_academic_program_id', 'vc_academic_program_id')
            .addSelect('offer.vc_course_id', 'vc_course_id')
            .addSelect('subsection.vc_section_id', 'vc_section_id')
            .addSelect('vc_section.name', 'vc_section_name')
            .addSelect('section.source_payload_json', 'section_source_payload_json')
            .addSelect('responsible_teacher.id', 'responsible_teacher_id')
            .addSelect('responsible_teacher.name', 'responsible_teacher_name')
            .addSelect('responsible_teacher.full_name', 'responsible_teacher_full_name')
            .addSelect('responsible_teacher.dni', 'responsible_teacher_dni')
            .addSelect('section_teacher.id', 'section_teacher_id')
            .addSelect('section_teacher.name', 'section_teacher_name')
            .addSelect('section_teacher.full_name', 'section_teacher_full_name')
            .addSelect('section_teacher.dni', 'section_teacher_dni')
            .addSelect('subsection_modality.code', 'subsection_modality_code')
            .addSelect('subsection_modality.name', 'subsection_modality_name')
            .addSelect('section_modality.code', 'section_modality_code')
            .addSelect('section_modality.name', 'section_modality_name')
            .addSelect('schedule.day_of_week', 'day_of_week')
            .addSelect('schedule.start_time', 'start_time')
            .addSelect('schedule.end_time', 'end_time')
            .addSelect('schedule.duration_minutes', 'duration_minutes');

        this.applyScheduleFilters(qb, filters, scheduleIds);

        const rawRows = await qb
            .orderBy('campus.name', 'ASC')
            .addOrderBy('faculty.name', 'ASC')
            .addOrderBy('program.name', 'ASC')
            .addOrderBy('offer.course_name', 'ASC')
            .addOrderBy('section.code', 'ASC')
            .addOrderBy('subsection.code', 'ASC')
            .addOrderBy('schedule.day_of_week', 'ASC')
            .addOrderBy('schedule.start_time', 'ASC')
            .getRawMany<Record<string, unknown>>();

        const mappedRows = rawRows.map((row) => ({
            offer_id: readString(row.offer_id),
            cycle: readNullableNumber(row.cycle),
            section_id: readString(row.section_id),
            section_code: readString(row.section_code),
            section_external_code: readNullableString(row.section_external_code),
            section_is_cepea: Boolean(row.section_is_cepea),
            section_projected_vacancies: readNullableNumber(row.section_projected_vacancies),
            subsection_id: readString(row.subsection_id),
            subsection_code: readString(row.subsection_code),
            subsection_projected_vacancies: readNullableNumber(row.subsection_projected_vacancies),
            schedule_id: readString(row.schedule_id),
            campus_id: readNullableString(row.campus_id),
            semester_id: readNullableString(row.semester_id),
            semester_name: readNullableString(row.semester_name),
            campus_name: readNullableString(row.campus_name),
            faculty_id: readNullableString(row.faculty_id),
            faculty_name: readNullableString(row.faculty_name),
            program_id: readNullableString(row.program_id),
            program_name: readNullableString(row.program_name),
            course_id: readString(row.course_id),
            course_code: readNullableString(row.course_code),
            course_name: readNullableString(row.course_name),
            offer_source_payload_json: readNullableRecord(row.offer_source_payload_json),
            vc_period_id: readNullableString(row.vc_period_id),
            vc_faculty_id: readNullableString(row.vc_faculty_id),
            vc_academic_program_id: readNullableString(row.vc_academic_program_id),
            vc_course_id: readNullableString(row.vc_course_id),
            vc_section_id: readNullableString(row.vc_section_id),
            vc_section_name:
                readNullableString(row.vc_section_name) ??
                readNullableString(row.section_external_code),
            vc_faculty_name: null,
            vc_academic_program_name: null,
            vc_course_name: null,
            vc_source: null,
            vc_context_message: null,
            section_source_payload_json: readNullableRecord(row.section_source_payload_json),
            responsible_teacher_id: readNullableString(row.responsible_teacher_id),
            responsible_teacher_name: readNullableString(row.responsible_teacher_name),
            responsible_teacher_full_name: readNullableString(row.responsible_teacher_full_name),
            responsible_teacher_dni: readNullableString(row.responsible_teacher_dni),
            section_teacher_id: readNullableString(row.section_teacher_id),
            section_teacher_name: readNullableString(row.section_teacher_name),
            section_teacher_full_name: readNullableString(row.section_teacher_full_name),
            section_teacher_dni: readNullableString(row.section_teacher_dni),
            subsection_modality_code: readNullableString(row.subsection_modality_code),
            subsection_modality_name: readNullableString(row.subsection_modality_name),
            section_modality_code: readNullableString(row.section_modality_code),
            section_modality_name: readNullableString(row.section_modality_name),
            day_of_week: readString(row.day_of_week),
            start_time: compactTime(readString(row.start_time)),
            end_time: compactTime(readString(row.end_time)),
            duration_minutes: readNumber(row.duration_minutes) || calculateDurationMinutes(
                readString(row.start_time),
                readString(row.end_time),
            ),
        }));

        return this.resolveRowsWithAvContext(filterRowsByModality(mappedRows, filters?.modalities ?? filters?.modality));
    }

    private pickCatalogFilters(
        filters: FilterOptionsDto | undefined,
        allowedKeys: FilterOptionKey[],
    ): FilterOptionsDto | undefined {
        if (!filters) {
            return undefined;
        }

        const picked: FilterOptionsDto = {};
        const pickedRecord = picked as Record<FilterOptionKey, string | string[] | undefined>;
        for (const key of allowedKeys) {
            const value = filters[key];
            if (Array.isArray(value)) {
                if (value.length) {
                    pickedRecord[key] = [...value];
                }
                continue;
            }
            if (typeof value === 'string' && value.trim()) {
                pickedRecord[key] = value.trim();
            }
        }

        return Object.keys(picked).length ? picked : undefined;
    }

    private async getFilterOptionRows(filters?: FilterOptionsDto, ignoredFilters: FilterOptionKey[] = []) {
        const qb = this.createScheduleBaseQuery()
            .select('offer.semester_id', 'semester_id')
            .addSelect('semester.name', 'semester_name')
            .addSelect('offer.cycle', 'cycle')
            .addSelect('offer.campus_id', 'campus_id')
            .addSelect('campus.name', 'campus_name')
            .addSelect('offer.faculty_id', 'faculty_id')
            .addSelect('faculty.name', 'faculty_name')
            .addSelect('offer.academic_program_id', 'program_id')
            .addSelect('program.name', 'program_name')
            .addSelect('offer.study_plan_course_id', 'course_id')
            .addSelect('offer.course_code', 'course_code')
            .addSelect('offer.course_name', 'course_name')
            .addSelect('vc_section.name', 'vc_section_name')
            .addSelect('section.external_code', 'section_external_code')
            .addSelect('section.is_cepea', 'section_is_cepea')
            .addSelect('subsection_modality.code', 'subsection_modality_code')
            .addSelect('subsection_modality.name', 'subsection_modality_name')
            .addSelect('section_modality.code', 'section_modality_code')
            .addSelect('section_modality.name', 'section_modality_name')
            .addSelect('schedule.day_of_week', 'day_of_week');

        this.applyScheduleFilters(qb, filters, undefined, ignoredFilters);

        const rows = await qb.getRawMany<Record<string, unknown>>();
        const mappedRows = rows.map((row) => ({
            semester_id: readNullableString(row.semester_id),
            semester_name: readNullableString(row.semester_name),
            cycle: readNullableNumber(row.cycle),
            campus_id: readNullableString(row.campus_id),
            campus_name: readNullableString(row.campus_name),
            faculty_id: readNullableString(row.faculty_id),
            faculty_name: readNullableString(row.faculty_name),
            program_id: readNullableString(row.program_id),
            program_name: readNullableString(row.program_name),
            course_id: readNullableString(row.course_id),
            course_code: readNullableString(row.course_code),
            course_name: readNullableString(row.course_name),
            vc_section_name: readNullableString(row.vc_section_name),
            section_external_code: readNullableString(row.section_external_code),
            section_is_cepea: Boolean(row.section_is_cepea),
            subsection_modality_code: readNullableString(row.subsection_modality_code),
            subsection_modality_name: readNullableString(row.subsection_modality_name),
            section_modality_code: readNullableString(row.section_modality_code),
            section_modality_name: readNullableString(row.section_modality_name),
            day_of_week: readNullableString(row.day_of_week),
        }));

        if (!ignoredFilters.includes('modalities')) {
            return filterOptionRowsByModality(mappedRows, filters?.modalities ?? filters?.modality);
        }

        return mappedRows;
    }

    private createScheduleBaseQuery() {
        return this.schedulesRepo
            .createQueryBuilder('schedule')
            .innerJoin(
                PlanningSubsectionEntity,
                'subsection',
                'subsection.id = schedule.planning_subsection_id',
            )
            .innerJoin(
                PlanningSectionEntity,
                'section',
                'section.id = subsection.planning_section_id',
            )
            .innerJoin(PlanningOfferEntity, 'offer', 'offer.id = section.planning_offer_id')
            .leftJoin(SemesterEntity, 'semester', 'semester.id = offer.semester_id')
            .leftJoin(CampusEntity, 'campus', 'campus.id = offer.campus_id')
            .leftJoin(FacultyEntity, 'faculty', 'faculty.id = offer.faculty_id')
            .leftJoin(AcademicProgramEntity, 'program', 'program.id = offer.academic_program_id')
            .leftJoin(
                TeacherEntity,
                'responsible_teacher',
                'responsible_teacher.id = subsection.responsible_teacher_id',
            )
            .leftJoin(TeacherEntity, 'section_teacher', 'section_teacher.id = section.teacher_id')
            .leftJoin(
                CourseModalityEntity,
                'subsection_modality',
                'subsection_modality.id = subsection.course_modality_id',
            )
            .leftJoin(
                CourseModalityEntity,
                'section_modality',
                'section_modality.id = section.course_modality_id',
            )
            .leftJoin(VcSectionEntity, 'vc_section', 'vc_section.id = subsection.vc_section_id');
    }

    private applyScheduleFilters(
        qb: SelectQueryBuilder<PlanningSubsectionScheduleEntity>,
        filters?: FilterOptionsDto,
        scheduleIds?: string[],
        ignoredFilters: FilterOptionKey[] = [],
    ) {
        if (scheduleIds?.length) {
            qb.andWhere('schedule.id IN (:...scheduleIds)', { scheduleIds: normalizeIdArray(scheduleIds) });
        }
        if (!ignoredFilters.includes('semesterId') && filters?.semesterId?.trim()) {
            qb.andWhere('offer.semester_id = :semesterId', { semesterId: filters.semesterId.trim() });
        }
        if (!ignoredFilters.includes('campusIds') && filters?.campusIds?.length) {
            qb.andWhere('offer.campus_id IN (:...campusIds)', { campusIds: normalizeIdArray(filters.campusIds) });
        }
        if (!ignoredFilters.includes('facultyIds') && filters?.facultyIds?.length) {
            qb.andWhere('offer.faculty_id IN (:...facultyIds)', { facultyIds: normalizeIdArray(filters.facultyIds) });
        }
        if (!ignoredFilters.includes('programIds') && filters?.programIds?.length) {
            qb.andWhere('offer.academic_program_id IN (:...programIds)', {
                programIds: normalizeIdArray(filters.programIds),
            });
        }
        if (!ignoredFilters.includes('courseIds') && filters?.courseIds?.length) {
            qb.andWhere('offer.study_plan_course_id IN (:...courseIds)', {
                courseIds: normalizeIdArray(filters.courseIds),
            });
        }
        if (!ignoredFilters.includes('days') && filters?.days?.length) {
            qb.andWhere('schedule.day_of_week IN (:...days)', { days: normalizeIdArray(filters.days) });
        }
    }

    private buildNamedOptions(
        rows: FilterOptionRow[],
        idKey: keyof Pick<FilterOptionRow, 'campus_id' | 'faculty_id' | 'program_id'>,
        labelKey: keyof Pick<FilterOptionRow, 'campus_name' | 'faculty_name' | 'program_name'>,
        fallbackLabel: string,
    ) {
        const options = new Map<string, string>();
        for (const row of rows) {
            const id = row[idKey];
            if (!id) {
                continue;
            }
            options.set(id, row[labelKey] ?? fallbackLabel);
        }
        return sortCatalogOptions(options);
    }

    private buildPeriodOptions(rows: FilterOptionRow[]) {
        const options = new Map<string, string>();
        for (const row of rows) {
            if (!row.semester_id) {
                continue;
            }
            options.set(row.semester_id, row.semester_name ?? '(Sin periodo)');
        }
        return sortCatalogOptions(options);
    }

    private buildCourseOptions(rows: FilterOptionRow[]) {
        const options = new Map<string, string>();
        for (const row of rows) {
            if (!row.course_id) {
                continue;
            }
            options.set(row.course_id, buildCourseLabel(row.course_code, row.course_name));
        }
        return sortCatalogOptions(options);
    }

    private buildModalityOptions(rows: FilterOptionRow[]) {
        const options = new Map<string, string>();
        for (const row of rows) {
            const modality = resolveFilterRowModality(row);
            const modalityCode = normalizeModalityCode(modality.code);
            if (!modalityCode) {
                continue;
            }
            options.set(modalityCode, displayModality(modalityCode, modality.name));
        }
        return sortCatalogOptions(options);
    }

    private buildDayOptions(rows: FilterOptionRow[]) {
        const options = new Map<string, string>();
        for (const row of rows) {
            const dayCode = (row.day_of_week ?? '').trim().toUpperCase();
            if (!dayCode) {
                continue;
            }
            options.set(dayCode, displayDay(dayCode));
        }
        return [...options.entries()]
            .map(([id, label]) => ({ id, label }))
            .sort((left, right) => getDaySortOrder(left.id) - getDaySortOrder(right.id));
    }

    private async resolveRowsWithAvContext(rows: ScheduleContextRow[]) {
        if (!rows.length) {
            return rows;
        }

        const sourceFacultyIds = rows
            .map((row) => extractSourceFacultyIdFromPayload(row.offer_source_payload_json))
            .filter((value): value is string => Boolean(value));
        const sourceAcademicProgramIds = rows
            .map((row) => extractSourceAcademicProgramIdFromPayload(row.offer_source_payload_json))
            .filter((value): value is string => Boolean(value));
        const sourceCourseIds = rows
            .map((row) => extractSourceCourseIdFromPayload(row.offer_source_payload_json))
            .filter((value): value is string => Boolean(value));
        const sourceSectionIds = rows
            .map((row) => extractSourceSectionIdFromPayload(row.section_source_payload_json))
            .filter((value): value is string => Boolean(value));

        const [vcFaculties, vcAcademicPrograms, vcCourses, vcSections] = await Promise.all([
            this.findManyByIds(
                this.vcFacultiesRepo,
                uniqueIds([...rows.map((row) => row.vc_faculty_id), ...sourceFacultyIds]),
            ),
            this.findManyByIds(
                this.vcAcademicProgramsRepo,
                uniqueIds([...rows.map((row) => row.vc_academic_program_id), ...sourceAcademicProgramIds]),
            ),
            this.findManyByIds(
                this.vcCoursesRepo,
                uniqueIds([...rows.map((row) => row.vc_course_id), ...sourceCourseIds]),
            ),
            this.findManyByIds(
                this.vcSectionsRepo,
                uniqueIds([...rows.map((row) => row.vc_section_id), ...sourceSectionIds]),
            ),
        ]);

        const vcFacultyMap = mapById(vcFaculties);
        const vcAcademicProgramMap = mapById(vcAcademicPrograms);
        const vcCourseMap = mapById(vcCourses);
        const vcSectionMap = mapById(vcSections);

        return rows.map((row) => {
            const offerVcContext = readOfferVcContextMetadata(row.offer_source_payload_json);
            const sectionVcContext = readSectionVcContextMetadata(row.section_source_payload_json);
            const sourceCourse = vcCourseMap.get(offerVcContext.source_vc_course_id ?? '') ?? null;
            const sourceAcademicProgram =
                vcAcademicProgramMap.get(
                    sourceCourse?.program_id ?? offerVcContext.source_vc_academic_program_id ?? '',
                ) ?? null;
            const sourceFaculty =
                vcFacultyMap.get(
                    sourceAcademicProgram?.faculty_id ?? offerVcContext.source_vc_faculty_id ?? '',
                ) ?? null;
            const sourceSection = vcSectionMap.get(sectionVcContext.source_vc_section_id ?? '') ?? null;

            const currentCourse = vcCourseMap.get(row.vc_course_id ?? '') ?? null;
            const currentAcademicProgram =
                vcAcademicProgramMap.get(
                    currentCourse?.program_id ?? row.vc_academic_program_id ?? '',
                ) ?? null;
            const currentFaculty =
                vcFacultyMap.get(
                    currentAcademicProgram?.faculty_id ?? row.vc_faculty_id ?? '',
                ) ?? null;
            const currentSection = vcSectionMap.get(row.vc_section_id ?? '') ?? null;

            const useSourceContext =
                Boolean(sourceCourse || sourceAcademicProgram || sourceFaculty || sourceSection) &&
                (!currentCourse ||
                    !currentAcademicProgram ||
                    !currentFaculty ||
                    !currentSection ||
                    (sourceFaculty && currentFaculty && sourceFaculty.id !== currentFaculty.id) ||
                    (sourceAcademicProgram && currentAcademicProgram && sourceAcademicProgram.id !== currentAcademicProgram.id) ||
                    (sourceCourse && currentCourse && sourceCourse.id !== currentCourse.id) ||
                    (sourceSection && currentSection && sourceSection.id !== currentSection.id) ||
                    (currentAcademicProgram && currentFaculty && currentAcademicProgram.faculty_id !== currentFaculty.id) ||
                    (currentCourse && currentAcademicProgram && currentCourse.program_id !== currentAcademicProgram.id) ||
                    (currentSection && currentCourse && currentSection.course_id !== currentCourse.id));

            const effectiveCourse = useSourceContext ? sourceCourse : currentCourse;
            const effectiveAcademicProgram =
                useSourceContext
                    ? sourceAcademicProgram ?? (effectiveCourse ? vcAcademicProgramMap.get(effectiveCourse.program_id) ?? null : null)
                    : currentAcademicProgram;
            const effectiveFaculty =
                useSourceContext
                    ? sourceFaculty ?? (effectiveAcademicProgram ? vcFacultyMap.get(effectiveAcademicProgram.faculty_id) ?? null : null)
                    : currentFaculty;
            const effectiveSection = useSourceContext ? sourceSection : currentSection;

            return {
                ...row,
                vc_faculty_id: effectiveFaculty?.id ?? row.vc_faculty_id,
                vc_faculty_name: effectiveFaculty?.name ?? null,
                vc_academic_program_id: effectiveAcademicProgram?.id ?? row.vc_academic_program_id,
                vc_academic_program_name: effectiveAcademicProgram?.name ?? null,
                vc_course_id: effectiveCourse?.id ?? row.vc_course_id,
                vc_course_name: effectiveCourse?.name ?? null,
                vc_section_id: effectiveSection?.id ?? row.vc_section_id,
                vc_section_name: effectiveSection?.name ?? row.vc_section_name,
                vc_source: useSourceContext ? 'sync_source' : offerVcContext.vc_source,
                vc_context_message: useSourceContext
                    ? 'Contexto AV restaurado desde la sincronizacion.'
                    : offerVcContext.vc_context_message,
            };
        });
    }

    private async findManyByIds<T extends { id: string }>(
        repo: Repository<T>,
        ids: Array<string | null | undefined>,
    ) {
        const normalizedIds = uniqueIds(ids);
        if (!normalizedIds.length) {
            return [] as T[];
        }
        return repo.find({ where: { id: In(normalizedIds) } as never });
    }

    private serializeBasePreviewRow(
        row: ScheduleContextRow,
        inheritance: ScheduleInheritanceInfo,
        groupedScheduleIds: string[] = [row.schedule_id],
        groupedSubsectionLabels: string[] = [buildGroupLabel(row)],
    ) {
        const teacher = resolveTeacher(row);
        const modality = resolveModality(row);
        return {
            id: row.schedule_id,
            occurrence_key: `schedule:${row.schedule_id}`,
            schedule_id: row.schedule_id,
            grouped_schedule_ids: groupedScheduleIds,
            section_id: row.section_id,
            section_code: row.section_code,
            section_label: buildSectionLabel(row),
            subsection_id: row.subsection_id,
            subsection_code: row.subsection_code,
            subsection_label: groupedSubsectionLabels.filter(Boolean).join(' + ') || buildGroupLabel(row),
            campus_id: row.campus_id,
            campus_name: row.campus_name,
            faculty_id: row.faculty_id,
            faculty_name: row.faculty_name,
            program_id: row.program_id,
            program_name: row.program_name,
            course_id: row.course_id,
            cycle: row.cycle,
            course_code: row.course_code,
            course_name: row.course_name,
            course_label: buildCourseLabel(row.course_code, row.course_name),
            modality_code: modality.code,
            modality_name: modality.name,
            teacher_id: teacher.id,
            teacher_name: teacher.name,
            teacher_dni: teacher.dni,
            day_of_week: row.day_of_week,
            day_label: displayDay(row.day_of_week),
            start_time: compactTime(row.start_time),
            end_time: compactTime(row.end_time),
            duration_minutes: row.duration_minutes,
            vc_period_id: row.vc_period_id,
            vc_faculty_id: row.vc_faculty_id,
            vc_faculty_name: row.vc_faculty_name,
            vc_academic_program_id: row.vc_academic_program_id,
            vc_academic_program_name: row.vc_academic_program_name,
            vc_course_id: row.vc_course_id,
            vc_course_name: row.vc_course_name,
            vc_section_id: row.vc_section_id,
            vc_section_name: row.vc_section_name,
            vc_source: row.vc_source,
            vc_context_message: row.vc_context_message,
            occurrence_type: 'BASE' as const,
            base_conference_date: '',
            effective_conference_date: '',
            effective_start_time: compactTime(row.start_time),
            effective_end_time: compactTime(row.end_time),
            override_id: null,
            override_reason_code: null,
            override_notes: null,
            selectable: true,
            inheritance: {
                is_inherited: inheritance.is_inherited,
                mapping_id: inheritance.mapping?.id ?? null,
                parent_schedule_id: inheritance.parent_schedule_id,
                parent_occurrence_key: null,
                parent_label: inheritance.parent_label,
                family_owner_schedule_id: inheritance.family_owner_schedule_id,
            },
            section_projected_vacancies: row.section_projected_vacancies,
        };
    }

    private serializeOccurrencePreviewRow(
        occurrence: ExpandedOccurrence,
        groupedScheduleIds: string[] = [occurrence.row.schedule_id],
        groupedSubsectionLabels: string[] = [buildGroupLabel(occurrence.row)],
    ) {
        const row = occurrence.row;
        const teacher = resolveTeacher(row);
        const modality = resolveModality(row);
        return {
            id: occurrence.occurrence_key,
            occurrence_key: occurrence.occurrence_key,
            schedule_id: row.schedule_id,
            grouped_schedule_ids: groupedScheduleIds,
            section_id: row.section_id,
            section_code: row.section_code,
            section_label: buildSectionLabel(row),
            subsection_id: row.subsection_id,
            subsection_code: row.subsection_code,
            subsection_label: groupedSubsectionLabels.filter(Boolean).join(' + ') || buildGroupLabel(row),
            campus_id: row.campus_id,
            campus_name: row.campus_name,
            faculty_id: row.faculty_id,
            faculty_name: row.faculty_name,
            program_id: row.program_id,
            program_name: row.program_name,
            course_id: row.course_id,
            cycle: row.cycle,
            course_code: row.course_code,
            course_name: row.course_name,
            course_label: buildCourseLabel(row.course_code, row.course_name),
            modality_code: modality.code,
            modality_name: modality.name,
            teacher_id: teacher.id,
            teacher_name: teacher.name,
            teacher_dni: teacher.dni,
            day_of_week: dayCodeForDate(occurrence.effective_conference_date),
            day_label: displayDay(dayCodeForDate(occurrence.effective_conference_date)),
            start_time: compactTime(occurrence.effective_start_time),
            end_time: compactTime(occurrence.effective_end_time),
            duration_minutes: calculateDurationMinutes(
                occurrence.effective_start_time,
                occurrence.effective_end_time,
            ),
            vc_period_id: row.vc_period_id,
            vc_faculty_id: row.vc_faculty_id,
            vc_faculty_name: row.vc_faculty_name,
            vc_academic_program_id: row.vc_academic_program_id,
            vc_academic_program_name: row.vc_academic_program_name,
            vc_course_id: row.vc_course_id,
            vc_course_name: row.vc_course_name,
            vc_section_id: row.vc_section_id,
            vc_section_name: row.vc_section_name,
            vc_source: row.vc_source,
            vc_context_message: row.vc_context_message,
            occurrence_type: occurrence.occurrence_type,
            base_conference_date: occurrence.base_conference_date,
            effective_conference_date: occurrence.effective_conference_date,
            effective_start_time: compactTime(occurrence.effective_start_time),
            effective_end_time: compactTime(occurrence.effective_end_time),
            override_id: occurrence.override_id,
            override_reason_code: occurrence.override_reason_code,
            override_notes: occurrence.override_notes,
            selectable: occurrence.occurrence_type !== 'SKIPPED',
            inheritance: occurrence.inheritance,
            section_projected_vacancies: row.section_projected_vacancies,
        };
    }

    private async getActiveZoomPoolUsers(zoomGroupId: string, allowWarnings: boolean) {
        await this.ensureZoomGroupsBootstrap();
        const group = await this.getZoomGroupOrFail(zoomGroupId);
        if (!group.is_active) {
            throw new BadRequestException(
                `El grupo Zoom ${group.name} (${group.code}) esta inactivo.`,
            );
        }

        const rawRows = await this.zoomGroupUsersRepo
            .createQueryBuilder('group_user')
            .innerJoin(ZoomUserEntity, 'zoom_user', 'zoom_user.id = group_user.zoom_user_id')
            .select('group_user.id', 'pool_id')
            .addSelect('group_user.zoom_user_id', 'zoom_user_id')
            .addSelect('group_user.sort_order', 'sort_order')
            .addSelect('group_user.is_active', 'is_active')
            .addSelect('zoom_user.name', 'name')
            .addSelect('zoom_user.email', 'email')
            .where('group_user.group_id = :groupId', { groupId: group.id })
            .andWhere('group_user.is_active = :isActive', { isActive: true })
            .orderBy('group_user.sort_order', 'ASC')
            .addOrderBy('zoom_user.name', 'ASC')
            .getRawMany<Record<string, unknown>>();

        const rows = rawRows
            .map((row) => ({
                pool_id: readString(row.pool_id),
                zoom_user_id: readString(row.zoom_user_id),
                sort_order: readNumber(row.sort_order),
                is_active: Boolean(row.is_active),
                name: readNullableString(row.name),
                email: readNullableString(row.email),
            }))
            .filter((item) => Boolean(item.email));

        const licenseSnapshot = await this.loadZoomPoolLicenseSnapshot();

        if (!licenseSnapshot.ok) {
            const warnings = [
                `No se pudo validar licencias Zoom en tiempo real: ${licenseSnapshot.error ?? 'sin detalle'}.`,
            ];
            if (!allowWarnings) {
                throw new BadRequestException(`${warnings[0]} Confirma si deseas continuar de todos modos.`);
            }
            return {
                users: rows,
                warnings,
                warning_count: warnings.length,
                group,
            };
        }

        const usersWithLicense = rows.map((row) => ({
            ...row,
            license: this.resolveZoomLicenseMetadata(row.email, licenseSnapshot, row.zoom_user_id),
        }));
        const warnedUsers = usersWithLicense.filter((row) => row.license.is_licensed !== true);

        const warnings = warnedUsers.length
            ? [
                `${warnedUsers.length} usuario(s) activo(s) del pool no aparecen con licencia Zoom verificada y podrian usarse solo si lo confirmas: ${warnedUsers
                    .map(
                        (item) =>
                            `${item.email || item.name || item.zoom_user_id} (${item.license.license_label})`,
                    )
                    .join(', ')}.`,
            ]
            : [];

        if (warnings.length && !allowWarnings) {
            throw new BadRequestException(`${warnings[0]} Confirma si deseas continuar y usarlos de todos modos.`);
        }

        return {
            users: rows,
            warnings,
            warning_count: warnings.length,
            group,
        };
    }

    private async loadZoomPoolLicenseSnapshot() {
        try {
            const zoomUsers = await this.zoomAccountService.listAccountUsers();
            const byEmail = new Map(
                zoomUsers
                    .filter((item) => item.email?.trim())
                    .map((item) => [item.email!.trim().toLowerCase(), item] as const),
            );
            const byId = new Map(
                zoomUsers
                    .filter((item) => item.id?.trim())
                    .map((item) => [item.id.trim().toLowerCase(), item] as const),
            );
            return {
                ok: true,
                error: null,
                byEmail,
                byId,
            } satisfies ZoomPoolLicenseSnapshot;
        } catch (error) {
            return {
                ok: false,
                error: toErrorMessage(error),
                byEmail: new Map<string, ZoomAccountUserSummary>(),
                byId: new Map<string, ZoomAccountUserSummary>(),
            } satisfies ZoomPoolLicenseSnapshot;
        }
    }

    private resolveZoomLicenseMetadata(
        email: string | null | undefined,
        snapshot: ZoomPoolLicenseSnapshot,
        zoomUserId?: string | null,
    ) {
        if (!snapshot.ok) {
            return {
                license_status: 'UNKNOWN' as const,
                license_label: 'Licencia no verificada',
                is_licensed: null,
            };
        }
        const normalizedEmail = `${email ?? ''}`.trim().toLowerCase();
        const normalizedId = `${zoomUserId ?? ''}`.trim().toLowerCase();
        const found =
            (normalizedEmail ? snapshot.byEmail.get(normalizedEmail) : null) ??
            (normalizedId ? snapshot.byId.get(normalizedId) : null) ??
            null;
        if (!found) {
            // Zoom Rooms accounts (auto-generated email prefix "rooms_") are licensed room
            // credentials managed by Aula Virtual. They may not appear in the standard Zoom
            // /users API, but they are valid. Treat them as licensed Sala Zoom accounts.
            if (normalizedEmail.startsWith('rooms_')) {
                return {
                    license_status: 'LICENSED' as const,
                    license_label: 'Sala Zoom',
                    is_licensed: true,
                };
            }
            return {
                license_status: 'UNKNOWN' as const,
                license_label: 'No verificado en API de usuarios',
                is_licensed: null,
            };
        }
        return {
            license_status: found.license_status,
            license_label: displayZoomLicenseStatus(found.license_status),
            is_licensed: found.is_licensed,
        };
    }

    private async resolveOccurrences(
        rows: ScheduleContextRow[],
        startDate: string,
        endDate: string,
        timezone: string,
        inheritanceIndex?: InheritanceIndex,
    ) {
        const effectiveInheritanceIndex =
            inheritanceIndex ?? (await this.buildOperationalInheritanceIndex(rows));
        const ownerRows = rows.filter(
            (row) => !this.resolveScheduleInheritance(row.schedule_id, effectiveInheritanceIndex, rows).is_inherited,
        );
        const overrides = await this.listOverridesForRange(
            ownerRows.map((row) => row.schedule_id),
            startDate,
            endDate,
        );
        const overrideMap = new Map(
            overrides.map((item) => [`${item.schedule_id}::${item.conference_date}`, item] as const),
        );
        const ownerOccurrenceMap = new Map<string, ExpandedOccurrence>();
        for (const row of ownerRows) {
            for (const conferenceDate of enumerateConferenceDates(row.day_of_week, startDate, endDate)) {
                const override = overrideMap.get(`${row.schedule_id}::${conferenceDate}`) ?? null;
                const occurrence = buildOccurrence(row, conferenceDate, override, timezone, {
                    is_inherited: false,
                    mapping_id: null,
                    parent_schedule_id: null,
                    parent_occurrence_key: null,
                    parent_label: null,
                    family_owner_schedule_id: row.schedule_id,
                });
                ownerOccurrenceMap.set(occurrence.occurrence_key, occurrence);
            }
        }

        const occurrences: ExpandedOccurrence[] = [];
        for (const row of rows) {
            const inheritance = this.resolveScheduleInheritance(row.schedule_id, effectiveInheritanceIndex, rows);
            if (!inheritance.is_inherited) {
                for (const conferenceDate of enumerateConferenceDates(row.day_of_week, startDate, endDate)) {
                    const key = buildOccurrenceKey(row.schedule_id, conferenceDate);
                    const ownerOccurrence = ownerOccurrenceMap.get(key);
                    if (ownerOccurrence) {
                        occurrences.push(ownerOccurrence);
                    }
                }
                continue;
            }

            const parentRow = rows.find((item) => item.schedule_id === inheritance.parent_schedule_id) ?? null;
            for (const conferenceDate of enumerateConferenceDates(row.day_of_week, startDate, endDate)) {
                const parentOccurrence = ownerOccurrenceMap.get(
                    buildOccurrenceKey(inheritance.family_owner_schedule_id, conferenceDate),
                );
                if (!parentOccurrence) {
                    continue;
                }
                const childBaseOccurrence = buildOccurrence(
                    row,
                    conferenceDate,
                    null,
                    timezone,
                    {
                        is_inherited: true,
                        mapping_id: inheritance.mapping?.id ?? null,
                        parent_schedule_id: inheritance.parent_schedule_id,
                        parent_occurrence_key: parentOccurrence.occurrence_key,
                        parent_label: parentRow ? `${buildSectionLabel(parentRow)} | ${buildScheduleLabel(parentRow)}` : null,
                        family_owner_schedule_id: inheritance.family_owner_schedule_id,
                    },
                );

                const shouldMirrorParentWindow = parentOccurrence.occurrence_type !== 'BASE';
                occurrences.push({
                    ...(shouldMirrorParentWindow ? parentOccurrence : childBaseOccurrence),
                    occurrence_key: buildOccurrenceKey(row.schedule_id, conferenceDate),
                    row,
                    inheritance: {
                        is_inherited: true,
                        mapping_id: inheritance.mapping?.id ?? null,
                        parent_schedule_id: inheritance.parent_schedule_id,
                        parent_occurrence_key: parentOccurrence.occurrence_key,
                        parent_label: parentRow ? `${buildSectionLabel(parentRow)} | ${buildScheduleLabel(parentRow)}` : null,
                        family_owner_schedule_id: inheritance.family_owner_schedule_id,
                    },
                });
            }
        }

        return occurrences.sort((left, right) => {
            if (left.effective_conference_date !== right.effective_conference_date) {
                return left.effective_conference_date.localeCompare(right.effective_conference_date);
            }
            if (left.occurrence_type !== right.occurrence_type) {
                return left.occurrence_type.localeCompare(right.occurrence_type);
            }
            if (left.effective_start_time !== right.effective_start_time) {
                return left.effective_start_time.localeCompare(right.effective_start_time);
            }
            return left.occurrence_key.localeCompare(right.occurrence_key);
        });
    }

    private async listOverridesForRange(scheduleIds: string[], startDate: string, endDate: string) {
        const normalizedScheduleIds = normalizeIdArray(scheduleIds);
        if (!normalizedScheduleIds.length) {
            return [];
        }

        const entities = await this.planningVideoconferenceOverridesRepo
            .createQueryBuilder('override')
            .where('override.planning_subsection_schedule_id IN (:...scheduleIds)', {
                scheduleIds: normalizedScheduleIds,
            })
            .andWhere('override.conference_date BETWEEN :startDate AND :endDate', {
                startDate,
                endDate,
            })
            .getMany();

        return entities.map((item) => this.serializeOverride(item));
    }

    private serializeOverride(entity: PlanningSubsectionVideoconferenceOverrideEntity): StoredOccurrenceOverride {
        return {
            id: entity.id,
            schedule_id: entity.planning_subsection_schedule_id,
            conference_date: entity.conference_date,
            action: entity.action,
            override_date: entity.override_date,
            override_start_time: entity.override_start_time ? compactTime(entity.override_start_time) : null,
            override_end_time: entity.override_end_time ? compactTime(entity.override_end_time) : null,
            reason_code: entity.reason_code,
            notes: entity.notes,
        };
    }

    private async listInheritedMappings(scheduleIds: string[]) {
        const normalized = normalizeIdArray(scheduleIds);
        if (!normalized.length) {
            return [] as ScheduleInheritanceMapping[];
        }
        const rows = await this.inheritanceRepo.find({
            where: [
                { parent_schedule_id: In(normalized) },
                { child_schedule_id: In(normalized) },
            ],
        });
        return rows.map((row) => ({
            id: row.id,
            parent_schedule_id: row.parent_schedule_id,
            child_schedule_id: row.child_schedule_id,
            notes: row.notes,
            is_active: Boolean(row.is_active),
            created_at: row.created_at,
            updated_at: row.updated_at,
        }));
    }

    private async buildInheritanceIndex(scheduleIds: string[]): Promise<InheritanceIndex> {
        const mappings = (await this.listInheritedMappings(scheduleIds)).filter((item) => item.is_active);
        const byChild = new Map<string, ScheduleInheritanceMapping>();
        const childrenByParent = new Map<string, ScheduleInheritanceMapping[]>();
        for (const mapping of mappings) {
            byChild.set(mapping.child_schedule_id, mapping);
            const siblings = childrenByParent.get(mapping.parent_schedule_id) ?? [];
            siblings.push(mapping);
            childrenByParent.set(mapping.parent_schedule_id, siblings);
        }
        return { byChild, childrenByParent };
    }

    private async buildOperationalInheritanceIndex(rows: ScheduleContextRow[]): Promise<InheritanceIndex> {
        const baseIndex = await this.buildInheritanceIndex(rows.map((row) => row.schedule_id));
        const byChild = new Map(baseIndex.byChild);
        const childrenByParent = new Map<string, ScheduleInheritanceMapping[]>();
        for (const [parentId, mappings] of baseIndex.childrenByParent.entries()) {
            childrenByParent.set(parentId, [...mappings]);
        }

        const continuousMappings = this.buildContinuousBlockMappings(rows, baseIndex);
        for (const mapping of continuousMappings) {
            byChild.set(mapping.child_schedule_id, mapping);
            const siblings = childrenByParent.get(mapping.parent_schedule_id) ?? [];
            siblings.push(mapping);
            childrenByParent.set(mapping.parent_schedule_id, siblings);
        }

        return { byChild, childrenByParent };
    }

    private buildContinuousBlockMappings(rows: ScheduleContextRow[], inheritanceIndex: InheritanceIndex) {
        const explicitChildIds = new Set(inheritanceIndex.byChild.keys());
        const groups = new Map<string, ScheduleContextRow[]>();

        for (const row of rows) {
            if (explicitChildIds.has(row.schedule_id)) {
                continue;
            }

            const teacher = resolveTeacher(row);
            const teacherKey = String(teacher.id ?? teacher.name ?? '').trim().toUpperCase();
            if (!teacherKey || teacherKey === 'POR ASIGNAR') {
                continue;
            }

            const key = [
                row.section_id,
                row.course_id,
                row.day_of_week,
                row.campus_id ?? '',
                row.faculty_id ?? '',
                row.program_id ?? '',
                teacherKey,
            ].join('|');
            const current = groups.get(key) ?? [];
            current.push(row);
            groups.set(key, current);
        }

        const mappings: ScheduleInheritanceMapping[] = [];
        for (const groupRows of groups.values()) {
            if (groupRows.length < 2) {
                continue;
            }

            const ordered = [...groupRows].sort((left, right) => {
                const startCmp = compactTime(left.start_time).localeCompare(compactTime(right.start_time));
                if (startCmp !== 0) return startCmp;
                return compactTime(right.end_time).localeCompare(compactTime(left.end_time)); // Mas largo primero si empiezan igual
            });

            let owner = ordered[0];
            let blockMaxEndTime = compactTime(ordered[0].end_time);

            for (let index = 1; index < ordered.length; index += 1) {
                const current = ordered[index];
                const currentStartTime = compactTime(current.start_time);

                // Si el siguiente empieza antes o justo cuando termina el bloque actual, es continuo/traslapado
                if (currentStartTime <= blockMaxEndTime) {
                    mappings.push({
                        id: null,
                        parent_schedule_id: owner.schedule_id,
                        child_schedule_id: current.schedule_id,
                        notes: 'Bloque continuo consolidado automaticamente.',
                        is_active: true,
                        created_at: new Date(0),
                        updated_at: new Date(0),
                    });

                    const currentEndTime = compactTime(current.end_time);
                    if (currentEndTime > blockMaxEndTime) {
                        blockMaxEndTime = currentEndTime;
                    }
                    continue;
                }

                owner = current;
                blockMaxEndTime = compactTime(current.end_time);
            }
        }

        return mappings;
    }

    private buildContinuousBlockMap(rows: ScheduleContextRow[], inheritanceIndex: InheritanceIndex) {
        const infoByOwner = new Map<string, ContinuousBlockInfo>();

        for (const row of rows) {
            const inheritance = this.resolveScheduleInheritance(row.schedule_id, inheritanceIndex, rows);
            const ownerId = inheritance.family_owner_schedule_id;
            const current = infoByOwner.get(ownerId) ?? {
                owner_schedule_id: ownerId,
                family_schedule_ids: [],
                grouped_subsection_codes: [],
                grouped_subsection_labels: [],
            };

            current.family_schedule_ids.push(row.schedule_id);

            const code = String(row.subsection_code ?? '').trim();
            if (code && !current.grouped_subsection_codes.includes(code)) {
                current.grouped_subsection_codes.push(code);
            }

            const label = buildGroupLabel(row);
            if (label && !current.grouped_subsection_labels.includes(label)) {
                current.grouped_subsection_labels.push(label);
            }

            infoByOwner.set(ownerId, current);
        }

        return infoByOwner;
    }

    private resolveScheduleInheritance(
        scheduleId: string,
        inheritanceIndex: InheritanceIndex,
        rows: ScheduleContextRow[],
    ): ScheduleInheritanceInfo {
        const mapping = inheritanceIndex.byChild.get(scheduleId) ?? null;
        if (!mapping) {
            return {
                mapping: null,
                parent_schedule_id: null,
                family_owner_schedule_id: scheduleId,
                is_inherited: false,
                parent_label: null,
            };
        }
        let familyOwnerScheduleId = mapping.parent_schedule_id;
        const visited = new Set<string>([scheduleId]);
        while (!visited.has(familyOwnerScheduleId)) {
            visited.add(familyOwnerScheduleId);
            const parentMapping = inheritanceIndex.byChild.get(familyOwnerScheduleId) ?? null;
            if (!parentMapping) {
                break;
            }
            familyOwnerScheduleId = parentMapping.parent_schedule_id;
        }
        const parentRow = rows.find((item) => item.schedule_id === mapping.parent_schedule_id) ?? null;
        const ownerRow = rows.find((item) => item.schedule_id === familyOwnerScheduleId) ?? null;
        return {
            mapping,
            parent_schedule_id: mapping.parent_schedule_id,
            family_owner_schedule_id: ownerRow?.schedule_id ?? familyOwnerScheduleId,
            is_inherited: true,
            parent_label: parentRow ? `${buildSectionLabel(parentRow)} | ${buildScheduleLabel(parentRow)}` : null,
        };
    }

    private async validateInheritanceDefinition(parentScheduleIdRaw: string, childScheduleIdRaw: string, currentId?: string) {
        const parentScheduleId = String(parentScheduleIdRaw ?? '').trim();
        const childScheduleId = String(childScheduleIdRaw ?? '').trim();
        if (!parentScheduleId || !childScheduleId) {
            throw new BadRequestException('parentScheduleId y childScheduleId son requeridos.');
        }
        if (parentScheduleId === childScheduleId) {
            throw new BadRequestException('El horario padre y el hijo no pueden ser el mismo.');
        }

        const rows = await this.getScheduleRows(undefined, [parentScheduleId, childScheduleId]);
        const parent = rows.find((row) => row.schedule_id === parentScheduleId) ?? null;
        const child = rows.find((row) => row.schedule_id === childScheduleId) ?? null;
        if (!parent || !child) {
            throw new BadRequestException('No se encontro el horario padre o hijo en planificacion.');
        }

        if (
            parent.semester_id !== child.semester_id ||
            parent.faculty_id !== child.faculty_id
        ) {
            throw new BadRequestException(
                'Padre e hijo deben pertenecer al mismo periodo y facultad.',
            );
        }
        if (normalizeLoose(parent.day_of_week) !== normalizeLoose(child.day_of_week)) {
            throw new BadRequestException('Padre e hijo deben tener el mismo dia.');
        }

        const contextRows = await this.getScheduleRows({
            semesterId: parent.semester_id ?? undefined,
            facultyIds: parent.faculty_id ? [parent.faculty_id] : undefined,
        });
        const parentBlock = this.resolveContinuousSectionBlock(parent, contextRows);
        const childBlock = this.resolveContinuousSectionBlock(child, contextRows);

        if (
            compactTime(parentBlock.start_time) !== compactTime(childBlock.start_time) ||
            compactTime(parentBlock.end_time) !== compactTime(childBlock.end_time)
        ) {
            throw new BadRequestException(
                `Padre e hijo deben compartir la misma franja consolidada. Padre: ${compactTime(parentBlock.start_time)}-${compactTime(parentBlock.end_time)} / Hijo: ${compactTime(childBlock.start_time)}-${compactTime(childBlock.end_time)}.`,
            );
        }

        const activeMappings = await this.inheritanceRepo.find({
            where: [
                { child_schedule_id: childScheduleId, is_active: true },
                { child_schedule_id: parentScheduleId, is_active: true },
                { parent_schedule_id: childScheduleId, is_active: true },
            ],
        });
        for (const mapping of activeMappings) {
            if (currentId && mapping.id === currentId) {
                continue;
            }
            if (mapping.child_schedule_id === childScheduleId) {
                throw new BadRequestException('El horario hijo ya tiene una herencia activa.');
            }
            if (mapping.child_schedule_id === parentScheduleId) {
                throw new BadRequestException('El horario padre no puede ser hijo de otro horario.');
            }
            if (mapping.parent_schedule_id === childScheduleId) {
                throw new BadRequestException('El horario hijo no puede actuar como padre de otro horario.');
            }
        }

        return { parent, child };
    }

    private resolveContinuousSectionBlock(targetRow: ScheduleContextRow, contextRows: ScheduleContextRow[]) {
        const selectedBlock = this.resolveContinuousSectionBlockRows(targetRow, contextRows);
        return {
            start_time: selectedBlock[0].start_time,
            end_time: selectedBlock[selectedBlock.length - 1].end_time,
        };
    }

    private resolveContinuousSectionBlockRows(targetRow: ScheduleContextRow, contextRows: ScheduleContextRow[]) {
        const teacher = resolveTeacher(targetRow);
        const teacherKey = String(teacher.id ?? teacher.name ?? '').trim().toUpperCase();
        const scopedRows = contextRows
            .filter((row) => {
                const rowTeacher = resolveTeacher(row);
                const rowTeacherKey = String(rowTeacher.id ?? rowTeacher.name ?? '').trim().toUpperCase();
                return (
                    row.section_id === targetRow.section_id &&
                    row.course_id === targetRow.course_id &&
                    normalizeLoose(row.day_of_week) === normalizeLoose(targetRow.day_of_week) &&
                    (row.campus_id ?? '') === (targetRow.campus_id ?? '') &&
                    (row.faculty_id ?? '') === (targetRow.faculty_id ?? '') &&
                    (row.program_id ?? '') === (targetRow.program_id ?? '') &&
                    rowTeacherKey === teacherKey
                );
            })
            .sort((left, right) => compactTime(left.start_time).localeCompare(compactTime(right.start_time)));

        if (!scopedRows.length) {
            return [targetRow];
        }

        let currentBlock: ScheduleContextRow[] = [scopedRows[0]];
        const blocks: ScheduleContextRow[][] = [];
        for (let index = 1; index < scopedRows.length; index += 1) {
            const previous = currentBlock[currentBlock.length - 1];
            const row = scopedRows[index];
            if (compactTime(previous.end_time) === compactTime(row.start_time)) {
                currentBlock.push(row);
                continue;
            }
            blocks.push(currentBlock);
            currentBlock = [row];
        }
        blocks.push(currentBlock);

        return blocks.find((block) => block.some((row) => row.schedule_id === targetRow.schedule_id)) ?? [targetRow];
    }

    private async resolveInheritanceBlockPairs(parent: ScheduleContextRow, child: ScheduleContextRow) {
        const contextRows = await this.getScheduleRows({
            semesterId: parent.semester_id ?? undefined,
            facultyIds: parent.faculty_id ? [parent.faculty_id] : undefined,
        });
        const parentBlockRows = this.resolveContinuousSectionBlockRows(parent, contextRows);
        const childBlockRows = this.resolveContinuousSectionBlockRows(child, contextRows);

        return this.matchInheritanceBlockRows(parentBlockRows, childBlockRows);
    }

    private matchInheritanceBlockRows(parentRowsRaw: ScheduleContextRow[], childRowsRaw: ScheduleContextRow[]) {
        const parentRows = [...parentRowsRaw].sort((left, right) =>
            compactTime(left.start_time).localeCompare(compactTime(right.start_time)),
        );
        const childRows = [...childRowsRaw].sort((left, right) =>
            compactTime(left.start_time).localeCompare(compactTime(right.start_time)),
        );
        const pairs: InheritanceBlockPair[] = [];

        for (const childRow of childRows) {
            const bestParent = this.findBestParentForChild(childRow, parentRows);
            if (!bestParent) {
                throw new BadRequestException(
                    `No se pudo emparejar todo el bloque continuo para el horario hijo ${childRow.schedule_id}.`,
                );
            }
            pairs.push({
                parent_schedule_id: bestParent.schedule_id,
                child_schedule_id: childRow.schedule_id,
            });
        }

        return pairs;
    }

    private findBestParentForChild(childRow: ScheduleContextRow, parentRows: ScheduleContextRow[]) {
        let bestParent: ScheduleContextRow | null = null;
        let bestOverlap = -1;
        for (const parentRow of parentRows) {
            const overlap = this.getTimeOverlapMinutes(
                compactTime(childRow.start_time),
                compactTime(childRow.end_time),
                compactTime(parentRow.start_time),
                compactTime(parentRow.end_time),
            );
            if (overlap <= 0) {
                continue;
            }
            if (overlap > bestOverlap) {
                bestOverlap = overlap;
                bestParent = parentRow;
            }
        }
        return bestParent;
    }

    private getTimeOverlapMinutes(startA: string, endA: string, startB: string, endB: string) {
        const toMinutes = (value: string) => {
            const [h, m] = String(value ?? '').split(':').map((part) => Number(part));
            return (Number.isFinite(h) ? h : 0) * 60 + (Number.isFinite(m) ? m : 0);
        };
        const start = Math.max(toMinutes(startA), toMinutes(startB));
        const end = Math.min(toMinutes(endA), toMinutes(endB));
        return Math.max(0, end - start);
    }

    private async serializeInheritanceMappings(mappings: PlanningSubsectionScheduleVcInheritanceEntity[] | ScheduleInheritanceMapping[]) {
        if (!mappings.length) {
            return [];
        }
        const scheduleIds = Array.from(
            new Set(
                mappings.flatMap((item) => [item.parent_schedule_id, item.child_schedule_id]).filter(Boolean),
            ),
        );
        const rows = await this.getScheduleRows(undefined, scheduleIds);
        const rowMap = new Map(rows.map((row) => [row.schedule_id, row] as const));

        return mappings.map((item) => {
            const parent = rowMap.get(item.parent_schedule_id) ?? null;
            const child = rowMap.get(item.child_schedule_id) ?? null;
            const isActive = Boolean(item.is_active);
            const validity = computeInheritanceValidity(isActive, parent, child);
            return {
                id: item.id,
                parent_schedule_id: item.parent_schedule_id,
                child_schedule_id: item.child_schedule_id,
                notes: item.notes ?? null,
                is_active: isActive,
                created_at: item.created_at,
                updated_at: item.updated_at,
                validity,
                parent: parent
                    ? {
                        schedule_id: parent.schedule_id,
                        course_label: buildCourseLabel(parent.course_code, parent.course_name),
                        section_label: buildSectionLabel(parent),
                        subsection_label: buildGroupLabel(parent),
                        schedule_label: buildScheduleLabel(parent),
                        teacher_name: resolveTeacher(parent).name ?? null,
                        day_of_week: parent.day_of_week,
                        start_time: parent.start_time,
                        end_time: parent.end_time,
                    }
                    : null,
                child: child
                    ? {
                        schedule_id: child.schedule_id,
                        course_label: buildCourseLabel(child.course_code, child.course_name),
                        section_label: buildSectionLabel(child),
                        subsection_label: buildGroupLabel(child),
                        schedule_label: buildScheduleLabel(child),
                        teacher_name: resolveTeacher(child).name ?? null,
                        day_of_week: child.day_of_week,
                        start_time: child.start_time,
                        end_time: child.end_time,
                    }
                    : null,
            };
        });
    }

    private validateScheduleForGeneration(row: ScheduleContextRow) {
        const teacher = resolveTeacher(row);
        if (!row.course_code?.trim()) return 'Falta course_code en la oferta de planificacion.';
        if (!row.course_name?.trim()) return 'Falta course_name en la oferta de planificacion.';
        if (!row.vc_period_id?.trim()) return 'Falta vc_period_id en la oferta de planificacion.';
        if (!row.vc_faculty_id?.trim()) return 'Falta vc_faculty_id en la oferta de planificacion.';
        if (!row.vc_academic_program_id?.trim()) {
            return 'Falta vc_academic_program_id en la oferta de planificacion.';
        }
        if (!row.vc_course_id?.trim()) return 'Falta vc_course_id en la oferta de planificacion.';
        if (!row.vc_section_id?.trim()) return 'Falta vc_section_id en la subseccion.';
        if (!row.vc_section_name?.trim()) return 'Falta el nombre de la seccion VC en la subseccion.';
        if (!row.vc_faculty_name?.trim()) return 'El vc_faculty_id no existe o no es valido en Aula Virtual.';
        if (!row.vc_academic_program_name?.trim()) {
            return 'El vc_academic_program_id no existe o no es valido en Aula Virtual.';
        }
        if (!row.vc_course_name?.trim()) return 'El vc_course_id no existe o no es valido en Aula Virtual.';
        if (!teacher.id) return 'La subseccion no tiene docente responsable ni docente de seccion.';
        if (!teacher.name?.trim()) return 'El docente asignado no tiene nombre valido.';
        if (!teacher.dni?.trim()) return 'El docente asignado no tiene DNI.';
        if (!(row.day_of_week in DAY_TO_AULA_VIRTUAL_NUMBER)) {
            return `Dia de semana no soportado: ${row.day_of_week}`;
        }
        return null;
    }

    private async getAulaVirtualRequestContext(): Promise<AulaVirtualRequestContext> {
        const validation = await this.settingsSyncService.validateSourceSession(AULA_VIRTUAL_SOURCE_CODE);
        if (!validation.ok) {
            throw new BadRequestException(
                validation.reason || 'No se pudo validar la sesion de Aula Virtual.',
            );
        }

        const session = await this.settingsSyncService.getSourceSessionCookie(AULA_VIRTUAL_SOURCE_CODE);
        if (!session.has_cookie || !session.cookie_text.trim()) {
            throw new BadRequestException('No existe cookie valida para Aula Virtual.');
        }

        const source = await this.sourcesRepo.findOne({
            where: { code: AULA_VIRTUAL_SOURCE_CODE },
        });

        return {
            baseUrl: source?.base_url?.trim() || 'https://aulavirtual2.autonomadeica.edu.pe',
            cookie: session.cookie_text,
        };
    }

    private buildAulaVirtualPayload(
        occurrence: ExpandedOccurrence,
        zoomUserId: string,
        teacher: ResolvedTeacher,
        topic: string,
    ): AulaVirtualPayload {
        const startTime = compactTime(occurrence.effective_start_time);
        const endTime = compactTime(occurrence.effective_end_time);
        const dayOfWeek = dayCodeForDate(occurrence.effective_conference_date);
        const minutes = calculateDurationMinutes(
            occurrence.effective_start_time,
            occurrence.effective_end_time,
        );
        return {
            courseCode: occurrence.row.course_code?.trim() ?? '',
            courseName: occurrence.row.course_name?.trim() ?? '',
            section: occurrence.row.vc_section_name?.trim() ?? '',
            dni: teacher.dni?.trim() ?? '',
            teacher: teacher.name?.trim() ?? '',
            day: dayOfWeek,
            startTime,
            endTime,
            termId: occurrence.row.vc_period_id?.trim() ?? '',
            facultyId: occurrence.row.vc_faculty_id?.trim() ?? '',
            careerId: occurrence.row.vc_academic_program_id?.trim() ?? '',
            courseId: occurrence.row.vc_course_id?.trim() ?? '',
            name: topic,
            sectionId: occurrence.row.vc_section_id?.trim() ?? '',
            start: `${formatDateForAulaVirtual(occurrence.effective_conference_date)} ${startTime}`,
            end: `${formatDateForAulaVirtual(occurrence.effective_conference_date)} ${endTime}`,
            minutes: String(minutes),
            'daysOfWeek[0]': String(DAY_TO_AULA_VIRTUAL_NUMBER[dayOfWeek]),
            credentialId: zoomUserId,
        };
    }

    private async postAulaVirtualConference(
        context: AulaVirtualRequestContext,
        payload: AulaVirtualPayload,
        attempt = 0,
    ): Promise<{ status: number; final_url: string; content_type: string; body: object }> {
        // Throttle: small fixed delay before each request to avoid flooding Aula Virtual
        if (attempt === 0) {
            await new Promise((resolve) => setTimeout(resolve, AULA_VIRTUAL_THROTTLE_MS));
        }

        const form = new FormData();
        for (const [key, value] of Object.entries(payload)) {
            form.append(key, value);
        }

        const url = new URL('/web/conference/videoconferencias/agregar', context.baseUrl);
        const response = await fetch(url.toString(), {
            method: 'POST',
            headers: {
                accept: 'application/json, text/plain, */*',
                cookie: context.cookie,
                referer: context.baseUrl,
                'user-agent': 'Mozilla/5.0 (UAI Videoconferencias)',
                'x-requested-with': 'XMLHttpRequest',
            },
            body: form,
            redirect: 'follow',
        });

        const contentType = response.headers.get('content-type') ?? '';
        const bodyText = await response.text();
        const payloadBody = parseMaybeJson(bodyText) ?? { raw_text: bodyText.slice(0, 4000) };
        const redirectedToLogin =
            response.redirected && /login|signin|account\/login/i.test(response.url);
        const redirectedToErrorPage =
            response.redirected && /\/50[0-9](\.s?html?)?$/i.test(response.url);
        const htmlLooksLikeLogin =
            contentType.includes('text/html') && /login|iniciar sesi[oó]n|password/i.test(bodyText);
        const bodyLooksLikeGatewayError =
            /Cannot GET \/50[0-9]|Bad Gateway|Service Unavailable/i.test(bodyText);

        if (redirectedToErrorPage || bodyLooksLikeGatewayError) {
            if (attempt < AULA_VIRTUAL_MAX_RETRIES) {
                const waitMs = AULA_VIRTUAL_RETRY_BASE_DELAY_MS * (attempt + 1);
                await new Promise((resolve) => setTimeout(resolve, waitMs));
                return this.postAulaVirtualConference(context, payload, attempt + 1);
            }
            throw new BadRequestException(
                'Aula Virtual no esta disponible en este momento (error de servidor). Intenta nuevamente en unos minutos.',
            );
        }
        if (redirectedToLogin) {
            throw new BadRequestException(
                'La sesion de Aula Virtual expiro. Renueva la cookie en Configuracion y vuelve a intentarlo.',
            );
        }
        if (!response.ok || htmlLooksLikeLogin) {
            // Retry on 5xx transient server errors (not 4xx which are logic errors)
            if (response.status >= 500 && attempt < AULA_VIRTUAL_MAX_RETRIES) {
                const waitMs = AULA_VIRTUAL_RETRY_BASE_DELAY_MS * (attempt + 1);
                await new Promise((resolve) => setTimeout(resolve, waitMs));
                return this.postAulaVirtualConference(context, payload, attempt + 1);
            }
            throw new BadRequestException(
                `Aula Virtual rechazo la creacion (${response.status}): ${bodyText.slice(0, 300)}`,
            );
        }

        return {
            status: response.status,
            final_url: response.url,
            content_type: contentType,
            body: payloadBody,
        };
    }

    private async findAvailableZoomUser<T extends ZoomPoolUser>(
        scheduledStart: Date,
        scheduledEnd: Date,
        zoomUsers: T[],
        maxConcurrent: number,
        remoteMeetingsCache: Map<string, ZoomMeetingSummary[] | null>,
        simulatedReservations?: Map<string, SimulatedReservation[]>,
    ): Promise<T | null> {
        const windowStart = addMinutes(scheduledStart, -MEETING_MARGIN_MINUTES);
        const windowEnd = addMinutes(scheduledEnd, MEETING_MARGIN_MINUTES);

        for (const zoomUser of zoomUsers) {
            if (!zoomUser.email) {
                continue;
            }

            let remoteMeetings = remoteMeetingsCache.get(zoomUser.zoom_user_id) ?? null;
            if (!remoteMeetingsCache.has(zoomUser.zoom_user_id)) {
                try {
                    remoteMeetings = await this.zoomAccountService.listUserMeetingsByTypes(
                        zoomUser.email,
                        ['live', 'upcoming'],
                    );
                } catch {
                    remoteMeetings = null;
                }
                remoteMeetingsCache.set(zoomUser.zoom_user_id, remoteMeetings);
            }
            if (!remoteMeetings) {
                continue;
            }

            const localMeetings = await this.planningVideoconferencesRepo
                .createQueryBuilder('conference')
                .where('conference.zoom_user_id = :zoomUserId', { zoomUserId: zoomUser.zoom_user_id })
                .andWhere('conference.status IN (:...statuses)', {
                    statuses: [...ACTIVE_CONFERENCE_STATUSES],
                })
                .andWhere('conference.scheduled_start < :windowEnd', { windowEnd })
                .andWhere('conference.scheduled_end > :windowStart', { windowStart })
                .getMany();

            const localMeetingIds = new Set(
                localMeetings
                    .map((item) => item.zoom_meeting_id)
                    .filter((item): item is string => Boolean(item)),
            );

            const remoteOverlapCount = remoteMeetings.filter((meeting) => {
                if (localMeetingIds.has(meeting.id)) {
                    return false;
                }
                const remoteStart = parseRemoteMeetingStart(meeting.start_time);
                if (!remoteStart) {
                    return false;
                }
                // For live meetings the scheduled duration may be shorter than the
                // actual running time. Use a generous fallback so a live meeting that
                // started before the window is not incorrectly dismissed as finished.
                const isLive = meeting.source_type === 'live';
                const effectiveDuration = meeting.duration_minutes
                    ? (isLive
                        ? Math.max(meeting.duration_minutes, LIVE_MEETING_FALLBACK_DURATION_MINUTES)
                        : meeting.duration_minutes)
                    : (isLive ? LIVE_MEETING_FALLBACK_DURATION_MINUTES : DEFAULT_REMOTE_MEETING_DURATION_MINUTES);
                const remoteEnd = addMinutes(remoteStart, effectiveDuration);
                return doesOverlap(windowStart, windowEnd, remoteStart, remoteEnd);
            }).length;

            const simulatedOverlapCount = (simulatedReservations?.get(zoomUser.zoom_user_id) ?? []).filter(
                (item) => doesOverlap(windowStart, windowEnd, item.scheduled_start, item.scheduled_end),
            ).length;

            if (localMeetings.length + remoteOverlapCount + simulatedOverlapCount < maxConcurrent) {
                return zoomUser;
            }
        }

        return null;
    }

    private findAvailableZoomUserForBaseSlot(
        scheduledStart: Date,
        scheduledEnd: Date,
        zoomUsers: PreviewZoomPoolUser[],
        maxConcurrent: number,
        simulatedReservations: Map<string, SimulatedReservation[]>,
    ) {
        const windowStart = addMinutes(scheduledStart, -MEETING_MARGIN_MINUTES);
        const windowEnd = addMinutes(scheduledEnd, MEETING_MARGIN_MINUTES);

        for (const zoomUser of zoomUsers) {
            const overlaps = (simulatedReservations.get(zoomUser.zoom_user_id) ?? []).filter((item) =>
                doesOverlap(windowStart, windowEnd, item.scheduled_start, item.scheduled_end),
            ).length;
            if (overlaps < maxConcurrent) {
                return zoomUser;
            }
        }

        return null;
    }

    private async matchZoomMeeting(
        userEmail: string,
        topic: string,
        scheduledStart: Date,
        durationMinutes: number,
    ): Promise<ZoomMatchResult | null> {
        if (!userEmail.trim()) {
            return null;
        }

        for (let attempt = 1; attempt <= ZOOM_MATCH_ATTEMPTS; attempt += 1) {
            const meetings = await this.zoomAccountService.listUserMeetingsByTypes(userEmail, [
                'upcoming',
                'live',
            ]);
            const candidates = meetings.filter((meeting) => {
                const meetingStart = parseRemoteMeetingStart(meeting.start_time);
                if (!meetingStart) {
                    return false;
                }
                const sameTopic = normalizeLoose(meeting.topic) === normalizeLoose(topic);
                const durationDelta =
                    meeting.duration_minutes === null
                        ? 0
                        : Math.abs(meeting.duration_minutes - durationMinutes);
                return (
                    sameTopic &&
                    Math.abs(meetingStart.getTime() - scheduledStart.getTime()) <= 2 * 60 * 1000 &&
                    durationDelta <= 2
                );
            });

            if (candidates.length === 1) {
                const meetingDetail = await this.zoomAccountService.getMeeting(candidates[0].id);
                return {
                    ...candidates[0],
                    start_url: meetingDetail?.start_url ?? candidates[0].start_url ?? null,
                    attempts: attempt,
                };
            }

            if (attempt < ZOOM_MATCH_ATTEMPTS) {
                await sleep(ZOOM_MATCH_DELAY_MS);
            }
        }

        return null;
    }
}

function buildGenerationMessage(summary: {
    matched: number;
    createdUnmatched: number;
    blockedExisting: number;
    noAvailableZoomUser: number;
    validationErrors: number;
    errors: number;
}, warningCount = 0) {
    const parts = [
        `${summary.matched} conciliadas`,
        `${summary.createdUnmatched} creadas sin match`,
        `${summary.blockedExisting} bloqueadas por existente`,
        `${summary.noAvailableZoomUser} sin usuario Zoom`,
        `${summary.validationErrors} con validacion pendiente`,
        `${summary.errors} con error`,
    ];
    if (warningCount > 0) {
        parts.push(`${warningCount} advertencia(s) de licencias`);
    }
    return `Proceso completado: ${parts.join(', ')}.`;
}

function buildSectionLabel(row: ScheduleContextRow) {
    const course = buildCourseLabel(row.course_code, row.course_name);
    return `${course} | Seccion ${row.section_code}`;
}

function buildGroupLabel(row: ScheduleContextRow) {
    return row.subsection_code?.trim() ? `Grupo ${row.subsection_code.trim()}` : 'Grupo sin codigo';
}

function buildScheduleLabel(row: Pick<ScheduleContextRow, 'day_of_week' | 'start_time' | 'end_time'>) {
    return `${displayDay(row.day_of_week)} ${compactTime(row.start_time)}-${compactTime(row.end_time)}`;
}

function buildVacancyLabel(
    row: Pick<ScheduleContextRow, 'section_projected_vacancies' | 'subsection_projected_vacancies'>,
) {
    const sectionVacancies = row.section_projected_vacancies;
    const subsectionVacancies = row.subsection_projected_vacancies;
    return `Vac. seccion: ${sectionVacancies ?? 0} | Vac. grupo: ${subsectionVacancies ?? 0}`;
}

function buildCourseLabel(courseCode: string | null, courseName: string | null) {
    const parts = [courseCode?.trim(), courseName?.trim()].filter(Boolean);
    return parts.join(' - ') || '(Sin curso)';
}

function buildDefaultInheritance(scheduleId: string): ExpandedOccurrence['inheritance'] {
    return {
        is_inherited: false,
        mapping_id: null,
        parent_schedule_id: null,
        parent_occurrence_key: null,
        parent_label: null,
        family_owner_schedule_id: scheduleId,
    };
}

function readRecordInheritance(payload: Record<string, unknown> | null | undefined) {
    if (!payload || typeof payload !== 'object') {
        return null;
    }
    const raw = (payload as Record<string, unknown>).inheritance;
    if (!raw || typeof raw !== 'object') {
        return null;
    }
    const item = raw as Record<string, unknown>;
    return {
        is_inherited: Boolean(item.is_inherited),
        mapping_id: readNullableString(item.mapping_id),
        parent_schedule_id: readNullableString(item.parent_schedule_id),
        parent_occurrence_key: readNullableString(item.parent_occurrence_key),
        parent_label: readNullableString(item.parent_label),
        family_owner_schedule_id: readNullableString(item.family_owner_schedule_id) ?? '',
    };
}

function normalizeRecordStatusFromResult(
    value: GenerateResultItem['status'],
): PlanningSubsectionVideoconferenceEntity['status'] {
    switch (value) {
        case 'MATCHED':
            return 'MATCHED';
        case 'CREATED_UNMATCHED':
            return 'CREATED_UNMATCHED';
        case 'BLOCKED_EXISTING':
        case 'NO_AVAILABLE_ZOOM_USER':
        case 'VALIDATION_ERROR':
        case 'ERROR':
        default:
            return 'ERROR';
    }
}

function buildMeetingTopic(occurrence: ExpandedOccurrence, teacher: ResolvedTeacher) {
    const topicParts = [
        occurrence.row.course_name?.trim() ?? '',
        occurrence.row.vc_section_name?.trim() ?? '',
        teacher.dni?.trim() ?? '',
        teacher.name?.trim() ?? '',
        `${dayCodeForDate(occurrence.effective_conference_date)} ${compactTime(occurrence.effective_start_time)}-${compactTime(occurrence.effective_end_time)}`,
    ];

    const courseCode = occurrence.row.course_code?.trim() ?? '';
    if (courseCode) {
        topicParts.unshift(courseCode);
    }

    if (occurrence.occurrence_type === 'RESCHEDULED') {
        topicParts.unshift('REP');
    }

    return topicParts.join('|');
}

function resolveTeacher(row: ScheduleContextRow): ResolvedTeacher {
    return {
        id: row.responsible_teacher_id ?? row.section_teacher_id,
        name:
            coalesceText(row.responsible_teacher_full_name, row.responsible_teacher_name)
            ?? coalesceText(row.section_teacher_full_name, row.section_teacher_name),
        dni: row.responsible_teacher_dni ?? row.section_teacher_dni,
    };
}

function computeInheritanceValidity(
    isActive: boolean,
    parent: ScheduleContextRow | null,
    child: ScheduleContextRow | null,
): 'ok' | 'inactive' | 'schedule_missing' | 'schedule_mismatch' | 'teacher_mismatch' {
    if (!isActive) return 'inactive';
    if (!parent || !child) return 'schedule_missing';
    if (
        parent.day_of_week !== child.day_of_week ||
        parent.start_time !== child.start_time ||
        parent.end_time !== child.end_time
    ) {
        return 'schedule_mismatch';
    }
    const parentTeacher = resolveTeacher(parent);
    const childTeacher = resolveTeacher(child);
    if (parentTeacher.id && childTeacher.id && parentTeacher.id !== childTeacher.id) {
        return 'teacher_mismatch';
    }
    return 'ok';
}

function resolveModality(row: ScheduleContextRow): ResolvedModality {
    const sectionCodeModality = resolveSectionCodeModality(
        row.vc_section_name,
        row.section_external_code,
        row.section_is_cepea,
    );
    if (sectionCodeModality) {
        return sectionCodeModality;
    }
    return resolveModalityValues(
        row.subsection_modality_code,
        row.subsection_modality_name,
        row.section_modality_code,
        row.section_modality_name,
    );
}

function resolveFilterRowModality(row: FilterOptionRow): ResolvedModality {
    const sectionCodeModality = resolveSectionCodeModality(
        row.vc_section_name,
        row.section_external_code,
        row.section_is_cepea,
    );
    if (sectionCodeModality) {
        return sectionCodeModality;
    }
    return resolveModalityValues(
        row.subsection_modality_code,
        row.subsection_modality_name,
        row.section_modality_code,
        row.section_modality_name,
    );
}

function resolveModalityValues(
    subsectionCode: string | null | undefined,
    subsectionName: string | null | undefined,
    sectionCode: string | null | undefined,
    sectionName: string | null | undefined,
): ResolvedModality {
    return {
        code: subsectionCode ?? sectionCode ?? null,
        name: subsectionName ?? sectionName ?? null,
    };
}

function normalizeModalityCode(value: string | null | undefined) {
    const normalized = (value ?? '').trim().toUpperCase();
    if (!normalized) {
        return null;
    }
    return normalized;
}

function normalizeGenerationResultStatus(
    value: string | null | undefined,
): GenerateResultItem['status'] {
    switch (`${value ?? ''}`.trim().toUpperCase()) {
        case 'MATCHED':
            return 'MATCHED';
        case 'CREATED_UNMATCHED':
            return 'CREATED_UNMATCHED';
        case 'BLOCKED_EXISTING':
            return 'BLOCKED_EXISTING';
        case 'NO_AVAILABLE_ZOOM_USER':
            return 'NO_AVAILABLE_ZOOM_USER';
        case 'VALIDATION_ERROR':
            return 'VALIDATION_ERROR';
        case 'ERROR':
            return 'ERROR';
        default:
            return 'ERROR';
    }
}

function displayModality(code: string | null | undefined, fallbackName?: string | null | undefined) {
    switch (normalizeModalityCode(code)) {
        case 'PRESENCIAL':
            return 'Presencial';
        case 'VIRTUAL':
            return 'Virtual';
        case 'HIBRIDO_PRESENCIAL':
            return 'Hibrido presencial';
        case 'HIBRIDO_VIRTUAL':
            return 'Hibrido virtual';
        default:
            return coalesceText(fallbackName, code) ?? 'Sin modalidad';
    }
}

function displayZoomLicenseStatus(value: 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN') {
    switch (value) {
        case 'LICENSED':
            return 'Licenciado';
        case 'ON_PREM':
            return 'On-prem';
        case 'BASIC':
            return 'Basico';
        default:
            return 'No verificado';
    }
}

function resolveSectionCodeModality(
    vcSectionName: string | null | undefined,
    sectionExternalCode: string | null | undefined,
    isCepea: boolean,
): ResolvedModality | null {
    if (isCepea) {
        return { code: 'VIRTUAL', name: 'Virtual' };
    }
    const sectionToken = extractSectionToken(vcSectionName) ?? extractSectionToken(sectionExternalCode);
    if (!sectionToken) {
        return null;
    }
    if (sectionToken.length > 2 && sectionToken.endsWith('HV')) {
        return { code: 'HIBRIDO_VIRTUAL', name: 'Hibrido virtual' };
    }
    if (sectionToken.length > 2 && sectionToken.endsWith('HP')) {
        return { code: 'HIBRIDO_PRESENCIAL', name: 'Hibrido presencial' };
    }
    if (sectionToken.endsWith('V')) {
        return { code: 'VIRTUAL', name: 'Virtual' };
    }
    if (sectionToken.endsWith('P')) {
        return { code: 'PRESENCIAL', name: 'Presencial' };
    }
    return null;
}

function extractSectionToken(value: string | null | undefined) {
    const normalized = String(value ?? '').trim().toUpperCase();
    if (!normalized) {
        return null;
    }
    return normalized.split('-')[0]?.replace(/\s+/g, '') ?? null;
}

function filterRowsByModality(rows: ScheduleContextRow[], modality: string | string[] | null | undefined) {
    const normalized = new Set(normalizeModalityFilterInput(modality));
    if (!normalized.size) {
        return rows;
    }
    return rows.filter((row) => {
        const rowModality = normalizeModalityCode(resolveModality(row).code);
        if (!rowModality) {
            return false;
        }
        return normalized.has(rowModality);
    });
}

function filterOptionRowsByModality(rows: FilterOptionRow[], modality: string | string[] | null | undefined) {
    const normalized = new Set(normalizeModalityFilterInput(modality));
    if (!normalized.size) {
        return rows;
    }
    return rows.filter((row) => {
        const rowModality = normalizeModalityCode(resolveFilterRowModality(row).code);
        if (!rowModality) {
            return false;
        }
        return normalized.has(rowModality);
    });
}

function normalizeModalityFilterInput(value: string | string[] | null | undefined) {
    const rawValues = Array.isArray(value) ? value : value ? [value] : [];
    return Array.from(
        new Set(
            rawValues
                .map((item) => normalizeModalityCode(item))
                .filter((item): item is string => Boolean(item)),
        ),
    );
}

function buildOccurrence(
    row: ScheduleContextRow,
    conferenceDate: string,
    override: StoredOccurrenceOverride | null,
    timezone: string,
    inheritance: ExpandedOccurrence['inheritance'],
): ExpandedOccurrence {
    const action = override?.action ?? 'KEEP';
    const occurrenceType: ExpandedOccurrence['occurrence_type'] =
        action === 'RESCHEDULE' ? 'RESCHEDULED' : action === 'SKIP' ? 'SKIPPED' : 'BASE';
    const effectiveConferenceDate =
        action === 'RESCHEDULE' ? normalizeIsoDate(override?.override_date ?? conferenceDate) : conferenceDate;
    const effectiveStartTime =
        action === 'RESCHEDULE' ? normalizeTimeValue(override?.override_start_time ?? row.start_time) : compactTime(row.start_time);
    const effectiveEndTime =
        action === 'RESCHEDULE' ? normalizeTimeValue(override?.override_end_time ?? row.end_time) : compactTime(row.end_time);

    return {
        occurrence_key: buildOccurrenceKey(row.schedule_id, conferenceDate),
        row,
        base_conference_date: conferenceDate,
        effective_conference_date: effectiveConferenceDate,
        effective_start_time: effectiveStartTime,
        effective_end_time: effectiveEndTime,
        scheduled_start: buildDateTime(effectiveConferenceDate, effectiveStartTime, timezone),
        scheduled_end: buildDateTime(effectiveConferenceDate, effectiveEndTime, timezone),
        occurrence_type: occurrenceType,
        override_id: override?.id ?? null,
        override_reason_code: override?.reason_code ?? null,
        override_notes: override?.notes ?? null,
        inheritance,
    };
}

function buildOccurrenceKey(scheduleId: string, conferenceDate: string) {
    return `${scheduleId}::${conferenceDate}`;
}

function parseOccurrenceKey(value: string) {
    const [schedule_id = '', base_conference_date = ''] = String(value ?? '').split('::');
    return {
        schedule_id: schedule_id.trim(),
        base_conference_date: base_conference_date.trim(),
    };
}

function normalizeTimeValue(value: string | null | undefined) {
    const normalized = compactTime(value);
    if (!/^\d{2}:\d{2}$/.test(normalized)) {
        throw new BadRequestException(`Hora invalida: ${value}`);
    }
    return normalized;
}

function normalizeOverrideReason(
    value: string | null | undefined,
): 'HOLIDAY' | 'WEATHER' | 'OTHER' {
    const normalized = String(value ?? '').trim().toUpperCase();
    if (normalized === 'HOLIDAY' || normalized === 'WEATHER' || normalized === 'OTHER') {
        return normalized;
    }
    return 'OTHER';
}

function emptyTextToNull(value: string | null | undefined) {
    const normalized = String(value ?? '').trim();
    return normalized || null;
}

function coalesceText(...values: Array<string | null | undefined>) {
    for (const value of values) {
        if (value && value.trim()) {
            return value.trim();
        }
    }
    return null;
}

function displayDay(value: string | null | undefined) {
    switch ((value ?? '').trim().toUpperCase()) {
        case 'LUNES':
            return 'Lunes';
        case 'MARTES':
            return 'Martes';
        case 'MIERCOLES':
            return 'Miercoles';
        case 'JUEVES':
            return 'Jueves';
        case 'VIERNES':
            return 'Viernes';
        case 'SABADO':
            return 'Sabado';
        case 'DOMINGO':
            return 'Domingo';
        default:
            return value ?? 'Sin dia';
    }
}

function compactTime(value: string | null | undefined) {
    if (!value) {
        return '--:--';
    }
    return value.slice(0, 5);
}

function dayCodeForDate(value: string) {
    return jsDayToCode(parseUtcDate(value).getUTCDay());
}

function getDaySortOrder(dayCode: string) {
    switch ((dayCode ?? '').trim().toUpperCase()) {
        case 'LUNES':
            return 1;
        case 'MARTES':
            return 2;
        case 'MIERCOLES':
            return 3;
        case 'JUEVES':
            return 4;
        case 'VIERNES':
            return 5;
        case 'SABADO':
            return 6;
        case 'DOMINGO':
            return 7;
        default:
            return 99;
    }
}

function normalizeIdArray(values: string[] | undefined) {
    return Array.from(
        new Set(
            (values ?? [])
                .map((value) => value.trim())
                .filter(Boolean),
        ),
    );
}

function sortCatalogOptions(options: Map<string, string>): FilterCatalogOption[] {
    return [...options.entries()]
        .map(([id, label]) => ({ id, label }))
        .sort((left, right) => left.label.localeCompare(right.label));
}

function normalizeIsoDate(value: string) {
    const normalized = value.trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
        throw new BadRequestException(`Fecha invalida: ${value}`);
    }
    return normalized;
}

function enumerateConferenceDates(dayOfWeek: string, startDate: string, endDate: string) {
    const targetJsDay = DAY_TO_JS_NUMBER[dayOfWeek];
    if (targetJsDay === undefined) {
        return [];
    }

    const result: string[] = [];
    const cursor = parseUtcDate(startDate);
    const limit = parseUtcDate(endDate);
    while (cursor.getTime() <= limit.getTime()) {
        if (cursor.getUTCDay() === targetJsDay) {
            result.push(formatUtcDate(cursor));
        }
        cursor.setUTCDate(cursor.getUTCDate() + 1);
    }
    return result;
}

function parseUtcDate(value: string) {
    return new Date(`${value}T00:00:00.000Z`);
}

function formatUtcDate(value: Date) {
    return value.toISOString().slice(0, 10);
}

function jsDayToCode(day: number) {
    switch (day) {
        case 0:
            return 'DOMINGO';
        case 1:
            return 'LUNES';
        case 2:
            return 'MARTES';
        case 3:
            return 'MIERCOLES';
        case 4:
            return 'JUEVES';
        case 5:
            return 'VIERNES';
        case 6:
            return 'SABADO';
        default:
            return 'LUNES';
    }
}

function buildDateTime(date: string, time: string, timezone: string) {
    const offset = timezoneToOffset(timezone);
    return new Date(`${date}T${compactTime(time)}:00${offset}`);
}

function timezoneToOffset(timezone: string) {
    switch ((timezone || '').trim()) {
        case 'UTC':
            return 'Z';
        case 'America/Bogota':
        case 'America/Lima':
        case 'America/Guayaquil':
        case 'America/Quito':
            return '-05:00';
        default:
            return '-05:00';
    }
}

function formatDateForAulaVirtual(value: string) {
    const [year, month, day] = value.split('-');
    return `${day}/${month}/${year}`;
}

function calculateDurationMinutes(startTime: string, endTime: string) {
    const [startHour, startMinute] = startTime.split(':').map((value) => Number(value));
    const [endHour, endMinute] = endTime.split(':').map((value) => Number(value));
    return endHour * 60 + endMinute - (startHour * 60 + startMinute);
}

function addMinutes(value: Date, minutes: number) {
    return new Date(value.getTime() + minutes * 60 * 1000);
}

function doesOverlap(startA: Date, endA: Date, startB: Date, endB: Date) {
    return startA < endB && endA > startB;
}

function parseRemoteMeetingStart(value: string | null) {
    if (!value?.trim()) {
        return null;
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
}

// Lima is UTC-5. This constant is used to convert Zoom UTC timestamps to local day/time.
const LIMA_UTC_OFFSET_MS = 5 * 60 * 60 * 1000;

const JS_DAY_TO_CODE: Record<number, string> = {
    0: 'DOMINGO',
    1: 'LUNES',
    2: 'MARTES',
    3: 'MIERCOLES',
    4: 'JUEVES',
    5: 'VIERNES',
    6: 'SABADO',
};

/**
 * Converts a ZoomMeetingSummary to a reference-week slot compatible with the base (weekly)
 * simulation. The base simulation uses fictional reference dates (Jan 2026) where hours/minutes
 * encode local Lima time directly, so we must project the Zoom meeting from its real UTC timestamp
 * into the same reference week using day-of-week + local time.
 */
function zoomMeetingToReferenceWeekSlot(meeting: ZoomMeetingSummary): { start: Date; end: Date } | null {
    const remoteStart = parseRemoteMeetingStart(meeting.start_time);
    if (!remoteStart) {
        return null;
    }
    // Shift UTC → Lima local (subtract 5 hours)
    const localStart = new Date(remoteStart.getTime() - LIMA_UTC_OFFSET_MS);
    const dayCode = JS_DAY_TO_CODE[localStart.getUTCDay()];
    if (!dayCode) {
        return null;
    }
    const hh = String(localStart.getUTCHours()).padStart(2, '0');
    const mm = String(localStart.getUTCMinutes()).padStart(2, '0');
    const refStart = buildWeeklyScheduleDateTime(dayCode, `${hh}:${mm}`);

    // For live meetings the scheduled duration may already be over; use a generous ceiling
    // so that a live meeting active right now blocks the slot.
    const isLive = meeting.source_type === 'live';
    const durationMin = meeting.duration_minutes
        ? (isLive
            ? Math.max(meeting.duration_minutes, LIVE_MEETING_FALLBACK_DURATION_MINUTES)
            : meeting.duration_minutes)
        : (isLive ? LIVE_MEETING_FALLBACK_DURATION_MINUTES : DEFAULT_REMOTE_MEETING_DURATION_MINUTES);

    const refEnd = addMinutes(refStart, durationMin);
    return { start: refStart, end: refEnd };
}

function parseMaybeJson(value: string) {
    if (!value.trim()) {
        return null;
    }
    try {
        return JSON.parse(value) as Record<string, unknown>;
    } catch {
        return null;
    }
}

function normalizeLoose(value: string | null | undefined) {
    return (value ?? '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .trim()
        .toUpperCase();
}

function readString(value: unknown) {
    if (typeof value !== 'string' || !value.trim()) {
        throw new BadRequestException('Se esperaba un valor string no vacio desde la consulta.');
    }
    return value;
}

function readNullableString(value: unknown) {
    return typeof value === 'string' && value.trim() ? value : null;
}

function readNumber(value: unknown) {
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
    }
    if (typeof value === 'string' && value.trim()) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : 0;
    }
    return 0;
}

function readNullableNumber(value: unknown) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
    }
    if (typeof value === 'string' && value.trim()) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    }
    return null;
}

function readNullableRecord(value: unknown) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        return null;
    }
    return value as Record<string, unknown>;
}

function uniqueIds(values: Array<string | null | undefined>) {
    return Array.from(new Set(values.map((value) => `${value ?? ''}`.trim()).filter(Boolean)));
}

function mapById<T extends { id: string }>(items: T[]) {
    return new Map(items.map((item) => [item.id, item] as const));
}

function payloadPickString(value: Record<string, unknown> | null | undefined, ...paths: string[]) {
    const payload = readNullableRecord(value);
    if (!payload) {
        return null;
    }
    for (const path of paths) {
        const resolved = path.split('.').reduce<unknown>((acc, key) => {
            if (!acc || typeof acc !== 'object' || Array.isArray(acc)) {
                return undefined;
            }
            return (acc as Record<string, unknown>)[key];
        }, payload);
        const normalized = `${resolved ?? ''}`.trim();
        if (normalized) {
            return normalized;
        }
    }
    return null;
}

function readOfferVcContextMetadata(sourcePayloadJson: Record<string, unknown> | null | undefined) {
    const payload = readNullableRecord(sourcePayloadJson) ?? {};
    const rawContext = readNullableRecord(payload.__vc_context);
    return {
        vc_source: readNullableString(rawContext?.vc_source),
        vc_context_message: readNullableString(rawContext?.vc_context_message),
        source_vc_faculty_id:
            readNullableString(rawContext?.source_vc_faculty_id) ?? extractSourceFacultyIdFromPayload(payload),
        source_vc_academic_program_id:
            readNullableString(rawContext?.source_vc_academic_program_id)
            ?? extractSourceAcademicProgramIdFromPayload(payload),
        source_vc_course_id:
            readNullableString(rawContext?.source_vc_course_id) ?? extractSourceCourseIdFromPayload(payload),
    };
}

function readSectionVcContextMetadata(sourcePayloadJson: Record<string, unknown> | null | undefined) {
    const payload = readNullableRecord(sourcePayloadJson) ?? {};
    const rawContext = readNullableRecord(payload.__vc_context);
    return {
        source_vc_section_id:
            readNullableString(rawContext?.source_vc_section_id) ?? extractSourceSectionIdFromPayload(payload),
    };
}

function extractSourceCourseIdFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
    return payloadPickString(sourcePayloadJson, 'id', 'course.id');
}

function extractSourceAcademicProgramIdFromPayload(
    sourcePayloadJson: Record<string, unknown> | null | undefined,
) {
    return payloadPickString(
        sourcePayloadJson,
        'career.id',
        'careerId',
        'programId',
        'program.id',
        'detail.career.id',
        'detail.careerId',
    );
}

function extractSourceFacultyIdFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
    return payloadPickString(
        sourcePayloadJson,
        'career.facultyId',
        'career.faculty.id',
        'facultyId',
        'faculty.id',
        'detail.career.facultyId',
        'detail.career.faculty.id',
    );
}

function extractSourceSectionIdFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
    return payloadPickString(sourcePayloadJson, 'id', 'section.id', 'sectionId');
}

function sortBaseRowsForSimulation(rows: ScheduleContextRow[]) {
    return [...rows].sort((left, right) => {
        const comparisons = [
            getDaySortOrder(left.day_of_week) - getDaySortOrder(right.day_of_week),
            compactTime(left.start_time).localeCompare(compactTime(right.start_time)),
            compactTime(left.end_time).localeCompare(compactTime(right.end_time)),
            (left.campus_name ?? '').localeCompare(right.campus_name ?? ''),
            (left.faculty_name ?? '').localeCompare(right.faculty_name ?? ''),
            (left.program_name ?? '').localeCompare(right.program_name ?? ''),
            buildCourseLabel(left.course_code, left.course_name).localeCompare(
                buildCourseLabel(right.course_code, right.course_name),
            ),
            left.section_code.localeCompare(right.section_code),
            left.subsection_code.localeCompare(right.subsection_code),
            left.schedule_id.localeCompare(right.schedule_id),
        ];
        return comparisons.find((value) => value !== 0) ?? 0;
    });
}

function sortAssignmentPreviewItems(items: AssignmentPreviewItem[]) {
    return [...items].sort((left, right) => {
        const comparisons = [
            (left.conference_date ?? '').localeCompare(right.conference_date ?? ''),
            getDaySortOrder(left.day_of_week) - getDaySortOrder(right.day_of_week),
            left.start_time.localeCompare(right.start_time),
            left.end_time.localeCompare(right.end_time),
            left.schedule_id.localeCompare(right.schedule_id),
            (left.occurrence_key ?? '').localeCompare(right.occurrence_key ?? ''),
        ];
        return comparisons.find((value) => value !== 0) ?? 0;
    });
}

function buildAssignmentHostKey(item: Pick<AssignmentPreviewItem, 'zoom_user_id' | 'zoom_user_email' | 'zoom_user_name'>) {
    const zoomUserId = `${item.zoom_user_id ?? ''}`.trim();
    const zoomUserEmail = `${item.zoom_user_email ?? ''}`.trim().toLowerCase();
    const zoomUserName = `${item.zoom_user_name ?? ''}`.trim().toLowerCase();
    return zoomUserId || zoomUserEmail || zoomUserName || null;
}

function buildWeeklyScheduleDateTime(dayCode: string, timeValue: string) {
    const dayOffset = Math.max(0, getDaySortOrder(dayCode) - 1);
    const [hours, minutes] = compactTime(timeValue)
        .split(':')
        .map((value) => Number(value ?? 0));
    return new Date(Date.UTC(2026, 0, 5 + dayOffset, hours || 0, minutes || 0, 0, 0));
}

function assignReservationToVirtualHost(
    scheduledStart: Date,
    scheduledEnd: Date,
    maxConcurrent: number,
    virtualReservations: Map<string, SimulatedReservation[]>,
    createKey: () => string,
) {
    const windowStart = addMinutes(scheduledStart, -MEETING_MARGIN_MINUTES);
    const windowEnd = addMinutes(scheduledEnd, MEETING_MARGIN_MINUTES);

    for (const [hostKey, reservations] of virtualReservations.entries()) {
        const overlapCount = reservations.filter((item) =>
            doesOverlap(windowStart, windowEnd, item.scheduled_start, item.scheduled_end),
        ).length;
        if (overlapCount < maxConcurrent) {
            reservations.push({ scheduled_start: scheduledStart, scheduled_end: scheduledEnd });
            virtualReservations.set(hostKey, reservations);
            return hostKey;
        }
    }

    const nextKey = createKey();
    virtualReservations.set(nextKey, [{ scheduled_start: scheduledStart, scheduled_end: scheduledEnd }]);
    return nextKey;
}

function toErrorMessage(error: unknown) {
    if (error instanceof Error) {
        return error.message;
    }
    if (typeof error === 'string') {
        return error;
    }
    return 'Error no identificado';
}

function sleep(ms: number) {
    return new Promise<void>((resolve) => setTimeout(resolve, ms));
}
