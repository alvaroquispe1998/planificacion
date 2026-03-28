import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository, SelectQueryBuilder } from 'typeorm';
import { newId } from '../common';
import { ZoomUserEntity } from '../entities/audit.entities';
import {
    AcademicProgramEntity,
    CampusEntity,
    ExternalSourceEntity,
    FacultyEntity,
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
    FilterOptionsDto,
    GenerateVideoconferenceDto,
    UpdateZoomPoolDto,
} from './videoconference.dto';
import {
    PlanningSubsectionVideoconferenceEntity,
    VcSectionEntity,
    VideoconferenceZoomPoolUserEntity,
} from './videoconference.entity';
import { ZoomAccountService, ZoomMeetingSummary } from './zoom-account.service';

const ACTIVE_CONFERENCE_STATUSES = ['CREATING', 'CREATED_UNMATCHED', 'MATCHED'] as const;
const ZOOM_MATCH_ATTEMPTS = 5;
const ZOOM_MATCH_DELAY_MS = 3000;
const MEETING_MARGIN_MINUTES = 10;
const DEFAULT_REMOTE_MEETING_DURATION_MINUTES = 60;
const AULA_VIRTUAL_SOURCE_CODE = 'AULAVIRTUAL';
const DAY_TO_AULA_VIRTUAL_NUMBER: Record<string, number> = {
    LUNES: 0,
    MARTES: 1,
    MIERCOLES: 2,
    JUEVES: 3,
    VIERNES: 4,
    SABADO: 5,
    DOMINGO: 6,
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

type ScheduleContextRow = {
    offer_id: string;
    section_id: string;
    section_code: string;
    subsection_id: string;
    subsection_code: string;
    schedule_id: string;
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
    'campusIds' | 'facultyIds' | 'programIds' | 'courseIds' | 'modality' | 'days'
>;

type FilterCatalogOption = {
    id: string;
    label: string;
};

type FilterOptionRow = {
    campus_id: string | null;
    campus_name: string | null;
    faculty_id: string | null;
    faculty_name: string | null;
    program_id: string | null;
    program_name: string | null;
    course_id: string | null;
    course_code: string | null;
    course_name: string | null;
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

type ExpandedOccurrence = {
    row: ScheduleContextRow;
    conference_date: string;
    scheduled_start: Date;
    scheduled_end: Date;
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
};

type ZoomMatchResult = ZoomMeetingSummary & {
    attempts: number;
};

@Injectable()
export class VideoconferenceService {
    constructor(
        @InjectRepository(PlanningOfferEntity)
        private readonly offersRepo: Repository<PlanningOfferEntity>,
        @InjectRepository(PlanningSubsectionScheduleEntity)
        private readonly schedulesRepo: Repository<PlanningSubsectionScheduleEntity>,
        @InjectRepository(VideoconferenceZoomPoolUserEntity)
        private readonly zoomPoolRepo: Repository<VideoconferenceZoomPoolUserEntity>,
        @InjectRepository(PlanningSubsectionVideoconferenceEntity)
        private readonly planningVideoconferencesRepo: Repository<PlanningSubsectionVideoconferenceEntity>,
        @InjectRepository(ZoomUserEntity)
        private readonly zoomUsersRepo: Repository<ZoomUserEntity>,
        @InjectRepository(ExternalSourceEntity)
        private readonly sourcesRepo: Repository<ExternalSourceEntity>,
        private readonly settingsSyncService: SettingsSyncService,
        private readonly zoomAccountService: ZoomAccountService,
    ) { }

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
        const [campusRows, facultyRows, programRows, courseRows, modalityRows, dayRows] = await Promise.all([
            this.getFilterOptionRows(filters, ['campusIds']),
            this.getFilterOptionRows(filters, ['facultyIds']),
            this.getFilterOptionRows(filters, ['programIds']),
            this.getFilterOptionRows(filters, ['courseIds']),
            this.getFilterOptionRows(filters, ['modality']),
            this.getFilterOptionRows(filters, ['days']),
        ]);

        return {
            campuses: this.buildNamedOptions(campusRows, 'campus_id', 'campus_name', '(Sin sede)'),
            faculties: this.buildNamedOptions(facultyRows, 'faculty_id', 'faculty_name', '(Sin facultad)'),
            programs: this.buildNamedOptions(programRows, 'program_id', 'program_name', '(Sin programa)'),
            courses: this.buildCourseOptions(courseRows),
            modalities: this.buildModalityOptions(modalityRows),
            days: this.buildDayOptions(dayRows),
        };
    }

    async preview(filters: FilterOptionsDto) {
        const rows = await this.getScheduleRows(filters);
        return rows.map((row) => {
            const teacher = resolveTeacher(row);
            const modality = resolveModality(row);
            return {
                id: row.schedule_id,
                schedule_id: row.schedule_id,
                section_id: row.section_id,
                section_code: row.section_code,
                section_label: buildSectionLabel(row),
                subsection_id: row.subsection_id,
                subsection_code: row.subsection_code,
                subsection_label: row.subsection_code,
                campus_id: row.campus_id,
                campus_name: row.campus_name,
                faculty_id: row.faculty_id,
                faculty_name: row.faculty_name,
                program_id: row.program_id,
                program_name: row.program_name,
                course_id: row.course_id,
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
                vc_academic_program_id: row.vc_academic_program_id,
                vc_course_id: row.vc_course_id,
                vc_section_id: row.vc_section_id,
                vc_section_name: row.vc_section_name,
            };
        });
    }

    async getZoomPool() {
        const poolRows = await this.zoomPoolRepo
            .createQueryBuilder('pool')
            .leftJoin(ZoomUserEntity, 'zoom_user', 'zoom_user.id = pool.zoom_user_id')
            .select('pool.id', 'pool_id')
            .addSelect('pool.zoom_user_id', 'zoom_user_id')
            .addSelect('pool.sort_order', 'sort_order')
            .addSelect('pool.is_active', 'is_active')
            .addSelect('zoom_user.name', 'name')
            .addSelect('zoom_user.email', 'email')
            .orderBy('pool.sort_order', 'ASC')
            .addOrderBy('zoom_user.name', 'ASC')
            .getRawMany<Record<string, unknown>>();

        const users = await this.zoomUsersRepo.find({
            order: { name: 'ASC', email: 'ASC' },
        });
        const selectedIds = new Set(poolRows.map((row) => readString(row.zoom_user_id)));

        return {
            items: poolRows.map((row) => ({
                id: readString(row.pool_id),
                zoom_user_id: readString(row.zoom_user_id),
                sort_order: readNumber(row.sort_order),
                is_active: Boolean(row.is_active),
                name: readNullableString(row.name),
                email: readNullableString(row.email),
            })),
            users: users.map((user) => ({
                id: user.id,
                name: user.name,
                email: user.email,
                in_pool: selectedIds.has(user.id),
            })),
        };
    }

    async replaceZoomPool(dto: UpdateZoomPoolDto) {
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

        await this.zoomPoolRepo.clear();
        if (normalizedItems.length) {
            const now = new Date();
            await this.zoomPoolRepo.save(
                normalizedItems.map((item) =>
                    this.zoomPoolRepo.create({
                        id: newId(),
                        zoom_user_id: item.zoom_user_id,
                        sort_order: item.sort_order,
                        is_active: item.is_active,
                        created_at: now,
                        updated_at: now,
                    }),
                ),
            );
        }

        return this.getZoomPool();
    }

    async generate(payload: GenerateVideoconferenceDto) {
        const scheduleIds = normalizeIdArray(payload.scheduleIds);
        if (!scheduleIds.length) {
            throw new BadRequestException('Debe enviar al menos un scheduleId.');
        }

        const startDate = normalizeIsoDate(payload.startDate);
        const endDate = normalizeIsoDate(payload.endDate);
        if (startDate > endDate) {
            throw new BadRequestException('startDate no puede ser mayor que endDate.');
        }

        const rows = await this.getScheduleRows(undefined, scheduleIds);
        const rowMap = new Map(rows.map((row) => [row.schedule_id, row] as const));
        const missingScheduleIds = scheduleIds.filter((scheduleId) => !rowMap.has(scheduleId));

        const aulaVirtualContext = await this.getAulaVirtualRequestContext();
        const zoomConfig = await this.zoomAccountService.requireConfiguredConfig();
        const zoomUsers = await this.getActiveZoomPoolUsers();
        if (!zoomUsers.length) {
            throw new BadRequestException(
                'No hay usuarios Zoom activos en el pool de videoconferencias.',
            );
        }

        const occurrences = this.expandOccurrences(
            Array.from(rowMap.values()),
            startDate,
            endDate,
            zoomConfig.timezone,
        );
        const remoteMeetingsCache = new Map<string, ZoomMeetingSummary[] | null>();
        const results: GenerateResultItem[] = [];

        for (const missingScheduleId of missingScheduleIds) {
            results.push({
                schedule_id: missingScheduleId,
                conference_date: null,
                status: 'ERROR',
                message: 'No se encontro el horario seleccionado en planificacion.',
                record_id: null,
                zoom_user_id: null,
                zoom_user_email: null,
                zoom_meeting_id: null,
            });
        }

        for (const occurrence of occurrences) {
            const result = await this.generateOccurrence(
                occurrence,
                aulaVirtualContext,
                zoomConfig.maxConcurrent,
                zoomUsers,
                remoteMeetingsCache,
            );
            results.push(result);
        }

        const summary = {
            requestedSchedules: scheduleIds.length,
            requestedOccurrences: occurrences.length,
            matched: results.filter((item) => item.status === 'MATCHED').length,
            createdUnmatched: results.filter((item) => item.status === 'CREATED_UNMATCHED').length,
            blockedExisting: results.filter((item) => item.status === 'BLOCKED_EXISTING').length,
            noAvailableZoomUser: results.filter((item) => item.status === 'NO_AVAILABLE_ZOOM_USER').length,
            validationErrors: results.filter((item) => item.status === 'VALIDATION_ERROR').length,
            errors: results.filter((item) => item.status === 'ERROR').length,
        };

        return {
            success: true,
            message: buildGenerationMessage(summary),
            summary,
            results,
        };
    }

    private async generateOccurrence(
        occurrence: ExpandedOccurrence,
        aulaVirtualContext: AulaVirtualRequestContext,
        maxConcurrent: number,
        zoomUsers: ZoomPoolUser[],
        remoteMeetingsCache: Map<string, ZoomMeetingSummary[] | null>,
    ): Promise<GenerateResultItem> {
        const validationError = this.validateScheduleForGeneration(occurrence.row);
        if (validationError) {
            return {
                schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.conference_date,
                status: 'VALIDATION_ERROR',
                message: validationError,
                record_id: null,
                zoom_user_id: null,
                zoom_user_email: null,
                zoom_meeting_id: null,
            };
        }

        const existing = await this.planningVideoconferencesRepo.findOne({
            where: {
                planning_subsection_schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.conference_date,
            },
        });
        if (existing) {
            return {
                schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.conference_date,
                status: 'BLOCKED_EXISTING',
                message: 'Ya existe un registro para este horario en la fecha indicada.',
                record_id: existing.id,
                zoom_user_id: existing.zoom_user_id,
                zoom_user_email: existing.zoom_user_email,
                zoom_meeting_id: existing.zoom_meeting_id,
            };
        }

        const selectedZoomUser = await this.findAvailableZoomUser(
            occurrence.scheduled_start,
            occurrence.scheduled_end,
            zoomUsers,
            maxConcurrent,
            remoteMeetingsCache,
        );
        if (!selectedZoomUser) {
            return {
                schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.conference_date,
                status: 'NO_AVAILABLE_ZOOM_USER',
                message:
                    'No se encontro un usuario Zoom disponible para el horario con el margen definido.',
                record_id: null,
                zoom_user_id: null,
                zoom_user_email: null,
                zoom_meeting_id: null,
            };
        }

        const teacher = resolveTeacher(occurrence.row);
        const topic = buildMeetingTopic(occurrence.row, teacher);
        const aulaVirtualPayload = this.buildAulaVirtualPayload(
            occurrence.row,
            occurrence.conference_date,
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
            conference_date: occurrence.conference_date,
            day_of_week: occurrence.row.day_of_week,
            start_time: compactTime(occurrence.row.start_time),
            end_time: compactTime(occurrence.row.end_time),
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
            status: 'CREATING',
            match_attempts: 0,
            matched_at: null,
            error_message: null,
            payload_json: aulaVirtualPayload,
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

            const matched = await this.matchZoomMeeting(
                selectedZoomUser.email ?? '',
                topic,
                occurrence.scheduled_start,
                occurrence.row.duration_minutes,
            );
            if (matched) {
                record.zoom_meeting_id = matched.id;
                record.join_url = matched.join_url;
                record.start_url = matched.start_url;
                record.status = 'MATCHED';
                record.match_attempts = matched.attempts;
                record.matched_at = new Date();
                record.updated_at = new Date();
                await this.planningVideoconferencesRepo.save(record);
                return {
                    schedule_id: occurrence.row.schedule_id,
                    conference_date: occurrence.conference_date,
                    status: 'MATCHED',
                    message: 'Videoconferencia creada y conciliada con Zoom.',
                    record_id: record.id,
                    zoom_user_id: record.zoom_user_id,
                    zoom_user_email: record.zoom_user_email,
                    zoom_meeting_id: record.zoom_meeting_id,
                };
            }

            record.match_attempts = ZOOM_MATCH_ATTEMPTS;
            record.updated_at = new Date();
            await this.planningVideoconferencesRepo.save(record);
            return {
                schedule_id: occurrence.row.schedule_id,
                conference_date: occurrence.conference_date,
                status: 'CREATED_UNMATCHED',
                message:
                    'La videoconferencia fue creada en Aula Virtual, pero no se pudo conciliar el zoom_meeting_id.',
                record_id: record.id,
                zoom_user_id: record.zoom_user_id,
                zoom_user_email: record.zoom_user_email,
                zoom_meeting_id: null,
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
                conference_date: occurrence.conference_date,
                status: 'ERROR',
                message: toErrorMessage(error),
                record_id: record.id,
                zoom_user_id: record.zoom_user_id,
                zoom_user_email: record.zoom_user_email,
                zoom_meeting_id: record.zoom_meeting_id,
            };
        }
    }

    private async getScheduleRows(filters?: FilterOptionsDto, scheduleIds?: string[]) {
        const qb = this.createScheduleBaseQuery()
            .select('offer.id', 'offer_id')
            .addSelect('section.id', 'section_id')
            .addSelect('section.code', 'section_code')
            .addSelect('subsection.id', 'subsection_id')
            .addSelect('subsection.code', 'subsection_code')
            .addSelect('schedule.id', 'schedule_id')
            .addSelect('offer.campus_id', 'campus_id')
            .addSelect('campus.name', 'campus_name')
            .addSelect('offer.faculty_id', 'faculty_id')
            .addSelect('faculty.name', 'faculty_name')
            .addSelect('offer.academic_program_id', 'program_id')
            .addSelect('program.name', 'program_name')
            .addSelect('offer.study_plan_course_id', 'course_id')
            .addSelect('offer.course_code', 'course_code')
            .addSelect('offer.course_name', 'course_name')
            .addSelect('offer.vc_period_id', 'vc_period_id')
            .addSelect('offer.vc_faculty_id', 'vc_faculty_id')
            .addSelect('offer.vc_academic_program_id', 'vc_academic_program_id')
            .addSelect('offer.vc_course_id', 'vc_course_id')
            .addSelect('subsection.vc_section_id', 'vc_section_id')
            .addSelect('vc_section.name', 'vc_section_name')
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

        return rawRows.map((row) => ({
            offer_id: readString(row.offer_id),
            section_id: readString(row.section_id),
            section_code: readString(row.section_code),
            subsection_id: readString(row.subsection_id),
            subsection_code: readString(row.subsection_code),
            schedule_id: readString(row.schedule_id),
            campus_id: readNullableString(row.campus_id),
            campus_name: readNullableString(row.campus_name),
            faculty_id: readNullableString(row.faculty_id),
            faculty_name: readNullableString(row.faculty_name),
            program_id: readNullableString(row.program_id),
            program_name: readNullableString(row.program_name),
            course_id: readString(row.course_id),
            course_code: readNullableString(row.course_code),
            course_name: readNullableString(row.course_name),
            vc_period_id: readNullableString(row.vc_period_id),
            vc_faculty_id: readNullableString(row.vc_faculty_id),
            vc_academic_program_id: readNullableString(row.vc_academic_program_id),
            vc_course_id: readNullableString(row.vc_course_id),
            vc_section_id: readNullableString(row.vc_section_id),
            vc_section_name: readNullableString(row.vc_section_name),
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
    }

    private async getFilterOptionRows(filters?: FilterOptionsDto, ignoredFilters: FilterOptionKey[] = []) {
        const qb = this.createScheduleBaseQuery()
            .select('offer.campus_id', 'campus_id')
            .addSelect('campus.name', 'campus_name')
            .addSelect('offer.faculty_id', 'faculty_id')
            .addSelect('faculty.name', 'faculty_name')
            .addSelect('offer.academic_program_id', 'program_id')
            .addSelect('program.name', 'program_name')
            .addSelect('offer.study_plan_course_id', 'course_id')
            .addSelect('offer.course_code', 'course_code')
            .addSelect('offer.course_name', 'course_name')
            .addSelect('subsection_modality.code', 'subsection_modality_code')
            .addSelect('subsection_modality.name', 'subsection_modality_name')
            .addSelect('section_modality.code', 'section_modality_code')
            .addSelect('section_modality.name', 'section_modality_name')
            .addSelect('schedule.day_of_week', 'day_of_week');

        this.applyScheduleFilters(qb, filters, undefined, ignoredFilters);

        const rows = await qb.getRawMany<Record<string, unknown>>();
        return rows.map((row) => ({
            campus_id: readNullableString(row.campus_id),
            campus_name: readNullableString(row.campus_name),
            faculty_id: readNullableString(row.faculty_id),
            faculty_name: readNullableString(row.faculty_name),
            program_id: readNullableString(row.program_id),
            program_name: readNullableString(row.program_name),
            course_id: readNullableString(row.course_id),
            course_code: readNullableString(row.course_code),
            course_name: readNullableString(row.course_name),
            subsection_modality_code: readNullableString(row.subsection_modality_code),
            subsection_modality_name: readNullableString(row.subsection_modality_name),
            section_modality_code: readNullableString(row.section_modality_code),
            section_modality_name: readNullableString(row.section_modality_name),
            day_of_week: readNullableString(row.day_of_week),
        }));
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
        if (!ignoredFilters.includes('modality') && filters?.modality?.trim()) {
            const normalizedModality = normalizeModalityCode(filters.modality);
            if (normalizedModality === 'HIBRIDO') {
                qb.andWhere("COALESCE(subsection_modality.code, section_modality.code, '') LIKE 'HIBRIDO%'");
            } else if (normalizedModality) {
                qb.andWhere('COALESCE(subsection_modality.code, section_modality.code) = :modality', {
                    modality: normalizedModality,
                });
            }
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
            const modality = resolveModalityValues(
                row.subsection_modality_code,
                row.subsection_modality_name,
                row.section_modality_code,
                row.section_modality_name,
            );
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

    private async getActiveZoomPoolUsers() {
        const rows = await this.zoomPoolRepo
            .createQueryBuilder('pool')
            .innerJoin(ZoomUserEntity, 'zoom_user', 'zoom_user.id = pool.zoom_user_id')
            .select('pool.id', 'pool_id')
            .addSelect('pool.zoom_user_id', 'zoom_user_id')
            .addSelect('pool.sort_order', 'sort_order')
            .addSelect('pool.is_active', 'is_active')
            .addSelect('zoom_user.name', 'name')
            .addSelect('zoom_user.email', 'email')
            .where('pool.is_active = :isActive', { isActive: true })
            .orderBy('pool.sort_order', 'ASC')
            .addOrderBy('zoom_user.name', 'ASC')
            .getRawMany<Record<string, unknown>>();

        return rows
            .map((row) => ({
                pool_id: readString(row.pool_id),
                zoom_user_id: readString(row.zoom_user_id),
                sort_order: readNumber(row.sort_order),
                is_active: Boolean(row.is_active),
                name: readNullableString(row.name),
                email: readNullableString(row.email),
            }))
            .filter((item) => Boolean(item.email));
    }

    private expandOccurrences(rows: ScheduleContextRow[], startDate: string, endDate: string, timezone: string) {
        const occurrences: ExpandedOccurrence[] = [];
        for (const row of rows) {
            for (const conferenceDate of enumerateConferenceDates(row.day_of_week, startDate, endDate)) {
                occurrences.push({
                    row,
                    conference_date: conferenceDate,
                    scheduled_start: buildDateTime(conferenceDate, row.start_time, timezone),
                    scheduled_end: buildDateTime(conferenceDate, row.end_time, timezone),
                });
            }
        }

        return occurrences.sort((left, right) => {
            if (left.conference_date !== right.conference_date) {
                return left.conference_date.localeCompare(right.conference_date);
            }
            if (left.row.day_of_week !== right.row.day_of_week) {
                return left.row.day_of_week.localeCompare(right.row.day_of_week);
            }
            return left.row.start_time.localeCompare(right.row.start_time);
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
        row: ScheduleContextRow,
        conferenceDate: string,
        zoomUserId: string,
        teacher: ResolvedTeacher,
        topic: string,
    ): AulaVirtualPayload {
        const startTime = compactTime(row.start_time);
        const endTime = compactTime(row.end_time);
        return {
            courseCode: row.course_code?.trim() ?? '',
            courseName: row.course_name?.trim() ?? '',
            section: row.vc_section_name?.trim() ?? '',
            dni: teacher.dni?.trim() ?? '',
            teacher: teacher.name?.trim() ?? '',
            day: row.day_of_week,
            startTime,
            endTime,
            termId: row.vc_period_id?.trim() ?? '',
            facultyId: row.vc_faculty_id?.trim() ?? '',
            careerId: row.vc_academic_program_id?.trim() ?? '',
            courseId: row.vc_course_id?.trim() ?? '',
            name: topic,
            sectionId: row.vc_section_id?.trim() ?? '',
            start: `${formatDateForAulaVirtual(conferenceDate)} ${startTime}`,
            end: `${formatDateForAulaVirtual(conferenceDate)} ${endTime}`,
            minutes: String(row.duration_minutes),
            'daysOfWeek[0]': String(DAY_TO_AULA_VIRTUAL_NUMBER[row.day_of_week]),
            credentialId: zoomUserId,
        };
    }

    private async postAulaVirtualConference(
        context: AulaVirtualRequestContext,
        payload: AulaVirtualPayload,
    ) {
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
        const htmlLooksLikeLogin =
            contentType.includes('text/html') && /login|iniciar sesi[oó]n|password/i.test(bodyText);

        if (!response.ok || redirectedToLogin || htmlLooksLikeLogin) {
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

    private async findAvailableZoomUser(
        scheduledStart: Date,
        scheduledEnd: Date,
        zoomUsers: ZoomPoolUser[],
        maxConcurrent: number,
        remoteMeetingsCache: Map<string, ZoomMeetingSummary[] | null>,
    ) {
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
                const remoteEnd = addMinutes(
                    remoteStart,
                    meeting.duration_minutes ?? DEFAULT_REMOTE_MEETING_DURATION_MINUTES,
                );
                return doesOverlap(windowStart, windowEnd, remoteStart, remoteEnd);
            }).length;

            if (localMeetings.length + remoteOverlapCount < maxConcurrent) {
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
                return {
                    ...candidates[0],
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
}) {
    const parts = [
        `${summary.matched} conciliadas`,
        `${summary.createdUnmatched} creadas sin match`,
        `${summary.blockedExisting} bloqueadas por existente`,
        `${summary.noAvailableZoomUser} sin usuario Zoom`,
        `${summary.validationErrors} con validacion pendiente`,
        `${summary.errors} con error`,
    ];
    return `Proceso completado: ${parts.join(', ')}.`;
}

function buildSectionLabel(row: ScheduleContextRow) {
    const course = buildCourseLabel(row.course_code, row.course_name);
    return `${course} | Seccion ${row.section_code}`;
}

function buildCourseLabel(courseCode: string | null, courseName: string | null) {
    const parts = [courseCode?.trim(), courseName?.trim()].filter(Boolean);
    return parts.join(' - ') || '(Sin curso)';
}

function buildMeetingTopic(row: ScheduleContextRow, teacher: ResolvedTeacher) {
    return [
        row.course_code?.trim() ?? '',
        row.course_name?.trim() ?? '',
        row.vc_section_name?.trim() ?? '',
        teacher.dni?.trim() ?? '',
        teacher.name?.trim() ?? '',
        `${row.day_of_week} ${compactTime(row.start_time)}-${compactTime(row.end_time)}`,
    ].join('|');
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

function resolveModality(row: ScheduleContextRow): ResolvedModality {
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
    if (normalized.startsWith('HIBRIDO')) {
        return 'HIBRIDO';
    }
    return normalized;
}

function displayModality(code: string | null | undefined, fallbackName?: string | null | undefined) {
    switch (normalizeModalityCode(code)) {
        case 'PRESENCIAL':
            return 'Presencial';
        case 'VIRTUAL':
            return 'Virtual';
        case 'HIBRIDO':
            return 'Hibrido';
        default:
            return coalesceText(fallbackName, code) ?? 'Sin modalidad';
    }
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
