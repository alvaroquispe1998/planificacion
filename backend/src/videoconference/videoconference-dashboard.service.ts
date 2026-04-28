import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
    PlanningOfferEntity,
    PlanningScheduleConflictV2Entity,
    PlanningSectionEntity,
    PlanningSubsectionEntity,
    PlanningSubsectionScheduleEntity,
} from '../entities/planning.entities';
import {
    AcademicProgramEntity,
    CampusEntity,
    FacultyEntity,
    TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
    PlanningSubsectionVideoconferenceEntity,
    PlanningSubsectionVideoconferenceOverrideEntity,
    VideoconferenceZoomPoolUserEntity,
} from './videoconference.entity';
import {
    CoverageDimension,
    DashboardCoverageConflictRow,
    DashboardCoverageDailyPoint,
    DashboardCoverageDimensionRow,
    DashboardCoverageMissingItem,
    DashboardCoverageOverrideRow,
    DashboardCoverageSummary,
    DashboardHostCalendarResponse,
    DashboardHostOption,
    DashboardHostSession,
    DashboardTodayErrorItem,
    DashboardTodayHostUtilizationBucket,
    DashboardTodayHostUtilizationResponse,
    DashboardTodaySummary,
    DashboardTodayUpcomingItem,
} from './videoconference-dashboard.dto';

/**
 * Servicio que produce las agregaciones del dashboard de videoconferencias.
 *
 * Iteración 1: tab "Hoy" (operativo).
 * - getTodaySummary
 * - getTodayUpcoming
 * - getTodayErrors
 * - getTodayHostUtilization
 *
 * Diseño:
 * - Toda la agregación se hace en BD (GROUP BY) salvo el desglose por hora,
 *   que itera en JS sobre las VCs del día porque son pocas (<<1000).
 * - El parámetro `date` (YYYY-MM-DD) se valida sólo en formato; las fechas
 *   futuras o pasadas son válidas (el dashboard puede consultarlas).
 */
@Injectable()
export class VideoconferenceDashboardService {
    constructor(
        @InjectRepository(PlanningSubsectionVideoconferenceEntity)
        private readonly vcRepo: Repository<PlanningSubsectionVideoconferenceEntity>,
        @InjectRepository(PlanningSubsectionVideoconferenceOverrideEntity)
        private readonly overridesRepo: Repository<PlanningSubsectionVideoconferenceOverrideEntity>,
        @InjectRepository(VideoconferenceZoomPoolUserEntity)
        private readonly zoomPoolRepo: Repository<VideoconferenceZoomPoolUserEntity>,
        @InjectRepository(PlanningOfferEntity)
        private readonly offersRepo: Repository<PlanningOfferEntity>,
        @InjectRepository(PlanningSubsectionScheduleEntity)
        private readonly schedulesRepo: Repository<PlanningSubsectionScheduleEntity>,
        @InjectRepository(PlanningScheduleConflictV2Entity)
        private readonly conflictsRepo: Repository<PlanningScheduleConflictV2Entity>,
    ) { }

    /**
     * Normaliza la fecha de entrada a `YYYY-MM-DD`. Si no se pasa o es invalida,
     * devuelve la fecha actual del servidor.
     */
    private normalizeDate(input?: string): string {
        if (input && /^\d{4}-\d{2}-\d{2}$/.test(input)) {
            return input;
        }
        const now = new Date();
        const yyyy = now.getFullYear();
        const mm = String(now.getMonth() + 1).padStart(2, '0');
        const dd = String(now.getDate()).padStart(2, '0');
        return `${yyyy}-${mm}-${dd}`;
    }

    async getTodaySummary(rawDate?: string): Promise<DashboardTodaySummary> {
        const date = this.normalizeDate(rawDate);

        // Agrupar VCs del día por status y audit_sync_status. delete_status='DELETED' se excluye.
        const rows = await this.vcRepo
            .createQueryBuilder('vc')
            .select('vc.status', 'status')
            .addSelect('vc.audit_sync_status', 'audit_sync_status')
            .addSelect('COUNT(*)', 'cnt')
            .where('vc.conference_date = :date', { date })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .groupBy('vc.status')
            .addGroupBy('vc.audit_sync_status')
            .getRawMany<{ status: string; audit_sync_status: string; cnt: string }>();

        const totals = {
            total: 0,
            matched: 0,
            createdUnmatched: 0,
            creating: 0,
            error: 0,
            cancelled: 0,
            rescheduled: 0,
        };
        const audit = { pending: 0, synced: 0, error: 0 };

        for (const row of rows) {
            const cnt = Number(row.cnt) || 0;
            totals.total += cnt;
            switch (row.status) {
                case 'MATCHED':
                    totals.matched += cnt;
                    break;
                case 'CREATED_UNMATCHED':
                    totals.createdUnmatched += cnt;
                    break;
                case 'CREATING':
                    totals.creating += cnt;
                    break;
                case 'ERROR':
                    totals.error += cnt;
                    break;
            }
            switch (row.audit_sync_status) {
                case 'SYNCED':
                    audit.synced += cnt;
                    break;
                case 'PENDING':
                    audit.pending += cnt;
                    break;
                case 'ERROR':
                    audit.error += cnt;
                    break;
            }
        }

        // Overrides del día (acciones que no generan VC: SKIP / RESCHEDULE).
        const overrideRows = await this.overridesRepo
            .createQueryBuilder('ovr')
            .select('ovr.action', 'action')
            .addSelect('COUNT(*)', 'cnt')
            .where('ovr.conference_date = :date', { date })
            .groupBy('ovr.action')
            .getRawMany<{ action: string; cnt: string }>();
        for (const row of overrideRows) {
            const cnt = Number(row.cnt) || 0;
            if (row.action === 'SKIP') {
                totals.cancelled += cnt;
            } else if (row.action === 'RESCHEDULE') {
                totals.rescheduled += cnt;
            }
        }

        // Cobertura del día: schedules distintos con VC vs schedules distintos
        // afectados (VC + overrides). Es una métrica conservadora que no
        // requiere recorrer todos los schedules del semestre.
        const distinctSchedulesRow = await this.vcRepo
            .createQueryBuilder('vc')
            .select('COUNT(DISTINCT vc.planning_subsection_schedule_id)', 'cnt')
            .where('vc.conference_date = :date', { date })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .getRawOne<{ cnt: string }>();
        const distinctSchedulesWithVc = Number(distinctSchedulesRow?.cnt ?? 0);

        const distinctOverrideSchedulesRow = await this.overridesRepo
            .createQueryBuilder('ovr')
            .select('COUNT(DISTINCT ovr.planning_subsection_schedule_id)', 'cnt')
            .where('ovr.conference_date = :date', { date })
            .getRawOne<{ cnt: string }>();
        const distinctSchedulesWithOverride = Number(distinctOverrideSchedulesRow?.cnt ?? 0);

        const expectedSchedules = distinctSchedulesWithVc + distinctSchedulesWithOverride;
        const generatedVideoconferences = totals.matched + totals.createdUnmatched;
        const coveragePercent = expectedSchedules > 0
            ? Math.round((generatedVideoconferences / expectedSchedules) * 1000) / 10
            : 0;

        return {
            date,
            totals,
            audit,
            coverage: {
                expectedSchedules,
                generatedVideoconferences,
                coveragePercent,
            },
        };
    }

    async getTodayUpcoming(
        rawDate: string | undefined,
        rawWithinMinutes: number | undefined,
    ): Promise<DashboardTodayUpcomingItem[]> {
        const date = this.normalizeDate(rawDate);
        const withinMinutes = Math.min(
            Math.max(15, Math.trunc(Number(rawWithinMinutes ?? 60) || 60)),
            720,
        );

        const now = new Date();
        const upper = new Date(now.getTime() + withinMinutes * 60_000);

        // Filtrar por la fecha solicitada para acotar el índice; si la fecha
        // es distinta de hoy, devolvemos el inicio del día (útil para revisar
        // sesiones futuras).
        const todayStr = this.normalizeDate();
        const lowerBound = date === todayStr ? now : new Date(`${date}T00:00:00`);
        const upperBound = date === todayStr ? upper : new Date(`${date}T23:59:59`);

        const rows = await this.vcRepo
            .createQueryBuilder('vc')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = vc.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = vc.planning_offer_id')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = vc.planning_subsection_id')
            .leftJoin(TeacherEntity, 'teacher', 'teacher.id = sub.responsible_teacher_id')
            .where('vc.conference_date = :date', { date })
            .andWhere('vc.scheduled_start >= :lower AND vc.scheduled_start <= :upper', {
                lower: lowerBound,
                upper: upperBound,
            })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .select('vc.id', 'id')
            .addSelect('vc.scheduled_start', 'scheduled_start')
            .addSelect('vc.scheduled_end', 'scheduled_end')
            .addSelect('vc.topic', 'topic')
            .addSelect('vc.status', 'status')
            .addSelect('vc.zoom_user_email', 'zoom_user_email')
            .addSelect('vc.zoom_user_name', 'zoom_user_name')
            .addSelect('vc.join_url', 'join_url')
            .addSelect('off.course_code', 'course_code')
            .addSelect('off.course_name', 'course_name')
            .addSelect('sec.code', 'section_code')
            .addSelect('sub.code', 'subsection_code')
            .addSelect('teacher.full_name', 'teacher_full_name')
            .addSelect('teacher.name', 'teacher_name')
            .orderBy('vc.scheduled_start', 'ASC')
            .limit(50)
            .getRawMany<{
                id: string;
                scheduled_start: Date;
                scheduled_end: Date;
                topic: string | null;
                status: string;
                zoom_user_email: string | null;
                zoom_user_name: string | null;
                join_url: string | null;
                course_code: string | null;
                course_name: string | null;
                section_code: string | null;
                subsection_code: string | null;
                teacher_full_name: string | null;
                teacher_name: string | null;
            }>();

        return rows.map((r) => ({
            videoconferenceId: r.id,
            scheduledStart: this.toIso(r.scheduled_start),
            scheduledEnd: this.toIso(r.scheduled_end),
            topic: r.topic,
            status: r.status,
            courseCode: r.course_code,
            courseName: r.course_name,
            sectionLabel: this.buildSectionLabel(r.section_code, r.subsection_code),
            teacherName: r.teacher_full_name ?? r.teacher_name ?? null,
            zoomUserEmail: r.zoom_user_email,
            zoomUserName: r.zoom_user_name,
            joinUrl: r.join_url,
        }));
    }

    async getTodayErrors(
        rawDate: string | undefined,
        rawLimit: number | undefined,
    ): Promise<DashboardTodayErrorItem[]> {
        const date = this.normalizeDate(rawDate);
        const limit = Math.min(Math.max(1, Math.trunc(Number(rawLimit ?? 20) || 20)), 200);

        const rows = await this.vcRepo
            .createQueryBuilder('vc')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = vc.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = vc.planning_offer_id')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = vc.planning_subsection_id')
            .leftJoin(TeacherEntity, 'teacher', 'teacher.id = sub.responsible_teacher_id')
            .where('vc.conference_date = :date', { date })
            .andWhere(
                `(
                    vc.status = 'ERROR'
                    OR vc.audit_sync_status = 'ERROR'
                    OR vc.delete_status = 'ERROR'
                )`,
            )
            .select('vc.id', 'id')
            .addSelect('vc.conference_date', 'conference_date')
            .addSelect('vc.scheduled_start', 'scheduled_start')
            .addSelect('vc.topic', 'topic')
            .addSelect('vc.status', 'status')
            .addSelect('vc.audit_sync_status', 'audit_sync_status')
            .addSelect('vc.delete_status', 'delete_status')
            .addSelect('vc.error_message', 'error_message')
            .addSelect('vc.audit_sync_error', 'audit_sync_error')
            .addSelect('vc.delete_error', 'delete_error')
            .addSelect('off.course_code', 'course_code')
            .addSelect('off.course_name', 'course_name')
            .addSelect('teacher.full_name', 'teacher_full_name')
            .addSelect('teacher.name', 'teacher_name')
            .orderBy('vc.scheduled_start', 'ASC')
            .limit(limit)
            .getRawMany<{
                id: string;
                conference_date: string;
                scheduled_start: Date;
                topic: string | null;
                status: string;
                audit_sync_status: string;
                delete_status: string | null;
                error_message: string | null;
                audit_sync_error: string | null;
                delete_error: string | null;
                course_code: string | null;
                course_name: string | null;
                teacher_full_name: string | null;
                teacher_name: string | null;
            }>();

        return rows.map((r) => ({
            videoconferenceId: r.id,
            conferenceDate: typeof r.conference_date === 'string'
                ? r.conference_date.slice(0, 10)
                : this.toIso(r.conference_date as unknown as Date).slice(0, 10),
            scheduledStart: this.toIso(r.scheduled_start),
            topic: r.topic,
            status: r.status,
            auditSyncStatus: r.audit_sync_status,
            deleteStatus: r.delete_status,
            errorMessage: r.error_message,
            auditError: r.audit_sync_error,
            deleteError: r.delete_error,
            courseCode: r.course_code,
            courseName: r.course_name,
            teacherName: r.teacher_full_name ?? r.teacher_name ?? null,
        }));
    }

    async getTodayHostUtilization(
        rawDate?: string,
    ): Promise<DashboardTodayHostUtilizationResponse> {
        const date = this.normalizeDate(rawDate);

        const poolCountRow = await this.zoomPoolRepo
            .createQueryBuilder('zp')
            .select('COUNT(*)', 'cnt')
            .where('zp.is_active = true')
            .getRawOne<{ cnt: string }>();
        const poolSize = Number(poolCountRow?.cnt ?? 0);

        const vcs = await this.vcRepo
            .createQueryBuilder('vc')
            .select('vc.id', 'id')
            .addSelect('vc.scheduled_start', 'scheduled_start')
            .addSelect('vc.scheduled_end', 'scheduled_end')
            .addSelect('vc.zoom_user_id', 'zoom_user_id')
            .where('vc.conference_date = :date', { date })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .andWhere('vc.scheduled_start IS NOT NULL')
            .andWhere('vc.scheduled_end IS NOT NULL')
            .getRawMany<{
                id: string;
                scheduled_start: Date;
                scheduled_end: Date;
                zoom_user_id: string | null;
            }>();

        const buckets: DashboardTodayHostUtilizationBucket[] = [];
        for (let h = 0; h < 24; h++) {
            buckets.push({ hour: h, activeMeetings: 0, distinctZoomUsers: 0 });
        }
        const usersPerBucket: Set<string>[] = Array.from({ length: 24 }, () => new Set<string>());

        const dayStart = new Date(`${date}T00:00:00`).getTime();
        for (const vc of vcs) {
            const start = new Date(vc.scheduled_start).getTime();
            const end = new Date(vc.scheduled_end).getTime();
            if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
                continue;
            }
            // Para cada hora h del día, la VC está activa si su rango se solapa
            // con [dayStart + h*3600s, dayStart + (h+1)*3600s).
            for (let h = 0; h < 24; h++) {
                const hourStart = dayStart + h * 3_600_000;
                const hourEnd = hourStart + 3_600_000;
                if (start < hourEnd && end > hourStart) {
                    buckets[h].activeMeetings += 1;
                    if (vc.zoom_user_id) {
                        usersPerBucket[h].add(vc.zoom_user_id);
                    }
                }
            }
        }
        for (let h = 0; h < 24; h++) {
            buckets[h].distinctZoomUsers = usersPerBucket[h].size;
        }

        let peakActiveMeetings = 0;
        let peakHour: number | null = null;
        for (const b of buckets) {
            if (b.activeMeetings > peakActiveMeetings) {
                peakActiveMeetings = b.activeMeetings;
                peakHour = b.hour;
            }
        }

        return {
            date,
            poolSize,
            peakActiveMeetings,
            peakHour,
            buckets,
        };
    }

    private toIso(value: Date | string | null): string {
        if (!value) return '';
        const d = value instanceof Date ? value : new Date(value);
        if (Number.isNaN(d.getTime())) return '';
        return d.toISOString();
    }

    private buildSectionLabel(
        sectionCode: string | null,
        subsectionCode: string | null,
    ): string | null {
        const parts = [sectionCode, subsectionCode].filter((p): p is string => !!p);
        return parts.length > 0 ? parts.join(' / ') : null;
    }

    /**
     * Devuelve las sesiones del día agrupadas en 3 fases respecto al "ahora":
     * - ongoing: scheduled_start <= now <= scheduled_end
     * - upcoming: scheduled_start > now
     * - past: scheduled_end < now
     * Solo VC owner (link_mode='OWNED') y no eliminadas.
     */
    async getTodaySessions(rawDate?: string): Promise<{
        date: string;
        ongoing: DashboardTodayUpcomingItem[];
        upcoming: DashboardTodayUpcomingItem[];
        past: DashboardTodayUpcomingItem[];
    }> {
        const date = this.normalizeDate(rawDate);
        const rows = await this.vcRepo
            .createQueryBuilder('vc')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = vc.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = vc.planning_offer_id')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = vc.planning_subsection_id')
            .leftJoin(TeacherEntity, 'teacher', 'teacher.id = sub.responsible_teacher_id')
            .where('vc.conference_date = :date', { date })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .andWhere("vc.link_mode = 'OWNED'")
            .select('vc.id', 'id')
            .addSelect('vc.scheduled_start', 'scheduled_start')
            .addSelect('vc.scheduled_end', 'scheduled_end')
            .addSelect('vc.topic', 'topic')
            .addSelect('vc.status', 'status')
            .addSelect('vc.zoom_user_email', 'zoom_user_email')
            .addSelect('vc.zoom_user_name', 'zoom_user_name')
            .addSelect('vc.join_url', 'join_url')
            .addSelect('off.course_code', 'course_code')
            .addSelect('off.course_name', 'course_name')
            .addSelect('sec.code', 'section_code')
            .addSelect('sub.code', 'subsection_code')
            .addSelect('teacher.full_name', 'teacher_full_name')
            .addSelect('teacher.name', 'teacher_name')
            .orderBy('vc.scheduled_start', 'ASC')
            .getRawMany<{
                id: string;
                scheduled_start: Date;
                scheduled_end: Date;
                topic: string | null;
                status: string;
                zoom_user_email: string | null;
                zoom_user_name: string | null;
                join_url: string | null;
                course_code: string | null;
                course_name: string | null;
                section_code: string | null;
                subsection_code: string | null;
                teacher_full_name: string | null;
                teacher_name: string | null;
            }>();

        const items: DashboardTodayUpcomingItem[] = rows.map((r) => ({
            videoconferenceId: r.id,
            scheduledStart: this.toIso(r.scheduled_start),
            scheduledEnd: this.toIso(r.scheduled_end),
            topic: r.topic,
            status: r.status,
            courseCode: r.course_code,
            courseName: r.course_name,
            sectionLabel: this.buildSectionLabel(r.section_code, r.subsection_code),
            teacherName: r.teacher_full_name ?? r.teacher_name ?? null,
            zoomUserEmail: r.zoom_user_email,
            zoomUserName: r.zoom_user_name,
            joinUrl: r.join_url,
        }));

        const now = Date.now();
        const ongoing: DashboardTodayUpcomingItem[] = [];
        const upcoming: DashboardTodayUpcomingItem[] = [];
        const past: DashboardTodayUpcomingItem[] = [];
        for (const it of items) {
            const start = new Date(it.scheduledStart).getTime();
            const end = new Date(it.scheduledEnd).getTime();
            if (Number.isFinite(start) && Number.isFinite(end)) {
                if (start <= now && end >= now) {
                    ongoing.push(it);
                } else if (start > now) {
                    upcoming.push(it);
                } else {
                    past.push(it);
                }
            }
        }
        // Past: ordenar descendente (las más recientes primero)
        past.sort((a, b) => (a.scheduledStart < b.scheduledStart ? 1 : -1));
        return { date, ongoing, upcoming, past };
    }

    // =====================================================================
    // PERIOD COVERAGE
    // =====================================================================

    private requirePeriodId(periodId?: string): string {
        const id = (periodId ?? '').trim();
        if (!id) {
            throw new Error('periodId es requerido');
        }
        return id;
    }

    async getCoverageSummary(rawPeriodId?: string): Promise<DashboardCoverageSummary> {
        const periodId = this.requirePeriodId(rawPeriodId);

        const offersCountRow = await this.offersRepo
            .createQueryBuilder('off')
            .select('COUNT(*)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .getRawOne<{ cnt: string }>();
        const offersCount = Number(offersCountRow?.cnt ?? 0);

        const sectionsCountRow = await this.offersRepo.manager
            .createQueryBuilder(PlanningSectionEntity, 'sec')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .select('COUNT(*)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .getRawOne<{ cnt: string }>();
        const sectionsCount = Number(sectionsCountRow?.cnt ?? 0);

        const subsectionsCountRow = await this.offersRepo.manager
            .createQueryBuilder(PlanningSubsectionEntity, 'sub')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .select('COUNT(*)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .getRawOne<{ cnt: string }>();
        const subsectionsCount = Number(subsectionsCountRow?.cnt ?? 0);

        const schedulesCountRow = await this.schedulesRepo
            .createQueryBuilder('sched')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = sched.planning_subsection_id')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .select('COUNT(*)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .getRawOne<{ cnt: string }>();
        const schedulesCount = Number(schedulesCountRow?.cnt ?? 0);

        const vcRows = await this.vcRepo
            .createQueryBuilder('vc')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = vc.planning_offer_id')
            .select('vc.status', 'status')
            .addSelect('vc.audit_sync_status', 'audit_sync_status')
            .addSelect('COUNT(*)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .groupBy('vc.status')
            .addGroupBy('vc.audit_sync_status')
            .getRawMany<{ status: string; audit_sync_status: string; cnt: string }>();

        const videoconferences = {
            total: 0,
            matched: 0,
            createdUnmatched: 0,
            creating: 0,
            error: 0,
        };
        const audit = { pending: 0, synced: 0, error: 0 };
        for (const row of vcRows) {
            const cnt = Number(row.cnt) || 0;
            videoconferences.total += cnt;
            switch (row.status) {
                case 'MATCHED':
                    videoconferences.matched += cnt;
                    break;
                case 'CREATED_UNMATCHED':
                    videoconferences.createdUnmatched += cnt;
                    break;
                case 'CREATING':
                    videoconferences.creating += cnt;
                    break;
                case 'ERROR':
                    videoconferences.error += cnt;
                    break;
            }
            switch (row.audit_sync_status) {
                case 'SYNCED':
                    audit.synced += cnt;
                    break;
                case 'PENDING':
                    audit.pending += cnt;
                    break;
                case 'ERROR':
                    audit.error += cnt;
                    break;
            }
        }

        const schedulesWithVcRow = await this.vcRepo
            .createQueryBuilder('vc')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = vc.planning_offer_id')
            .select('COUNT(DISTINCT vc.planning_subsection_schedule_id)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .getRawOne<{ cnt: string }>();
        const schedulesWithVideoconference = Number(schedulesWithVcRow?.cnt ?? 0);

        const schedulesWithOverrideRow = await this.overridesRepo
            .createQueryBuilder('ovr')
            .innerJoin(
                PlanningSubsectionScheduleEntity,
                'sched',
                'sched.id = ovr.planning_subsection_schedule_id',
            )
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = sched.planning_subsection_id')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .select('COUNT(DISTINCT ovr.planning_subsection_schedule_id)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .getRawOne<{ cnt: string }>();
        const schedulesWithOverride = Number(schedulesWithOverrideRow?.cnt ?? 0);

        const coveredRow = await this.schedulesRepo
            .createQueryBuilder('sched')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = sched.planning_subsection_id')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .select('COUNT(DISTINCT sched.id)', 'cnt')
            .where('off.semester_id = :periodId', { periodId })
            .andWhere(
                `(
                    EXISTS (
                        SELECT 1 FROM planning_subsection_videoconferences vc
                        WHERE vc.planning_subsection_schedule_id = sched.id
                          AND (vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')
                    )
                    OR EXISTS (
                        SELECT 1 FROM planning_subsection_videoconference_overrides ovr
                        WHERE ovr.planning_subsection_schedule_id = sched.id
                    )
                )`,
            )
            .getRawOne<{ cnt: string }>();
        const schedulesCovered = Number(coveredRow?.cnt ?? 0);
        const schedulesMissing = Math.max(0, schedulesCount - schedulesCovered);
        const coveragePercent = schedulesCount > 0
            ? Math.round((schedulesCovered / schedulesCount) * 1000) / 10
            : 0;

        return {
            periodId,
            totals: {
                offers: offersCount,
                sections: sectionsCount,
                subsections: subsectionsCount,
                schedules: schedulesCount,
            },
            videoconferences,
            coverage: {
                schedulesWithVideoconference,
                schedulesWithOverride,
                schedulesCovered,
                schedulesMissing,
                coveragePercent,
            },
            audit,
        };
    }

    async getCoverageByDimension(
        rawPeriodId: string | undefined,
        rawDimension: CoverageDimension | undefined,
    ): Promise<DashboardCoverageDimensionRow[]> {
        const periodId = this.requirePeriodId(rawPeriodId);
        const dimension: CoverageDimension = (rawDimension as CoverageDimension) || 'faculty';

        const baseQb = this.schedulesRepo
            .createQueryBuilder('sched')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = sched.planning_subsection_id')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .where('off.semester_id = :periodId', { periodId });

        let idCol = 'off.faculty_id';
        let labelExpr = 'fac.name';
        if (dimension === 'faculty') {
            baseQb.leftJoin(FacultyEntity, 'fac', 'fac.id = off.faculty_id');
            idCol = 'off.faculty_id';
            labelExpr = 'fac.name';
        } else if (dimension === 'campus') {
            baseQb.leftJoin(CampusEntity, 'cmp', 'cmp.id = off.campus_id');
            idCol = 'off.campus_id';
            labelExpr = 'cmp.name';
        } else {
            baseQb.leftJoin(AcademicProgramEntity, 'prg', 'prg.id = off.academic_program_id');
            idCol = 'off.academic_program_id';
            labelExpr = 'prg.name';
        }

        const rows = await baseQb
            .select(idCol, 'id')
            .addSelect(labelExpr, 'label')
            .addSelect('COUNT(DISTINCT sched.id)', 'total')
            .addSelect(
                `COUNT(DISTINCT CASE WHEN EXISTS (
                    SELECT 1 FROM planning_subsection_videoconferences vc
                    WHERE vc.planning_subsection_schedule_id = sched.id
                      AND (vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')
                ) THEN sched.id END)`,
                'with_vc',
            )
            .groupBy(idCol)
            .addGroupBy(labelExpr)
            .orderBy('total', 'DESC')
            .getRawMany<{ id: string | null; label: string | null; total: string; with_vc: string }>();

        return rows.map((row) => {
            const total = Number(row.total) || 0;
            const withVc = Number(row.with_vc) || 0;
            const pct = total > 0 ? Math.round((withVc / total) * 1000) / 10 : 0;
            return {
                id: row.id ?? null,
                label: row.label ?? '(sin asignar)',
                totalSchedules: total,
                schedulesWithVideoconference: withVc,
                coveragePercent: pct,
            };
        });
    }

    async getCoverageMissingSchedules(
        rawPeriodId: string | undefined,
        rawLimit: number | undefined,
    ): Promise<DashboardCoverageMissingItem[]> {
        const periodId = this.requirePeriodId(rawPeriodId);
        const limit = Math.min(Math.max(1, Math.trunc(Number(rawLimit ?? 200) || 200)), 1000);

        const rows = await this.schedulesRepo
            .createQueryBuilder('sched')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = sched.planning_subsection_id')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .leftJoin(TeacherEntity, 'teacher', 'teacher.id = sub.responsible_teacher_id')
            .leftJoin(FacultyEntity, 'fac', 'fac.id = off.faculty_id')
            .leftJoin(CampusEntity, 'cmp', 'cmp.id = off.campus_id')
            .where('off.semester_id = :periodId', { periodId })
            .andWhere(
                `NOT EXISTS (
                    SELECT 1 FROM planning_subsection_videoconferences vc
                    WHERE vc.planning_subsection_schedule_id = sched.id
                      AND (vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')
                )`,
            )
            .andWhere(
                `NOT EXISTS (
                    SELECT 1 FROM planning_subsection_videoconference_overrides ovr
                    WHERE ovr.planning_subsection_schedule_id = sched.id
                )`,
            )
            .select('sched.id', 'schedule_id')
            .addSelect('off.id', 'planning_offer_id')
            .addSelect('sec.id', 'planning_section_id')
            .addSelect('sub.id', 'planning_subsection_id')
            .addSelect('off.course_code', 'course_code')
            .addSelect('off.course_name', 'course_name')
            .addSelect('sec.code', 'section_code')
            .addSelect('sub.code', 'subsection_code')
            .addSelect('sched.day_of_week', 'day_of_week')
            .addSelect('sched.start_time', 'start_time')
            .addSelect('sched.end_time', 'end_time')
            .addSelect('teacher.full_name', 'teacher_full_name')
            .addSelect('teacher.name', 'teacher_name')
            .addSelect('fac.name', 'faculty_name')
            .addSelect('cmp.name', 'campus_name')
            .addSelect('sub.responsible_teacher_id', 'responsible_teacher_id')
            .orderBy('off.course_code', 'ASC')
            .addOrderBy('sec.code', 'ASC')
            .limit(limit)
            .getRawMany<{
                schedule_id: string;
                planning_offer_id: string;
                planning_section_id: string;
                planning_subsection_id: string;
                course_code: string | null;
                course_name: string | null;
                section_code: string | null;
                subsection_code: string | null;
                day_of_week: string;
                start_time: string;
                end_time: string;
                teacher_full_name: string | null;
                teacher_name: string | null;
                faculty_name: string | null;
                campus_name: string | null;
                responsible_teacher_id: string | null;
            }>();

        return rows.map((r) => ({
            scheduleId: r.schedule_id,
            planningOfferId: r.planning_offer_id,
            planningSectionId: r.planning_section_id,
            planningSubsectionId: r.planning_subsection_id,
            courseCode: r.course_code,
            courseName: r.course_name,
            sectionCode: r.section_code,
            subsectionCode: r.subsection_code,
            dayOfWeek: r.day_of_week,
            startTime: r.start_time,
            endTime: r.end_time,
            teacherName: r.teacher_full_name ?? r.teacher_name ?? null,
            facultyName: r.faculty_name,
            campusName: r.campus_name,
            reason: r.responsible_teacher_id ? 'Sin generar' : 'Sin docente asignado',
        }));
    }

    async getCoverageOverrides(
        rawPeriodId?: string,
    ): Promise<DashboardCoverageOverrideRow[]> {
        const periodId = this.requirePeriodId(rawPeriodId);
        const rows = await this.overridesRepo
            .createQueryBuilder('ovr')
            .innerJoin(
                PlanningSubsectionScheduleEntity,
                'sched',
                'sched.id = ovr.planning_subsection_schedule_id',
            )
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = sched.planning_subsection_id')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = sub.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = sec.planning_offer_id')
            .where('off.semester_id = :periodId', { periodId })
            .select('ovr.action', 'action')
            .addSelect('COUNT(*)', 'cnt')
            .groupBy('ovr.action')
            .getRawMany<{ action: string; cnt: string }>();
        return rows.map((r) => ({ action: r.action, count: Number(r.cnt) || 0 }));
    }

    async getCoverageDailySeries(
        rawPeriodId?: string,
    ): Promise<DashboardCoverageDailyPoint[]> {
        const periodId = this.requirePeriodId(rawPeriodId);
        const rows = await this.vcRepo
            .createQueryBuilder('vc')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = vc.planning_offer_id')
            .where('off.semester_id = :periodId', { periodId })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .select('vc.conference_date', 'conference_date')
            .addSelect('COUNT(*)', 'total')
            .addSelect("SUM(CASE WHEN vc.status = 'MATCHED' THEN 1 ELSE 0 END)", 'matched')
            .addSelect(
                "SUM(CASE WHEN vc.status = 'CREATED_UNMATCHED' THEN 1 ELSE 0 END)",
                'created_unmatched',
            )
            .addSelect("SUM(CASE WHEN vc.status = 'ERROR' THEN 1 ELSE 0 END)", 'error')
            .groupBy('vc.conference_date')
            .orderBy('vc.conference_date', 'ASC')
            .getRawMany<{
                conference_date: string | Date;
                total: string;
                matched: string;
                created_unmatched: string;
                error: string;
            }>();
        return rows.map((r) => ({
            conferenceDate: typeof r.conference_date === 'string'
                ? r.conference_date.slice(0, 10)
                : new Date(r.conference_date).toISOString().slice(0, 10),
            total: Number(r.total) || 0,
            matched: Number(r.matched) || 0,
            createdUnmatched: Number(r.created_unmatched) || 0,
            error: Number(r.error) || 0,
        }));
    }

    async getCoverageConflicts(
        rawPeriodId?: string,
    ): Promise<DashboardCoverageConflictRow[]> {
        const periodId = this.requirePeriodId(rawPeriodId);
        const rows = await this.conflictsRepo
            .createQueryBuilder('c')
            .where('c.semester_id = :periodId', { periodId })
            .select('c.conflict_type', 'conflict_type')
            .addSelect('c.severity', 'severity')
            .addSelect('COUNT(*)', 'cnt')
            .groupBy('c.conflict_type')
            .addGroupBy('c.severity')
            .orderBy('cnt', 'DESC')
            .getRawMany<{ conflict_type: string; severity: string; cnt: string }>();
        return rows.map((r) => ({
            conflictType: r.conflict_type,
            severity: r.severity,
            count: Number(r.cnt) || 0,
        }));
    }

    // =====================================================================
    // HOST CALENDAR
    // =====================================================================

    /**
     * Lista los hosts (zoom users) que tienen al menos una VC activa en el
     * rango [from, to]. Si el rango no se pasa, usa la semana actual del
     * servidor (lunes a domingo).
     */
    async getHostOptions(
        rawFrom?: string,
        rawTo?: string,
    ): Promise<DashboardHostOption[]> {
        const { from, to } = this.normalizeRange(rawFrom, rawTo);
        const rows = await this.vcRepo
            .createQueryBuilder('vc')
            .select('vc.zoom_user_id', 'zoom_user_id')
            .addSelect('MAX(vc.zoom_user_email)', 'zoom_user_email')
            .addSelect('MAX(vc.zoom_user_name)', 'zoom_user_name')
            .addSelect('COUNT(*)', 'cnt')
            .where('vc.conference_date BETWEEN :from AND :to', { from, to })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .andWhere("vc.link_mode = 'OWNED'")
            .andWhere('vc.zoom_user_id IS NOT NULL')
            .groupBy('vc.zoom_user_id')
            .orderBy('cnt', 'DESC')
            .getRawMany<{
                zoom_user_id: string;
                zoom_user_email: string | null;
                zoom_user_name: string | null;
                cnt: string;
            }>();
        return rows.map((r) => ({
            zoomUserId: r.zoom_user_id,
            email: r.zoom_user_email,
            name: r.zoom_user_name,
            sessionCount: Number(r.cnt) || 0,
        }));
    }

    /**
     * Devuelve todas las VCs de un host en el rango [from, to] con metadata
     * suficiente para pintarlas en una vista calendario.
     */
    async getHostCalendar(
        rawZoomUserId: string | undefined,
        rawFrom?: string,
        rawTo?: string,
    ): Promise<DashboardHostCalendarResponse> {
        const zoomUserId = (rawZoomUserId ?? '').trim();
        if (!zoomUserId) {
            throw new Error('zoomUserId es requerido');
        }
        const { from, to } = this.normalizeRange(rawFrom, rawTo);
        const rows = await this.vcRepo
            .createQueryBuilder('vc')
            .innerJoin(PlanningSectionEntity, 'sec', 'sec.id = vc.planning_section_id')
            .innerJoin(PlanningOfferEntity, 'off', 'off.id = vc.planning_offer_id')
            .innerJoin(PlanningSubsectionEntity, 'sub', 'sub.id = vc.planning_subsection_id')
            .leftJoin(TeacherEntity, 'teacher', 'teacher.id = sub.responsible_teacher_id')
            .where('vc.zoom_user_id = :zoomUserId', { zoomUserId })
            .andWhere('vc.conference_date BETWEEN :from AND :to', { from, to })
            .andWhere("(vc.delete_status IS NULL OR vc.delete_status <> 'DELETED')")
            .andWhere("vc.link_mode = 'OWNED'")
            .select('vc.id', 'id')
            .addSelect('vc.conference_date', 'conference_date')
            .addSelect('vc.scheduled_start', 'scheduled_start')
            .addSelect('vc.scheduled_end', 'scheduled_end')
            .addSelect('vc.topic', 'topic')
            .addSelect('vc.status', 'status')
            .addSelect('vc.join_url', 'join_url')
            .addSelect('off.course_code', 'course_code')
            .addSelect('off.course_name', 'course_name')
            .addSelect('sec.code', 'section_code')
            .addSelect('sub.code', 'subsection_code')
            .addSelect('teacher.full_name', 'teacher_full_name')
            .addSelect('teacher.name', 'teacher_name')
            .orderBy('vc.scheduled_start', 'ASC')
            .getRawMany<{
                id: string;
                conference_date: string | Date;
                scheduled_start: Date;
                scheduled_end: Date;
                topic: string | null;
                status: string;
                join_url: string | null;
                course_code: string | null;
                course_name: string | null;
                section_code: string | null;
                subsection_code: string | null;
                teacher_full_name: string | null;
                teacher_name: string | null;
            }>();

        const sessions: DashboardHostSession[] = rows.map((r) => ({
            videoconferenceId: r.id,
            scheduledStart: this.toIso(r.scheduled_start),
            scheduledEnd: this.toIso(r.scheduled_end),
            conferenceDate: typeof r.conference_date === 'string'
                ? r.conference_date.slice(0, 10)
                : this.toIso(r.conference_date as unknown as Date).slice(0, 10),
            status: r.status,
            topic: r.topic,
            courseCode: r.course_code,
            courseName: r.course_name,
            sectionLabel: this.buildSectionLabel(r.section_code, r.subsection_code),
            teacherName: r.teacher_full_name ?? r.teacher_name ?? null,
            joinUrl: r.join_url,
        }));

        return { zoomUserId, from, to, sessions };
    }

    /**
     * Normaliza un rango [from, to] a YYYY-MM-DD. Si ambos faltan, devuelve
     * la semana actual del servidor (lunes a domingo). Acota max 60 días.
     */
    private normalizeRange(
        rawFrom?: string,
        rawTo?: string,
    ): { from: string; to: string } {
        const isoRe = /^\d{4}-\d{2}-\d{2}$/;
        if (rawFrom && rawTo && isoRe.test(rawFrom) && isoRe.test(rawTo)) {
            const from = rawFrom <= rawTo ? rawFrom : rawTo;
            const to = rawFrom <= rawTo ? rawTo : rawFrom;
            // Acotar a 60 días por seguridad.
            const fromDate = new Date(`${from}T00:00:00`);
            const toDate = new Date(`${to}T00:00:00`);
            const diffDays = Math.round(
                (toDate.getTime() - fromDate.getTime()) / 86_400_000,
            );
            if (diffDays > 60) {
                const capped = new Date(fromDate.getTime() + 60 * 86_400_000);
                return { from, to: this.dateOnly(capped) };
            }
            return { from, to };
        }
        // Default: semana actual (lunes-domingo)
        const today = new Date();
        const day = today.getDay(); // 0=domingo, 1=lunes
        const diffToMonday = (day + 6) % 7;
        const monday = new Date(today);
        monday.setDate(today.getDate() - diffToMonday);
        const sunday = new Date(monday);
        sunday.setDate(monday.getDate() + 6);
        return { from: this.dateOnly(monday), to: this.dateOnly(sunday) };
    }

    private dateOnly(d: Date): string {
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
    }
}
