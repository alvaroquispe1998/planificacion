import {
    BadRequestException,
    ForbiddenException,
    Injectable,
    NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { DataSource, In, Repository } from 'typeorm';
import { newId } from '../common';
import {
    MeetingInstanceEntity,
    MeetingParticipantEntity,
    MeetingRecordingEntity,
    ZoomUserEntity,
} from '../entities/audit.entities';
import { AuthUserEntity } from '../entities/auth.entities';
import {
    ManualVideoconferenceEntity,
    ManualVideoconferenceUserZoomGroupEntity,
} from '../entities/videoconference-creator.entities';
import {
    ZoomGroupEntity,
    ZoomGroupUserEntity,
    ZoomConfigEntity,
} from '../videoconference/videoconference.entity';
import { ZoomAccountService } from '../videoconference/zoom-account.service';
import {
    ApproveDraftBackupDto,
    CreateManualVideoconferenceDto,
    SetUserZoomGroupsDto,
} from './videoconference-creator.dto';

const MEETING_MARGIN_MINUTES = 10;
const DEFAULT_MAX_CONCURRENT_MEETINGS = 2;

@Injectable()
export class VideoconferenceCreatorService {
    constructor(
        @InjectRepository(ManualVideoconferenceEntity)
        private readonly mvRepo: Repository<ManualVideoconferenceEntity>,

        @InjectRepository(ManualVideoconferenceUserZoomGroupEntity)
        private readonly userGroupRepo: Repository<ManualVideoconferenceUserZoomGroupEntity>,

        @InjectRepository(ZoomGroupEntity)
        private readonly zoomGroupRepo: Repository<ZoomGroupEntity>,

        @InjectRepository(ZoomGroupUserEntity)
        private readonly zoomGroupUserRepo: Repository<ZoomGroupUserEntity>,

        @InjectRepository(ZoomUserEntity)
        private readonly zoomUserRepo: Repository<ZoomUserEntity>,

        @InjectRepository(MeetingInstanceEntity)
        private readonly instanceRepo: Repository<MeetingInstanceEntity>,

        @InjectRepository(MeetingParticipantEntity)
        private readonly participantRepo: Repository<MeetingParticipantEntity>,

        @InjectRepository(MeetingRecordingEntity)
        private readonly recordingRepo: Repository<MeetingRecordingEntity>,

        @InjectRepository(ZoomConfigEntity)
        private readonly zoomConfigRepo: Repository<ZoomConfigEntity>,

        @InjectRepository(AuthUserEntity)
        private readonly authUserRepo: Repository<AuthUserEntity>,

        private readonly zoomService: ZoomAccountService,
        private readonly dataSource: DataSource,
    ) { }

    // ── Profile ───────────────────────────────────────────────────────────────

    async getProfile(
        userId: string,
        permissions: string[],
        isAdmin: boolean,
    ) {
        const canView = isAdmin || permissions.includes('action.videoconference_creator.view');
        const canUnique = isAdmin || permissions.includes('action.videoconference_creator.create_unique');
        const canWeekly = isAdmin || permissions.includes('action.videoconference_creator.create_weekly');
        const canApprove = isAdmin || permissions.includes('action.videoconference_creator.approve_backup');

        const assignments = await this.userGroupRepo.find({
            where: { user_id: userId, is_active: true },
        });
        const groupIds = assignments.map((a) => a.zoom_group_id);
        const groups = groupIds.length > 0
            ? await this.zoomGroupRepo.find({ where: { id: In(groupIds), is_active: true } })
            : [];

        return {
            can_view: canView,
            can_create_unique: canUnique,
            can_create_weekly: canWeekly,
            can_approve_backup: canApprove,
            assigned_groups: groups.map((g) => ({ id: g.id, name: g.name, code: g.code })),
        };
    }

    // ── List meetings ─────────────────────────────────────────────────────────

    async listMeetings(userId: string, isAdminOrTI: boolean) {
        const qb = this.mvRepo.createQueryBuilder('mv').orderBy('mv.start_time', 'DESC');
        if (!isAdminOrTI) {
            qb.where('mv.created_by_user_id = :userId', { userId });
        }
        const meetings = await qb.getMany();
        return this.enrichMeetings(meetings);
    }

    // ── Get single meeting ─────────────────────────────────────────────────────

    async getMeeting(id: string, userId: string, isAdminOrTI: boolean) {
        const mv = await this.mvRepo.findOne({ where: { id } });
        if (!mv) throw new NotFoundException('Videoconferencia no encontrada.');
        if (!isAdminOrTI && mv.created_by_user_id !== userId) {
            throw new ForbiddenException('Sin acceso a esta videoconferencia.');
        }

        const instances = await this.instanceRepo.find({
            where: { manual_videoconference_id: id },
            order: { created_at: 'ASC' },
        });

        const participants = instances.length > 0
            ? await this.participantRepo.find({
                where: { meeting_instance_id: In(instances.map((i) => i.id)) },
                order: { created_at: 'ASC' },
            })
            : [];

        // Only return MP4 recordings (the ones users can actually watch)
        const recordings = instances.length > 0
            ? await this.recordingRepo.find({
                where: {
                    meeting_instance_id: In(instances.map((i) => i.id)),
                    recording_type: 'MP4',
                    status: 'AVAILABLE',
                },
                order: { start_time: 'ASC' },
            })
            : [];

        const [meeting] = await this.enrichMeetings([mv]);
        return { meeting, instances, participants, recordings };
    }

    async cancelMeeting(id: string, userId: string, isAdminOrTI: boolean) {
        const mv = await this.mvRepo.findOne({ where: { id } });
        if (!mv) throw new NotFoundException('Videoconferencia no encontrada.');
        if (!isAdminOrTI && mv.created_by_user_id !== userId) {
            throw new ForbiddenException('Sin acceso a esta videoconferencia.');
        }
        if (mv.status === 'CANCELLED') {
            throw new BadRequestException('La videoconferencia ya está cancelada.');
        }
        if (this.isMeetingInProgress(mv)) {
            throw new BadRequestException('La videoconferencia está en proceso. No se puede cancelar mientras está en curso.');
        }
        if (this.isMeetingFinished(mv)) {
            throw new BadRequestException('La videoconferencia ya finalizó. No se puede cancelar una reunión finalizada.');
        }

        if (mv.zoom_meeting_id) {
            const deleteResult = await this.zoomService.deleteZoomMeeting(mv.zoom_meeting_id);
            if (!deleteResult.deleted) {
                throw new BadRequestException(`No se pudo cancelar en Zoom: ${deleteResult.reason}`);
            }
        }

        mv.status = 'CANCELLED';
        mv.error_message = null;
        mv.updated_at = new Date();
        await this.mvRepo.save(mv);

        const [meeting] = await this.enrichMeetings([mv]);
        return meeting;
    }

    // ── Create meeting ─────────────────────────────────────────────────────────

    async createMeeting(userId: string, dto: CreateManualVideoconferenceDto) {
        // 1. Verify group is assigned to the user
        const assignment = await this.userGroupRepo.findOne({
            where: { user_id: userId, zoom_group_id: dto.zoom_group_id, is_active: true },
        });
        if (!assignment) {
            throw new ForbiddenException('No tienes acceso a este grupo Zoom.');
        }

        const group = await this.zoomGroupRepo.findOne({ where: { id: dto.zoom_group_id, is_active: true } });
        if (!group) throw new NotFoundException('Grupo Zoom no encontrado.');

        // 2. Compute end time
        const startDate = new Date(dto.start_time);
        const endDate = new Date(startDate.getTime() + dto.duration_minutes * 60 * 1000);
        const maxConcurrent = await this.getMaxConcurrentMeetings();

        // 3. Find an available host in the group
        const groupUsers = await this.zoomGroupUserRepo.find({
            where: { group_id: dto.zoom_group_id, is_active: true },
            order: { sort_order: 'ASC' },
        });

        let availableHostId: string | null = null;
        let availableHostEmail: string | null = null;

        for (const gu of groupUsers) {
            const zoomUser = await this.zoomUserRepo.findOne({ where: { id: gu.zoom_user_id } });
            if (!zoomUser) continue;

            const hasCapacity = await this.hasHostCapacity(zoomUser.id, startDate, endDate, maxConcurrent, null);
            if (hasCapacity) {
                availableHostId = zoomUser.id;
                availableHostEmail = zoomUser.email;
                break;
            }
        }

        const now = new Date();
        const mvId = newId();

        // 4a. If no host is available, ask the frontend to confirm whether this should become an approval request.
        if (!availableHostEmail) {
            if (!dto.request_approval_if_no_host) {
                throw new BadRequestException({
                    code: 'NO_HOST_AVAILABLE',
                    message:
                        'No fue posible crear la reunión porque no hay usuarios Zoom disponibles en el grupo seleccionado para ese horario.',
                });
            }
            const entity = this.mvRepo.create({
                id: mvId,
                created_by_user_id: userId,
                zoom_group_id: dto.zoom_group_id,
                assigned_zoom_user_id: null,
                backup_zoom_user_id: null,
                type: dto.type,
                topic: dto.topic,
                agenda: dto.agenda ?? null,
                start_time: startDate,
                end_time: endDate,
                duration_minutes: dto.duration_minutes,
                recurrence_json: this.buildRecurrenceJson(dto),
                status: 'DRAFT_NO_HOST',
                zoom_meeting_id: null,
                join_url: null,
                start_url: null,
                zoom_payload_json: null,
                zoom_response_json: null,
                error_message: null,
                created_at: now,
                updated_at: now,
            });
            await this.mvRepo.save(entity);
            await this.sendDraftAlertToTI(dto.topic, dto.zoom_group_id);
            return entity;
        }

        // 4b. Create in Zoom
        const payload = this.buildZoomPayload(dto, availableHostEmail);
        const result = await this.zoomService.createZoomMeeting(availableHostEmail, payload);

        const status = result.ok ? 'CREATED' : 'ERROR';
        const entity = this.mvRepo.create({
            id: mvId,
            created_by_user_id: userId,
            zoom_group_id: dto.zoom_group_id,
            assigned_zoom_user_id: availableHostId,
            backup_zoom_user_id: null,
            type: dto.type,
            topic: dto.topic,
            agenda: dto.agenda ?? null,
            start_time: startDate,
            end_time: endDate,
            duration_minutes: dto.duration_minutes,
            recurrence_json: this.buildRecurrenceJson(dto),
            status,
            zoom_meeting_id: result.ok ? result.meetingId : null,
            join_url: result.ok ? result.joinUrl : null,
            start_url: result.ok ? result.startUrl : null,
            zoom_payload_json: payload as Record<string, unknown>,
            zoom_response_json: result.ok ? result.response : (result.response ?? null),
            error_message: result.ok ? null : result.reason,
            created_at: now,
            updated_at: now,
        });

        await this.mvRepo.save(entity);
        return entity;
    }

    // ── Sync from Zoom ─────────────────────────────────────────────────────────

    async syncFromZoom(id: string, userId: string, isAdminOrTI: boolean) {
        const mv = await this.mvRepo.findOne({ where: { id } });
        if (!mv) throw new NotFoundException('Videoconferencia no encontrada.');
        if (!isAdminOrTI && mv.created_by_user_id !== userId) {
            throw new ForbiddenException('Sin acceso a esta videoconferencia.');
        }
        if (!mv.zoom_meeting_id) {
            throw new BadRequestException('Esta videoconferencia no tiene un Zoom meeting ID asignado.');
        }

        // Sync past instances
        const zoomInstances = await this.zoomService.listPastMeetingInstances(mv.zoom_meeting_id);
        const now = new Date();
        let synced = 0;

        for (const inst of zoomInstances) {
            const existing = await this.instanceRepo.findOne({
                where: { zoom_meeting_id: mv.zoom_meeting_id, zoom_meeting_uuid: inst.uuid },
            });
            if (existing) continue;

            const instanceEntity = this.instanceRepo.create({
                id: newId(),
                video_conference_id: null,
                planning_subsection_videoconference_id: null,
                manual_videoconference_id: id,
                zoom_meeting_id: mv.zoom_meeting_id,
                zoom_meeting_uuid: inst.uuid,
                scheduled_start: mv.start_time,
                scheduled_end: mv.end_time,
                actual_start: inst.start_time ? new Date(inst.start_time) : null,
                actual_end: null,
                duration_minutes: inst.duration_minutes,
                status: 'ENDED',
                raw_json: inst.raw,
                created_at: now,
                updated_at: now,
            });
            await this.instanceRepo.save(instanceEntity);
            synced++;
        }

        return { synced_instances: synced };
    }

    // ── Approve draft with backup ──────────────────────────────────────────────

    async approveDraftWithBackup(id: string, dto: ApproveDraftBackupDto) {
        const mv = await this.mvRepo.findOne({ where: { id } });
        if (!mv) throw new NotFoundException('Videoconferencia no encontrada.');
        if (mv.status !== 'DRAFT_NO_HOST') {
            throw new BadRequestException('Solo se pueden aprobar videoconferencias en estado DRAFT_NO_HOST.');
        }

        const group = await this.zoomGroupRepo.findOne({ where: { id: mv.zoom_group_id } });
        if (!group) throw new NotFoundException('Grupo Zoom no encontrado.');

        const maxConcurrent = await this.getMaxConcurrentMeetings();
        const backupHost = dto.override_backup_zoom_user_id
            ? await this.getBackupHostFromUser(dto.override_backup_zoom_user_id)
            : group.backup_zoom_group_id
                ? await this.findAvailableHostInGroup(group.backup_zoom_group_id, mv.start_time, mv.end_time, maxConcurrent)
                : null;
        if (!backupHost) {
            const reason = group.backup_zoom_group_id
                ? 'No hay host disponible en el grupo backup configurado.'
                : 'No hay grupo backup configurado para este grupo Zoom.';
            throw new BadRequestException(reason);
        }

        const backupUser = backupHost.user;
        if (!backupUser) throw new NotFoundException('Usuario Zoom backup no encontrado.');

        const payload = this.buildZoomPayload(
            {
                type: mv.type,
                zoom_group_id: mv.zoom_group_id,
                topic: mv.topic,
                agenda: mv.agenda ?? undefined,
                start_time: mv.start_time.toISOString(),
                duration_minutes: mv.duration_minutes,
                recurrence_end_date: (mv.recurrence_json as any)?.end_date_time?.slice(0, 10) ?? undefined,
                recurrence_weekly_days: (mv.recurrence_json as any)?.weekly_days ?? undefined,
            },
            backupUser.email,
        );

        const result = await this.zoomService.createZoomMeeting(backupUser.email, payload);
        const now = new Date();

        mv.status = result.ok ? 'APPROVED_WITH_BACKUP' : 'ERROR';
        mv.assigned_zoom_user_id = backupUser.id;
        mv.backup_zoom_user_id = backupUser.id;
        mv.zoom_meeting_id = result.ok ? result.meetingId : mv.zoom_meeting_id;
        mv.join_url = result.ok ? result.joinUrl : mv.join_url;
        mv.start_url = result.ok ? result.startUrl : mv.start_url;
        mv.zoom_payload_json = payload as Record<string, unknown>;
        mv.zoom_response_json = result.ok ? result.response : (result.response ?? null);
        mv.error_message = result.ok ? null : result.reason;
        mv.updated_at = now;

        await this.mvRepo.save(mv);
        return mv;
    }

    // ── User ↔ Zoom group assignments (security panel) ─────────────────────────

    async getUserZoomGroups(userId: string) {
        const assignments = await this.userGroupRepo.find({ where: { user_id: userId } });
        const groupIds = assignments.map((a) => a.zoom_group_id);
        const groups = groupIds.length > 0
            ? await this.zoomGroupRepo.find({ where: { id: In(groupIds) } })
            : [];
        return assignments.map((a) => ({
            ...a,
            group: groups.find((g) => g.id === a.zoom_group_id) ?? null,
        }));
    }

    async setUserZoomGroups(userId: string, dto: SetUserZoomGroupsDto) {
        const now = new Date();
        await this.dataSource.transaction(async (manager) => {
            // Deactivate all existing
            await manager.update(
                ManualVideoconferenceUserZoomGroupEntity,
                { user_id: userId },
                { is_active: false, updated_at: now },
            );

            for (const groupId of dto.zoom_group_ids) {
                const existing = await manager.findOne(ManualVideoconferenceUserZoomGroupEntity, {
                    where: { user_id: userId, zoom_group_id: groupId },
                });
                if (existing) {
                    await manager.update(
                        ManualVideoconferenceUserZoomGroupEntity,
                        { id: existing.id },
                        { is_active: true, updated_at: now },
                    );
                } else {
                    await manager.insert(ManualVideoconferenceUserZoomGroupEntity, {
                        id: newId(),
                        user_id: userId,
                        zoom_group_id: groupId,
                        is_active: true,
                        created_at: now,
                        updated_at: now,
                    });
                }
            }
        });
        return this.getUserZoomGroups(userId);
    }

    // ── Drafts (for TI / admin) ────────────────────────────────────────────────

    async listDrafts() {
        return this.mvRepo.find({
            where: { status: 'DRAFT_NO_HOST' },
            order: { created_at: 'ASC' },
        });
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private async enrichMeetings(meetings: ManualVideoconferenceEntity[]) {
        if (meetings.length === 0) {
            return [];
        }

        const creatorIds = [...new Set(meetings.map((m) => m.created_by_user_id).filter(Boolean))];
        const assignedHostIds = [...new Set(meetings.map((m) => m.assigned_zoom_user_id).filter(Boolean))] as string[];
        const backupHostIds = [...new Set(meetings.map((m) => m.backup_zoom_user_id).filter(Boolean))] as string[];
        const zoomUserIds = [...new Set([...assignedHostIds, ...backupHostIds])];

        const [creators, zoomUsers, zoomGroups] = await Promise.all([
            creatorIds.length
                ? this.authUserRepo.find({ where: { id: In(creatorIds) }, select: ['id', 'display_name', 'username'] })
                : Promise.resolve([]),
            zoomUserIds.length
                ? this.zoomUserRepo.find({ where: { id: In(zoomUserIds) } })
                : Promise.resolve([]),
            this.zoomGroupRepo.find(),
        ]);

        const creatorMap = new Map(creators.map((u) => [u.id, u.display_name || u.username]));
        const zoomUserMap = new Map(zoomUsers.map((u) => [u.id, u.name || u.email]));
        const zoomGroupMap = new Map(zoomGroups.map((group) => [group.id, group]));

        const now = new Date();
        return meetings.map((m) => {
            const isInProgress = this.isMeetingInProgress(m, now);
            const isFinished = this.isMeetingFinished(m, now);
            return {
                ...m,
                creator_display_name: creatorMap.get(m.created_by_user_id) ?? null,
                assigned_zoom_user_name: m.assigned_zoom_user_id
                    ? (zoomUserMap.get(m.assigned_zoom_user_id) ?? null)
                    : null,
                backup_zoom_user_name: m.backup_zoom_user_id
                    ? (zoomUserMap.get(m.backup_zoom_user_id) ?? null)
                    : null,
                zoom_group_name: zoomGroupMap.get(m.zoom_group_id)?.name ?? null,
                backup_zoom_group_id: zoomGroupMap.get(m.zoom_group_id)?.backup_zoom_group_id ?? null,
                backup_zoom_group_name: zoomGroupMap.get(zoomGroupMap.get(m.zoom_group_id)?.backup_zoom_group_id ?? '')?.name ?? null,
                is_in_progress: isInProgress,
                can_cancel: m.status !== 'CANCELLED' && !isInProgress && !isFinished && (m.status === 'DRAFT_NO_HOST' || m.start_time > now),
            };
        });
    }

    private isMeetingInProgress(meeting: ManualVideoconferenceEntity, now = new Date()): boolean {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST') {
            return false;
        }

        if (meeting.type === 'WEEKLY') {
            return this.isWeeklyMeetingInProgress(meeting, now);
        }

        return meeting.start_time <= now && now < meeting.end_time;
    }

    private isMeetingFinished(meeting: ManualVideoconferenceEntity, now = new Date()): boolean {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST') {
            return false;
        }

        if (meeting.type === 'WEEKLY') {
            return this.isWeeklyMeetingFinished(meeting, now);
        }

        return now >= meeting.end_time;
    }

    private isWeeklyMeetingInProgress(meeting: ManualVideoconferenceEntity, now: Date): boolean {
        const recurrence = meeting.recurrence_json as Record<string, unknown> | null;
        const weeklyDays = String(recurrence?.['weekly_days'] ?? '').split(',').map((item) => item.trim()).filter(Boolean);
        const todayZoomDay = String(now.getDay() + 1);
        if (weeklyDays.length && !weeklyDays.includes(todayZoomDay)) {
            return false;
        }

        if (now < meeting.start_time) {
            return false;
        }
        const endDateTime = typeof recurrence?.['end_date_time'] === 'string'
            ? new Date(recurrence['end_date_time'])
            : null;
        if (endDateTime && now > endDateTime) {
            return false;
        }

        const startMinutes = meeting.start_time.getHours() * 60 + meeting.start_time.getMinutes();
        const endMinutes = startMinutes + meeting.duration_minutes;
        const nowMinutes = now.getHours() * 60 + now.getMinutes();
        return nowMinutes >= startMinutes && nowMinutes < endMinutes;
    }

    private isWeeklyMeetingFinished(meeting: ManualVideoconferenceEntity, now: Date): boolean {
        const recurrence = meeting.recurrence_json as Record<string, unknown> | null;
        const endDateTime = typeof recurrence?.['end_date_time'] === 'string'
            ? new Date(recurrence['end_date_time'])
            : null;
        return Boolean(endDateTime && now > endDateTime);
    }

    private async findAvailableHostInGroup(
        groupId: string,
        start: Date,
        end: Date,
        maxConcurrent: number,
    ): Promise<{ user: ZoomUserEntity; groupUser: ZoomGroupUserEntity } | null> {
        const group = await this.zoomGroupRepo.findOne({ where: { id: groupId, is_active: true } });
        if (!group) {
            return null;
        }
        const groupUsers = await this.zoomGroupUserRepo.find({
            where: { group_id: group.id, is_active: true },
            order: { sort_order: 'ASC' },
        });
        for (const groupUser of groupUsers) {
            const zoomUser = await this.zoomUserRepo.findOne({ where: { id: groupUser.zoom_user_id } });
            if (!zoomUser) continue;
            const hasCapacity = await this.hasHostCapacity(zoomUser.id, start, end, maxConcurrent, null);
            if (hasCapacity) {
                return { user: zoomUser, groupUser };
            }
        }
        return null;
    }

    private async getBackupHostFromUser(zoomUserId: string): Promise<{ user: ZoomUserEntity; groupUser: ZoomGroupUserEntity | null } | null> {
        const zoomUser = await this.zoomUserRepo.findOne({ where: { id: zoomUserId } });
        return zoomUser ? { user: zoomUser, groupUser: null } : null;
    }

    private async hasHostCapacity(
        zoomUserId: string,
        start: Date,
        end: Date,
        maxConcurrent: number,
        excludeId: string | null,
    ): Promise<boolean> {
        const windowStart = addMinutes(start, -MEETING_MARGIN_MINUTES);
        const windowEnd = addMinutes(end, MEETING_MARGIN_MINUTES);
        const qb = this.mvRepo
            .createQueryBuilder('mv')
            .where('mv.assigned_zoom_user_id = :zoomUserId', { zoomUserId })
            .andWhere("mv.status NOT IN ('CANCELLED', 'ERROR', 'DRAFT_NO_HOST')")
            .andWhere('mv.start_time < :windowEnd', { windowEnd })
            .andWhere('mv.end_time > :windowStart', { windowStart });

        if (excludeId) {
            qb.andWhere('mv.id != :excludeId', { excludeId });
        }

        const count = await qb.getCount();
        return count < maxConcurrent;
    }

    private async getMaxConcurrentMeetings(): Promise<number> {
        const config = await this.zoomConfigRepo.find({ order: { created_at: 'ASC' }, take: 1 });
        const value = Number(config[0]?.maxConcurrent ?? DEFAULT_MAX_CONCURRENT_MEETINGS);
        if (!Number.isFinite(value)) {
            return DEFAULT_MAX_CONCURRENT_MEETINGS;
        }
        return Math.max(1, Math.floor(value));
    }

    private buildRecurrenceJson(dto: CreateManualVideoconferenceDto): Record<string, unknown> | null {
        if (dto.type !== 'WEEKLY') return null;
        return {
            type: 2, // weekly
            weekly_days: dto.recurrence_weekly_days ?? '2',
            end_date_time: dto.recurrence_end_date
                ? `${dto.recurrence_end_date}T23:59:00Z`
                : null,
        };
    }

    private buildZoomPayload(
        dto: Pick<
            CreateManualVideoconferenceDto,
            'type' | 'topic' | 'agenda' | 'start_time' | 'duration_minutes' | 'recurrence_end_date' | 'recurrence_weekly_days'
        > & { zoom_group_id?: string },
        hostEmail: string,
    ): Record<string, unknown> {
        const isWeekly = dto.type === 'WEEKLY';

        const payload: Record<string, unknown> = {
            topic: dto.topic,
            type: isWeekly ? 8 : 2, // 2 = scheduled, 8 = recurring fixed-time
            start_time: toZoomLocalDateTime(dto.start_time),
            duration: dto.duration_minutes,
            agenda: dto.agenda ?? '',
            timezone: 'America/Lima',
            settings: {
                host_video: true,
                participant_video: false,
                join_before_host: false,
                mute_upon_entry: true,
                waiting_room: true,
                auto_recording: 'cloud',
            },
        };

        if (isWeekly && dto.recurrence_end_date) {
            payload['recurrence'] = {
                type: 2,
                weekly_days: dto.recurrence_weekly_days ?? '2',
                end_date_time: `${dto.recurrence_end_date}T23:59:00Z`,
            };
        }

        return payload;
    }

    private async sendDraftAlertToTI(topic: string, groupId: string): Promise<void> {
        try {
            const configs = await this.zoomConfigRepo.find({ take: 1 });
            const emails = configs[0]?.ti_alert_emails;
            if (!emails?.trim()) return;

            const recipients = emails
                .split(',')
                .map((e) => e.trim())
                .filter(Boolean);
            if (recipients.length === 0) return;

            // Basic nodemailer-free approach: just log the alert.
            // Replace with a real mailer (e.g. Nodemailer) if desired.
            console.warn(
                `[VideoconferenceCreator] DRAFT_NO_HOST alert — topic="${topic}" group=${groupId} — TI emails: ${recipients.join(', ')}`,
            );
        } catch {
            // Non-critical: do not throw
        }
    }
}

function toZoomLocalDateTime(value: string) {
    const raw = `${value ?? ''}`.trim();
    const localMatch = raw.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})(?::(\d{2}))?$/);
    if (localMatch) {
        return `${localMatch[1]}T${localMatch[2]}:${localMatch[3] ?? '00'}`;
    }

    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) {
        return raw;
    }

    const limaTime = new Date(parsed.getTime() - 5 * 60 * 60 * 1000);
    const p = (n: number) => String(n).padStart(2, '0');
    return [
        limaTime.getUTCFullYear(),
        p(limaTime.getUTCMonth() + 1),
        p(limaTime.getUTCDate()),
    ].join('-') + `T${p(limaTime.getUTCHours())}:${p(limaTime.getUTCMinutes())}:${p(limaTime.getUTCSeconds())}`;
}

function addMinutes(value: Date, minutes: number) {
    return new Date(value.getTime() + minutes * 60 * 1000);
}
