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

        // Return all AVAILABLE recordings so the creator can watch/download any type
        const recordings = instances.length > 0
            ? await this.recordingRepo.find({
                where: {
                    meeting_instance_id: In(instances.map((i) => i.id)),
                    status: 'AVAILABLE',
                },
                order: { start_time: 'ASC', recording_type: 'ASC' },
            })
            : [];

        const [meeting] = await this.enrichMeetings([mv]);
        return { meeting, instances, participants, recordings };
    }

    async getZoomMeetingStatus(id: string, userId: string, isAdminOrTI: boolean) {
        const mv = await this.mvRepo.findOne({ where: { id } });
        if (!mv) throw new NotFoundException('Videoconferencia no encontrada.');
        if (!isAdminOrTI && mv.created_by_user_id !== userId) {
            throw new ForbiddenException('Sin acceso a esta videoconferencia.');
        }
        if (!mv.zoom_meeting_id) {
            return { zoom_status: null, reason: 'NO_ZOOM_ID' as const };
        }
        const zoomMeeting = await this.zoomService.getMeeting(mv.zoom_meeting_id);
        if (!zoomMeeting) {
            return { zoom_status: null, reason: 'NOT_FOUND' as const };
        }
        return {
            zoom_status: zoomMeeting.status,
            host_email: zoomMeeting.host_email,
        };
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
            await this.sendDraftAlertToTI(dto.topic, dto.zoom_group_id, userId, startDate, dto.duration_minutes, entity.id);
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
        let syncedParticipants = 0;
        let syncedRecordings = 0;
        const recordingErrors: string[] = [];

        for (const inst of zoomInstances) {
            const existing = await this.instanceRepo.findOne({
                where: { zoom_meeting_id: mv.zoom_meeting_id, zoom_meeting_uuid: inst.uuid },
            });

            let instanceId: string;
            if (existing) {
                instanceId = existing.id;
            } else {
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
                instanceId = instanceEntity.id;
                synced++;
            }

            // Sync participants for this instance (skip if already present)
            const existingParticipants = await this.participantRepo.count({ where: { meeting_instance_id: instanceId } });
            if (existingParticipants === 0) {
                try {
                    const participants = await this.zoomService.listPastMeetingParticipants(inst.uuid);
                    for (const p of participants) {
                        const roleMap: Record<string, string> = { HOST: 'HOST', CO_HOST: 'CO_HOST', PANELIST: 'PANELIST', ATTENDEE: 'ATTENDEE' };
                        const mappedRole = (p.role && roleMap[p.role]) ? roleMap[p.role] as 'HOST' | 'CO_HOST' | 'PANELIST' | 'ATTENDEE' | 'UNKNOWN' : 'UNKNOWN';
                        await this.participantRepo.save(this.participantRepo.create({
                            id: newId(),
                            meeting_instance_id: instanceId,
                            zoom_participant_id: p.zoom_participant_id,
                            zoom_user_id: p.zoom_user_id,
                            display_name: p.display_name || '—',
                            email: p.email,
                            role: mappedRole,
                            teacher_id: null,
                            created_at: now,
                        }));
                        syncedParticipants++;
                    }
                } catch { /* ignore per-instance participant errors */ }
            }

            // Sync recordings — always replace (delete + re-insert) so status changes (processing→available) are picked up
            try {
                // Try UUID first (instance-specific); fall back to numeric ID if Zoom returns nothing
                let recordings = await this.zoomService.listMeetingRecordings(inst.uuid);
                if (!recordings.length && mv.zoom_meeting_id) {
                    recordings = await this.zoomService.listMeetingRecordings(mv.zoom_meeting_id);
                    if (recordings.length) {
                        console.log(`[VideoconferenceCreator] syncFromZoom: recordings fetched via numeric meeting ID ${mv.zoom_meeting_id}`);
                    }
                }
                const validTypes = ['MP4', 'M4A', 'CHAT', 'TRANSCRIPT', 'VTT', 'OTHER'] as const;
                // Delete existing recordings for this instance before re-inserting
                await this.recordingRepo.delete({ meeting_instance_id: instanceId });
                for (const r of recordings) {
                    const recType = (r.recording_type && validTypes.includes(r.recording_type as any))
                        ? r.recording_type as typeof validTypes[number]
                        : 'OTHER';
                    // Normalize status the same way audit does: toUpperCase, default to AVAILABLE
                    const statusNorm = `${r.status ?? ''}`.trim().toUpperCase();
                    const recStatus = (['DELETED', 'EXPIRED', 'ERROR'] as string[]).includes(statusNorm)
                        ? (statusNorm as 'DELETED' | 'EXPIRED' | 'ERROR')
                        : 'AVAILABLE';
                    await this.recordingRepo.save(this.recordingRepo.create({
                        id: newId(),
                        meeting_instance_id: instanceId,
                        zoom_recording_id: r.zoom_recording_id,
                        recording_type: recType,
                        file_extension: r.file_extension,
                        file_size_bytes: r.file_size_bytes,
                        download_url: r.download_url,
                        play_url: r.play_url,
                        start_time: r.start_time ? new Date(r.start_time) : null,
                        end_time: r.end_time ? new Date(r.end_time) : null,
                        status: recStatus,
                        raw_json: r.raw,
                        created_at: now,
                    }));
                    syncedRecordings++;
                }
            } catch (err) {
                recordingErrors.push(err instanceof Error ? err.message : 'Error desconocido al obtener grabaciones');
            }
        }

        return {
            synced_instances: synced,
            synced_participants: syncedParticipants,
            synced_recordings: syncedRecordings,
            recording_errors: recordingErrors,
        };
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

        // Send approval reply email (non-blocking)
        if (result.ok) {
            void this.sendReplyEmail(mv, 'approved', backupUser.email);
        }

        return mv;
    }

    async denyDraft(id: string): Promise<ManualVideoconferenceEntity> {
        const mv = await this.mvRepo.findOne({ where: { id } });
        if (!mv) throw new NotFoundException('Videoconferencia no encontrada.');
        if (mv.status !== 'DRAFT_NO_HOST') {
            throw new BadRequestException('Solo se pueden denegar videoconferencias en estado DRAFT_NO_HOST.');
        }

        const now = new Date();
        mv.status = 'DENIED';
        mv.updated_at = now;
        await this.mvRepo.save(mv);

        // Send denial reply email (non-blocking)
        void this.sendReplyEmail(mv, 'denied', null);

        return mv;
    }

    /** Sends a reply (approve or deny) to the requester. Tries to thread it as a reply in TI's mailbox. */
    private async sendReplyEmail(
        mv: ManualVideoconferenceEntity,
        action: 'approved' | 'denied',
        hostEmail: string | null,
    ): Promise<void> {
        try {
            const configs = await this.zoomConfigRepo.find({ order: { created_at: 'ASC' }, take: 1 });
            const dbConfig = configs[0];

            const tenantId = (dbConfig?.ms_tenant_id ?? process.env['MS_TENANT_ID'] ?? '').trim();
            const clientId = (dbConfig?.ms_client_id ?? process.env['MS_CLIENT_ID'] ?? '').trim();
            const clientSecret = (dbConfig?.ms_client_secret ?? process.env['MS_CLIENT_SECRET'] ?? '').trim();
            const tiMailbox = (dbConfig?.mail_ti_recipient ?? process.env['MAIL_TI_RECIPIENT'] ?? '').trim();
            const systemUrl = (dbConfig?.system_public_url ?? process.env['SYSTEM_PUBLIC_URL'] ?? '').trim().replace(/\/$/, '');

            if (!tenantId || !clientId || !clientSecret || !tiMailbox) return;

            // Get creator info — this is the RECIPIENT of the reply
            const creator = await this.authUserRepo.findOne({
                where: { id: mv.created_by_user_id },
                select: ['id', 'email', 'display_name', 'username'] as any,
            });
            const creatorEmail = creator?.email?.trim();
            if (!creatorEmail) return;
            const creatorName = (creator as any).display_name || (creator as any).username || creatorEmail;

            const accessToken = await this.getMsGraphToken(tenantId, clientId, clientSecret);
            if (!accessToken) return;

            const meetingUrl = systemUrl ? `${systemUrl}/videoconferences/creator/${mv.id}` : '';

            let subject: string;
            let html: string;

            if (action === 'approved') {
                subject = `[Videoconferencia] ✅ Solicitud aprobada — ${mv.topic}`;
                html = this.buildReplyHtml({
                    topic: mv.topic,
                    creatorName,
                    action: 'approved',
                    hostEmail: hostEmail ?? '',
                    meetingUrl,
                });
            } else {
                subject = `[Videoconferencia] ❌ Solicitud denegada — ${mv.topic}`;
                html = this.buildReplyHtml({
                    topic: mv.topic,
                    creatorName,
                    action: 'denied',
                    hostEmail: '',
                    meetingUrl,
                });
            }

            let emailSent = false;

            // ── Attempt 1: threaded reply via createReply in TI's mailbox ──────────
            if (mv.ti_alert_message_id) {
                try {
                    // Find the original alert in TI's inbox by its RFC Message-ID
                    const escaped = mv.ti_alert_message_id.replace(/'/g, "''");
                    const searchRes = await fetch(
                        `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(tiMailbox)}/messages` +
                        `?$filter=internetMessageId eq '${escaped}'&$select=id&$top=1`,
                        { headers: { Authorization: `Bearer ${accessToken}` }, signal: AbortSignal.timeout(15_000) },
                    );
                    if (searchRes.ok) {
                        const searchData = await searchRes.json() as Record<string, unknown>;
                        const msgs = Array.isArray(searchData['value']) ? searchData['value'] as Record<string, unknown>[] : [];
                        const origGraphId = typeof msgs[0]?.['id'] === 'string' ? msgs[0]['id'] as string : undefined;
                        if (origGraphId) {
                            // Create a draft reply (automatically FROM TI, threads correctly)
                            const replyDraftRes = await fetch(
                                `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(tiMailbox)}/messages/${encodeURIComponent(origGraphId)}/createReply`,
                                {
                                    method: 'POST',
                                    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        message: {
                                            subject,
                                            body: { contentType: 'HTML', content: html },
                                            toRecipients: [{ emailAddress: { address: creatorEmail, name: creatorName } }],
                                        },
                                    }),
                                    signal: AbortSignal.timeout(15_000),
                                },
                            );
                            if (replyDraftRes.ok) {
                                const replyDraft = await replyDraftRes.json() as Record<string, unknown>;
                                const replyDraftId = typeof replyDraft['id'] === 'string' ? replyDraft['id'] as string : undefined;
                                if (replyDraftId) {
                                    const sendRes = await fetch(
                                        `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(tiMailbox)}/messages/${encodeURIComponent(replyDraftId)}/send`,
                                        { method: 'POST', headers: { Authorization: `Bearer ${accessToken}` }, signal: AbortSignal.timeout(15_000) },
                                    );
                                    if (sendRes.ok) {
                                        emailSent = true;
                                        console.log(`[VideoconferenceCreator] sendReplyEmail: ${action} threaded reply sent from ${tiMailbox} to ${creatorEmail}`);
                                    } else {
                                        console.warn(`[VideoconferenceCreator] sendReplyEmail: send threaded ${sendRes.status}`);
                                    }
                                }
                            } else {
                                console.warn(`[VideoconferenceCreator] sendReplyEmail: createReply ${replyDraftRes.status}`);
                            }
                        }
                    }
                } catch (threadErr) {
                    console.warn('[VideoconferenceCreator] sendReplyEmail: threaded send failed, falling back', threadErr);
                }
            }

            // ── Attempt 2: fallback — new message FROM TI TO creator ─────────────
            if (!emailSent) {
                const messageBody: Record<string, unknown> = {
                    subject,
                    body: { contentType: 'HTML', content: html },
                    toRecipients: [{ emailAddress: { address: creatorEmail, name: creatorName } }],
                    from: { emailAddress: { address: tiMailbox, name: 'Asistencia TI' } },
                };
                if (mv.ti_alert_message_id) {
                    messageBody['singleValueExtendedProperties'] = [
                        { id: 'String 0x0070', value: mv.ti_alert_message_id },
                        { id: 'String 0x1042', value: mv.ti_alert_message_id },
                    ];
                }
                const createRes = await fetch(
                    `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(tiMailbox)}/messages`,
                    {
                        method: 'POST',
                        headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
                        body: JSON.stringify(messageBody),
                        signal: AbortSignal.timeout(15_000),
                    },
                );
                if (!createRes.ok) {
                    const errBody = await createRes.text();
                    console.warn(`[VideoconferenceCreator] sendReplyEmail fallback createMessage ${createRes.status}: ${errBody.slice(0, 300)}`);
                    return;
                }
                const createdMsg = await createRes.json() as Record<string, unknown>;
                const graphMsgId = typeof createdMsg['id'] === 'string' ? createdMsg['id'] as string : undefined;
                if (!graphMsgId) return;
                const sendRes = await fetch(
                    `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(tiMailbox)}/messages/${encodeURIComponent(graphMsgId)}/send`,
                    { method: 'POST', headers: { Authorization: `Bearer ${accessToken}` }, signal: AbortSignal.timeout(15_000) },
                );
                if (sendRes.ok) {
                    console.log(`[VideoconferenceCreator] sendReplyEmail: ${action} reply sent from ${tiMailbox} to ${creatorEmail}`);
                } else {
                    console.warn(`[VideoconferenceCreator] sendReplyEmail fallback send ${sendRes.status}`);
                }
            }
        } catch (err) {
            console.warn('[VideoconferenceCreator] sendReplyEmail error:', err);
        }
    }

    private async getMsGraphToken(tenantId: string, clientId: string, clientSecret: string): Promise<string | null> {
        try {
            const res = await fetch(
                `https://login.microsoftonline.com/${encodeURIComponent(tenantId)}/oauth2/v2.0/token`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: new URLSearchParams({
                        grant_type: 'client_credentials',
                        client_id: clientId,
                        client_secret: clientSecret,
                        scope: 'https://graph.microsoft.com/.default',
                    }).toString(),
                    signal: AbortSignal.timeout(12_000),
                },
            );
            const data = await res.json() as Record<string, unknown>;
            return typeof data['access_token'] === 'string' ? data['access_token'] : null;
        } catch {
            return null;
        }
    }

    private buildReplyHtml(p: {
        topic: string;
        creatorName: string;
        action: 'approved' | 'denied';
        hostEmail: string;
        meetingUrl: string;
    }): string {
        const isApproved = p.action === 'approved';
        const headerBg = isApproved
            ? 'linear-gradient(135deg,#1a5c36 0%,#1e8a50 100%)'
            : 'linear-gradient(135deg,#6b1a1a 0%,#9b2020 100%)';
        const icon = isApproved ? '✅' : '❌';
        const title = isApproved ? 'Solicitud aprobada' : 'Solicitud denegada';
        const bodyText = isApproved
            ? `La solicitud de videoconferencia fue <strong style="color:#1a6b3a">aprobada</strong>. Se asignó el host <strong>${p.hostEmail}</strong> y la reunión fue creada en Zoom.`
            : `La solicitud de videoconferencia fue <strong style="color:#9b2020">denegada</strong>. No se creará la reunión en Zoom.`;
        const btnHtml = p.meetingUrl
            ? `<div style="margin-top:24px;text-align:center">
                 <a href="${p.meetingUrl}" style="display:inline-block;padding:11px 24px;background:#1e3458;color:#fff;border-radius:9px;text-decoration:none;font-weight:700;font-size:13px">
                   Ver solicitud
                 </a>
               </div>`
            : '';

        return `<!DOCTYPE html>
<html lang="es">
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#e8edf4;font-family:'Segoe UI',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#e8edf4;padding:32px 16px">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.12)">
        <tr>
          <td style="background:${headerBg};padding:24px 36px">
            <p style="margin:0 0 4px;color:rgba(255,255,255,.8);font-size:11px;letter-spacing:.12em;text-transform:uppercase;font-weight:700">SISTEMA DE VIDEOCONFERENCIAS</p>
            <h1 style="margin:0;color:#ffffff;font-size:19px;font-weight:700">${icon} ${title}</h1>
          </td>
        </tr>
        <tr>
          <td style="padding:28px 36px;background:#ffffff">
            <p style="margin:0 0 20px;color:#334e6b;font-size:14px;line-height:1.6">${bodyText}</p>
            <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #c8d8ea;border-radius:10px;overflow:hidden;font-size:14px">
              <tr style="background:#eef4fb">
                <td style="padding:11px 16px;color:#4a6a88;font-weight:700;width:38%;border-bottom:1px solid #c8d8ea">Reunión</td>
                <td style="padding:11px 16px;color:#1a2e4a;font-weight:700;border-bottom:1px solid #c8d8ea">${p.topic}</td>
              </tr>
              <tr style="background:#ffffff">
                <td style="padding:11px 16px;color:#4a6a88;font-weight:700">Solicitante</td>
                <td style="padding:11px 16px;color:#1a2e4a;font-weight:600">${p.creatorName}</td>
              </tr>
            </table>
            ${btnHtml}
          </td>
        </tr>
        <tr>
          <td style="padding:14px 36px;background:#eef4fb;border-top:1px solid #c8d8ea;text-align:center;color:#6688aa;font-size:11px">
            Universidad Autónoma de Ica — Sistema de Planificación Académica
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`;
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

    private async sendDraftAlertToTI(
        topic: string,
        groupId: string,
        creatorUserId: string,
        startTime: Date,
        durationMinutes: number,
        meetingId: string,
    ): Promise<void> {
        try {
            // Read MS Graph config from DB (falls back to env for local dev)
            const configs = await this.zoomConfigRepo.find({ order: { created_at: 'ASC' }, take: 1 });
            const dbConfig = configs[0];

            const tenantId = (dbConfig?.ms_tenant_id ?? process.env['MS_TENANT_ID'] ?? '').trim();
            const clientId = (dbConfig?.ms_client_id ?? process.env['MS_CLIENT_ID'] ?? '').trim();
            const clientSecret = (dbConfig?.ms_client_secret ?? process.env['MS_CLIENT_SECRET'] ?? '').trim();
            const tiRecipient = (dbConfig?.mail_ti_recipient ?? process.env['MAIL_TI_RECIPIENT'] ?? 'asistencia.ti@autonomadeica.edu.pe').trim();
            const systemUrl = (dbConfig?.system_public_url ?? process.env['SYSTEM_PUBLIC_URL'] ?? '').trim().replace(/\/$/, '');

            if (!tenantId || !clientId || !clientSecret) {
                console.warn('[VideoconferenceCreator] sendDraftAlertToTI: MS Graph no configurado en BD ni en env, omitiendo correo.');
                return;
            }

            // Resolve creator info
            const creator = await this.authUserRepo.findOne({
                where: { id: creatorUserId },
                select: ['id', 'email', 'display_name', 'username'] as any,
            });
            const fromEmail = creator?.email?.trim();
            if (!fromEmail) {
                console.warn('[VideoconferenceCreator] sendDraftAlertToTI: creator has no email, skipping.');
                return;
            }
            const creatorName = (creator as any).display_name || (creator as any).username || fromEmail;

            // Resolve group name
            const group = await this.zoomGroupRepo.findOne({ where: { id: groupId } });
            const groupName = group?.name ?? 'Grupo principal';

            // Format date/time (Lima, Perú)
            const dateStr = startTime.toLocaleDateString('es-PE', {
                weekday: 'long', day: '2-digit', month: 'long', year: 'numeric',
                timeZone: 'America/Lima',
            });
            const timeStr = startTime.toLocaleTimeString('es-PE', {
                hour: '2-digit', minute: '2-digit',
                timeZone: 'America/Lima',
            });

            // ── Get MS Graph token ───────────────────────────────────────────
            const accessToken = await this.getMsGraphToken(tenantId, clientId, clientSecret);
            if (!accessToken) {
                console.warn('[VideoconferenceCreator] sendDraftAlertToTI: failed to get MS Graph token.');
                return;
            }

            // ── Send email via Graph createMessage → send (captures internetMessageId) ──
            const subject = `[Videoconferencia] Solicitud pendiente de host — ${topic}`;
            const html = this.buildDraftAlertHtml({
                topic, creatorName, fromEmail, dateStr, timeStr, durationMinutes, groupName, systemUrl, meetingId,
            });

            // Step 1: create the message in the mailbox (returns the message object with id)
            const createRes = await fetch(
                `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(fromEmail)}/messages`,
                {
                    method: 'POST',
                    headers: {
                        Authorization: `Bearer ${accessToken}`,
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        subject,
                        body: { contentType: 'HTML', content: html },
                        toRecipients: [{ emailAddress: { address: tiRecipient } }],
                        from: { emailAddress: { address: fromEmail, name: creatorName } },
                    }),
                    signal: AbortSignal.timeout(15_000),
                },
            );

            if (!createRes.ok) {
                const errorBody = await createRes.text();
                console.warn(`[VideoconferenceCreator] sendDraftAlertToTI: createMessage ${createRes.status}: ${errorBody.slice(0, 400)}`);
                return;
            }

            const createdMsg = await createRes.json() as Record<string, unknown>;
            const graphMsgId = createdMsg['id'] as string | undefined;           // internal Graph ID for send
            const internetMsgId = createdMsg['internetMessageId'] as string | undefined; // RFC Message-ID for threading

            if (!graphMsgId) {
                console.warn('[VideoconferenceCreator] sendDraftAlertToTI: no message id returned from Graph.');
                return;
            }

            // Step 2: send the created message
            const sendRes = await fetch(
                `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(fromEmail)}/messages/${encodeURIComponent(graphMsgId)}/send`,
                {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${accessToken}` },
                    signal: AbortSignal.timeout(15_000),
                },
            );

            if (!sendRes.ok) {
                const errorBody = await sendRes.text();
                console.warn(`[VideoconferenceCreator] sendDraftAlertToTI: send ${sendRes.status}: ${errorBody.slice(0, 400)}`);
                return;
            }

            console.log(`[VideoconferenceCreator] sendDraftAlertToTI: email enviado de ${fromEmail} a ${tiRecipient}`);

            // Persist internetMessageId for reply threading
            if (internetMsgId) {
                await this.mvRepo.update({ id: meetingId }, { ti_alert_message_id: internetMsgId });
            }
        } catch (err) {
            // Non-critical: do not throw — the meeting was already saved
            console.warn('[VideoconferenceCreator] sendDraftAlertToTI error:', err);
        }
    }

    private buildDraftAlertHtml(p: {
        topic: string;
        creatorName: string;
        fromEmail: string;
        dateStr: string;
        timeStr: string;
        durationMinutes: number;
        groupName: string;
        systemUrl: string;
        meetingId: string;
    }): string {
        const meetingUrl = p.systemUrl ? `${p.systemUrl}/videoconferences/creator/${p.meetingId}` : '';
        const btnHtml = meetingUrl
            ? `<div style="margin-top:28px;text-align:center">
                 <a href="${meetingUrl}" style="display:inline-block;padding:13px 28px;background:#1e3458;color:#fff;border-radius:9px;text-decoration:none;font-weight:700;font-size:14px;letter-spacing:.02em">
                   Ver solicitud
                 </a>
               </div>`
            : '';

        return `<!DOCTYPE html>
<html lang="es">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0f4f9;font-family:'Segoe UI',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f4f9;padding:32px 16px">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.10)">

        <!-- Header -->
        <tr>
          <td style="background:linear-gradient(135deg,#1e3458 0%,#1e5799 100%);padding:28px 36px">
            <p style="margin:0 0 6px;color:#a8c4e8;font-size:11px;letter-spacing:.12em;text-transform:uppercase;font-weight:700">SISTEMA DE VIDEOCONFERENCIAS</p>
            <h1 style="margin:0;color:#fff;font-size:21px;font-weight:700;line-height:1.3">📹 Solicitud pendiente de host Zoom</h1>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:32px 36px">
            <p style="margin:0 0 24px;color:#60799b;font-size:14px;line-height:1.6">
              Se envió una nueva solicitud de videoconferencia que <strong style="color:#c45000">no pudo asignarse automáticamente</strong> porque no hay hosts Zoom disponibles en el horario indicado.
            </p>

            <!-- Info table -->
            <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #dbe8f4;border-radius:10px;overflow:hidden;font-size:14px">
              <tr style="background:#f7fafd">
                <td style="padding:12px 16px;color:#6688aa;font-weight:700;width:38%;border-bottom:1px solid #dbe8f4">Reunión</td>
                <td style="padding:12px 16px;color:#1e3458;font-weight:700;border-bottom:1px solid #dbe8f4">${p.topic}</td>
              </tr>
              <tr>
                <td style="padding:12px 16px;color:#6688aa;font-weight:700;border-bottom:1px solid #dbe8f4">Solicitante</td>
                <td style="padding:12px 16px;color:#1e3458;font-weight:600;border-bottom:1px solid #dbe8f4">${p.creatorName} &lt;${p.fromEmail}&gt;</td>
              </tr>
              <tr style="background:#f7fafd">
                <td style="padding:12px 16px;color:#6688aa;font-weight:700;border-bottom:1px solid #dbe8f4">Fecha</td>
                <td style="padding:12px 16px;color:#1e3458;font-weight:600;border-bottom:1px solid #dbe8f4;text-transform:capitalize">${p.dateStr}</td>
              </tr>
              <tr>
                <td style="padding:12px 16px;color:#6688aa;font-weight:700;border-bottom:1px solid #dbe8f4">Hora · Duración</td>
                <td style="padding:12px 16px;color:#1e3458;font-weight:600;border-bottom:1px solid #dbe8f4">${p.timeStr} · ${p.durationMinutes} min</td>
              </tr>
              <tr style="background:#f7fafd">
                <td style="padding:12px 16px;color:#6688aa;font-weight:700">Grupo Zoom</td>
                <td style="padding:12px 16px;color:#1e3458;font-weight:600">${p.groupName}</td>
              </tr>
            </table>

            <!-- Warning banner removed -->

            ${btnHtml}
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:16px 36px;background:#f7fafd;border-top:1px solid #dbe8f4;text-align:center;color:#9ab0c8;font-size:11px">
            Universidad Autónoma de Ica — Sistema de Planificación Académica
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>`;
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
