import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { Subject, of } from 'rxjs';
import { takeUntil, timeout, catchError, retry } from 'rxjs/operators';
import {
    MeetingDetail,
    ManualMeeting,
    ManualMeetingDisplayStatus,
    MeetingInstance,
    MeetingParticipant,
    MeetingRecording,
    VideoconferenceCreatorApiService,
} from '../../services/videoconference-creator-api.service';
import { AuthService } from '../../core/auth.service';
import { DialogService } from '../../core/dialog.service';

type ParticipantSessionGroup = {
    instance: MeetingInstance | null;
    participants: MeetingParticipant[];
};

@Component({
    selector: 'app-videoconference-creator-detail-page',
    standalone: true,
    imports: [
        CommonModule,
        MatButtonModule,
        MatCardModule,
        MatIconModule,
        MatProgressSpinnerModule,
        MatTooltipModule,
    ],
    templateUrl: './videoconference-creator-detail.page.html',
    styleUrl: './videoconference-creator-detail.page.css',
})
export class VideoconferenceCreatorDetailPageComponent implements OnInit, OnDestroy {
    loading = true;
    syncing = false;
    syncMsg = '';
    approving = false;
    denying = false;
    cancelling = false;
    error = '';
    zoomStatus: { zoom_status: string | null; reason?: string; host_email?: string | null } | null = null;
    checkingZoom = false;
    detail: MeetingDetail | null = null;
    private currentId = '';
    private clockTimer: ReturnType<typeof setInterval> | null = null;
    private readonly destroy$ = new Subject<void>();

    get meeting(): ManualMeeting | null {
        return this.detail?.meeting ?? null;
    }

    get canApproveBackup(): boolean {
        return this.auth.hasPermission('action.videoconference_creator.approve_backup');
    }

    get canDeny(): boolean {
        return this.meeting?.status === 'DRAFT_NO_HOST' && this.auth.hasPermission('action.videoconference_creator.approve_backup');
    }

    /** Sólo reuniones futuras (aún no iniciadas) pueden cancelarse. */
    get canCancel(): boolean {
        if (!this.meeting || this.meeting.status === 'CANCELLED' || this.meeting.status === 'DENIED') return false;
        if (this.meeting.can_cancel === false || this.isMeetingInProgress(this.meeting) || this.isMeetingFinished(this.meeting)) return false;
        if (this.meeting.status === 'DRAFT_NO_HOST') return true;
        const now = new Date();
        const start = new Date(this.meeting.start_time);
        return start > now;
    }

    /** Sincronizar no tiene sentido en reuniones canceladas. */
    get canSync(): boolean {
        return Boolean(this.meeting?.zoom_meeting_id) && this.meeting?.status !== 'CANCELLED';
    }

    /** Días de la semana legibles (nombre completo). */
    get recurrenceDays(): string | null {
        const r = this.meeting?.recurrence_json;
        if (!r) return null;
        const dayNames: Record<string, string> = {
            '1': 'Domingo', '2': 'Lunes', '3': 'Martes', '4': 'Miércoles',
            '5': 'Jueves', '6': 'Viernes', '7': 'Sábado',
        };
        const weeklyDays = r['weekly_days'] as string | undefined;
        return weeklyDays
            ? weeklyDays.split(',').map((d) => dayNames[d.trim()] ?? d).join(', ')
            : null;
    }

    /** Fecha fin de recurrencia formateada. */
    get recurrenceEndDate(): string | null {
        const r = this.meeting?.recurrence_json;
        if (!r) return null;
        const endDt = r['end_date_time'] as string | undefined;
        return endDt
            ? new Date(endDt).toLocaleDateString('es-PE', { day: '2-digit', month: '2-digit', year: 'numeric' })
            : null;
    }

    /** @deprecated — mantener para compatibilidad temporal */
    get recurrenceLabel(): string | null {
        const days = this.recurrenceDays;
        const end = this.recurrenceEndDate;
        if (!days && !end) return null;
        const parts: string[] = [];
        if (days) parts.push(`Cada semana los: ${days}`);
        if (end) parts.push(`Hasta el ${end}`);
        return parts.join(' · ');
    }

    constructor(
        private readonly route: ActivatedRoute,
        private readonly router: Router,
        private readonly api: VideoconferenceCreatorApiService,
        private readonly auth: AuthService,
        private readonly dialog: DialogService,
        private readonly cdr: ChangeDetectorRef,
        private readonly zone: NgZone,
    ) { }

    ngOnInit(): void {
        this.clockTimer = setInterval(() => this.cdr.markForCheck(), 30000);
        // Subscribe to param changes so navigating directly to a different ID works
        this.route.paramMap.pipe(takeUntil(this.destroy$)).subscribe((params) => {
            const id = params.get('id') ?? '';
            if (id && id !== this.currentId) {
                this.currentId = id;
                this.load(id);
            }
        });
    }

    ngOnDestroy(): void {
        if (this.clockTimer) {
            clearInterval(this.clockTimer);
            this.clockTimer = null;
        }
        this.destroy$.next();
        this.destroy$.complete();
    }

    private load(id: string): void {
        this.loading = true;
        this.error = '';
        this.detail = null;
        this.cdr.markForCheck();
        this.api.getMeeting(id).pipe(
            timeout(15000),
            retry({ count: 1, delay: 400 }),
            catchError(() => {
                this.zone.run(() => {
                    this.error = 'No se pudo cargar la videoconferencia. Verifica tu conexión o intenta de nuevo.';
                    this.loading = false;
                    this.cdr.markForCheck();
                });
                return of(null);
            }),
        ).subscribe((detail) => {
            this.zone.run(() => {
                if (!detail) {
                    this.cdr.markForCheck();
                    return;
                }
                this.detail = detail;
                this.loading = false;
                this.setInitialZoomStatus();
                this.cdr.markForCheck();
            });
        });
    }

    retryLoad(): void {
        if (!this.currentId) {
            return;
        }
        this.load(this.currentId);
    }

    sync(): void {
        if (!this.meeting) return;
        this.syncing = true;
        this.syncMsg = '';
        this.api.syncMeeting(this.meeting.id).subscribe({
            next: (result) => {
                this.syncing = false;
                const hasErrors = result.recording_errors && result.recording_errors.length > 0;
                if (hasErrors) {
                    this.syncMsg = `Sin datos nuevos, pero hubo un error al obtener grabaciones: ${result.recording_errors![0]}`;
                } else {
                    this.syncMsg = 'Sincronización completada. La información visible fue actualizada desde Zoom.';
                }
                this.load(this.meeting!.id);
            },
            error: () => {
                this.syncing = false;
            },
        });
    }

    async approveBackup(): Promise<void> {
        if (!this.meeting) return;
        const confirmed = await this.dialog.confirm({
            title: 'Aprobar solicitud',
            message: `Se aprobará la solicitud "${this.meeting.topic}" usando el grupo backup disponible. Se creará la reunión en Zoom y se notificará al solicitante.`,
            confirmLabel: 'Sí, aprobar',
            cancelLabel: 'No, volver',
            tone: 'success',
        });
        if (!confirmed) return;

        this.approving = true;
        this.error = '';
        this.cdr.markForCheck();
        this.api.approveDraft(this.meeting.id).subscribe({
            next: () => {
                this.zone.run(() => {
                    this.approving = false;
                    this.load(this.meeting!.id);
                });
            },
            error: (err) => {
                this.zone.run(() => {
                    this.approving = false;
                    this.error = err?.error?.message ?? 'Error al aprobar.';
                    this.cdr.markForCheck();
                });
            },
        });
    }

    async denyMeeting(): Promise<void> {
        if (!this.meeting || !this.canDeny) return;
        const reason = await this.dialog.prompt({
            title: 'Denegar solicitud',
            message: `Se denegará la solicitud "${this.meeting.topic}" y se notificará al solicitante. Puedes indicar un motivo para incluirlo en el correo.`,
            inputLabel: 'Motivo',
            inputPlaceholder: 'Ej. No hay disponibilidad de host Zoom para el horario solicitado.',
            confirmLabel: 'Sí, denegar',
            cancelLabel: 'No, volver',
            tone: 'danger',
            maxLength: 1000,
        });
        if (reason === null) return;

        this.denying = true;
        this.error = '';
        this.cdr.markForCheck();
        this.api.denyDraft(this.meeting.id, { reason: reason.trim() || undefined }).subscribe({
            next: (meeting) => {
                this.zone.run(() => {
                    this.denying = false;
                    if (this.detail) this.detail = { ...this.detail, meeting };
                    this.cdr.markForCheck();
                });
            },
            error: (err) => {
                this.zone.run(() => {
                    this.denying = false;
                    this.error = err?.error?.message ?? 'No se pudo denegar la solicitud.';
                    this.cdr.markForCheck();
                });
            },
        });
    }

    async cancelMeeting(): Promise<void> {
        if (!this.meeting || !this.canCancel) return;

        const isHostRequest = this.meeting.status === 'DRAFT_NO_HOST';
        const confirmed = await this.dialog.confirm({
            title: isHostRequest ? 'Cancelar solicitud' : 'Cancelar reunión',
            message: isHostRequest
                ? 'La solicitud pendiente de host se marcará como cancelada. No se eliminará nada en Zoom porque todavía no se creó una reunión.'
                : 'Se eliminará la reunión en Zoom y quedará marcada como cancelada en el sistema. Esta acción no se puede deshacer.',
            confirmLabel: isHostRequest ? 'Sí, cancelar solicitud' : 'Sí, cancelar reunión',
            cancelLabel: 'No, mantener',
            tone: 'danger',
        });
        if (!confirmed) return;

        this.cancelling = true;
        this.error = '';
        this.cdr.markForCheck();
        this.api.cancelMeeting(this.meeting.id).subscribe({
            next: (meeting) => {
                this.zone.run(() => {
                    this.cancelling = false;
                    if (this.detail) this.detail = { ...this.detail, meeting };
                    this.cdr.markForCheck();
                });
            },
            error: (err) => {
                this.zone.run(() => {
                    this.cancelling = false;
                    this.error = err?.error?.message ?? 'No se pudo cancelar la videoconferencia.';
                    this.cdr.markForCheck();
                });
            },
        });
    }

    copyToClipboard(text: string | null | undefined): void {
        if (!text) return;
        void navigator.clipboard.writeText(text);
    }

    checkZoomStatus(): void {
        if (!this.meeting?.zoom_meeting_id || this.checkingZoom) return;
        if (this.displayStatus(this.meeting) === 'FINISHED') {
            this.zoomStatus = { zoom_status: null, reason: 'LOCAL_FINISHED' };
            this.cdr.markForCheck();
            return;
        }
        this.checkingZoom = true;
        this.zoomStatus = null;
        this.cdr.markForCheck();
        this.api.getZoomStatus(this.meeting.id).subscribe({
            next: (status) => {
                this.checkingZoom = false;
                this.zoomStatus = status;
                this.cdr.markForCheck();
            },
            error: () => {
                this.checkingZoom = false;
                this.zoomStatus = { zoom_status: null, reason: 'ERROR' };
                this.cdr.markForCheck();
            },
        });
    }

    zoomStatusLabel(): string {
        if (!this.zoomStatus) return '';
        switch (this.zoomStatus.reason) {
            case 'LOCAL_FINISHED': return 'Finalizada';
            case 'LOCAL_CANCELLED': return 'Cancelada';
            case 'LOCAL_DENIED': return 'Denegada';
            case 'LOCAL_ERROR': return 'Con error';
        }
        switch (this.zoomStatus.zoom_status) {
            case 'started': return '🔴 En curso ahora';
            case 'waiting': return '⏳ Esperando inicio';
            default: return this.zoomStatus.reason === 'NOT_FOUND' ? 'No encontrada en Zoom' : '— Sin estado';
        }
    }

    zoomStatusClass(): string {
        switch (this.zoomStatus?.reason) {
            case 'LOCAL_FINISHED': return 'zoom-finished';
            case 'LOCAL_CANCELLED': return 'zoom-cancelled';
            case 'LOCAL_DENIED':
            case 'LOCAL_ERROR':
            case 'NOT_FOUND': return 'zoom-error';
        }
        switch (this.zoomStatus?.zoom_status) {
            case 'started': return 'zoom-live';
            case 'waiting': return 'zoom-waiting';
            default: return 'zoom-offline';
        }
    }

    private setInitialZoomStatus(): void {
        if (!this.meeting) return;
        const status = this.displayStatus(this.meeting);
        if (status === 'FINISHED') {
            this.zoomStatus = { zoom_status: null, reason: 'LOCAL_FINISHED' };
            return;
        }
        if (status === 'CANCELLED') {
            this.zoomStatus = { zoom_status: null, reason: 'LOCAL_CANCELLED' };
            return;
        }
        if (status === 'DENIED') {
            this.zoomStatus = { zoom_status: null, reason: 'LOCAL_DENIED' };
            return;
        }
        if (status === 'ERROR') {
            this.zoomStatus = { zoom_status: null, reason: 'LOCAL_ERROR' };
            return;
        }
        this.zoomStatus = null;
        if (this.meeting.zoom_meeting_id && !this.checkingZoom) {
            this.checkZoomStatus();
        }
    }

    back(): void {
        this.router.navigate(['/videoconferences/creator']);
    }

    get recordings(): MeetingRecording[] {
        return this.detail?.recordings ?? [];
    }

    get videoRecordings(): MeetingRecording[] {
        const bestByInstance = new Map<string, MeetingRecording>();
        for (const recording of this.recordings.filter((item) => this.isVideoRecording(item))) {
            const key = recording.meeting_instance_id || recording.id;
            const current = bestByInstance.get(key);
            if (!current || this.recordingScore(recording) > this.recordingScore(current)) {
                bestByInstance.set(key, recording);
            }
        }
        return [...bestByInstance.values()].sort((a, b) => {
            const aTime = a.start_time ? new Date(a.start_time).getTime() : 0;
            const bTime = b.start_time ? new Date(b.start_time).getTime() : 0;
            return aTime - bTime;
        });
    }

    get uniqueParticipants(): MeetingParticipant[] {
        const participants = this.detail?.participants ?? [];
        const seen = new Set<string>();
        return participants.filter((participant) => {
            const email = `${participant.email ?? ''}`.trim().toLowerCase();
            const name = `${participant.display_name ?? ''}`.trim().toLowerCase();
            const role = `${participant.role ?? ''}`.trim().toLowerCase();
            const key = email || [name, role].filter(Boolean).join('|');
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return true;
        });
    }

    get participantSessionGroups(): ParticipantSessionGroup[] {
        const participantsByInstance = new Map<string, MeetingParticipant[]>();
        for (const participant of this.detail?.participants ?? []) {
            const instanceId = `${participant.meeting_instance_id ?? ''}`.trim() || 'unknown';
            const bucket = participantsByInstance.get(instanceId) ?? [];
            bucket.push(participant);
            participantsByInstance.set(instanceId, bucket);
        }

        const groups: ParticipantSessionGroup[] = [];
        for (const instance of this.detail?.instances ?? []) {
            const participants = this.deduplicateParticipants(participantsByInstance.get(instance.id) ?? []);
            if (participants.length) groups.push({ instance, participants });
            participantsByInstance.delete(instance.id);
        }

        for (const participants of participantsByInstance.values()) {
            const unique = this.deduplicateParticipants(participants);
            if (unique.length) groups.push({ instance: null, participants: unique });
        }
        return groups;
    }

    get totalVisibleParticipants(): number {
        const seen = new Set<string>();
        for (const group of this.participantSessionGroups) {
            for (const participant of group.participants) {
                const email = `${participant.email ?? ''}`.trim().toLowerCase();
                const name = `${participant.display_name ?? ''}`.trim().toLowerCase();
                const role = `${participant.role ?? ''}`.trim().toLowerCase();
                const key = email || [name, role].filter(Boolean).join('|');
                if (!key) continue;
                seen.add(key);
            }
        }
        return seen.size;
    }

    get hasEndedInstances(): boolean {
        return (this.detail?.instances ?? []).some((i) => i['status'] === 'ENDED');
    }

    get hasAnyInstances(): boolean {
        return (this.detail?.instances ?? []).length > 0;
    }

    get latestActivityTime(): Date | null {
        const candidates: Date[] = [];
        for (const instance of this.detail?.instances ?? []) {
            const value = instance.actual_start;
            if (value) {
                const date = new Date(value);
                if (!Number.isNaN(date.getTime())) candidates.push(date);
            }
        }
        for (const recording of this.videoRecordings) {
            if (recording.start_time) {
                const date = new Date(recording.start_time);
                if (!Number.isNaN(date.getTime())) candidates.push(date);
            }
        }
        if (!candidates.length) return null;
        return candidates.reduce((latest, current) => current > latest ? current : latest);
    }

    instanceStatusLabel(status: string | undefined): string {
        const map: Record<string, string> = {
            ENDED: 'Finalizada',
            IN_PROGRESS: 'En curso',
            CREATED: 'Pendiente',
            ERROR: 'Error',
        };
        return status ? (map[status] ?? status) : '—';
    }

    formatBytes(bytes: string | null | undefined): string {
        const n = Number(bytes);
        if (!bytes || isNaN(n)) return '';
        if (n < 1024 * 1024) return `${(n / 1024).toFixed(0)} KB`;
        return `${(n / (1024 * 1024)).toFixed(1)} MB`;
    }

    calculatedEndTime(meeting: ManualMeeting): Date {
        const start = new Date(meeting.start_time);
        if (Number.isNaN(start.getTime())) {
            return new Date(meeting.end_time);
        }
        return new Date(start.getTime() + meeting.duration_minutes * 60 * 1000);
    }

    displayStatus(meeting: ManualMeeting): ManualMeetingDisplayStatus {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST' || meeting.status === 'DENIED') {
            return meeting.status;
        }
        if (this.isMeetingInProgress(meeting)) return 'IN_PROGRESS';
        if (this.isMeetingFinished(meeting)) return 'FINISHED';
        return 'PENDING';
    }

    statusLabel(meeting: ManualMeeting): string {
        const map: Record<string, string> = {
            PENDING: meeting.status === 'APPROVED_WITH_BACKUP' ? 'Pendiente (backup)' : 'Pendiente',
            IN_PROGRESS: 'En proceso',
            FINISHED: 'Finalizada',
            DRAFT_NO_HOST: 'Pendiente de host',
            DENIED: 'Denegada',
            ERROR: 'Error',
            CANCELLED: 'Cancelada',
        };
        const status = this.displayStatus(meeting);
        return map[status] ?? status;
    }

    statusClass(meeting: ManualMeeting): string {
        const map: Record<string, string> = {
            PENDING: 'badge-pending',
            IN_PROGRESS: 'badge-progress',
            FINISHED: 'badge-finished',
            DRAFT_NO_HOST: 'badge-warn',
            DENIED: 'badge-denied',
            ERROR: 'badge-err',
            CANCELLED: 'badge-cancelled',
        };
        const status = this.displayStatus(meeting);
        return map[status] ?? '';
    }

    statusContext(meeting: ManualMeeting): string {
        const status = this.displayStatus(meeting);
        const end = this.calculatedEndTime(meeting);
        if (status === 'PENDING') {
            return meeting.type === 'WEEKLY' ? 'Reunión recurrente pendiente' : `Inicia el ${this.formatDateTime(meeting.start_time)}`;
        }
        if (status === 'IN_PROGRESS') {
            return `Reunión en curso. Finaliza a las ${this.formatTime(end)}`;
        }
        if (status === 'FINISHED') {
            const finishedAt = this.latestActivityTime ?? end;
            return meeting.type === 'WEEKLY' ? 'Recurrencia finalizada' : `Última sesión registrada el ${this.formatDateTime(finishedAt)}`;
        }
        if (status === 'CANCELLED') return 'Esta reunión fue cancelada';
        if (status === 'DENIED') return 'La solicitud fue denegada por TI';
        if (status === 'DRAFT_NO_HOST') return 'Pendiente de asignar host Zoom';
        if (status === 'ERROR') return 'Requiere revisión por error de creación';
        return '';
    }

    isMeetingInProgress(meeting: ManualMeeting): boolean {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST' || meeting.status === 'DENIED') {
            return false;
        }
        // If a session was actually ended (host closed early), don't show as in-progress
        if (meeting.type === 'UNIQUE' && this.hasEndedInstances) return false;
        const now = new Date();
        if (meeting.type === 'WEEKLY') {
            return this.isWeeklyMeetingInProgress(meeting, now);
        }
        const start = new Date(meeting.start_time);
        const end = this.calculatedEndTime(meeting);
        return start <= now && now < end;
    }

    private isMeetingFinished(meeting: ManualMeeting): boolean {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST' || meeting.status === 'DENIED') {
            return false;
        }
        // If the host actually ended the session, treat as finished regardless of scheduled end
        if (meeting.type === 'UNIQUE' && this.hasEndedInstances) return true;
        const now = new Date();
        if (meeting.type === 'WEEKLY') {
            return this.isWeeklyMeetingFinished(meeting, now);
        }
        const end = this.calculatedEndTime(meeting);
        return !Number.isNaN(end.getTime()) && now >= end;
    }

    private isWeeklyMeetingInProgress(meeting: ManualMeeting, now: Date): boolean {
        const weeklyDays = String(meeting.recurrence_json?.['weekly_days'] ?? '')
            .split(',')
            .map((item) => item.trim())
            .filter(Boolean);
        const todayZoomDay = String(now.getDay() + 1);
        if (weeklyDays.length && !weeklyDays.includes(todayZoomDay)) return false;

        const firstStart = new Date(meeting.start_time);
        if (now < firstStart) return false;
        const recurrenceEndRaw = meeting.recurrence_json?.['end_date_time'];
        if (typeof recurrenceEndRaw === 'string' && now > new Date(recurrenceEndRaw)) return false;

        const startMinutes = firstStart.getHours() * 60 + firstStart.getMinutes();
        const nowMinutes = now.getHours() * 60 + now.getMinutes();
        return nowMinutes >= startMinutes && nowMinutes < startMinutes + meeting.duration_minutes;
    }

    private isWeeklyMeetingFinished(meeting: ManualMeeting, now: Date): boolean {
        const recurrenceEndRaw = meeting.recurrence_json?.['end_date_time'];
        if (typeof recurrenceEndRaw !== 'string') return false;
        const recurrenceEnd = new Date(recurrenceEndRaw);
        return !Number.isNaN(recurrenceEnd.getTime()) && now > recurrenceEnd;
    }

    recordingTypeLabel(type: string): string {
        const map: Record<string, string> = {
            MP4: 'Video',
            M4A: 'Audio',
            VTT: 'Subtitulos',
            TRANSCRIPT: 'Transcripcion',
            CHAT: 'Chat',
            OTHER: 'Otro',
        };
        return map[type] ?? type;
    }

    recordingIcon(type: string): string {
        const map: Record<string, string> = {
            MP4: '🎥',
            M4A: '🔊',
            VTT: '📝',
            TRANSCRIPT: '📝',
            CHAT: '💬',
            OTHER: '📄',
        };
        return map[type] ?? '📄';
    }

    videoRecordingLabel(): string {
        return 'Video';
    }

    private isVideoRecording(recording: MeetingRecording): boolean {
        const type = `${recording.recording_type ?? ''}`.trim().toUpperCase();
        const extension = `${recording.file_extension ?? ''}`.trim().toLowerCase();
        return type === 'MP4' || extension === 'mp4';
    }

    private recordingScore(recording: MeetingRecording): number {
        const size = Number(recording.file_size_bytes ?? 0);
        return (recording.play_url ? 1_000_000_000 : 0)
            + (recording.download_url ? 500_000_000 : 0)
            + (Number.isFinite(size) ? size : 0);
    }

    private deduplicateParticipants(participants: MeetingParticipant[]): MeetingParticipant[] {
        const seen = new Set<string>();
        return participants.filter((participant) => {
            const email = `${participant.email ?? ''}`.trim().toLowerCase();
            const name = `${participant.display_name ?? ''}`.trim().toLowerCase();
            const role = `${participant.role ?? ''}`.trim().toLowerCase();
            const key = email || [name, role].filter(Boolean).join('|');
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return true;
        });
    }

    private formatDateTime(value: string | Date): string {
        return new Date(value).toLocaleString('es-PE', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
        });
    }

    private formatTime(value: Date): string {
        return value.toLocaleTimeString('es-PE', { hour: '2-digit', minute: '2-digit' });
    }
}

