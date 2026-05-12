import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
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

@Component({
    selector: 'app-videoconference-creator-detail-page',
    standalone: true,
    imports: [CommonModule],
    templateUrl: './videoconference-creator-detail.page.html',
    styleUrl: './videoconference-creator-detail.page.css',
})
export class VideoconferenceCreatorDetailPageComponent implements OnInit, OnDestroy {
    loading = true;
    syncing = false;
    approving = false;
    cancelling = false;
    error = '';
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

    /** Sólo reuniones futuras (aún no iniciadas) pueden cancelarse. */
    get canCancel(): boolean {
        if (!this.meeting || this.meeting.status === 'CANCELLED') return false;
        if (this.meeting.can_cancel === false || this.isMeetingInProgress(this.meeting) || this.isMeetingFinished(this.meeting)) return false;
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
        this.api.syncMeeting(this.meeting.id).subscribe({
            next: () => {
                this.syncing = false;
                this.load(this.meeting!.id);
            },
            error: () => {
                this.syncing = false;
            },
        });
    }

    approveBackup(): void {
        if (!this.meeting) return;
        this.approving = true;
        this.api.approveDraft(this.meeting.id).subscribe({
            next: () => {
                this.approving = false;
                this.load(this.meeting!.id);
            },
            error: (err) => {
                this.approving = false;
                this.error = err?.error?.message ?? 'Error al aprobar.';
            },
        });
    }

    async cancelMeeting(): Promise<void> {
        if (!this.meeting || !this.canCancel) return;

        const confirmed = await this.dialog.confirm({
            title: 'Cancelar reunión',
            message: 'Se eliminará la reunión en Zoom y quedará marcada como cancelada en el sistema. Esta acción no se puede deshacer.',
            confirmLabel: 'Sí, cancelar reunión',
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

    back(): void {
        this.router.navigate(['/videoconferences/creator']);
    }

    get recordings(): MeetingRecording[] {
        return this.detail?.recordings ?? [];
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
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST') {
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
            DRAFT_NO_HOST: 'Borrador sin host',
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
            return meeting.type === 'WEEKLY' ? 'Recurrencia finalizada' : `Finalizó el ${this.formatDateTime(end)}`;
        }
        if (status === 'CANCELLED') return 'Esta reunión fue cancelada';
        if (status === 'DRAFT_NO_HOST') return 'Pendiente de asignar host Zoom';
        if (status === 'ERROR') return 'Requiere revisión por error de creación';
        return '';
    }

    isMeetingInProgress(meeting: ManualMeeting): boolean {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST') {
            return false;
        }
        const now = new Date();
        if (meeting.type === 'WEEKLY') {
            return this.isWeeklyMeetingInProgress(meeting, now);
        }
        const start = new Date(meeting.start_time);
        const end = this.calculatedEndTime(meeting);
        return start <= now && now < end;
    }

    private isMeetingFinished(meeting: ManualMeeting): boolean {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST') {
            return false;
        }
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

