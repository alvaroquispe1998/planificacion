import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subject, of } from 'rxjs';
import { takeUntil, timeout, catchError, retry } from 'rxjs/operators';
import {
    MeetingDetail,
    ManualMeeting,
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
            CREATED: 'Creada',
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

    statusLabel(status: ManualMeeting['status']): string {
        const map: Record<string, string> = {
            CREATED: 'Creada',
            DRAFT_NO_HOST: 'Borrador sin host',
            APPROVED_WITH_BACKUP: 'Aprobada (backup)',
            ERROR: 'Error',
            CANCELLED: 'Cancelada',
        };
        return map[status] ?? status;
    }

    statusClass(status: ManualMeeting['status']): string {
        const map: Record<string, string> = {
            CREATED: 'badge-ok',
            DRAFT_NO_HOST: 'badge-warn',
            APPROVED_WITH_BACKUP: 'badge-backup',
            ERROR: 'badge-err',
            CANCELLED: 'badge-cancelled',
        };
        return map[status] ?? '';
    }
}

