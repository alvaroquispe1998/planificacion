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

    /** Etiqueta legible de recurrencia (días + hasta). */
    get recurrenceLabel(): string | null {
        const r = this.meeting?.recurrence_json;
        if (!r) return null;
        const dayNames: Record<string, string> = {
            '1': 'Dom', '2': 'Lun', '3': 'Mar', '4': 'Mié',
            '5': 'Jue', '6': 'Vie', '7': 'Sáb',
        };
        const weeklyDays = r['weekly_days'] as string | undefined;
        const dayLabels = weeklyDays
            ? weeklyDays.split(',').map((d) => dayNames[d.trim()] ?? d).join(', ')
            : '';
        const endDt = r['end_date_time'] as string | undefined;
        const endLabel = endDt
            ? new Date(endDt).toLocaleDateString('es-PE', { day: '2-digit', month: '2-digit', year: 'numeric' })
            : null;
        const parts: string[] = [];
        if (dayLabels) parts.push(`Cada semana los: ${dayLabels}`);
        if (endLabel) parts.push(`Hasta el ${endLabel}`);
        return parts.length ? parts.join(' · ') : null;
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

