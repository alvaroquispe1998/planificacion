import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subject, of } from 'rxjs';
import { takeUntil, timeout, catchError } from 'rxjs/operators';
import {
    MeetingDetail,
    ManualMeeting,
    MeetingInstance,
    MeetingParticipant,
    VideoconferenceCreatorApiService,
} from '../../services/videoconference-creator-api.service';
import { AuthService } from '../../core/auth.service';

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

    constructor(
        private readonly route: ActivatedRoute,
        private readonly router: Router,
        private readonly api: VideoconferenceCreatorApiService,
        private readonly auth: AuthService,
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
        this.api.getMeeting(id).pipe(
            timeout(15000),
            catchError(() => {
                this.error = 'No se pudo cargar la videoconferencia. Verifica tu conexión o intenta de nuevo.';
                this.loading = false;
                return of(null);
            }),
        ).subscribe((detail) => {
            if (!detail) return;
            this.detail = detail;
            this.loading = false;
        });
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

