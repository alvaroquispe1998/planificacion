import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { timeout, catchError } from 'rxjs/operators';
import { of } from 'rxjs';
import {
    CreateMeetingDto,
    CreatorProfile,
    ManualMeeting,
    VideoconferenceCreatorApiService,
    ZoomGroupSummary,
} from '../../services/videoconference-creator-api.service';

type FormMode = 'UNIQUE' | 'WEEKLY';

@Component({
    selector: 'app-videoconference-creator-page',
    standalone: true,
    imports: [CommonModule, FormsModule],
    templateUrl: './videoconference-creator.page.html',
    styleUrl: './videoconference-creator.page.css',
})
export class VideoconferenceCreatorPageComponent implements OnInit {
    profile: CreatorProfile | null = null;
    meetings: ManualMeeting[] = [];
    loading = true;
    saving = false;
    error = '';
    successMsg = '';

    // Form state
    formMode: FormMode = 'UNIQUE';
    formGroupId = '';
    formTopic = '';
    formAgenda = '';
    formStartTime = '';
    formDuration = 60;
    formRecurrenceEndDate = '';
    formRecurrenceWeeklyDays = '2'; // Monday

    readonly weekdayOptions = [
        { value: '1', label: 'Dom' },
        { value: '2', label: 'Lun' },
        { value: '3', label: 'Mar' },
        { value: '4', label: 'Mié' },
        { value: '5', label: 'Jue' },
        { value: '6', label: 'Vie' },
        { value: '7', label: 'Sáb' },
    ];

    constructor(
        private readonly api: VideoconferenceCreatorApiService,
        private readonly router: Router,
    ) { }

    ngOnInit(): void {
        this.load();
    }

    load(): void {
        this.loading = true;
        this.error = '';
        this.api.getProfile().pipe(
            timeout(15000),
            catchError((err) => {
                const status = err?.status;
                if (status === 403) {
                    this.error = 'No tienes permisos para acceder a este módulo. Contacta al administrador.';
                } else if (status === 0 || err?.name === 'TimeoutError') {
                    this.error = 'No se pudo conectar con el servidor. Verifica tu conexión o intenta más tarde.';
                } else {
                    this.error = 'No se pudo cargar el perfil. Verifica tus permisos.';
                }
                this.loading = false;
                return of(null);
            }),
        ).subscribe((profile) => {
            if (!profile) return;
            this.profile = profile;
            if (profile.can_create_unique && !profile.can_create_weekly) {
                this.formMode = 'UNIQUE';
            } else if (!profile.can_create_unique && profile.can_create_weekly) {
                this.formMode = 'WEEKLY';
            }
            if (profile.assigned_groups.length > 0) {
                this.formGroupId = profile.assigned_groups[0].id;
            }
            this.loadMeetings();
        });
    }

    private loadMeetings(): void {
        this.api.listMeetings().pipe(
            timeout(15000),
            catchError(() => of([] as ManualMeeting[])),
        ).subscribe((meetings) => {
            this.meetings = meetings;
            this.loading = false;
        });
    }

    get canCreate(): boolean {
        if (!this.profile) return false;
        if (this.formMode === 'UNIQUE') return this.profile.can_create_unique;
        return this.profile.can_create_weekly;
    }

    get availableGroups(): ZoomGroupSummary[] {
        return this.profile?.assigned_groups ?? [];
    }

    submit(): void {
        if (!this.formTopic.trim() || !this.formGroupId || !this.formStartTime) return;
        this.saving = true;
        this.error = '';
        this.successMsg = '';

        const dto: CreateMeetingDto = {
            type: this.formMode,
            zoom_group_id: this.formGroupId,
            topic: this.formTopic.trim(),
            agenda: this.formAgenda.trim() || undefined,
            start_time: this.formStartTime,
            duration_minutes: this.formDuration,
        };

        if (this.formMode === 'WEEKLY') {
            dto.recurrence_end_date = this.formRecurrenceEndDate || undefined;
            dto.recurrence_weekly_days = this.formRecurrenceWeeklyDays || '2';
        }

        this.api.createMeeting(dto).subscribe({
            next: (result) => {
                this.saving = false;
                this.successMsg =
                    result.status === 'DRAFT_NO_HOST'
                        ? 'Sin host disponible: videoconferencia guardada como borrador. TI será notificado.'
                        : result.status === 'ERROR'
                            ? `Creada pero con error en Zoom: ${result.error_message}`
                            : `Videoconferencia creada exitosamente (Zoom ID: ${result.zoom_meeting_id}).`;
                this.resetForm();
                this.loadMeetings();
            },
            error: (err) => {
                this.saving = false;
                this.error = err?.error?.message ?? 'Error al crear la videoconferencia.';
            },
        });
    }

    openDetail(id: string): void {
        this.router.navigate(['/videoconferences/creator', id]);
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

    private resetForm(): void {
        this.formTopic = '';
        this.formAgenda = '';
        this.formStartTime = '';
        this.formDuration = 60;
        this.formRecurrenceEndDate = '';
        this.formRecurrenceWeeklyDays = '2';
    }
}
