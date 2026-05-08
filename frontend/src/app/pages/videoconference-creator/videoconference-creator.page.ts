import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, NgZone, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { timeout, catchError } from 'rxjs/operators';
import { of } from 'rxjs';
import {
    CreateMeetingDto,
    CreatorProfile,
    ManualMeeting,
    ManualMeetingStatus,
    VideoconferenceCreatorApiService,
    ZoomGroupSummary,
} from '../../services/videoconference-creator-api.service';

type FormMode = 'UNIQUE' | 'WEEKLY';
type StatusFilter = 'ALL' | ManualMeetingStatus;

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
    loadingMeetings = false;
    saving = false;
    error = '';
    successMsg = '';
    statusFilter: StatusFilter = 'ALL';
    view: 'list' | 'form' = 'list';

    formMode: FormMode = 'UNIQUE';
    formGroupId = '';
    formTopic = '';
    formAgenda = '';
    formStartTime = '';
    formDuration = 60;
    formRecurrenceEndDate = '';
    formRecurrenceWeeklyDays = '2';

    readonly weekdayOptions = [
        { value: '1', label: 'Dom' },
        { value: '2', label: 'Lun' },
        { value: '3', label: 'Mar' },
        { value: '4', label: 'Mi\u00e9' },
        { value: '5', label: 'Jue' },
        { value: '6', label: 'Vie' },
        { value: '7', label: 'S\u00e1b' },
    ];

    readonly statusFilters: Array<{ value: StatusFilter; label: string }> = [
        { value: 'ALL', label: 'Todas' },
        { value: 'CREATED', label: 'Creadas' },
        { value: 'DRAFT_NO_HOST', label: 'Borradores' },
        { value: 'APPROVED_WITH_BACKUP', label: 'Aprobadas' },
        { value: 'ERROR', label: 'Error' },
        { value: 'CANCELLED', label: 'Canceladas' },
    ];

    constructor(
        private readonly api: VideoconferenceCreatorApiService,
        private readonly router: Router,
        private readonly cdr: ChangeDetectorRef,
        private readonly zone: NgZone,
    ) { }

    ngOnInit(): void {
        this.load();
    }

    load(): void {
        this.loading = true;
        this.error = '';
        this.cdr.markForCheck();
        this.api.getProfile().pipe(
            timeout(15000),
            catchError((err) => {
                const status = err?.status;
                this.zone.run(() => {
                    if (status === 403) {
                        this.error = 'No tienes permisos para acceder a este m\u00f3dulo.';
                    } else if (status === 0 || err?.name === 'TimeoutError') {
                        this.error = 'No se pudo conectar con el servidor.';
                    } else {
                        this.error = 'No se pudo cargar el perfil. Verifica tus permisos.';
                    }
                    this.loading = false;
                    this.cdr.markForCheck();
                });
                return of(null);
            }),
        ).subscribe((profile) => {
            this.zone.run(() => {
                if (!profile) {
                    this.cdr.markForCheck();
                    return;
                }
                this.profile = profile;
                if (profile.can_create_unique && !profile.can_create_weekly) {
                    this.formMode = 'UNIQUE';
                } else if (!profile.can_create_unique && profile.can_create_weekly) {
                    this.formMode = 'WEEKLY';
                }
                if (profile.assigned_groups.length > 0) {
                    this.formGroupId = profile.assigned_groups[0].id;
                }
                this.cdr.markForCheck();
                this.loadMeetings();
            });
        });
    }

    private loadMeetings(): void {
        this.api.listMeetings().pipe(
            timeout(15000),
            catchError(() => of([] as ManualMeeting[])),
        ).subscribe((meetings) => {
            this.zone.run(() => {
                this.meetings = meetings;
                this.loading = false;
                this.loadingMeetings = false;
                this.cdr.markForCheck();
            });
        });
    }

    refreshMeetings(): void {
        this.loadingMeetings = true;
        this.loadMeetings();
    }

    get filteredMeetings(): ManualMeeting[] {
        if (this.statusFilter === 'ALL') return this.meetings;
        return this.meetings.filter((m) => m.status === this.statusFilter);
    }

    get draftCount(): number {
        return this.meetings.filter((m) => m.status === 'DRAFT_NO_HOST').length;
    }

    get canCreate(): boolean {
        if (!this.profile) return false;
        if (this.formMode === 'UNIQUE') return this.profile.can_create_unique;
        return this.profile.can_create_weekly;
    }

    get availableGroups(): ZoomGroupSummary[] {
        return this.profile?.assigned_groups ?? [];
    }

    /** Mínimo permitido para datetime-local: ahora + 5 min en formato YYYY-MM-DDTHH:mm */
    get minDateTime(): string {
        const d = new Date();
        d.setMinutes(d.getMinutes() + 5);
        const p = (n: number) => String(n).padStart(2, '0');
        return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`;
    }

    /** Mínimo para la fecha fin de recurrencia: la fecha de inicio o hoy */
    get minRecurrenceDate(): string {
        if (this.formStartTime) return this.formStartTime.slice(0, 10);
        const d = new Date();
        const p = (n: number) => String(n).padStart(2, '0');
        return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
    }

    get startTimeError(): string {
        if (!this.formStartTime) return '';
        const selected = new Date(this.formStartTime);
        const min = new Date();
        min.setMinutes(min.getMinutes() + 5);
        if (selected < min) return 'La fecha y hora deben ser al menos 5 minutos en el futuro.';
        return '';
    }

    get recurrenceEndError(): string {
        if (this.formMode !== 'WEEKLY' || !this.formRecurrenceEndDate) return '';
        const end = new Date(this.formRecurrenceEndDate + 'T00:00');
        const start = this.formStartTime ? new Date(this.formStartTime) : new Date();
        if (end <= start) return 'La fecha de fin debe ser posterior a la fecha de inicio.';
        return '';
    }

    submit(): void {
        if (!this.formTopic.trim() || !this.formGroupId || !this.formStartTime) return;
        if (this.startTimeError) { this.error = this.startTimeError; return; }
        if (this.recurrenceEndError) { this.error = this.recurrenceEndError; return; }
        if (this.formDuration < 15 || this.formDuration > 720) {
            this.error = 'La duración debe estar entre 15 y 720 minutos.';
            return;
        }
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
                if (result.status === 'DRAFT_NO_HOST') {
                    this.successMsg = 'Sin host disponible: guardada como borrador. TI ser\u00e1 notificado.';
                } else if (result.status === 'ERROR') {
                    this.successMsg = 'Creada pero con error en Zoom: ' + result.error_message;
                } else {
                    this.successMsg = 'Videoconferencia creada (Zoom ID: ' + result.zoom_meeting_id + ').';
                }
                this.resetForm();
                this.refreshMeetings();
                this.view = 'list';
            },
            error: (err) => {
                this.saving = false;
                this.error = err?.error?.message ?? 'Error al crear la videoconferencia.';
            },
        });
    }

    openDetail(id: string): void {
        if (this.loading) {
            return;
        }
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

    typeLabel(type: ManualMeeting['type']): string {
        return type === 'WEEKLY' ? 'Semanal' : '\u00danica';
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
