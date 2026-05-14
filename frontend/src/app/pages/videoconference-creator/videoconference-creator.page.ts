import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { MatButtonModule } from '@angular/material/button';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { timeout, catchError } from 'rxjs/operators';
import { firstValueFrom, of } from 'rxjs';
import {
    CreateMeetingDto,
    CreatorProfile,
    ManualMeeting,
    ManualMeetingDisplayStatus,
    VideoconferenceCreatorApiService,
    ZoomGroupSummary,
} from '../../services/videoconference-creator-api.service';
import { DialogService } from '../../core/dialog.service';

type FormMode = 'UNIQUE' | 'WEEKLY';
type StatusFilter = 'ALL' | ManualMeetingDisplayStatus;

@Component({
    selector: 'app-videoconference-creator-page',
    standalone: true,
    imports: [
        CommonModule,
        FormsModule,
        MatButtonModule,
        MatButtonToggleModule,
        MatCardModule,
        MatIconModule,
        MatProgressSpinnerModule,
        MatTooltipModule,
    ],
    templateUrl: './videoconference-creator.page.html',
    styleUrl: './videoconference-creator.page.css',
})
export class VideoconferenceCreatorPageComponent implements OnInit, OnDestroy {
    profile: CreatorProfile | null = null;
    meetings: ManualMeeting[] = [];
    loading = true;
    loadingMeetings = false;
    saving = false;
    error = '';
    successMsg = '';
    statusFilter: StatusFilter = 'ALL';
    view: 'list' | 'form' = 'list';
    currentPage = 1;
    readonly pageSize = 10;
    private clockTimer: ReturnType<typeof setInterval> | null = null;

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
        { value: 'PENDING', label: 'Pendientes' },
        { value: 'IN_PROGRESS', label: 'En proceso' },
        { value: 'FINISHED', label: 'Finalizadas' },
        { value: 'DRAFT_NO_HOST', label: 'Pendiente de host' },
        { value: 'ERROR', label: 'Con error' },
        { value: 'CANCELLED', label: 'Canceladas' },
    ];

    constructor(
        private readonly api: VideoconferenceCreatorApiService,
        private readonly router: Router,
        private readonly cdr: ChangeDetectorRef,
        private readonly zone: NgZone,
        private readonly dialog: DialogService,
    ) { }

    ngOnInit(): void {
        this.clockTimer = setInterval(() => this.cdr.markForCheck(), 30000);
        this.load();
    }

    ngOnDestroy(): void {
        if (this.clockTimer) {
            clearInterval(this.clockTimer);
            this.clockTimer = null;
        }
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
                this.clampCurrentPage();
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
        const filtered = this.statusFilter === 'ALL'
            ? this.meetings
            : this.meetings.filter((m) => this.displayStatus(m) === this.statusFilter);
        return [...filtered].sort((a, b) => this.meetingSortTime(b) - this.meetingSortTime(a));
    }

    get paginatedMeetings(): ManualMeeting[] {
        const start = (this.currentPage - 1) * this.pageSize;
        return this.filteredMeetings.slice(start, start + this.pageSize);
    }

    get totalPages(): number {
        return Math.max(1, Math.ceil(this.filteredMeetings.length / this.pageSize));
    }

    get pageStart(): number {
        return this.filteredMeetings.length === 0 ? 0 : (this.currentPage - 1) * this.pageSize + 1;
    }

    get pageEnd(): number {
        return Math.min(this.currentPage * this.pageSize, this.filteredMeetings.length);
    }

    get showPagination(): boolean {
        return this.filteredMeetings.length > this.pageSize;
    }

    get draftCount(): number {
        return this.meetings.filter((m) => m.status === 'DRAFT_NO_HOST').length;
    }

    get draftLabel(): string {
        const count = this.draftCount;
        return `${count} pendiente${count > 1 ? 's' : ''} de host`;
    }

    get canCreate(): boolean {
        if (!this.profile) return false;
        if (this.formMode === 'UNIQUE') return this.profile.can_create_unique;
        return this.profile.can_create_weekly;
    }

    get availableGroups(): ZoomGroupSummary[] {
        return this.profile?.assigned_groups ?? [];
    }

    /** Mínimo permitido para datetime-local: minuto actual en formato YYYY-MM-DDTHH:mm */
    get minDateTime(): string {
        return this.formatDateTimeLocal(this.currentMinute());
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
        if (selected < this.currentMinute()) return 'La fecha y hora no puede ser anterior al minuto actual.';
        return '';
    }

    get recurrenceEndError(): string {
        if (this.formMode !== 'WEEKLY' || !this.formRecurrenceEndDate) return '';
        const end = new Date(this.formRecurrenceEndDate + 'T00:00');
        const start = this.formStartTime ? new Date(this.formStartTime) : new Date();
        if (end <= start) return 'La fecha de fin debe ser posterior a la fecha de inicio.';
        return '';
    }

    async submit(): Promise<void> {
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

        try {
            const result = await firstValueFrom(this.api.createMeeting(this.buildCreateDto(false)));
            this.handleCreateResult(result);
        } catch (err: any) {
            this.saving = false;
            const errorBody = err?.error;
            if (errorBody?.code === 'NO_HOST_AVAILABLE') {
                const confirmed = await this.dialog.confirm({
                    title: 'Sin hosts disponibles',
                    message:
                        'No hay usuarios Zoom disponibles en el horario seleccionado.\n¿Deseas enviar una solicitud de aprobación para usar un host de respaldo?',
                    confirmLabel: 'Enviar solicitud',
                    cancelLabel: 'Cancelar',
                });
                if (confirmed) {
                    await this.submitApprovalRequest();
                    return;
                }
                this.successMsg = 'No se creó la reunión.';
                this.view = 'list';
                this.refreshMeetings();
                return;
            }
            this.error = errorBody?.message ?? 'Error al crear la videoconferencia.';
        }
    }

    private async submitApprovalRequest(): Promise<void> {
        this.saving = true;
        this.error = '';
        try {
            const result = await firstValueFrom(this.api.createMeeting(this.buildCreateDto(true)));
            this.handleCreateResult(result);
        } catch (err: any) {
            this.saving = false;
            this.error = err?.error?.message ?? 'No se pudo enviar la solicitud para aprobación.';
        }
    }

    private buildCreateDto(requestApprovalIfNoHost: boolean): CreateMeetingDto {
        const dto: CreateMeetingDto = {
            type: this.formMode,
            zoom_group_id: this.formGroupId,
            topic: this.formTopic.trim(),
            agenda: this.formAgenda.trim() || undefined,
            start_time: this.formStartTime,
            duration_minutes: this.formDuration,
            request_approval_if_no_host: requestApprovalIfNoHost,
        };

        if (this.formMode === 'WEEKLY') {
            dto.recurrence_end_date = this.formRecurrenceEndDate || undefined;
            dto.recurrence_weekly_days = this.formRecurrenceWeeklyDays || '2';
        }
        return dto;
    }

    private handleCreateResult(result: ManualMeeting): void {
        this.saving = false;
        if (result.status === 'DRAFT_NO_HOST') {
            this.successMsg = 'Solicitud enviada para aprobación.';
        } else if (result.status === 'ERROR') {
            this.successMsg = 'Registrada pero con error en Zoom: ' + result.error_message;
        } else {
            this.successMsg = 'Videoconferencia pendiente (Zoom ID: ' + result.zoom_meeting_id + ').';
        }
        this.resetForm();
        this.refreshMeetings();
        this.view = 'list';
    }

    openDetail(id: string): void {
        if (this.loading) {
            return;
        }
        this.router.navigate(['/videoconferences/creator', id]);
    }

    setStatusFilter(filter: StatusFilter): void {
        this.statusFilter = filter;
        this.currentPage = 1;
        this.clampCurrentPage();
    }

    previousPage(): void {
        this.currentPage = Math.max(1, this.currentPage - 1);
    }

    nextPage(): void {
        this.currentPage = Math.min(this.totalPages, this.currentPage + 1);
    }

    displayStatus(meeting: ManualMeeting): ManualMeetingDisplayStatus {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST' || meeting.status === 'DENIED') {
            return meeting.status;
        }
        if (meeting.is_finished === true) return 'FINISHED';
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

    filterClass(filter: StatusFilter): string {
        const map: Record<StatusFilter, string> = {
            ALL: 'filter-all',
            PENDING: 'filter-pending',
            IN_PROGRESS: 'filter-progress',
            FINISHED: 'filter-finished',
            DRAFT_NO_HOST: 'filter-warn',
            ERROR: 'filter-err',
            CANCELLED: 'filter-cancelled',
            DENIED: 'filter-denied',
        };
        return map[filter] ?? '';
    }

    typeLabel(type: ManualMeeting['type']): string {
        return type === 'WEEKLY' ? 'Semanal' : '\u00danica';
    }

    calculatedEndTime(meeting: ManualMeeting): Date {
        const start = new Date(meeting.start_time);
        if (Number.isNaN(start.getTime())) {
            return new Date(meeting.end_time);
        }
        return new Date(start.getTime() + meeting.duration_minutes * 60 * 1000);
    }

    private resetForm(): void {
        this.formTopic = '';
        this.formAgenda = '';
        this.formStartTime = '';
        this.formDuration = 60;
        this.formRecurrenceEndDate = '';
        this.formRecurrenceWeeklyDays = '2';
    }

    private meetingSortTime(meeting: ManualMeeting): number {
        const startTime = new Date(meeting.start_time).getTime();
        if (!Number.isNaN(startTime)) return startTime;
        const createdAt = new Date(meeting.created_at).getTime();
        return Number.isNaN(createdAt) ? 0 : createdAt;
    }

    private clampCurrentPage(): void {
        this.currentPage = Math.min(Math.max(1, this.currentPage), this.totalPages);
    }

    private currentMinute(): Date {
        const date = new Date();
        date.setSeconds(0, 0);
        return date;
    }

    private formatDateTimeLocal(date: Date): string {
        const p = (n: number) => String(n).padStart(2, '0');
        return `${date.getFullYear()}-${p(date.getMonth() + 1)}-${p(date.getDate())}T${p(date.getHours())}:${p(date.getMinutes())}`;
    }

    private isMeetingInProgress(meeting: ManualMeeting): boolean {
        if (meeting.status === 'CANCELLED' || meeting.status === 'ERROR' || meeting.status === 'DRAFT_NO_HOST') {
            return false;
        }
        if (meeting.is_finished === true) return false;
        if (meeting.is_in_progress === true) return true;
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
        if (meeting.is_finished === true) return true;
        const now = new Date();
        if (meeting.type === 'WEEKLY') {
            return this.isWeeklyMeetingFinished(meeting, now);
        }
        const end = new Date(meeting.end_time);
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
}
