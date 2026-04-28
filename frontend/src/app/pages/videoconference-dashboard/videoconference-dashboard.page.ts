import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { finalize, forkJoin, Subscription } from 'rxjs';
import {
    CoverageDimension,
    DashboardCoverageConflictRow,
    DashboardCoverageDailyPoint,
    DashboardCoverageDimensionRow,
    DashboardCoverageMissingItem,
    DashboardCoverageOverrideRow,
    DashboardCoverageSummary,
    DashboardHostOption,
    DashboardHostSession,
    DashboardTodayErrorItem,
    DashboardTodayHostUtilizationResponse,
    DashboardTodaySummary,
    DashboardTodayUpcomingItem,
    VideoconferenceDashboardApiService,
} from '../../services/videoconference-dashboard-api.service';
import {
    FilterCatalogOption,
    VideoconferenceApiService,
} from '../../services/videoconference-api.service';

type TabKey = 'today' | 'period' | 'host';

export interface HostCalendarBlock {
    session: DashboardHostSession;
    /** Minutos desde 00:00 del día (en hora local). */
    startMinutes: number;
    endMinutes: number;
}

@Component({
    selector: 'app-videoconference-dashboard-page',
    standalone: true,
    imports: [CommonModule, FormsModule, RouterLink],
    templateUrl: './videoconference-dashboard.page.html',
    styleUrl: './videoconference-dashboard.page.css',
})
export class VideoconferenceDashboardPageComponent implements OnInit, OnDestroy {
    activeTab: TabKey = 'today';

    selectedDate: string = this.todayIso();
    withinMinutes = 60;
    autoRefresh = false;
    private refreshTimer: ReturnType<typeof setInterval> | null = null;
    private inFlight: Subscription | null = null;
    private inFlightPeriod: Subscription | null = null;

    loading = false;
    error = '';

    // Today tab data
    summary: DashboardTodaySummary | null = null;
    upcoming: DashboardTodayUpcomingItem[] = [];
    errors: DashboardTodayErrorItem[] = [];
    hostUtilization: DashboardTodayHostUtilizationResponse | null = null;

    // Period tab data
    periodOptions: FilterCatalogOption[] = [];
    selectedPeriod = '';
    periodLoading = false;
    periodError = '';
    coverageSummary: DashboardCoverageSummary | null = null;
    coverageDimension: CoverageDimension = 'faculty';
    coverageRows: DashboardCoverageDimensionRow[] = [];
    coverageMissing: DashboardCoverageMissingItem[] = [];
    coverageOverrides: DashboardCoverageOverrideRow[] = [];
    coverageDaily: DashboardCoverageDailyPoint[] = [];
    coverageConflicts: DashboardCoverageConflictRow[] = [];

    // Host tab data
    hostOptions: DashboardHostOption[] = [];
    selectedHostId = '';
    weekStart: string = this.getMondayIso(new Date());
    hostLoading = false;
    hostError = '';
    hostSessions: DashboardHostSession[] = [];
    private inFlightHost: Subscription | null = null;
    /** 7 columnas: lunes -> domingo, cada una con sus bloques posicionados. */
    hostWeekColumns: Array<{ date: string; label: string; blocks: HostCalendarBlock[] }> = [];
    /** Hora inicial visible del calendario (default 7) y final (default 22). */
    readonly calendarStartHour = 7;
    readonly calendarEndHour = 22;

    constructor(
        private readonly api: VideoconferenceDashboardApiService,
        private readonly vcApi: VideoconferenceApiService,
        private readonly cdr: ChangeDetectorRef,
    ) { }

    ngOnInit(): void {
        this.refreshTodayData();
        this.loadPeriods();
    }

    ngOnDestroy(): void {
        this.stopAutoRefresh();
        this.inFlight?.unsubscribe();
        this.inFlightPeriod?.unsubscribe();
        this.inFlightHost?.unsubscribe();
    }

    private loadPeriods() {
        this.vcApi.getFilterOptions({}).subscribe({
            next: (opts) => {
                this.periodOptions = opts.periods ?? [];
                if (!this.selectedPeriod && this.periodOptions.length > 0) {
                    this.selectedPeriod = this.periodOptions[0].id;
                }
                this.cdr.detectChanges();
            },
            error: () => {
                /* opcional: períodos vacíos */
            },
        });
    }

    setTab(tab: TabKey) {
        if (tab === this.activeTab) return;
        this.activeTab = tab;
        if (tab === 'today') {
            this.refreshTodayData();
        } else if (tab === 'period') {
            this.refreshPeriodData();
        } else if (tab === 'host') {
            this.loadHostOptions();
        }
    }

    onDateChange() {
        if (this.activeTab === 'today') {
            this.refreshTodayData();
        }
    }

    onWithinMinutesChange() {
        if (this.activeTab === 'today') {
            this.refreshUpcoming();
        }
    }

    onPeriodChange() {
        if (this.activeTab === 'period') {
            this.refreshPeriodData();
        }
    }

    onDimensionChange(dim: CoverageDimension) {
        this.coverageDimension = dim;
        this.refreshDimension();
    }

    toggleAutoRefresh() {
        this.autoRefresh = !this.autoRefresh;
        if (this.autoRefresh) {
            this.refreshTimer = setInterval(() => this.refreshTodayData(), 60_000);
        } else {
            this.stopAutoRefresh();
        }
    }

    private stopAutoRefresh() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }
    }

    refreshTodayData() {
        if (this.activeTab !== 'today') return;
        this.inFlight?.unsubscribe();
        this.loading = true;
        this.error = '';
        this.inFlight = forkJoin({
            summary: this.api.getTodaySummary(this.selectedDate),
            upcoming: this.api.getTodayUpcoming(this.selectedDate, this.withinMinutes),
            errors: this.api.getTodayErrors(this.selectedDate, 20),
            host: this.api.getTodayHostUtilization(this.selectedDate),
        })
            .pipe(finalize(() => {
                this.loading = false;
                this.cdr.detectChanges();
            }))
            .subscribe({
                next: (data) => {
                    this.summary = data.summary;
                    this.upcoming = data.upcoming;
                    this.errors = data.errors;
                    this.hostUtilization = data.host;
                },
                error: (err) => {
                    this.error = err?.error?.message ?? 'No se pudo cargar el dashboard.';
                },
            });
    }

    private refreshUpcoming() {
        this.api.getTodayUpcoming(this.selectedDate, this.withinMinutes).subscribe({
            next: (data) => {
                this.upcoming = data;
                this.cdr.detectChanges();
            },
            error: () => {
                /* mantener el estado anterior */
            },
        });
    }

    refreshPeriodData() {
        if (this.activeTab !== 'period' || !this.selectedPeriod) return;
        this.inFlightPeriod?.unsubscribe();
        this.periodLoading = true;
        this.periodError = '';
        const period = this.selectedPeriod;
        this.inFlightPeriod = forkJoin({
            summary: this.api.getCoverageSummary(period),
            byDim: this.api.getCoverageByDimension(period, this.coverageDimension),
            missing: this.api.getCoverageMissing(period, 200),
            overrides: this.api.getCoverageOverrides(period),
            daily: this.api.getCoverageDaily(period),
            conflicts: this.api.getCoverageConflicts(period),
        })
            .pipe(finalize(() => {
                this.periodLoading = false;
                this.cdr.detectChanges();
            }))
            .subscribe({
                next: (data) => {
                    this.coverageSummary = data.summary;
                    this.coverageRows = data.byDim;
                    this.coverageMissing = data.missing;
                    this.coverageOverrides = data.overrides;
                    this.coverageDaily = data.daily;
                    this.coverageConflicts = data.conflicts;
                },
                error: (err) => {
                    this.periodError = err?.error?.message ?? 'No se pudo cargar la cobertura.';
                },
            });
    }

    private refreshDimension() {
        if (!this.selectedPeriod) return;
        this.api.getCoverageByDimension(this.selectedPeriod, this.coverageDimension).subscribe({
            next: (rows) => {
                this.coverageRows = rows;
                this.cdr.detectChanges();
            },
            error: () => {
                /* mantener estado */
            },
        });
    }

    todayIso(): string {
        const d = new Date();
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
    }

    formatTime(iso: string): string {
        if (!iso) return '';
        const d = new Date(iso);
        if (Number.isNaN(d.getTime())) return '';
        return d.toLocaleTimeString('es-PE', { hour: '2-digit', minute: '2-digit', hour12: false });
    }

    statusLabel(status: string): string {
        switch (status) {
            case 'MATCHED': return 'Vinculada';
            case 'CREATED_UNMATCHED': return 'Creada s/match';
            case 'CREATING': return 'Creando';
            case 'ERROR': return 'Error';
            default: return status || '-';
        }
    }

    statusClass(status: string): string {
        switch (status) {
            case 'MATCHED': return 'badge-success';
            case 'CREATED_UNMATCHED': return 'badge-warning';
            case 'CREATING': return 'badge-info';
            case 'ERROR': return 'badge-danger';
            default: return 'badge-muted';
        }
    }

    auditLabel(status: string): string {
        switch (status) {
            case 'SYNCED': return 'Sincronizada';
            case 'PENDING': return 'Pendiente';
            case 'ERROR': return 'Error';
            default: return status || '-';
        }
    }

    coverageBarStyle(): Record<string, string> {
        const pct = this.summary?.coverage.coveragePercent ?? 0;
        const clamped = Math.max(0, Math.min(100, pct));
        return { width: `${clamped}%` };
    }

    bucketBarHeight(value: number, max: number): string {
        if (!max || max <= 0) return '0%';
        const pct = Math.max(2, Math.round((value / max) * 100));
        return `${pct}%`;
    }

    get peakBucketsMax(): number {
        return this.hostUtilization?.peakActiveMeetings ?? 0;
    }

    primaryErrorReason(item: DashboardTodayErrorItem): string {
        return (
            item.errorMessage ||
            item.deleteError ||
            item.auditError ||
            'Sin detalle'
        );
    }

    errorScope(item: DashboardTodayErrorItem): string {
        if (item.status === 'ERROR') return 'Generación';
        if (item.deleteStatus === 'ERROR') return 'Eliminación';
        if (item.auditSyncStatus === 'ERROR') return 'Auditoría';
        return 'Otro';
    }

    overrideLabel(action: string): string {
        switch (action) {
            case 'KEEP': return 'Mantener';
            case 'SKIP': return 'Omitir';
            case 'RESCHEDULE': return 'Reprogramar';
            default: return action;
        }
    }

    severityClass(sev: string): string {
        switch (sev) {
            case 'CRITICAL': return 'badge-danger';
            case 'WARNING': return 'badge-warning';
            case 'INFO': return 'badge-info';
            default: return 'badge-muted';
        }
    }

    severityLabel(sev: string): string {
        switch (sev) {
            case 'CRITICAL': return 'Crítica';
            case 'WARNING': return 'Advertencia';
            case 'INFO': return 'Informativa';
            default: return sev;
        }
    }

    get coverageDailyMax(): number {
        return this.coverageDaily.reduce((m, p) => Math.max(m, p.total), 0);
    }

    formatDayShort(d: string): string {
        if (!d) return '';
        return d.slice(5); // MM-DD
    }

    // =========================================================
    // HOST CALENDAR
    // =========================================================

    private getMondayIso(d: Date): string {
        const day = d.getDay();
        const diffToMonday = (day + 6) % 7;
        const monday = new Date(d);
        monday.setHours(0, 0, 0, 0);
        monday.setDate(monday.getDate() - diffToMonday);
        return this.dateToIso(monday);
    }

    private dateToIso(d: Date): string {
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
    }

    get weekEnd(): string {
        const start = new Date(`${this.weekStart}T00:00:00`);
        const end = new Date(start);
        end.setDate(start.getDate() + 6);
        return this.dateToIso(end);
    }

    get weekRangeLabel(): string {
        const start = new Date(`${this.weekStart}T00:00:00`);
        const end = new Date(start);
        end.setDate(start.getDate() + 6);
        const fmt = (d: Date) => d.toLocaleDateString('es-PE', { day: '2-digit', month: 'short' });
        return `${fmt(start)} – ${fmt(end)} ${end.getFullYear()}`;
    }

    get calendarHours(): number[] {
        const hours: number[] = [];
        for (let h = this.calendarStartHour; h <= this.calendarEndHour; h++) hours.push(h);
        return hours;
    }

    shiftWeek(deltaDays: number) {
        const start = new Date(`${this.weekStart}T00:00:00`);
        start.setDate(start.getDate() + deltaDays);
        this.weekStart = this.getMondayIso(start);
        this.loadHostOptions();
    }

    onHostChange() {
        this.refreshHostCalendar();
    }

    loadHostOptions() {
        if (this.activeTab !== 'host') return;
        this.hostError = '';
        this.api.getHostOptions(this.weekStart, this.weekEnd).subscribe({
            next: (opts) => {
                this.hostOptions = opts;
                if (this.hostOptions.length === 0) {
                    this.selectedHostId = '';
                    this.hostSessions = [];
                    this.hostWeekColumns = this.buildEmptyWeek();
                } else {
                    const stillExists = this.hostOptions.some((o) => o.zoomUserId === this.selectedHostId);
                    if (!stillExists) {
                        this.selectedHostId = this.hostOptions[0].zoomUserId;
                    }
                    this.refreshHostCalendar();
                }
                this.cdr.detectChanges();
            },
            error: (err) => {
                this.hostError = err?.error?.message ?? 'No se pudieron cargar los hosts.';
                this.cdr.detectChanges();
            },
        });
    }

    refreshHostCalendar() {
        if (!this.selectedHostId) {
            this.hostSessions = [];
            this.hostWeekColumns = this.buildEmptyWeek();
            return;
        }
        this.inFlightHost?.unsubscribe();
        this.hostLoading = true;
        this.hostError = '';
        this.inFlightHost = this.api
            .getHostCalendar(this.selectedHostId, this.weekStart, this.weekEnd)
            .pipe(
                finalize(() => {
                    this.hostLoading = false;
                    this.cdr.detectChanges();
                }),
            )
            .subscribe({
                next: (resp) => {
                    this.hostSessions = resp.sessions;
                    this.hostWeekColumns = this.buildWeekColumns(resp.sessions);
                },
                error: (err) => {
                    this.hostError = err?.error?.message ?? 'No se pudo cargar el calendario.';
                    this.hostWeekColumns = this.buildEmptyWeek();
                },
            });
    }

    private buildEmptyWeek(): Array<{ date: string; label: string; blocks: HostCalendarBlock[] }> {
        const cols: Array<{ date: string; label: string; blocks: HostCalendarBlock[] }> = [];
        const start = new Date(`${this.weekStart}T00:00:00`);
        const dayNames = ['LUN', 'MAR', 'MIÉ', 'JUE', 'VIE', 'SÁB', 'DOM'];
        for (let i = 0; i < 7; i++) {
            const d = new Date(start);
            d.setDate(start.getDate() + i);
            cols.push({
                date: this.dateToIso(d),
                label: `${dayNames[i]} ${d.getDate()}/${d.getMonth() + 1}`,
                blocks: [],
            });
        }
        return cols;
    }

    private buildWeekColumns(
        sessions: DashboardHostSession[],
    ): Array<{ date: string; label: string; blocks: HostCalendarBlock[] }> {
        const cols = this.buildEmptyWeek();
        const indexByDate = new Map<string, number>();
        cols.forEach((c, idx) => indexByDate.set(c.date, idx));
        for (const s of sessions) {
            const idx = indexByDate.get(s.conferenceDate);
            if (idx === undefined) continue;
            const start = new Date(s.scheduledStart);
            const end = new Date(s.scheduledEnd);
            if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) continue;
            const startMinutes = start.getHours() * 60 + start.getMinutes();
            const endMinutes = end.getHours() * 60 + end.getMinutes();
            cols[idx].blocks.push({ session: s, startMinutes, endMinutes });
        }
        return cols;
    }

    blockTopPx(block: HostCalendarBlock): string {
        const baseMin = this.calendarStartHour * 60;
        const top = Math.max(0, block.startMinutes - baseMin);
        return `${top}px`;
    }

    blockHeightPx(block: HostCalendarBlock): string {
        const baseMin = this.calendarStartHour * 60;
        const endCap = this.calendarEndHour * 60 + 60;
        const start = Math.max(baseMin, block.startMinutes);
        const end = Math.min(endCap, block.endMinutes);
        const h = Math.max(20, end - start);
        return `${h}px`;
    }

    formatRange(session: DashboardHostSession): string {
        const a = new Date(session.scheduledStart);
        const b = new Date(session.scheduledEnd);
        const fmt = (d: Date) =>
            d.toLocaleTimeString('es-PE', { hour: '2-digit', minute: '2-digit', hour12: false });
        return `${fmt(a)}–${fmt(b)}`;
    }

    hostLabel(opt: DashboardHostOption): string {
        const name = opt.name ?? '';
        const email = opt.email ?? '';
        if (name && email) return `${name} (${email})`;
        return name || email || opt.zoomUserId;
    }

    get selectedHostMeta(): DashboardHostOption | null {
        return this.hostOptions.find((o) => o.zoomUserId === this.selectedHostId) ?? null;
    }
}
