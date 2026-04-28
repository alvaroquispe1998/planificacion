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

type TabKey = 'today' | 'period';

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
        } else {
            this.refreshPeriodData();
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
}
