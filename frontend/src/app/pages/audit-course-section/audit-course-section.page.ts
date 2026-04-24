import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { ApiService } from '../../core/api.service';
import { VideoconferenceApiService } from '../../services/videoconference-api.service';

@Component({
  selector: 'app-audit-course-section-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './audit-course-section.page.html',
  styleUrl: './audit-course-section.page.css',
})
export class AuditCourseSectionPageComponent implements OnInit {
  readonly pageSizeOptions = [20, 50, 100];
  loading = true;
  error = '';
  rows: any[] = [];
  page = 1;
  pageSize = 20;
  totals = {
    total: 0,
    matched: 0,
    errors: 0,
    pending_audit: 0,
  };
  filters: Record<string, string> = {};

  deletingId = '';

  // Confirm-delete modal state
  confirmDeleteOpen = false;
  confirmDeleteRow: any = null;
  deleteErrorMessage = '';
  deleteSuccessMessage = '';

  // Aula Virtual lookup preview
  aulaVirtualLookupLoading = false;
  aulaVirtualLookupId = '';
  aulaVirtualLookupMessage = '';
  aulaVirtualLookupSource: 'response_json' | 'list_lookup' | 'not_found' | '' = '';
  aulaVirtualLookupFound = false;

  constructor(
    private readonly api: ApiService,
    private readonly vcApi: VideoconferenceApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    const query = this.route.snapshot.queryParamMap;
    this.filters = {
      semester_id: query.get('semester_id') ?? '',
      campus_id: query.get('campus_id') ?? '',
      faculty_id: query.get('faculty_id') ?? '',
      academic_program_id: query.get('academic_program_id') ?? '',
      course_code: query.get('course_code') ?? '',
      planning_section_id: query.get('planning_section_id') ?? '',
      source_section_id: query.get('source_section_id') ?? '',
      planning_subsection_id: query.get('planning_subsection_id') ?? '',
      hide_inherited: query.get('hide_inherited') ?? 'true',
      show_deleted: query.get('show_deleted') ?? 'false',
    };
    this.loadRows();
  }

  loadRows() {
    this.loading = true;
    this.error = '';
    this.api.listPlanningVideoconferenceSessions({
      ...this.filters,
      page: this.page,
      page_size: this.pageSize,
    }).subscribe({
      next: (result) => {
        this.rows = Array.isArray(result?.items) ? result.items : [];
        this.totals = {
          total: Number(result?.totals?.total ?? 0),
          matched: Number(result?.totals?.matched ?? 0),
          errors: Number(result?.totals?.errors ?? 0),
          pending_audit: Number(result?.totals?.pending_audit ?? 0),
        };
        this.page = Number(result?.page ?? this.page);
        this.pageSize = Number(result?.page_size ?? this.pageSize);
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudieron cargar las sesiones.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  openDetail(row: any) {
    const url = this.router.serializeUrl(
      this.router.createUrlTree(['/videoconferences/audit', row.id]),
    );
    window.open(url, '_blank', 'noopener');
  }

  async deleteConference(row: any) {
    this.confirmDeleteRow = row;
    this.confirmDeleteOpen = true;
    this.deleteErrorMessage = '';
    this.deleteSuccessMessage = '';
    this.aulaVirtualLookupId = '';
    this.aulaVirtualLookupMessage = '';
    this.aulaVirtualLookupSource = '';
    this.aulaVirtualLookupFound = false;
    this.aulaVirtualLookupLoading = true;
    this.cdr.detectChanges();

    this.vcApi.lookupAulaVirtualId(row.id).subscribe({
      next: (res) => {
        this.aulaVirtualLookupLoading = false;
        this.aulaVirtualLookupId = res.aula_virtual_id || '';
        this.aulaVirtualLookupMessage = res.message || '';
        this.aulaVirtualLookupSource = res.source;
        this.aulaVirtualLookupFound = !!res.aula_virtual_id;
        // eslint-disable-next-line no-console
        console.log('[AV lookup]', res);
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.aulaVirtualLookupLoading = false;
        this.aulaVirtualLookupId = '';
        this.aulaVirtualLookupFound = false;
        this.aulaVirtualLookupSource = 'not_found';
        this.aulaVirtualLookupMessage =
          err?.error?.message ||
          err?.message ||
          'No se pudo consultar Aula Virtual.';
        this.cdr.detectChanges();
      },
    });
  }

  cancelDelete() {
    this.confirmDeleteOpen = false;
    this.confirmDeleteRow = null;
    this.deleteErrorMessage = '';
    this.cdr.detectChanges();
  }

  confirmDelete() {
    const row = this.confirmDeleteRow;
    if (!row || !row.id) {
      this.deleteErrorMessage = 'Registro invalido: no tiene ID.';
      this.cdr.detectChanges();
      return;
    }
    this.deletingId = row.id;
    this.deleteErrorMessage = '';
    this.cdr.detectChanges();

    // eslint-disable-next-line no-console
    console.log('[DELETE videoconference] id:', row.id, 'topic:', row.topic);

    this.vcApi.deleteVideoconference(row.id).subscribe({
      next: (result) => {
        const idx = this.rows.findIndex((r) => r.id === row.id);
        if (idx !== -1) {
          this.rows[idx] = {
            ...this.rows[idx],
            delete_status: 'DELETED',
            deleted_at: new Date().toISOString(),
          };
        }
        this.deletingId = '';
        this.deleteSuccessMessage = result?.message || 'Videoconferencia eliminada correctamente.';
        this.confirmDeleteOpen = false;
        this.confirmDeleteRow = null;
        this.cdr.detectChanges();
        // Auto-hide success message after 5s
        setTimeout(() => {
          this.deleteSuccessMessage = '';
          this.cdr.detectChanges();
        }, 5000);
      },
      error: (err) => {
        this.deletingId = '';
        const status = err?.status ? ` (HTTP ${err.status})` : '';
        const msg = err?.error?.message ?? err?.message ?? 'Error al eliminar la videoconferencia.';
        this.deleteErrorMessage = `${msg}${status}`;
        // eslint-disable-next-line no-console
        console.error('[DELETE videoconference] error:', err);
        this.cdr.detectChanges();
      },
    });
  }

  get confirmDeleteTopic(): string {
    return this.confirmDeleteRow?.topic || '';
  }
  get confirmDeleteDate(): string {
    return this.confirmDeleteRow ? this.dateLabel(this.confirmDeleteRow) : '';
  }
  get confirmDeleteTime(): string {
    return this.confirmDeleteRow ? this.timeLabel(this.confirmDeleteRow) : '';
  }
  get confirmDeleteId(): string {
    return this.confirmDeleteRow?.id || '';
  }
  get confirmDeleteZoomMeetingId(): string {
    return this.confirmDeleteRow?.zoom_meeting_id || '';
  }

  onPageSizeChange(value: string | number) {
    this.pageSize = Number(value) || 20;
    this.page = 1;
    this.loadRows();
  }

  get title() {
    const row = this.rows[0];
    if (row) {
      return `${row.course_code || 'Sin codigo'} - ${row.course_name || 'Sin curso'}`;
    }
    return this.filters['course_code'] || 'Sesiones del curso/seccion';
  }

  get subtitle() {
    const row = this.rows[0];
    if (!row) {
      return 'Cargando seccion y sesiones';
    }
    return `${this.sectionLabel(row)} | ${this.groupLabel(row)}`;
  }

  get totalPages() {
    return Math.max(1, Math.ceil(this.totals.total / this.pageSize));
  }

  get currentPageStart() {
    if (this.totals.total === 0) return 0;
    return (this.page - 1) * this.pageSize + 1;
  }

  get currentPageEnd() {
    if (this.totals.total === 0) return 0;
    return Math.min(this.currentPageStart + this.rows.length - 1, this.totals.total);
  }

  goToFirstPage() {
    if (this.page <= 1) return;
    this.page = 1;
    this.loadRows();
  }

  goToPreviousPage() {
    if (this.page <= 1) return;
    this.page -= 1;
    this.loadRows();
  }

  goToNextPage() {
    if (this.page >= this.totalPages) return;
    this.page += 1;
    this.loadRows();
  }

  goToLastPage() {
    if (this.page >= this.totalPages) return;
    this.page = this.totalPages;
    this.loadRows();
  }

  statusLabel(value: string | null | undefined) {
    switch (value) {
      case 'MATCHED':
        return 'Lista en Zoom';
      case 'CREATED_UNMATCHED':
        return 'Creada por revisar';
      case 'ERROR':
        return 'Con error';
      case 'CREATING':
        return 'En proceso';
      default:
        return value || 'Sin estado';
    }
  }

  statusToneClass(value: string | null | undefined) {
    switch (value) {
      case 'MATCHED':
        return 'pill-ok';
      case 'CREATED_UNMATCHED':
        return 'pill-warning';
      case 'ERROR':
        return 'pill-error';
      case 'CREATING':
        return 'pill-info';
      default:
        return '';
    }
  }

  linkModeLabel(value: string | null | undefined) {
    return value === 'INHERITED' ? 'Heredada' : 'Owner';
  }

  auditStatusLabel(value: string | null | undefined) {
    switch (value || 'PENDING') {
      case 'SYNCED':
        return 'Sincronizada';
      case 'ERROR':
        return 'Error de sync';
      default:
        return 'Sin sincronizar';
    }
  }

  planningSyncStatusLabel(value: string | null | undefined) {
    switch (value) {
      case 'ALIGNED':
        return 'Vigente';
      case 'OUTDATED':
        return 'Desfasada';
      case 'SCHEDULE_REMOVED':
        return 'Horario retirado';
      case 'GROUP_REMOVED':
        return 'Grupo retirado';
      case 'SECTION_REMOVED':
        return 'Seccion retirada';
      default:
        return 'Sin validar';
    }
  }

  sectionLabel(row: any) {
    return row.section_external_code || row.section_code || 'Seccion sin codigo';
  }

  groupLabel(row: any) {
    return row.subsection_code ? `Grupo ${row.subsection_code}` : 'Grupo sin codigo';
  }

  dateLabel(row: any) {
    return this.formatDateSafe(row.conference_date || row.scheduled_start);
  }

  timeLabel(row: any) {
    return `${this.shortTime(row.start_time)} - ${this.shortTime(row.end_time)}`;
  }

  shortTime(value: string | null | undefined) {
    return value ? String(value).slice(0, 5) : '--:--';
  }

  formatDateSafe(value: string | Date | null | undefined) {
    if (!value) return '--';
    const str = typeof value === 'string' ? value : value.toISOString();
    const match = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (match) {
      const [, y, m, d] = match;
      return `${parseInt(d, 10)}/${parseInt(m, 10)}/${y}`;
    }
    const date = new Date(str);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleDateString();
  }

  formatDateTime(value: string | Date | null | undefined) {
    if (!value) return '--';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }
}
