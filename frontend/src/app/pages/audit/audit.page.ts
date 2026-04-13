import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ApiService } from '../../core/api.service';

type AuditFilters = {
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
  status: string;
  audit_sync_status: string;
  search: string;
};

@Component({
  selector: 'app-audit-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './audit.page.html',
  styleUrl: './audit.page.css',
})
export class AuditPageComponent implements OnInit {
  readonly pageSizeOptions = [20, 50, 100];
  readonly creationStatusOptions = [
    { value: '', label: 'Todos' },
    { value: 'MATCHED', label: 'Lista en Zoom' },
    { value: 'CREATED_UNMATCHED', label: 'Creada por revisar' },
    { value: 'CREATING', label: 'En proceso' },
    { value: 'ERROR', label: 'Con error' },
  ];
  readonly auditStatusOptions = [
    { value: '', label: 'Todos' },
    { value: 'PENDING', label: 'Sin sincronizar' },
    { value: 'SYNCED', label: 'Sincronizada' },
    { value: 'ERROR', label: 'Error de sync' },
  ];
  loading = true;
  error = '';
  rows: any[] = [];
  totals = {
    total: 0,
    matched: 0,
    errors: 0,
    pending_audit: 0,
  };
  page = 1;
  pageSize = 20;
  catalog: any = {
    semesters: [],
    campuses: [],
    faculties: [],
    academic_programs: [],
  };

  filters: AuditFilters = {
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
    status: '',
    audit_sync_status: '',
    search: '',
  };

  constructor(
    private readonly api: ApiService,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadCatalog();
    this.loadRows();
  }

  loadRows() {
    this.loading = true;
    this.error = '';
    this.api.listPlanningVideoconferenceAudits(this.apiFilters()).subscribe({
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
        this.error = err?.error?.message ?? 'No se pudo cargar la auditoria Zoom.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  loadCatalog() {
    this.api.getPlanningCatalogFilters().subscribe({
      next: (catalog) => {
        this.catalog = catalog ?? this.catalog;
        this.cdr.detectChanges();
      },
      error: () => {
        this.cdr.detectChanges();
      },
    });
  }

  clearFilters() {
    this.filters = {
      semester_id: '',
      campus_id: '',
      faculty_id: '',
      academic_program_id: '',
      status: '',
      audit_sync_status: '',
      search: '',
    };
    this.page = 1;
    this.loadRows();
  }

  openDetail(row: any) {
    const url = this.router.serializeUrl(
      this.router.createUrlTree(['/videoconferences/audit', row.id]),
    );
    window.open(url, '_blank', 'noopener');
  }

  applyFilters() {
    this.page = 1;
    this.loadRows();
  }

  onPageSizeChange(value: string | number) {
    this.pageSize = Number(value) || 20;
    this.page = 1;
    this.loadRows();
  }

  get totalPages() {
    return Math.max(1, Math.ceil(this.totals.total / this.pageSize));
  }

  get resultBadgeLabel() {
    if (this.loading) {
      return 'Actualizando lista';
    }
    if (this.totals.total === 1) {
      return '1 resultado';
    }
    return `${this.totals.total} resultados`;
  }

  get currentPageStart() {
    if (this.totals.total === 0) {
      return 0;
    }
    return (this.page - 1) * this.pageSize + 1;
  }

  get currentPageEnd() {
    if (this.totals.total === 0) {
      return 0;
    }
    return Math.min(this.currentPageStart + this.rows.length - 1, this.totals.total);
  }

  goToFirstPage() {
    if (this.page <= 1) {
      return;
    }
    this.page = 1;
    this.loadRows();
  }

  goToPreviousPage() {
    if (this.page <= 1) {
      return;
    }
    this.page -= 1;
    this.loadRows();
  }

  goToNextPage() {
    if (this.page >= this.totalPages) {
      return;
    }
    this.page += 1;
    this.loadRows();
  }

  goToLastPage() {
    if (this.page >= this.totalPages) {
      return;
    }
    this.page = this.totalPages;
    this.loadRows();
  }

  get semesterOptions() {
    return Array.isArray(this.catalog.semesters) ? this.catalog.semesters : [];
  }

  get campusOptions() {
    return Array.isArray(this.catalog.campuses) ? this.catalog.campuses : [];
  }

  get facultyOptions() {
    return Array.isArray(this.catalog.faculties) ? this.catalog.faculties : [];
  }

  get programOptions() {
    const items = Array.isArray(this.catalog.academic_programs) ? this.catalog.academic_programs : [];
    if (!this.filters.faculty_id) {
      return items;
    }
    return items.filter((item: any) => item.faculty_id === this.filters.faculty_id);
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

  normalizeAuditSyncStatus(value: string | null | undefined) {
    return value || 'PENDING';
  }

  auditStatusLabel(value: string | null | undefined) {
    switch (this.normalizeAuditSyncStatus(value)) {
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

  hasActiveFilters() {
    return Object.values(this.filters).some((value) => `${value ?? ''}`.trim() !== '');
  }

  sectionLabel(row: any) {
    return row.section_external_code || row.section_code || 'Seccion sin codigo';
  }

  groupLabel(row: any) {
    return row.subsection_code ? `Grupo ${row.subsection_code}` : 'Grupo sin codigo';
  }

  dateLabel(row: any) {
    return row.conference_date || this.formatDate(row.scheduled_start);
  }

  timeLabel(row: any) {
    return `${this.shortTime(row.start_time)} - ${this.shortTime(row.end_time)}`;
  }

  shortTime(value: string | null | undefined) {
    return value ? String(value).slice(0, 5) : '--:--';
  }

  formatDate(value: string | Date | null | undefined) {
    if (!value) {
      return '--';
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleDateString();
  }

  formatDateTime(value: string | Date | null | undefined) {
    if (!value) {
      return '--';
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }

  private apiFilters() {
    return {
      ...this.filters,
      page: this.page,
      page_size: this.pageSize,
    };
  }
}
