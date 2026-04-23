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
    const dateLabel = row.conference_date ? this.formatDateSafe(row.conference_date) : '--';
    const timeLabel = `${this.shortTime(row.start_time)}-${this.shortTime(row.end_time)}`;
    const topic = row.topic ? `\n"${row.topic}"` : '';
    const confirmed = confirm(
      `¿Confirmar eliminación de la videoconferencia?${topic}\n\nFecha: ${dateLabel} ${timeLabel}\n\nSe eliminará primero en Aula Virtual, luego en Zoom.\nEl registro seguirá visible en esta lista marcado como ELIMINADO.\n\nSi Aula Virtual falla, la operación se cancela completamente.`
    );
    if (!confirmed) return;
    this.deletingId = row.id;
    this.cdr.detectChanges();
    this.vcApi.deleteVideoconference(row.id).subscribe({
      next: (result) => {
        // Mark row as deleted in-place (soft delete — keep it visible)
        const idx = this.rows.findIndex((r) => r.id === row.id);
        if (idx !== -1) {
          this.rows[idx] = { ...this.rows[idx], delete_status: 'DELETED', deleted_at: new Date().toISOString() };
        }
        this.deletingId = '';
        this.cdr.detectChanges();
      },
      error: (err) => {
        alert(err?.error?.message ?? 'Error al eliminar la videoconferencia.');
        this.deletingId = '';
        this.cdr.detectChanges();
      },
    });
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
