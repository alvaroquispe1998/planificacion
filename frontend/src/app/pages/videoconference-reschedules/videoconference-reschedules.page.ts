import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, RouterLink } from '@angular/router';
import { DialogService } from '../../core/dialog.service';
import {
  VideoconferenceApiService,
  VideoconferenceRescheduleItem,
} from '../../services/videoconference-api.service';

@Component({
  selector: 'app-videoconference-reschedules-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './videoconference-reschedules.page.html',
  styleUrl: './videoconference-reschedules.page.css',
})
export class VideoconferenceReschedulesPageComponent implements OnInit {
  loading = true;
  error = '';
  rows: VideoconferenceRescheduleItem[] = [];
  totals = {
    total: 0,
    created: 0,
    pending: 0,
  };
  payloadPreviewOpen = false;
  payloadPreviewTitle = '';
  payloadPreviewJson = '';
  filters = {
    semesterId: '',
    courseQuery: '',
  };

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly dialog: DialogService,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadRows();
  }

  loadRows() {
    this.loading = true;
    this.error = '';
    this.api.listReschedules().subscribe({
      next: (result) => {
        this.rows = Array.isArray(result?.items) ? result.items : [];
        this.totals = {
          total: Number(result?.totals?.total ?? 0),
          created: Number(result?.totals?.created ?? 0),
          pending: Number(result?.totals?.pending ?? 0),
        };
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo cargar el listado de reprogramaciones.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  get periodOptions() {
    const options = new Map<string, string>();
    for (const row of this.rows) {
      const id = (row.semester_id || '').trim();
      if (!id || options.has(id)) {
        continue;
      }
      options.set(id, (row.semester_name || '(Sin periodo)').trim());
    }
    return Array.from(options.entries())
      .map(([id, label]) => ({ id, label }))
      .sort((left, right) => left.label.localeCompare(right.label));
  }

  get filteredRows() {
    const semesterId = this.filters.semesterId.trim();
    const courseQuery = this.normalizeText(this.filters.courseQuery);
    return this.rows.filter((row) => {
      if (semesterId && (row.semester_id || '').trim() !== semesterId) {
        return false;
      }
      if (courseQuery) {
        const courseLabel = this.normalizeText(row.course_label);
        if (!courseLabel.includes(courseQuery)) {
          return false;
        }
      }
      return true;
    });
  }

  get filteredTotals() {
    const rows = this.filteredRows;
    return {
      total: rows.length,
      created: rows.filter((row) => Boolean(row.record_id)).length,
      pending: rows.filter((row) => !row.record_id).length,
    };
  }

  clearFilters() {
    this.filters = {
      semesterId: '',
      courseQuery: '',
    };
  }

  async resetOverride(row: VideoconferenceRescheduleItem) {
    if (!row.can_reset) {
      return;
    }
    const confirmed = await this.dialog.confirm({
      title: 'Restablecer reprogramacion',
      message: `Se eliminara la reprogramacion base del ${this.formatDateSafe(row.conference_date)}. Deseas continuar?`,
      confirmLabel: 'Restablecer',
      cancelLabel: 'Cancelar',
      tone: 'danger',
    });
    if (!confirmed) {
      return;
    }

    this.loading = true;
    this.api.deleteOverride(row.schedule_id, row.conference_date).subscribe({
      next: async () => {
        await this.dialog.alert({
          title: 'Reprogramacion restablecida',
          message: 'El override fue eliminado correctamente.',
          tone: 'success',
        });
        this.loadRows();
      },
      error: async (err) => {
        this.loading = false;
        this.cdr.detectChanges();
        await this.dialog.alert({
          title: 'No se pudo restablecer',
          message: err?.error?.message ?? 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  openDetail(row: VideoconferenceRescheduleItem) {
    if (!row.record_id) {
      return;
    }
    const url = this.router.serializeUrl(
      this.router.createUrlTree(['/videoconferences/audit', row.record_id]),
    );
    window.open(url, '_blank', 'noopener');
  }

  openPayloadPreview(row: VideoconferenceRescheduleItem) {
    this.payloadPreviewTitle = row.topic || row.course_label || 'Payload de reprogramacion';
    this.payloadPreviewJson = JSON.stringify(row.payload_json ?? { topic: row.topic }, null, 2);
    this.payloadPreviewOpen = true;
  }

  closePayloadPreview() {
    this.payloadPreviewOpen = false;
    this.payloadPreviewTitle = '';
    this.payloadPreviewJson = '';
  }

  reasonLabel(value: string | null | undefined) {
    switch ((value || '').trim().toUpperCase()) {
      case 'HOLIDAY':
        return 'Feriado';
      case 'WEATHER':
        return 'Clima';
      default:
        return 'Otro';
    }
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
        return 'Sin videoconferencia';
    }
  }

  auditStatusLabel(value: string | null | undefined) {
    switch ((value || 'PENDING').trim().toUpperCase()) {
      case 'SYNCED':
        return 'Sincronizada';
      case 'ERROR':
        return 'Error de sync';
      default:
        return 'Sin sincronizar';
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

  timeRange(row: VideoconferenceRescheduleItem) {
    return `${this.shortTime(row.override_start_time)} - ${this.shortTime(row.override_end_time)}`;
  }

  shortTime(value: string | null | undefined) {
    return value ? String(value).slice(0, 5) : '--:--';
  }

  formatDateSafe(value: string | Date | null | undefined) {
    if (!value) return '--';
    const str = typeof value === 'string' ? value : value.toISOString();
    const match = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (match) {
      const [, year, month, day] = match;
      return `${parseInt(day, 10)}/${parseInt(month, 10)}/${year}`;
    }
    const date = new Date(str);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleDateString();
  }

  formatDateTime(value: string | Date | null | undefined) {
    if (!value) {
      return '--';
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }

  private normalizeText(value: string | null | undefined) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim()
      .toUpperCase();
  }
}
