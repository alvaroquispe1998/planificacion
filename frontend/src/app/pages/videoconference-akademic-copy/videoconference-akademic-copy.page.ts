import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import {
  AkademicInheritanceCopyPayloadPreview,
  AkademicInheritanceCopyPreviewChild,
  AkademicInheritanceCopyPreviewItem,
  VideoconferenceApiService,
} from '../../services/videoconference-api.service';

@Component({
  selector: 'app-videoconference-akademic-copy-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './videoconference-akademic-copy.page.html',
  styleUrl: './videoconference-akademic-copy.page.css',
})
export class VideoconferenceAkademicCopyPageComponent {
  loading = false;
  error = '';
  hasSearched = false;
  rows: AkademicInheritanceCopyPreviewItem[] = [];
  expandedIds = new Set<string>();
  payloadPreviewOpen = false;
  payloadPreviewTitle = '';
  payloadPreviewJson = '';
  cloningIds = new Set<string>();
  cloneMessages: Record<string, string> = {};

  filters = {
    dateFrom: this.todayIso(),
    dateTo: this.todayIso(),
    parentVcSectionId: '',
  };

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  get totals() {
    return {
      total: this.rows.length,
      ready: this.rows.filter((row) => row.status === 'READY').length,
      missing: this.rows.filter((row) => row.status === 'MISSING_AKADEMIC_CONFERENCE').length,
      attention: this.rows.filter((row) => row.status !== 'READY').length,
    };
  }

  loadPreview() {
    if (!this.filters.dateFrom || !this.filters.dateTo) {
      this.error = 'Selecciona fecha inicio y fecha fin.';
      return;
    }
    if (this.filters.dateFrom > this.filters.dateTo) {
      this.error = 'La fecha inicio no puede ser mayor que la fecha fin.';
      return;
    }

    this.loading = true;
    this.error = '';
    this.hasSearched = true;
    this.api.previewAkademicInheritanceCopy({
      dateFrom: this.filters.dateFrom,
      dateTo: this.filters.dateTo,
      parentVcSectionId: this.filters.parentVcSectionId.trim() || undefined,
    }).subscribe({
      next: (result) => {
        this.rows = Array.isArray(result.items) ? result.items : [];
        this.expandedIds = new Set(this.rows.map((row) => row.parentLocalVideoconferenceId));
        this.cloningIds.clear();
        this.cloneMessages = {};
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo preparar la previsualizacion.';
        this.rows = [];
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  clearFilters() {
    const today = this.todayIso();
    this.filters = {
      dateFrom: today,
      dateTo: today,
      parentVcSectionId: '',
    };
    this.rows = [];
    this.error = '';
    this.hasSearched = false;
    this.expandedIds.clear();
  }

  toggleExpanded(row: AkademicInheritanceCopyPreviewItem) {
    const id = row.parentLocalVideoconferenceId;
    if (this.expandedIds.has(id)) {
      this.expandedIds.delete(id);
    } else {
      this.expandedIds.add(id);
    }
  }

  isExpanded(row: AkademicInheritanceCopyPreviewItem) {
    return this.expandedIds.has(row.parentLocalVideoconferenceId);
  }

  openPayload(child: AkademicInheritanceCopyPreviewChild, row: AkademicInheritanceCopyPreviewItem) {
    if (!child.payload) {
      return;
    }
    this.payloadPreviewTitle = `${row.courseLabel} | ${child.childDestinationSectionId || child.childVcSectionId || 'Sin destino'}`;
    this.payloadPreviewJson = this.stringifyPayload(child.payload);
    this.payloadPreviewOpen = true;
  }

  cloneChild(child: AkademicInheritanceCopyPreviewChild) {
    if (!child.payload || this.isCloning(child)) {
      return;
    }
    const key = this.childKey(child);
    this.cloningIds.add(key);
    this.cloneMessages = { ...this.cloneMessages, [key]: '' };

    this.api.cloneAkademicInheritanceCopy(child.payload).subscribe({
      next: (result) => {
        const message = result.ok
          ? 'Videoconferencia clonada'
          : `Akademic respondio ${result.status}`;
        this.cloneMessages = { ...this.cloneMessages, [key]: message };
        this.cloningIds.delete(key);
        this.cdr.detectChanges();
      },
      error: (err) => {
        const message = err?.error?.message ?? 'No se pudo clonar la videoconferencia.';
        this.cloneMessages = { ...this.cloneMessages, [key]: message };
        this.cloningIds.delete(key);
        this.cdr.detectChanges();
      },
    });
  }

  closePayload() {
    this.payloadPreviewOpen = false;
    this.payloadPreviewTitle = '';
    this.payloadPreviewJson = '';
  }

  statusLabel(row: AkademicInheritanceCopyPreviewItem) {
    switch (row.status) {
      case 'READY':
        return 'Listo';
      case 'MISSING_AKADEMIC_CONFERENCE':
        return 'No encontrado en Akademic';
      case 'NO_CHILDREN':
        return 'Sin hijos';
      case 'CHILDREN_INCOMPLETE':
        return 'Hijos incompletos';
      case 'LOOKUP_ERROR':
        return 'Error consultando Akademic';
      default:
        return 'Por revisar';
    }
  }

  childStatusLabel(child: AkademicInheritanceCopyPreviewChild) {
    switch (child.status) {
      case 'READY':
        return 'Payload listo';
      case 'MISSING_AKADEMIC_CONFERENCE':
        return 'Falta ID Akademic';
      case 'MISSING_CHILD_SECTION':
        return 'Hija sin ID seccion';
      default:
        return 'Por revisar';
    }
  }

  statusClass(status: string) {
    if (status === 'READY') return 'status-ready';
    if (status === 'MISSING_AKADEMIC_CONFERENCE' || status === 'LOOKUP_ERROR') return 'status-error';
    return 'status-warning';
  }

  matchLabel(row: AkademicInheritanceCopyPreviewItem) {
    if (!row.akademicConference) {
      return 'Sin ID Akademic';
    }
    if (row.akademicConference.matchType === 'section_topic_near_date') {
      return 'Match por fecha cercana';
    }
    return row.akademicConference.matchType === 'exact_section_and_topic'
      || row.akademicConference.matchType === 'section_and_date'
      ? 'Match exacto'
      : 'Match por topic y fecha';
  }

  childLabel(child: AkademicInheritanceCopyPreviewChild) {
    const section = child.childSectionExternalCode || child.childSectionCode || 'Seccion hija';
    const group = child.childSubsectionCode ? `Grupo ${child.childSubsectionCode}` : 'Grupo sin codigo';
    return `${section} | ${group}`;
  }

  parentSectionLabel(row: AkademicInheritanceCopyPreviewItem) {
    return `${row.parentSectionExternalCode || row.parentSectionCode || 'Seccion padre'} | Grupo ${row.parentSubsectionCode || '-'}`;
  }

  childSectionLabel(child: AkademicInheritanceCopyPreviewChild) {
    return `${child.childSectionExternalCode || child.childSectionCode || 'Seccion hija'} | Grupo ${child.childSubsectionCode || '-'}`;
  }

  childKey(child: AkademicInheritanceCopyPreviewChild) {
    return child.inheritanceId || child.childScheduleId;
  }

  isCloning(child: AkademicInheritanceCopyPreviewChild) {
    return this.cloningIds.has(this.childKey(child));
  }

  cloneMessage(child: AkademicInheritanceCopyPreviewChild) {
    return this.cloneMessages[this.childKey(child)] || '';
  }

  private stringifyPayload(payload: AkademicInheritanceCopyPayloadPreview) {
    return JSON.stringify(payload, null, 2);
  }

  private todayIso() {
    const now = new Date();
    const year = now.getFullYear();
    const month = `${now.getMonth() + 1}`.padStart(2, '0');
    const day = `${now.getDate()}`.padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
}
