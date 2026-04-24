import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { finalize } from 'rxjs';
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
  bulkCloning = false;
  bulkProgress = '';
  summaryFilter: 'all' | 'created' | 'pending' | 'review' = 'all';

  filters = {
    dateFrom: this.todayIso(),
    dateTo: this.todayIso(),
    courseCode: '',
    parentVcSectionId: '',
  };

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  get totals() {
    const children = this.rows.flatMap((row) => row.children);
    // Every child falls into exactly one bucket so created + pending + review = children.length
    const created = children.filter((child) => this.isAlreadyCopied(child)).length;
    const pending = children.filter((child) => !!child.payload && !this.isAlreadyCopied(child)).length;
    const review = children.filter((child) => !child.payload && !this.isAlreadyCopied(child)).length;
    // Parents that have no children at all also deserve attention but do not appear in `children`.
    const parentsWithoutChildren = this.rows.filter((row) => row.children.length === 0).length;
    return {
      total: this.rows.length,
      created,
      pending,
      missing: this.rows.filter((row) => row.status === 'MISSING_AKADEMIC_CONFERENCE').length,
      attention: review + parentsWithoutChildren,
    };
  }

  get cloneableVisibleChildren() {
    return this.visibleRows.flatMap((row) =>
      row.children
        .filter((child) => child.payload && !this.isAlreadyCopied(child))
        .map((child) => ({ row, child })),
    );
  }

  get visibleRows() {
    switch (this.summaryFilter) {
      case 'created':
        return this.rows.filter((row) => row.children.some((child) => this.isAlreadyCopied(child)));
      case 'pending':
        return this.rows.filter((row) => row.children.some((child) => child.payload && !this.isAlreadyCopied(child)));
      case 'review':
        return this.rows.filter((row) => row.status !== 'READY' || row.children.some((child) => !child.payload));
      default:
        return this.rows;
    }
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
      courseCode: this.filters.courseCode.trim() || undefined,
      parentVcSectionId: this.filters.parentVcSectionId.trim() || undefined,
    }).subscribe({
      next: (result) => {
        this.rows = Array.isArray(result.items) ? result.items : [];
        this.expandedIds = new Set(this.rows.map((row) => row.parentLocalVideoconferenceId));
        this.cloningIds.clear();
        this.cloneMessages = {};
        this.summaryFilter = 'all';
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
      courseCode: '',
      parentVcSectionId: '',
    };
    this.rows = [];
    this.error = '';
    this.hasSearched = false;
    this.expandedIds.clear();
    this.summaryFilter = 'all';
  }

  setSummaryFilter(filter: 'all' | 'created' | 'pending' | 'review') {
    this.summaryFilter = filter;
  }

  summaryActive(filter: 'all' | 'created' | 'pending' | 'review') {
    return this.summaryFilter === filter;
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

  cloneChild(child: AkademicInheritanceCopyPreviewChild, row: AkademicInheritanceCopyPreviewItem) {
    if (!child.payload || this.isCloning(child)) {
      return;
    }
    void this.cloneChildOnce(child, row).catch(() => undefined);
  }

  async cloneVisibleVideoconferences() {
    const targets = this.cloneableVisibleChildren;
    if (!targets.length || this.bulkCloning) {
      return;
    }

    this.bulkCloning = true;
    this.bulkProgress = `Clonando 0 de ${targets.length}`;
    this.cdr.detectChanges();

    let done = 0;
    for (const target of targets) {
      if (!target.child.payload || this.isAlreadyCopied(target.child)) {
        continue;
      }
      try {
        await this.cloneChildOnce(target.child, target.row);
      } catch {
        // The row-level message already captures the error. Continue with the rest.
      }
      done += 1;
      this.bulkProgress = `Clonando ${done} de ${targets.length}`;
      this.cdr.detectChanges();
    }

    this.bulkProgress = `Clonacion finalizada: ${done} procesadas`;
    this.bulkCloning = false;
    this.cdr.detectChanges();
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
        return child.akademicCopyStatus === 'COPIED' ? 'Ya clonada' : 'Payload listo';
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

  copyStatusLabel(value: string | null | undefined) {
    switch ((value || '').toUpperCase()) {
      case 'COPIED':
        return 'Copiada';
      case 'ERROR':
        return 'Error de copia';
      case 'PENDING':
        return 'Copia pendiente';
      default:
        return 'Sin marca de copia';
    }
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

  isAlreadyCopied(child: AkademicInheritanceCopyPreviewChild) {
    return child.akademicCopyStatus === 'COPIED';
  }

  cloneMessage(child: AkademicInheritanceCopyPreviewChild) {
    return this.cloneMessages[this.childKey(child)] || '';
  }

  private stringifyPayload(payload: AkademicInheritanceCopyPayloadPreview) {
    return JSON.stringify(payload, null, 2);
  }

  private cloneChildOnce(child: AkademicInheritanceCopyPreviewChild, row: AkademicInheritanceCopyPreviewItem) {
    const key = this.childKey(child);
    this.cloningIds.add(key);
    this.cloneMessages = { ...this.cloneMessages, [key]: '' };
    this.cdr.detectChanges();

    return new Promise<void>((resolve, reject) => {
      this.api.cloneAkademicInheritanceCopy({
        ...child.payload!,
        inheritanceId: child.inheritanceId,
        parentLocalVideoconferenceId: row.parentLocalVideoconferenceId,
      }).pipe(
        finalize(() => {
          this.cloningIds.delete(key);
          this.cdr.detectChanges();
        }),
      ).subscribe({
        next: (result) => {
          const message = result.ok
            ? 'Videoconferencia clonada'
            : `Akademic respondio ${result.status}`;
          if (result.ok) {
            child.akademicCopyStatus = 'COPIED';
            child.akademicCopiedAt = new Date().toISOString();
            if (row.children.every((item) => item.akademicCopyStatus === 'COPIED')) {
              row.akademicCopyStatus = 'COPIED';
            } else if (!row.akademicCopyStatus) {
              row.akademicCopyStatus = 'PENDING';
            }
          }
          this.cloneMessages = { ...this.cloneMessages, [key]: message };
          resolve();
        },
        error: (err) => {
          const message = err?.error?.message ?? 'No se pudo clonar la videoconferencia.';
          this.cloneMessages = { ...this.cloneMessages, [key]: message };
          reject(err);
        },
      });
    });
  }

  private todayIso() {
    const now = new Date();
    const year = now.getFullYear();
    const month = `${now.getMonth() + 1}`.padStart(2, '0');
    const day = `${now.getDate()}`.padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
}
