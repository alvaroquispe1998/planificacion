import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-audit-detail-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './audit-detail.page.html',
  styleUrl: './audit-detail.page.css',
})
export class AuditDetailPageComponent implements OnInit {
  loading = true;
  syncing = false;
  error = '';
  record: any | null = null;
  instances: any[] = [];
  selectedInstanceId = '';

  constructor(
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly api: ApiService,
  ) {}

  ngOnInit(): void {
    const id = this.route.snapshot.paramMap.get('id');
    if (!id) {
      this.error = 'No se recibio el id de la videoconferencia.';
      this.loading = false;
      return;
    }
    this.loadDetail(id);
  }

  loadDetail(id: string) {
    this.loading = true;
    this.error = '';
    this.api.getPlanningVideoconferenceAuditDetail(id).subscribe({
      next: (result) => {
        this.record = result?.record ?? null;
        this.instances = Array.isArray(result?.instances) ? result.instances : [];
        this.selectedInstanceId =
          result?.selected_instance_id ??
          this.instances[0]?.id ??
          '';
        this.loading = false;
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo cargar el detalle de auditoria.';
        this.loading = false;
      },
    });
  }

  syncZoomData() {
    if (!this.record?.id || !this.record?.can_sync) {
      return;
    }
    this.syncing = true;
    this.error = '';
    this.api.syncPlanningVideoconferenceAudit(this.record.id).subscribe({
      next: (result) => {
        this.record = result?.record ?? this.record;
        this.instances = Array.isArray(result?.instances) ? result.instances : [];
        this.selectedInstanceId =
          result?.selected_instance_id ??
          this.instances[0]?.id ??
          '';
        this.syncing = false;
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo sincronizar la reunion con Zoom.';
        this.syncing = false;
      },
    });
  }

  selectInstance(id: string) {
    this.selectedInstanceId = id;
  }

  get selectedInstance() {
    return this.instances.find((item) => item.id === this.selectedInstanceId) ?? this.instances[0] ?? null;
  }

  statusLabel(value: string | null | undefined) {
    switch (value) {
      case 'MATCHED':
        return 'Conciliada';
      case 'CREATED_UNMATCHED':
        return 'Creada sin match';
      case 'ERROR':
        return 'Error';
      case 'CREATING':
        return 'Creando';
      default:
        return value || 'Sin estado';
    }
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

  sectionLabel(record: any) {
    return record?.section_external_code || record?.section_code || 'Seccion sin codigo';
  }

  groupLabel(record: any) {
    return record?.subsection_code ? `Grupo ${record.subsection_code}` : 'Grupo sin codigo';
  }

  formatDateTime(value: string | Date | null | undefined) {
    if (!value) {
      return '--';
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }

  shortTime(value: string | null | undefined) {
    return value ? String(value).slice(0, 5) : '--:--';
  }

  backToList() {
    this.router.navigate(['/videoconferences/audit']);
  }
}
