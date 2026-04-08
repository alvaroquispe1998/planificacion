import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
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
  readonly iconPaths: Record<string, string> = {
    user:
      'M12 4.5a3.5 3.5 0 1 1 0 7 3.5 3.5 0 0 1 0-7Zm0 9c4.14 0 7.5 2.35 7.5 5.25V20h-15v-1.25C4.5 15.85 7.86 13.5 12 13.5Z',
    mail:
      'M4.5 6.75A2.25 2.25 0 0 1 6.75 4.5h10.5a2.25 2.25 0 0 1 2.25 2.25v6.5a2.25 2.25 0 0 1-2.25 2.25H6.75A2.25 2.25 0 0 1 4.5 13.25v-6.5Zm1.69-.75 5.81 4.36L17.81 6H6.19Z',
    badge:
      'M12 3.5 14.2 8h4.8l-3.6 3.3.95 4.95L12 13.9l-4.35 2.35.95-4.95L5 8h4.8L12 3.5Z',
    eye:
      'M12 5c5.25 0 8.78 4.42 9.74 5.78a1 1 0 0 1 0 1.15C20.78 13.28 17.25 17.7 12 17.7S3.22 13.28 2.26 11.93a1 1 0 0 1 0-1.15C3.22 9.42 6.75 5 12 5Zm0 2.2A4.3 4.3 0 1 0 12 15.8a4.3 4.3 0 0 0 0-8.6Zm0 1.8a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5Z',
    download:
      'M12 3.5a1 1 0 0 1 1 1v6.19l1.97-1.97 1.41 1.41-4.38 4.38-4.38-4.38 1.41-1.41L11 10.69V4.5a1 1 0 0 1 1-1ZM5 16.5h14V18.5H5v-2Z',
    video:
      'M5.5 6.5A2.5 2.5 0 0 1 8 4h6a2.5 2.5 0 0 1 2.5 2.5v.85l2.4-1.2a1 1 0 0 1 1.45.9v6.9a1 1 0 0 1-1.45.9l-2.4-1.2v.85A2.5 2.5 0 0 1 14 17h-6a2.5 2.5 0 0 1-2.5-2.5v-8Z',
    audio:
      'M14.5 4.5v8.42A3.25 3.25 0 1 1 13 10.18V6.55l5-1.43v6.8A3.25 3.25 0 1 1 16.5 9.2V4.5h-2Z',
    text:
      'M6.75 4.5h6.19L17.5 9.06v8.19A2.25 2.25 0 0 1 15.25 19.5h-8.5A2.25 2.25 0 0 1 4.5 17.25v-10.5A2.25 2.25 0 0 1 6.75 4.5Zm.75 6.25v1.5h7v-1.5h-7Zm0 3v1.5h5v-1.5h-5Z',
    code:
      'm9.22 8.47-3.53 3.53 3.53 3.53-1.06 1.06-4.59-4.59 4.59-4.59 1.06 1.06Zm5.56 0 1.06-1.06 4.59 4.59-4.59 4.59-1.06-1.06L18.31 12l-3.53-3.53ZM13.9 5l-3.2 14h-1.6l3.2-14h1.6Z',
    file:
      'M6.75 4.5h6.19L17.5 9.06v8.19A2.25 2.25 0 0 1 15.25 19.5h-8.5A2.25 2.25 0 0 1 4.5 17.25v-10.5A2.25 2.25 0 0 1 6.75 4.5Z',
  };
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
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    const id = this.route.snapshot.paramMap.get('id');
    if (!id) {
      this.error = 'No se recibio el id de la videoconferencia.';
      this.loading = false;
      this.cdr.detectChanges();
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
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo cargar el detalle de auditoria.';
        this.loading = false;
        this.cdr.detectChanges();
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
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo sincronizar la reunion con Zoom.';
        this.syncing = false;
        this.cdr.detectChanges();
      },
    });
  }

  selectInstance(id: string) {
    this.selectedInstanceId = id;
  }

  get selectedInstance() {
    return this.instances.find((item) => item.id === this.selectedInstanceId) ?? this.instances[0] ?? null;
  }

  get visibleParticipants() {
    return this.dedupeParticipants(this.selectedInstance?.participants ?? []);
  }

  get hiddenDuplicateParticipantsCount() {
    const rawCount = Array.isArray(this.selectedInstance?.participants) ? this.selectedInstance.participants.length : 0;
    return Math.max(0, rawCount - this.visibleParticipants.length);
  }

  get visibleRecordings() {
    return Array.isArray(this.selectedInstance?.recordings) ? this.selectedInstance.recordings : [];
  }

  get syncButtonLabel() {
    if (this.syncing) {
      return 'Sincronizando...';
    }
    if (this.record?.is_sync_owner === false) {
      return 'Sincronizar desde owner';
    }
    return 'Sincronizar datos Zoom';
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

  iconPath(name: string) {
    return this.iconPaths[name] ?? this.iconPaths['file'];
  }

  participantRoleLabel(value: string | null | undefined) {
    switch (`${value ?? ''}`.trim().toUpperCase()) {
      case 'HOST':
        return 'Host';
      case 'CO_HOST':
        return 'Co-host';
      case 'PANELIST':
        return 'Panelista';
      case 'ATTENDEE':
        return 'Asistente';
      default:
        return 'Rol no reportado';
    }
  }

  recordingTypeLabel(recording: any) {
    const type = `${recording?.recording_type ?? ''}`.trim().toUpperCase();
    switch (type) {
      case 'AUDIO_ONLY':
        return 'Audio';
      case 'SHARED_SCREEN_WITH_SPEAKER_VIEW':
      case 'SHARED_SCREEN_WITH_GALLERY_VIEW':
      case 'ACTIVE_SPEAKER':
      case 'GALLERY_VIEW':
      case 'SCREEN_SHARE':
        return 'Video';
      case 'CHAT_FILE':
      case 'TIMELINE':
      case 'TRANSCRIPT':
      case 'CC':
        return 'Texto';
      case 'JSON':
        return 'Datos';
      default:
        return type || 'Archivo';
    }
  }

  recordingStatusLabel(value: string | null | undefined) {
    switch (`${value ?? ''}`.trim().toUpperCase()) {
      case 'COMPLETED':
      case 'AVAILABLE':
        return 'Disponible';
      case 'PROCESSING':
        return 'Procesando';
      case 'DELETED':
        return 'Eliminado';
      case 'EXPIRED':
        return 'Expirado';
      case 'ERROR':
        return 'Con error';
      default:
        return value || 'Disponible';
    }
  }

  recordingIcon(recording: any) {
    const extension = `${recording?.file_extension ?? ''}`.trim().toUpperCase();
    if (['MP4', 'MOV', 'AVI', 'MPEG'].includes(extension)) {
      return 'video';
    }
    if (['M4A', 'MP3', 'WAV'].includes(extension)) {
      return 'audio';
    }
    if (['TXT', 'VTT', 'DOC', 'DOCX', 'PDF'].includes(extension)) {
      return 'text';
    }
    if (['JSON'].includes(extension)) {
      return 'code';
    }
    return 'file';
  }

  private dedupeParticipants(items: any[]) {
    const seen = new Set<string>();
    return items.filter((item) => {
      const key =
        [
          `${item?.display_name ?? ''}`.trim().toUpperCase(),
          `${item?.email ?? ''}`.trim().toLowerCase(),
          `${item?.role ?? ''}`.trim().toUpperCase(),
        ]
          .filter((value) => value !== '')
          .join('|') ||
        `${item?.zoom_participant_id ?? ''}`.trim() ||
        `${item?.zoom_user_id ?? ''}`.trim();

      if (!key || seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
  }

  backToList() {
    this.router.navigate(['/videoconferences/audit']);
  }
}
