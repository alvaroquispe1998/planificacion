import { CommonModule, NgClass } from '@angular/common';
import { ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { finalize, forkJoin, of } from 'rxjs';
import type { Subscription } from 'rxjs';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-planning-imports-page',
  standalone: true,
  imports: [CommonModule, FormsModule, NgClass],
  templateUrl: './planning-imports.page.html',
  styleUrl: './planning-imports.page.css',
})
export class PlanningImportsPageComponent implements OnInit, OnDestroy {
  loading = false;
  uploading = false;
  saving = false;
  executing = false;
  error = '';
  message = '';
  uploadProgress = 0;
  uploadStageLabel = '';
  aliasModalOpen = false;
  aliasModalLoading = false;
  aliasSaving = false;
  aliasError = '';
  aliasMessage = '';
  aliasModalMode: 'create' | 'edit' = 'create';
  aliasEditingId = '';

  selectedFile: File | null = null;
  batch: any = null;
  decisionDraftByScopeKey: Record<string, string> = {};
  aliasCatalog: any = {};
  aliasMappings: any[] = [];
  aliasMappingsLoaded = false;
  aliasForm = {
    namespace: '',
    source_value: '',
    target_id: '',
    target_label: '',
    is_active: true,
    notes: '',
  };
  private batchPollingTimer: ReturnType<typeof setInterval> | null = null;
  private routeSubscription: Subscription | null = null;

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.routeSubscription = this.route.queryParamMap.subscribe((params) => {
      const batchId = params.get('batch') ?? '';
      if (!batchId) {
        return;
      }
      if (this.batch?.id === batchId && this.batch?.status !== 'PREVIEW_PROCESSING') {
        return;
      }
      this.loadBatch(batchId, true);
    });
  }

  ngOnDestroy(): void {
    this.stopBatchPolling();
    this.routeSubscription?.unsubscribe();
    this.routeSubscription = null;
  }

  get hasBatch() {
    return Boolean(this.batch?.id);
  }

  get isPreviewProcessing() {
    return this.uploading || this.batch?.status === 'PREVIEW_PROCESSING';
  }

  get showBatchResults() {
    return Boolean(this.batch?.id) && this.batch?.status !== 'PREVIEW_PROCESSING';
  }

  get hasPendingScopeDecisions() {
    return Number(this.batch?.summary?.pending_scope_decision_count ?? 0) > 0;
  }

  get aliasAvailableTargets() {
    switch (this.aliasForm.namespace) {
      case 'campus':
        return this.aliasCatalog.campuses ?? [];
      case 'faculty_code':
        return this.aliasCatalog.faculties ?? [];
      case 'academic_program_code':
        return this.aliasCatalog.academic_programs ?? [];
      case 'study_plan_code':
        return this.aliasCatalog.study_plans ?? [];
      case 'course_modality':
        return this.aliasCatalog.course_modalities ?? [];
      case 'shift':
        return this.aliasCatalog.shift_options ?? [];
      case 'building':
        return this.aliasCatalog.buildings ?? [];
      case 'classroom':
      case 'laboratory':
        return this.aliasCatalog.classrooms ?? [];
      default:
        return [];
    }
  }

  onFileChange(event: Event) {
    const input = event.target as HTMLInputElement | null;
    this.selectedFile = input?.files?.[0] ?? null;
  }

  uploadPreview() {
    if (!this.selectedFile) {
      this.error = 'Selecciona un archivo Excel antes de generar el preview.';
      return;
    }
    this.error = '';
    this.message = '';
    this.stopBatchPolling();
    this.batch = null;
    this.uploading = true;
    this.startUploadProgress();
    this.api
      .previewPlanningImport(this.selectedFile)
      .pipe(
        finalize(() => {
          this.uploading = false;
        }),
      )
      .subscribe({
        next: (result) => {
          this.applyBatch(result);
          if (result?.status === 'PREVIEW_PROCESSING') {
            this.message = 'Preview en proceso. La pantalla se actualizara automaticamente.';
            this.startBatchPolling(result.id);
            return;
          }
          this.completeUploadProgress();
          this.message = 'Preview generado correctamente.';
        },
        error: (err) => {
          this.stopBatchPolling();
          this.uploadProgress = 0;
          this.uploadStageLabel = '';
          this.error = err?.error?.message ?? 'No se pudo generar el preview del archivo.';
        },
      });
  }

  loadBatch(batchId = this.batch?.id, silent = false) {
    if (!batchId) {
      return;
    }
    if (!silent) {
      this.loading = true;
      this.error = '';
    }
    this.api
      .getPlanningImportBatch(batchId)
      .pipe(
        finalize(() => {
          if (!silent) {
            this.loading = false;
          }
        }),
      )
      .subscribe({
        next: (result) => {
          this.applyBatch(result);
          if (result?.status === 'PREVIEW_READY') {
            this.message = this.message || 'Preview generado correctamente.';
          }
          if (result?.status === 'PREVIEW_FAILED') {
            this.error = result?.error_message ?? 'No se pudo generar el preview del archivo.';
          }
        },
        error: (err) => {
          this.stopBatchPolling();
          this.error = err?.error?.message ?? 'No se pudo cargar el batch de importacion.';
        },
      });
  }

  saveScopeDecisions() {
    if (!this.batch?.id) {
      return;
    }
    const decisions = (this.batch.scope_decisions ?? []).map((item: any) => ({
      scope_key: item.scope_key,
      decision: this.decisionDraftByScopeKey[item.scope_key] ?? item.decision,
      notes: item.notes ?? '',
    }));
    this.saving = true;
    this.error = '';
    this.api
      .updatePlanningImportScopeDecisions(this.batch.id, { decisions })
      .pipe(finalize(() => (this.saving = false)))
      .subscribe({
        next: (result) => {
          this.applyBatch(result);
          this.message = 'Decisiones por scope actualizadas.';
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudieron guardar las decisiones.';
        },
      });
  }

  executeBatch() {
    if (!this.batch?.id) {
      return;
    }
    this.executing = true;
    this.error = '';
    this.message = '';
    this.api
      .executePlanningImportBatch(this.batch.id)
      .pipe(finalize(() => (this.executing = false)))
      .subscribe({
        next: (result) => {
          this.applyBatch(result);
          this.message = 'Carga masiva ejecutada correctamente.';
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo ejecutar la carga masiva.';
        },
      });
  }

  openMappings(item?: any) {
    if (item?.namespace || item?.source_value) {
      this.openAliasModal(item);
      return;
    }
    this.router.navigate(['/planning/import-mappings'], {
      queryParams: {
        namespace: item?.namespace || null,
        source_value: item?.source_value || null,
        batch: this.batch?.id || null,
      },
    });
  }

  closeAliasModal() {
    this.aliasModalOpen = false;
    this.aliasModalMode = 'create';
    this.aliasEditingId = '';
    this.aliasError = '';
    this.aliasMessage = '';
  }

  onAliasNamespaceChange() {
    this.aliasForm.target_id = '';
    this.aliasForm.target_label = '';
  }

  onAliasTargetChange() {
    const selected = this.aliasAvailableTargets.find((item: any) => item.id === this.aliasForm.target_id);
    this.aliasForm.target_label = selected?.label ?? this.aliasForm.target_id;
  }

  getMappingActionLabel(item: any) {
    return this.findExistingAlias(item?.namespace, item?.source_value) ? 'Modificar mapeo' : 'Agregar mapeo';
  }

  saveAliasMapping() {
    if (!this.aliasForm.namespace || !this.aliasForm.source_value || !this.aliasForm.target_id) {
      this.aliasError = 'Completa namespace, valor origen y destino antes de guardar.';
      return;
    }
    this.aliasSaving = true;
    this.aliasError = '';
    this.aliasMessage = '';
    const payload = {
      namespace: this.aliasForm.namespace,
      source_value: this.aliasForm.source_value,
      target_id: this.aliasForm.target_id,
      target_label: this.aliasForm.target_label,
      is_active: this.aliasForm.is_active,
      notes: this.aliasForm.notes,
    };
    const request$ = this.aliasEditingId
      ? this.api.updatePlanningImportAlias(this.aliasEditingId, {
        target_id: payload.target_id,
        target_label: payload.target_label,
        is_active: payload.is_active,
        notes: payload.notes,
      })
      : this.api.createPlanningImportAlias(payload);
    request$
      .pipe(finalize(() => (this.aliasSaving = false)))
      .subscribe({
        next: (saved) => {
          this.upsertAliasMapping(saved);
          this.aliasMessage = this.aliasEditingId
            ? 'Alias actualizado correctamente.'
            : 'Alias creado correctamente.';
          this.message =
            'Alias guardado. El preview actual se mantiene; cuando quieras, genera nuevamente el preview para recalcular.';
          this.closeAliasModal();
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.aliasError = err?.error?.message ?? 'No se pudo guardar el alias.';
        },
      });
  }

  scopeLabel(scopeDecision: any) {
    const scope = scopeDecision?.scope ?? {};
    return [
      scope?.semester_name,
      scope?.campus_name,
      scope?.faculty_name,
      scope?.academic_program_name,
      scope?.study_plan_year,
      scope?.cycle ? `Ciclo ${scope.cycle}` : null,
    ]
      .filter(Boolean)
      .join(' · ');
  }

  existingSummaryLabel(scopeDecision: any) {
    const existing = scopeDecision?.existing_summary ?? {};
    if (!existing?.has_existing_data) {
      return 'Sin data previa';
    }
    return [
      `${existing.plan_rule_count ?? 0} planes`,
      `${existing.offer_count ?? 0} ofertas`,
      `${existing.section_count ?? 0} secciones`,
      `${existing.subsection_count ?? 0} subsecciones`,
      `${existing.schedule_count ?? 0} horarios`,
    ].join(' · ');
  }

  decisionClass(decision: string) {
    return {
      'decision-pending': decision === 'PENDING',
      'decision-replace': decision === 'REPLACE_SCOPE',
      'decision-skip': decision === 'SKIP_SCOPE',
    };
  }

  private applyBatch(result: any) {
    const previousStatus = this.batch?.status ?? '';
    this.batch = result;
    this.syncUploadProgressFromBatch(result);
    this.decisionDraftByScopeKey = {};
    for (const item of result?.scope_decisions ?? []) {
      this.decisionDraftByScopeKey[item.scope_key] = item.decision;
    }
    if (result?.status === 'PREVIEW_PROCESSING') {
      this.startBatchPolling(result.id);
    } else {
      this.stopBatchPolling();
    }
    if (previousStatus === 'PREVIEW_PROCESSING' && result?.status === 'PREVIEW_READY') {
      this.completeUploadProgress();
      this.message = 'Preview generado correctamente.';
    }
    if (previousStatus === 'PREVIEW_PROCESSING' && result?.status === 'PREVIEW_FAILED') {
      this.error = result?.error_message ?? 'No se pudo generar el preview del archivo.';
    }
    if (result?.status === 'PREVIEW_READY' && result?.unresolved_mappings?.length && !this.aliasMappingsLoaded) {
      this.loadAliasMappingsForPreview();
    }
    const currentBatchInUrl = this.route.snapshot.queryParamMap.get('batch') ?? '';
    if ((result?.id ?? '') !== currentBatchInUrl) {
      this.router.navigate([], {
        relativeTo: this.route,
        replaceUrl: true,
        queryParams: {
          batch: result?.id ?? null,
        },
      });
    }
    this.cdr.detectChanges();
  }

  private startUploadProgress() {
    this.uploadProgress = 5;
    this.uploadStageLabel = 'Registrando archivo para generar el preview...';
  }

  private completeUploadProgress() {
    this.uploadProgress = 100;
    this.uploadStageLabel = 'Preview listo.';
  }

  private startBatchPolling(batchId: string) {
    this.stopBatchPolling();
    this.loadBatch(batchId, true);
    this.batchPollingTimer = setInterval(() => {
      this.loadBatch(batchId, true);
    }, 800);
  }

  private stopBatchPolling() {
    if (this.batchPollingTimer) {
      clearInterval(this.batchPollingTimer);
      this.batchPollingTimer = null;
    }
  }

  private syncUploadProgressFromBatch(result: any) {
    const progress = result?.progress ?? null;
    if (!progress) {
      return;
    }
    this.uploadProgress = Number(progress.percent ?? 0);
    this.uploadStageLabel =
      String(progress.message ?? '').trim() || 'Procesando preview...';
    this.cdr.detectChanges();
  }

  private openAliasModal(item: any) {
    this.aliasModalOpen = true;
    this.aliasError = '';
    this.aliasMessage = '';
    const namespace = String(item?.namespace ?? '').trim();
    const sourceValue = String(item?.source_value ?? '').trim();
    this.setAliasModalForm(namespace, sourceValue);
    if (this.aliasCatalog?.namespaces?.length && this.aliasMappingsLoaded) {
      this.applyExistingAliasToModal(namespace, sourceValue);
      this.cdr.detectChanges();
      return;
    }
    this.aliasModalLoading = true;
    forkJoin({
      catalog: this.aliasCatalog?.namespaces?.length
        ? of(this.aliasCatalog)
        : this.api.getPlanningImportAliasCatalog(),
      mappings: this.aliasMappingsLoaded
        ? of(this.aliasMappings)
        : this.api.listPlanningImportAliases(),
    })
      .pipe(finalize(() => (this.aliasModalLoading = false)))
      .subscribe({
        next: (result) => {
          this.aliasCatalog = result.catalog ?? {};
          this.aliasMappings = Array.isArray(result.mappings) ? result.mappings : [];
          this.aliasMappingsLoaded = true;
          this.applyExistingAliasToModal(namespace, sourceValue);
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.aliasError = err?.error?.message ?? 'No se pudo cargar el catalogo de mapeos.';
        },
      });
  }

  private setAliasModalForm(namespace: string, sourceValue: string) {
    this.aliasModalMode = 'create';
    this.aliasEditingId = '';
    this.aliasForm = {
      namespace,
      source_value: sourceValue,
      target_id: '',
      target_label: '',
      is_active: true,
      notes: '',
    };
  }

  private applyExistingAliasToModal(namespace: string, sourceValue: string) {
    const existing = this.findExistingAlias(namespace, sourceValue);
    if (!existing) {
      this.aliasModalMode = 'create';
      this.aliasEditingId = '';
      this.aliasForm.namespace = namespace;
      this.aliasForm.source_value = sourceValue;
      return;
    }
    this.aliasModalMode = 'edit';
    this.aliasEditingId = String(existing.id ?? '').trim();
    this.aliasForm = {
      namespace: String(existing.namespace ?? namespace).trim(),
      source_value: String(existing.source_value ?? sourceValue).trim(),
      target_id: String(existing.target_id ?? '').trim(),
      target_label: String(existing.target_label ?? existing.target_id ?? '').trim(),
      is_active: existing.is_active !== false,
      notes: String(existing.notes ?? '').trim(),
    };
  }

  private findExistingAlias(namespace: string, sourceValue: string) {
    const normalizedNamespace = String(namespace ?? '').trim().toLowerCase();
    const normalizedSourceValue = String(sourceValue ?? '').trim().toUpperCase();
    return this.aliasMappings.find(
      (item) =>
        String(item?.namespace ?? '').trim().toLowerCase() === normalizedNamespace &&
        String(item?.source_value ?? '').trim().toUpperCase() === normalizedSourceValue,
    );
  }

  private upsertAliasMapping(saved: any) {
    const existingIndex = this.aliasMappings.findIndex((item) => String(item?.id ?? '') === String(saved?.id ?? ''));
    if (existingIndex >= 0) {
      this.aliasMappings[existingIndex] = {
        ...this.aliasMappings[existingIndex],
        ...saved,
      };
      return;
    }
    const duplicateIndex = this.aliasMappings.findIndex(
      (item) =>
        String(item?.namespace ?? '').trim().toLowerCase() === String(saved?.namespace ?? '').trim().toLowerCase() &&
        String(item?.source_value ?? '').trim().toUpperCase() === String(saved?.source_value ?? '').trim().toUpperCase(),
    );
    if (duplicateIndex >= 0) {
      this.aliasMappings[duplicateIndex] = {
        ...this.aliasMappings[duplicateIndex],
        ...saved,
      };
      return;
    }
    this.aliasMappings = [...this.aliasMappings, saved];
  }

  private loadAliasMappingsForPreview() {
    this.api.listPlanningImportAliases().subscribe({
      next: (mappings) => {
        this.aliasMappings = Array.isArray(mappings) ? mappings : [];
        this.aliasMappingsLoaded = true;
        this.cdr.detectChanges();
      },
      error: () => {
        this.aliasMappingsLoaded = true;
      },
    });
  }
}
