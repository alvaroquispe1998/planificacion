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
  aliasTargetQuery = '';
  showAliasTargetOptions = false;
  aliasContext: any = null;

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
  readonly optionalFieldLabels = [
    'docente',
    'turno',
    'modalidad',
    'pabellon o edificio',
    'aula',
    'laboratorio',
    'horario',
  ];
  readonly namespaceLabels: Record<string, string> = {
    vc_period: 'Periodo VC',
    campus: 'Sede o local',
    faculty_code: 'Facultad',
    academic_program_code: 'Programa academico',
    study_plan_code: 'Plan de estudios',
    course_code: 'Curso',
    course_modality: 'Modalidad del curso',
    shift: 'Turno',
    building: 'Pabellon o edificio',
    classroom: 'Aula',
    laboratory: 'Laboratorio',
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
      case 'vc_period':
        return this.aliasCatalog.vc_periods ?? [];
      case 'campus':
        return this.aliasCatalog.campuses ?? [];
      case 'faculty_code':
        return this.aliasCatalog.faculties ?? [];
      case 'academic_program_code':
        return this.aliasCatalog.academic_programs ?? [];
      case 'study_plan_code':
        return this.filteredStudyPlanTargets();
      case 'course_code':
        return this.filteredCourseTargets();
      case 'course_modality':
        return this.aliasCatalog.course_modalities ?? [];
      case 'shift':
        return this.aliasCatalog.shift_options ?? [];
      case 'building':
        return this.filteredBuildingTargets();
      case 'classroom':
      case 'laboratory':
        return this.filteredClassroomTargets();
      default:
        return [];
    }
  }

  get filteredAliasTargets() {
    const query = this.normalizeLoose(this.aliasTargetQuery);
    if (!query) {
      return this.aliasAvailableTargets;
    }
    return this.aliasAvailableTargets.filter((item: any) =>
      this.normalizeLoose(item?.label).includes(query),
    );
  }

  get aliasSourceValueDisplay() {
    return String(this.aliasContext?.source_value_display ?? this.aliasForm.source_value ?? '').trim();
  }

  get aliasSourceValueLocked() {
    return this.aliasModalMode === 'edit' || this.aliasSourceValueDisplay !== this.aliasForm.source_value;
  }

  get dependentAcademicPrograms() {
    return Array.isArray(this.aliasContext?.dependent_academic_programs)
      ? this.aliasContext.dependent_academic_programs
      : [];
  }

  get hasResolvedDependentAcademicProgram() {
    return this.dependentAcademicPrograms.some((item: any) => Boolean(item?.target_id));
  }

  get aliasTargetHelperText() {
    if (this.aliasForm.namespace === 'campus') {
      const detectedCampus = String(
        this.aliasContext?.target_label ?? this.aliasContext?.target_id ?? '',
      ).trim();
      return detectedCampus
        ? `Confirma si este valor del Excel corresponde a ${detectedCampus}.`
        : '';
    }
    if (this.aliasForm.namespace === 'building') {
      const campusLabels = this.aliasDependentTargetLabels('dependent_campuses');
      return campusLabels.length
        ? `Mostrando pabellones de ${campusLabels.join(', ')}.`
        : '';
    }
    if (this.aliasForm.namespace === 'classroom' || this.aliasForm.namespace === 'laboratory') {
      const targetLabel = this.aliasForm.namespace === 'laboratory' ? 'laboratorios' : 'aulas';
      const campusLabels = this.aliasDependentTargetLabels('dependent_campuses');
      const buildingLabels = this.aliasDependentTargetLabels('dependent_buildings');
      if (buildingLabels.length) {
        return `Mostrando ${targetLabel} del pabellon ${buildingLabels.join(', ')}${
          campusLabels.length ? ` en ${campusLabels.join(', ')}` : ''
        }.`;
      }
      if (campusLabels.length) {
        return `Mostrando ${targetLabel} de ${campusLabels.join(', ')}.`;
      }
      return '';
    }
    if (this.aliasForm.namespace !== 'study_plan_code') {
      if (this.aliasForm.namespace !== 'course_code') {
        return '';
      }
      const dependentStudyPlans = Array.isArray(this.aliasContext?.dependent_study_plans)
        ? this.aliasContext.dependent_study_plans
        : [];
      const resolvedStudyPlans = dependentStudyPlans.filter((item: any) => Boolean(item?.target_id));
      const cycles = Array.isArray(this.aliasContext?.dependent_cycles)
        ? this.aliasContext.dependent_cycles.filter(Boolean)
        : [];
      if (!resolvedStudyPlans.length) {
        return 'Primero confirma el plan de estudios relacionado para poder elegir el curso correcto.';
      }
      return `Mostrando cursos del plan ${resolvedStudyPlans
        .map((item: any) => item?.target_label || item?.source_value)
        .filter(Boolean)
        .join(', ')}${cycles.length ? ` en ciclo ${cycles.join(', ')}` : ''}.`;
    }
    const dependentPrograms = this.dependentAcademicPrograms;
    if (!dependentPrograms.length) {
      return '';
    }
    const resolvedPrograms = dependentPrograms.filter((item: any) => Boolean(item?.target_id));
    if (!resolvedPrograms.length) {
      const pending = dependentPrograms
        .map((item: any) => String(item?.source_value ?? '').trim())
        .filter(Boolean)
        .join(', ');
      return pending
        ? `Primero mapea el programa academico relacionado: ${pending}.`
        : 'Primero mapea el programa academico relacionado.';
    }
    return `Mostrando planes del programa: ${resolvedPrograms
      .map((item: any) => item?.target_label || item?.source_value)
      .filter(Boolean)
      .join(', ')}.`;
  }

  get optionalFieldsSummary() {
    return this.optionalFieldLabels.join(', ');
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
            this.message = 'La revision previa esta en proceso. La pantalla se actualizara automaticamente.';
            this.startBatchPolling(result.id);
            return;
          }
          this.completeUploadProgress();
          this.message = 'Revision previa generada correctamente.';
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
            this.message = this.message || 'Revision previa generada correctamente.';
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
          this.message = 'Decisiones por grupo guardadas correctamente.';
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
    if (!this.canExecuteBatch) {
      this.error = this.executeHelperText;
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
          this.executing = false;
          this.applyBatch(result);
          this.message = this.executionReportSummary
            ? `Carga masiva ejecutada correctamente. ${this.executionReportSummary}.`
            : 'Carga masiva ejecutada correctamente.';
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
    this.aliasTargetQuery = '';
    this.showAliasTargetOptions = false;
    this.aliasContext = null;
  }

  onAliasNamespaceChange() {
    this.aliasForm.target_id = '';
    this.aliasForm.target_label = '';
    this.aliasTargetQuery = '';
    this.showAliasTargetOptions = false;
  }

  onAliasTargetChange() {
    const selected = this.aliasAvailableTargets.find((item: any) => item.id === this.aliasForm.target_id);
    this.aliasForm.target_label = selected?.label ?? this.aliasForm.target_id;
    this.aliasTargetQuery = selected?.label ?? '';
  }

  onAliasTargetQueryChange(value: string) {
    this.aliasTargetQuery = value;
    this.aliasForm.target_id = '';
    this.aliasForm.target_label = value;
    this.showAliasTargetOptions = true;
  }

  onAliasTargetFocus() {
    this.showAliasTargetOptions = true;
  }

  onAliasTargetBlur() {
    setTimeout(() => {
      this.showAliasTargetOptions = false;
      const selected = this.aliasAvailableTargets.find((item: any) => item.id === this.aliasForm.target_id);
      if (selected) {
        this.aliasTargetQuery = selected.label;
        this.aliasForm.target_label = selected.label;
        this.cdr.detectChanges();
      }
    }, 120);
  }

  selectAliasTarget(target: any | null) {
    this.aliasForm.target_id = target?.id ?? '';
    this.aliasForm.target_label = target?.label ?? '';
    this.aliasTargetQuery = target?.label ?? '';
    this.showAliasTargetOptions = false;
  }

  getMappingActionLabel(item: any) {
    if (this.findExistingAlias(item?.namespace, item?.source_value)) {
      return 'Modificar mapeo';
    }
    if (item?.target_id) {
      return 'Confirmar mapeo';
    }
    return 'Agregar mapeo';
  }

  mappingSourceLabel(item: any) {
    return String(item?.source_value_display ?? item?.source_value ?? '').trim();
  }

  get importableRowCount() {
    return Number(this.batch?.summary?.importable_row_count ?? this.batch?.importable_row_count ?? 0);
  }

  get blockedRowCount() {
    return Number(this.batch?.summary?.blocked_row_count ?? this.batch?.blocked_row_count ?? 0);
  }

  get warningRowCount() {
    return Number(this.batch?.summary?.warning_row_count ?? this.batch?.warning_row_count ?? 0);
  }

  get pendingMappingConfirmationCount() {
    const items = Array.isArray(this.batch?.unresolved_mappings) ? this.batch.unresolved_mappings : [];
    return items.filter((item: any) => Boolean(item?.requires_confirmation)).length;
  }

  get hasPendingMappings() {
    return this.pendingMappingConfirmationCount > 0;
  }

  get canExecuteBatch() {
    return (
      this.batch?.status === 'PREVIEW_READY' &&
      this.importableRowCount > 0 &&
      !this.hasPendingScopeDecisions &&
      !this.hasPendingMappings
    );
  }

  get executeButtonDisabled() {
    return this.executing || !this.canExecuteBatch;
  }

  get executeButtonLabel() {
    if (this.executing) {
      return 'Ejecutando...';
    }
    if (this.batch?.status === 'EXECUTED') {
      return 'Carga aplicada';
    }
    return 'Ejecutar carga';
  }

  get executionReportSummary() {
    const report = this.batch?.report ?? null;
    if (!report) {
      return '';
    }
    return [
      `${Number(report.imported_scope_count ?? 0)} grupos cargados`,
      `${Number(report.created_plan_rule_count ?? 0)} planes`,
      `${Number(report.created_offer_count ?? 0)} ofertas`,
      `${Number(report.created_section_count ?? 0)} secciones`,
      `${Number(report.created_subsection_count ?? 0)} subsecciones`,
      `${Number(report.created_schedule_count ?? 0)} horarios`,
    ].join(' | ');
  }

  get executeHelperText() {
    if (!this.hasBatch) {
      return 'Genera la revision previa para habilitar la carga.';
    }
    const status = String(this.batch?.status ?? '').trim();
    if (status === 'PREVIEW_PROCESSING') {
      return 'Espera a que termine la revision previa antes de ejecutar la carga.';
    }
    if (status === 'PREVIEW_FAILED') {
      return this.batch?.error_message || 'La revision previa fallo. Genera una nueva antes de ejecutar.';
    }
    if (status === 'FAILED') {
      return (
        this.batch?.error_message ||
        this.batch?.report?.error_message ||
        'La ultima ejecucion fallo. Revisa el batch y vuelve a intentarlo.'
      );
    }
    if (status === 'EXECUTED') {
      return this.executionReportSummary || 'Esta revision ya fue ejecutada y guardada en la plataforma.';
    }
    if (this.importableRowCount <= 0) {
      return 'No hay filas validas para guardar en la plataforma.';
    }
    if (this.hasPendingScopeDecisions) {
      return 'Primero decide que hacer con los grupos que ya tienen informacion guardada.';
    }
    if (this.hasPendingMappings) {
      return 'Primero confirma los mapeos sugeridos que aparecen en la seccion de pendientes.';
    }
    if (this.blockedRowCount > 0) {
      return `Al ejecutar se guardaran ${this.importableRowCount} filas validas y se omitiran ${this.blockedRowCount} bloqueadas.`;
    }
    if (this.warningRowCount > 0) {
      return `Se guardaran ${this.importableRowCount} filas. ${this.warningRowCount} entraran con observaciones.`;
    }
    return `Se guardaran ${this.importableRowCount} filas validas en la planificacion.`;
  }

  get existingScopeDecisions() {
    const items = Array.isArray(this.batch?.scope_decisions) ? this.batch.scope_decisions : [];
    return items.filter((item: any) => this.scopeHasExistingData(item));
  }

  namespaceLabel(namespace: string) {
    return this.namespaceLabels[String(namespace ?? '').trim()] ?? this.humanizeCode(namespace);
  }

  countsLabel(counts: any) {
    return [
      `${Number(counts?.plan_rules ?? 0)} planes`,
      `${Number(counts?.offers ?? 0)} ofertas`,
      `${Number(counts?.sections ?? 0)} secciones`,
      `${Number(counts?.subsections ?? 0)} subsecciones`,
      `${Number(counts?.schedules ?? 0)} horarios`,
    ].join(' | ');
  }

  severityLabel(severity: string) {
    switch (String(severity ?? '').toUpperCase()) {
      case 'BLOCKING':
        return 'No se guardara';
      case 'WARNING':
        return 'Se guardara con observaciones';
      case 'INFO':
        return 'Informativo';
      default:
        return severity || 'Observacion';
    }
  }

  issueTitle(issueCode: string) {
    return this.issueMeta(issueCode).title;
  }

  issueDescription(issueCode: string) {
    return this.issueMeta(issueCode).description;
  }

  topBlockingReasons(limit = 3) {
    const items = Array.isArray(this.batch?.issue_summary) ? this.batch.issue_summary : [];
    return items
      .filter((item: any) => String(item?.severity ?? '').toUpperCase() === 'BLOCKING')
      .slice(0, limit);
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
            ? 'Mapeo actualizado correctamente.'
            : 'Mapeo creado correctamente.';
          this.message =
            'Mapeo guardado. Esta revision previa se mantiene; cuando quieras, genera nuevamente el preview para recalcular.';
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

  scopeLabelDisplay(scopeDecision: any) {
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
      .join(' | ');
  }

  existingSummaryLabelDisplay(scopeDecision: any) {
    const existing = scopeDecision?.existing_summary ?? {};
    if (!existing?.has_existing_data) {
      return 'No hay informacion previa en este grupo.';
    }
    return [
      `${existing.plan_rule_count ?? 0} planes`,
      `${existing.offer_count ?? 0} ofertas`,
      `${existing.section_count ?? 0} secciones`,
      `${existing.subsection_count ?? 0} subsecciones`,
      `${existing.schedule_count ?? 0} horarios`,
    ].join(' | ');
  }

  decisionLabel(decision: string) {
    switch (decision) {
      case 'REPLACE_SCOPE':
        return 'Borrar lo actual y cargar lo nuevo';
      case 'SKIP_SCOPE':
        return 'Mantener lo actual y no cargar este grupo';
      default:
        return 'Elige que hacer con este grupo';
    }
  }

  scopeHasExistingData(scopeDecision: any) {
    return Boolean(scopeDecision?.existing_summary?.has_existing_data);
  }

  private applyBatch(result: any) {
    const previousStatus = this.batch?.status ?? '';
    if (result?.status === 'EXECUTED' || result?.status === 'FAILED') {
      this.executing = false;
    }
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
      this.message = 'Revision previa generada correctamente.';
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
    this.uploadStageLabel = 'Revision previa lista.';
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
    this.aliasContext = item ?? null;
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
    this.aliasTargetQuery = '';
    this.showAliasTargetOptions = false;
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
      if (this.aliasContext?.target_id) {
        this.aliasForm.target_id = String(this.aliasContext.target_id ?? '').trim();
        this.aliasForm.target_label = String(
          this.aliasContext.target_label ?? this.aliasContext.target_id ?? '',
        ).trim();
        this.aliasTargetQuery = this.aliasForm.target_label;
      }
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
    this.aliasTargetQuery = this.aliasForm.target_label;
    this.showAliasTargetOptions = false;
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

  private filteredStudyPlanTargets() {
    const studyPlans = this.aliasCatalog.study_plans ?? [];
    if (this.aliasForm.namespace !== 'study_plan_code') {
      return studyPlans;
    }
    const programIds = this.dependentAcademicPrograms
      .map((item: any) => String(item?.target_id ?? '').trim())
      .filter(Boolean);
    if (!programIds.length) {
      return [];
    }
    return studyPlans.filter((item: any) => programIds.includes(String(item?.academic_program_id ?? '').trim()));
  }

  private filteredCourseTargets() {
    const courseTargets = this.aliasCatalog.course_targets ?? [];
    if (this.aliasForm.namespace !== 'course_code') {
      return courseTargets;
    }
    const studyPlanIds = (Array.isArray(this.aliasContext?.dependent_study_plans)
      ? this.aliasContext.dependent_study_plans
      : [])
      .map((item: any) => String(item?.target_id ?? '').trim())
      .filter(Boolean);
    const cycles = (Array.isArray(this.aliasContext?.dependent_cycles)
      ? this.aliasContext.dependent_cycles
      : [])
      .map((value: any) => Number(value))
      .filter((value: number) => Number.isFinite(value) && value > 0);
    return courseTargets.filter((item: any) => {
      const matchesPlan =
        !studyPlanIds.length || studyPlanIds.includes(String(item?.study_plan_id ?? '').trim());
      const itemCycle = Number(item?.cycle ?? 0);
      const matchesCycle = !cycles.length || cycles.includes(itemCycle);
      return matchesPlan && matchesCycle;
    });
  }

  private filteredBuildingTargets() {
    const buildings = this.aliasCatalog.buildings ?? [];
    if (this.aliasForm.namespace !== 'building') {
      return buildings;
    }
    const campusIds = this.aliasDependentTargetIds('dependent_campuses');
    if (!campusIds.length) {
      return buildings;
    }
    return buildings.filter((item: any) => campusIds.includes(String(item?.campus_id ?? '').trim()));
  }

  private filteredClassroomTargets() {
    const classrooms = this.aliasCatalog.classrooms ?? [];
    if (this.aliasForm.namespace !== 'classroom' && this.aliasForm.namespace !== 'laboratory') {
      return classrooms;
    }

    const campusIds = this.aliasDependentTargetIds('dependent_campuses');
    const buildingIds = this.aliasDependentTargetIds('dependent_buildings');
    if (!campusIds.length && !buildingIds.length) {
      return classrooms;
    }

    return classrooms.filter((item: any) => {
      const itemCampusId = String(item?.campus_id ?? '').trim();
      const itemBuildingId = String(item?.building_id ?? '').trim();

      if (buildingIds.length) {
        if (itemBuildingId && buildingIds.includes(itemBuildingId)) {
          return true;
        }
        if (campusIds.length && itemCampusId) {
          return campusIds.includes(itemCampusId);
        }
        return false;
      }

      if (campusIds.length) {
        return campusIds.includes(itemCampusId);
      }
      return true;
    });
  }

  private aliasDependentTargetIds(
    key: 'dependent_campuses' | 'dependent_buildings',
  ) {
    const values = this.aliasDependentEntries(key)
      .map((item: any) => String(item?.target_id ?? '').trim())
      .filter(Boolean);
    return [...new Set(values)];
  }

  private aliasDependentTargetLabels(
    key: 'dependent_campuses' | 'dependent_buildings',
  ) {
    const values = this.aliasDependentEntries(key)
      .map((item: any) => String(item?.target_label ?? item?.source_value ?? '').trim())
      .filter(Boolean);
    return [...new Set(values)];
  }

  private aliasDependentEntries(
    key: 'dependent_campuses' | 'dependent_buildings',
  ) {
    return Array.isArray(this.aliasContext?.[key]) ? this.aliasContext[key] : [];
  }

  private normalizeLoose(value: unknown) {
    return String(value ?? '')
      .normalize('NFD')
      .replace(/\p{Diacritic}/gu, '')
      .toLowerCase()
      .trim();
  }

  private issueMeta(issueCode: string) {
    const normalizedCode = String(issueCode ?? '').trim().toUpperCase();
    const staticMap: Record<string, { title: string; description: string }> = {
      INVALID_SECTION: {
        title: 'La seccion no se pudo interpretar',
        description: 'Esa fila no se puede cargar porque la seccion del Excel no tiene un formato valido.',
      },
      MISSING_CYCLE: {
        title: 'Falta el ciclo academico',
        description: 'Sin ciclo no se puede ubicar correctamente la fila dentro del plan.',
      },
      MULTIPLE_SCHEDULES_FOR_SUBSECTION: {
        title: 'La misma subseccion tiene mas de un horario',
        description: 'Conviene revisar esa fila antes de crear horarios duplicados para una sola subseccion.',
      },
      AMBIGUOUS_STUDY_PLAN_COURSE: {
        title: 'El curso coincide con varias opciones del plan',
        description: 'Hay mas de una coincidencia posible y el sistema no puede elegir una sola con seguridad.',
      },
      MISSING_STUDY_PLAN_COURSE: {
        title: 'No se encontro el curso dentro del plan de estudios',
        description: 'Esa fila no se guardara hasta resolver el curso correcto del plan.',
      },
      UNMATCHED_TEACHER: {
        title: 'No se encontro el docente',
        description: 'La estructura si se puede guardar, pero el docente quedara vacio.',
      },
      UNMATCHED_COURSE_MODALITY: {
        title: 'No se encontro la modalidad',
        description: 'La estructura si se puede guardar, pero la modalidad quedara vacia.',
      },
      UNMATCHED_SHIFT: {
        title: 'No se encontro el turno',
        description: 'La estructura si se puede guardar, pero el turno quedara vacio.',
      },
      UNMATCHED_BUILDING: {
        title: 'No se encontro el pabellon o edificio',
        description: 'La estructura si se puede guardar, pero ese dato fisico quedara vacio.',
      },
      UNMATCHED_CLASSROOM: {
        title: 'No se encontro el aula',
        description: 'La estructura si se puede guardar, pero el aula quedara vacia.',
      },
      UNMATCHED_LABORATORY: {
        title: 'No se encontro el laboratorio',
        description: 'La estructura si se puede guardar, pero el laboratorio quedara vacio.',
      },
      INVALID_SCHEDULE: {
        title: 'El horario no es valido',
        description: 'La estructura si se puede guardar, pero el horario no se creara con esos datos.',
      },
    };
    if (staticMap[normalizedCode]) {
      return staticMap[normalizedCode];
    }
    if (normalizedCode.startsWith('MISSING_')) {
      const namespace = normalizedCode.replace(/^MISSING_/, '').toLowerCase();
      const label = this.namespaceLabel(namespace);
      return {
        title: `Falta mapear ${label.toLowerCase()}`,
        description: 'Esa fila no se guardara hasta resolver ese dato obligatorio.',
      };
    }
    return {
      title: this.humanizeCode(issueCode),
      description: 'Hay una observacion tecnica que conviene revisar antes de ejecutar la carga.',
    };
  }

  private humanizeCode(value: string) {
    return String(value ?? '')
      .trim()
      .toLowerCase()
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (letter) => letter.toUpperCase());
  }
}
