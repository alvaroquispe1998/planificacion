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
  readonly akademicOnlyView = true;
  loading = false;
  semesterOptionsLoading = false;
  semesterOptionsLoaded = false;
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
  showSyncDiffDetail = false;
  aliasModalMode: 'create' | 'edit' = 'create';
  aliasEditingId = '';
  aliasTargetQuery = '';
  showAliasTargetOptions = false;
  aliasContext: any = null;
  sourceMode: 'AKADEMIC' | 'EXCEL' = 'AKADEMIC';
  importCatalogLoading = false;
  importCatalog: any = {
    semesters: [],
    vc_periods: [],
    campuses: [],
    faculties: [],
    academic_programs: [],
    study_plans: [],
  };
  akademicForm = {
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
    study_plan_id: '',
    cycle: '',
    study_plan_course_id: '',
    course_code: '',
  };

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
  compareFile: File | null = null;
  compareSemesterId = '';
  comparing = false;
  compareExporting = false;
  compareResult: any = null;
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
  private batchPollingId = '';
  private previewDurationTimer: ReturnType<typeof setInterval> | null = null;
  private previewStartedAtMs: number | null = null;
  private routeSubscription: Subscription | null = null;

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.sourceMode = 'AKADEMIC';
    this.loadSemesterOptions();
    this.loadImportCatalog();
    this.routeSubscription = this.route.queryParamMap.subscribe((params) => {
      const batchId = params.get('batch') ?? '';
      if (!batchId) {
        return;
      }
      if (this.batch?.id === batchId && !this.isBatchProcessingStatus(this.batch?.status)) {
        return;
      }
      this.loadBatch(batchId, true);
    });
  }

  ngOnDestroy(): void {
    this.stopBatchPolling();
    this.stopPreviewDurationTimer();
    this.routeSubscription?.unsubscribe();
    this.routeSubscription = null;
  }

  get hasBatch() {
    return Boolean(this.batch?.id);
  }

  get hasSemesterOptions() {
    return Array.isArray(this.importCatalog.semesters) && this.importCatalog.semesters.length > 0;
  }

  get sourceModeLabel() {
    return this.isAkademicSourceSelected ? 'Akademic' : 'Excel';
  }

  get isAkademicSourceSelected() {
    return this.akademicOnlyView || this.sourceMode === 'AKADEMIC';
  }

  get akademicSourceDisplay() {
    const semesterName = this.resolveSemesterName(this.batch?.source_scope?.semester_id ?? this.akademicForm.semester_id);
    return semesterName ? `Akademic ${semesterName}` : 'Consulta directa contra Akademic';
  }

  get akademicFilteredPrograms() {
    const items = Array.isArray(this.importCatalog.academic_programs)
      ? this.importCatalog.academic_programs
      : [];
    if (!this.akademicForm.faculty_id) {
      return items;
    }
    return items.filter((item: any) => item.faculty_id === this.akademicForm.faculty_id);
  }

  get akademicFilteredStudyPlans() {
    const items = Array.isArray(this.importCatalog.study_plans) ? this.importCatalog.study_plans : [];
    return items.filter((item: any) => {
      if (this.akademicForm.faculty_id && item.faculty_id !== this.akademicForm.faculty_id) {
        return false;
      }
      if (
        this.akademicForm.academic_program_id &&
        item.academic_program_id !== this.akademicForm.academic_program_id
      ) {
        return false;
      }
      return true;
    });
  }

  get isPreviewProcessing() {
    return this.uploading || this.batch?.status === 'PREVIEW_PROCESSING';
  }

  get isExecutionProcessing() {
    return this.batch?.status === 'EXECUTING';
  }

  get showExecutionProgressModal() {
    return this.executing || this.isExecutionProcessing;
  }

  get showBatchResults() {
    return Boolean(this.batch?.id) && this.batch?.status !== 'PREVIEW_PROCESSING';
  }

  get previewDurationLabel() {
    const startedAt = this.previewDurationStartMs();
    const finishedAt = this.previewDurationEndMs();
    if (!startedAt || !finishedAt || finishedAt < startedAt) {
      return '';
    }
    return this.formatDuration(finishedAt - startedAt);
  }

  get executionProgressPercent() {
    if (this.batch?.status === 'EXECUTED') {
      return 100;
    }
    const reportedPercent = Number(this.batch?.report?.percent ?? (this.executing ? 2 : 0));
    const processed = Number(this.batch?.report?.processed_scope_count ?? 0);
    const total = Number(this.batch?.report?.total_scope_count ?? 0);
    const derivedPercent =
      total > 0 ? Math.max(0, Math.min(100, Math.round((processed / total) * 100))) : 0;
    return Math.max(0, Math.min(100, Math.round(Math.max(reportedPercent, derivedPercent))));
  }

  get executionProgressMessage() {
    const processed = Number(this.batch?.report?.processed_scope_count ?? 0);
    const total = Number(this.batch?.report?.total_scope_count ?? 0);
    const message = String(this.batch?.report?.message ?? '').trim();
    if (processed > 0 && total > 0 && /^Preparando la carga/i.test(message)) {
      return `Aplicando grupos en la plataforma (${processed}/${total})...`;
    }
    if (message) {
      return message;
    }
    return this.executing ? 'Iniciando la carga masiva...' : 'Aplicando la carga en la plataforma';
  }

  get executionCurrentScopeLabel() {
    return String(this.batch?.report?.current_scope_label ?? '').trim();
  }

  get executionDurationLabel() {
    const startedAt = this.executionDurationStartMs();
    const finishedAt = this.executionDurationEndMs();
    if (!startedAt || !finishedAt || finishedAt < startedAt) {
      return '';
    }
    return this.formatDuration(finishedAt - startedAt);
  }

  get executionProcessedScopeSummary() {
    const processed = Number(this.batch?.report?.processed_scope_count ?? 0);
    const total = Number(this.batch?.report?.total_scope_count ?? 0);
    if (total > 0) {
      return `${processed} de ${total} grupos`;
    }
    return 'Preparando grupos';
  }

  get executionScopeOutcomeSummary() {
    const report = this.batch?.report ?? {};
    return [
      `${Number(report.imported_scope_count ?? 0)} cargados`,
      `${Number(report.skipped_scope_count ?? 0)} omitidos`,
      `${Number(report.replaced_scope_count ?? 0)} reemplazados`,
    ].join(' | ');
  }

  get executionCreatedCountsSummary() {
    const report = this.batch?.report ?? {};
    return [
      `${Number(report.created_plan_rule_count ?? 0)} planes`,
      `${Number(report.created_offer_count ?? 0)} ofertas`,
      `${Number(report.created_section_count ?? 0)} secciones`,
      `${Number(report.created_subsection_count ?? 0)} grupos`,
      `${Number(report.created_schedule_count ?? 0)} horarios`,
    ].join(' | ');
  }

  get executionLastUpdateLabel() {
    const updatedAt = this.executionProgressUpdatedAtMs();
    if (!updatedAt) {
      return '';
    }
    const delta = Math.max(0, Date.now() - updatedAt);
    if (delta < 5000) {
      return 'hace unos segundos';
    }
    return `hace ${this.formatDuration(delta)}`;
  }

  get executionTimingHelperText() {
    const duration = this.executionDurationLabel;
    if (!duration) {
      return '';
    }
    if (this.batch?.status === 'EXECUTING') {
      return this.executionLastUpdateLabel
        ? `Tiempo transcurrido: ${duration}. Ultima actualizacion ${this.executionLastUpdateLabel}.`
        : `Tiempo transcurrido: ${duration}.`;
    }
    if (this.batch?.status === 'FAILED') {
      return `La ejecucion alcanzo a correr ${duration} antes de fallar.`;
    }
    return `La ejecucion demoro ${duration}.`;
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
    return this.mappingSourceLabel(
      this.aliasContext ?? {
        namespace: this.aliasForm.namespace,
        source_value: this.aliasForm.source_value,
      },
    );
  }

  get aliasSourceValueLocked() {
    return this.aliasModalMode === 'edit' || this.aliasSourceValueDisplay !== this.aliasForm.source_value;
  }

  get aliasSourceHelperText() {
    return this.mappingAffectedRowsLabel(this.aliasContext);
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

  get compareSummary() {
    return this.compareResult?.summary ?? {};
  }

  get compareOnlyInExcel() {
    return Array.isArray(this.compareResult?.only_in_excel) ? this.compareResult.only_in_excel : [];
  }

  get compareOnlyInSystem() {
    return Array.isArray(this.compareResult?.only_in_system) ? this.compareResult.only_in_system : [];
  }

  get compareDifferences() {
    return Array.isArray(this.compareResult?.differences) ? this.compareResult.differences : [];
  }

  get compareWarnings() {
    return Array.isArray(this.compareResult?.warnings) ? this.compareResult.warnings : [];
  }

  onFileChange(event: Event) {
    const input = event.target as HTMLInputElement | null;
    this.selectedFile = input?.files?.[0] ?? null;
  }

  onCompareFileChange(event: Event) {
    const input = event.target as HTMLInputElement | null;
    this.compareFile = input?.files?.[0] ?? null;
  }

  setSourceMode(mode: 'AKADEMIC' | 'EXCEL') {
    this.sourceMode = this.akademicOnlyView ? 'AKADEMIC' : mode;
    this.error = '';
    this.message = '';
  }

  runExcelCompare() {
    if (!this.compareFile) {
      this.error = 'Selecciona un Excel antes de comparar.';
      return;
    }
    if (!this.compareSemesterId) {
      this.error = 'Selecciona el semestre contra el que quieres comparar.';
      return;
    }
    this.comparing = true;
    this.error = '';
    this.message = '';
    this.compareResult = null;
    this.api
      .comparePlanningExcelWithSystem(this.compareFile, this.compareSemesterId)
      .pipe(finalize(() => (this.comparing = false)))
      .subscribe({
        next: (result) => {
          this.compareResult = result;
          this.message = 'Comparacion generada correctamente.';
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo comparar el Excel con el sistema.';
          this.cdr.detectChanges();
        },
      });
  }

  exportCompareReport() {
    if (!this.compareFile || !this.compareSemesterId) {
      this.error = 'Selecciona semestre y archivo antes de descargar el reporte.';
      return;
    }
    this.compareExporting = true;
    this.error = '';
    this.api
      .exportPlanningExcelComparison(this.compareFile, this.compareSemesterId)
      .pipe(finalize(() => (this.compareExporting = false)))
      .subscribe({
        next: (response: any) => {
          if (!response?.body) {
            return;
          }
          const fileName =
            this.fileNameFromDisposition(response.headers?.get('content-disposition')) ||
            `comparacion-planificacion-${this.compareSemesterId}.xlsx`;
          const url = URL.createObjectURL(response.body);
          const anchor = document.createElement('a');
          anchor.href = url;
          anchor.download = fileName;
          anchor.click();
          URL.revokeObjectURL(url);
          this.message = 'Reporte de comparacion descargado correctamente.';
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo descargar el reporte de comparacion.';
          this.cdr.detectChanges();
        },
      });
  }

  onAkademicFacultyChange() {
    if (
      this.akademicForm.academic_program_id &&
      !this.akademicFilteredPrograms.some(
        (item: any) => item.id === this.akademicForm.academic_program_id,
      )
    ) {
      this.akademicForm.academic_program_id = '';
    }
    this.onAkademicProgramChange();
  }

  onAkademicProgramChange() {
    if (
      this.akademicForm.study_plan_id &&
      !this.akademicFilteredStudyPlans.some((item: any) => item.id === this.akademicForm.study_plan_id)
    ) {
      this.akademicForm.study_plan_id = '';
    }
  }

  uploadPreview() {
    this.error = '';
    this.message = '';
    this.stopBatchPolling();
    this.batch = null;
    this.previewStartedAtMs = Date.now();
    this.uploading = true;
    this.startUploadProgress();
    const request$ =
      this.isAkademicSourceSelected
        ? this.previewAkademicImportRequest()
        : this.previewExcelImportRequest();
    if (!request$) {
      this.uploading = false;
      this.uploadProgress = 0;
      this.uploadStageLabel = '';
      return;
    }
    request$
      .pipe(
        finalize(() => {
          this.uploading = false;
          this.syncPreviewDurationTimer();
        }),
      )
      .subscribe({
        next: (result) => {
          this.applyBatch(result);
          if (result?.status === 'PREVIEW_PROCESSING') {
            this.message = this.isAkademicSourceSelected
              ? 'El preview desde Akademic esta en proceso. La pantalla se actualizara automaticamente.'
              : 'La revision previa esta en proceso. La pantalla se actualizara automaticamente.';
            return;
          }
          this.completeUploadProgress();
          this.message = this.isAkademicSourceSelected
            ? 'Preview listo. Revisa el resultado y luego usa el boton para insertar en tablas.'
            : 'Revision previa generada correctamente.';
        },
        error: (err) => {
          this.stopBatchPolling();
          this.uploadProgress = 0;
          this.uploadStageLabel = '';
          this.error = err?.error?.message ?? (
            this.isAkademicSourceSelected
              ? 'No se pudo generar el preview desde Akademic.'
              : 'No se pudo generar el preview del archivo.'
          );
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
            this.error = '';
            this.message = this.message || (
              this.isAkademicSourceSelected
                ? 'Preview listo. Revisa el resultado y luego usa el boton para insertar en tablas.'
                : 'Revision previa generada correctamente.'
            );
          }
          if (result?.status === 'PREVIEW_FAILED') {
            this.message = '';
            this.error = result?.error_message ?? (
              this.isAkademicSourceSelected
                ? 'No se pudo preparar la sincronizacion desde Akademic.'
                : 'No se pudo generar el preview del archivo.'
            );
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
          this.applyBatch(result);
          if (result?.status === 'EXECUTING') {
            this.message = this.isAkademicSourceSelected
              ? 'La insercion en tablas se inicio correctamente. Veras el avance en la ventana de seguimiento.'
              : 'La carga se inicio correctamente. Veras el avance en la ventana de seguimiento.';
            return;
          }
          this.message = this.executionReportSummary
            ? `${this.isAkademicSourceSelected ? 'Insercion en tablas' : 'Carga masiva'} ejecutada correctamente. ${this.executionReportSummary}.`
            : `${this.isAkademicSourceSelected ? 'Insercion en tablas' : 'Carga masiva'} ejecutada correctamente.`;
        },
        error: (err) => {
          this.error = err?.error?.message ?? (
            this.isAkademicSourceSelected
              ? 'No se pudo insertar en tablas.'
              : 'No se pudo ejecutar la carga masiva.'
          );
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
    const sourceValueDisplay = String(item?.source_value_display ?? '').trim();
    if (sourceValueDisplay) {
      return sourceValueDisplay;
    }

    const sourceValue = String(item?.source_value ?? '').trim();
    const namespace = String(item?.namespace ?? '').trim().toLowerCase();
    if (!sourceValue || (namespace !== 'classroom' && namespace !== 'laboratory')) {
      return sourceValue;
    }

    const campusValues = this.mappingDependentSourceValues(item, 'dependent_campuses');
    if (!campusValues.length) {
      return sourceValue;
    }

    return `${sourceValue} | ${campusValues.join(', ')}`;
  }

  mappingAffectedRowsLabel(item: any) {
    const samples = Array.isArray(item?.sample_rows) ? item.sample_rows : [];
    const labels = samples.map((sample: any) => this.mappingSampleLabel(sample)).filter(Boolean);
    if (!labels.length) {
      return '';
    }

    const extraCount = Math.max(0, Number(item?.row_count ?? 0) - labels.length);
    const extraLabel =
      extraCount > 0 ? `; y ${extraCount} fila${extraCount === 1 ? '' : 's'} mas` : '';
    return `Cursos detectados: ${labels.join('; ')}${extraLabel}.`;
  }

  mappingAffectedContextLabel(item: any) {
    const samples = Array.isArray(item?.sample_rows) ? item.sample_rows : [];
    const labels = samples.map((sample: any) => this.mappingContextLabel(sample)).filter(Boolean);
    if (!labels.length) {
      return '';
    }

    const extraCount = Math.max(0, Number(item?.row_count ?? 0) - labels.length);
    const extraLabel =
      extraCount > 0 ? `; y ${extraCount} fila${extraCount === 1 ? '' : 's'} mas` : '';
    return `${labels.join('; ')}${extraLabel}.`;
  }

  rowPreviewContextLabel(item: any) {
    return this.mappingContextLabel(item);
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

  get allPendingMappingsAlreadySaved() {
    const items = Array.isArray(this.batch?.unresolved_mappings) ? this.batch.unresolved_mappings : [];
    return (
      items.length > 0 &&
      items.every((item: any) => Boolean(this.findExistingAlias(item?.namespace, item?.source_value)))
    );
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
    if (this.executing || this.batch?.status === 'EXECUTING') {
      return this.isAkademicSourceSelected ? 'Insertando...' : 'Ejecutando...';
    }
    if (this.batch?.status === 'EXECUTED') {
      return this.isAkademicSourceSelected ? 'Tablas actualizadas' : 'Carga aplicada';
    }
    return this.isAkademicSourceSelected ? 'Insertar en tablas' : 'Ejecutar carga';
  }

  get primaryAkademicActionLabel() {
    if (this.isPreviewProcessing || this.executing || this.isExecutionProcessing) {
      return this.isAkademicSourceSelected ? 'Generando preview...' : 'Procesando preview...';
    }
    return this.isAkademicSourceSelected ? 'Generar preview desde Akademic' : 'Generar revision previa desde Excel';
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
      `${Number(report.created_subsection_count ?? 0)} grupos`,
      `${Number(report.created_schedule_count ?? 0)} horarios`,
    ].join(' | ');
  }

  get executeHelperText() {
    if (this.isAkademicSourceSelected) {
      if (!this.hasBatch) {
        return 'Primero genera el preview desde Akademic. Despues revisa el resultado y decide cuando insertar en tablas.';
      }
      const status = String(this.batch?.status ?? '').trim();
      if (status === 'PREVIEW_PROCESSING') {
        return 'Espera a que termine el preview antes de insertar en tablas.';
      }
      if (status === 'PREVIEW_FAILED') {
        return this.batch?.error_message || 'La sincronizacion fallo durante el analisis previo.';
      }
      if (status === 'EXECUTING') {
        return 'La insercion en tablas esta en proceso. La pantalla seguira el avance automaticamente.';
      }
      if (status === 'FAILED') {
        return (
          this.batch?.error_message ||
          this.batch?.report?.error_message ||
          'La ultima sincronizacion fallo. Revisa el detalle y vuelve a intentarlo.'
        );
      }
      if (status === 'EXECUTED') {
        return this.executionReportSummary || 'La sincronizacion ya se aplico en la plataforma.';
      }
      if (this.importableRowCount <= 0) {
        return 'No se encontraron filas validas para insertar en tablas en este alcance.';
      }
      if (this.hasPendingScopeDecisions) {
        return 'Primero decide que hacer con los grupos que ya tienen informacion guardada.';
      }
      if (this.hasPendingMappings) {
        if (this.allPendingMappingsAlreadySaved) {
          return 'Los mapeos ya estan guardados, pero este preview aun no se recalculo. Genera nuevamente el preview para habilitar la insercion.';
        }
        return 'Primero confirma los mapeos sugeridos que aparecen en la seccion de pendientes.';
      }
      if (this.blockedRowCount > 0) {
        return `Al insertar en tablas entraran ${this.importableRowCount} filas validas y se omitiran ${this.blockedRowCount} bloqueadas.`;
      }
      if (this.warningRowCount > 0) {
        return `Al insertar en tablas entraran ${this.importableRowCount} filas. ${this.warningRowCount} quedaran con observaciones.`;
      }
      return `Listo para insertar ${this.importableRowCount} filas validas en tablas, reemplazando la informacion anterior del alcance detectado.`;
    }
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
    if (status === 'EXECUTING') {
      return 'La carga se esta ejecutando. Sigue el avance en la ventana de progreso.';
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
      if (this.allPendingMappingsAlreadySaved) {
        return 'Los mapeos ya estan guardados, pero este preview aun no se recalculo. Genera nuevamente el preview para habilitar la carga.';
      }
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

  get effectiveChangeMap() {
    return this.batch?.summary?.effective_change_map ?? this.batch?.summary?.change_map ?? null;
  }

  get syncChangeMapCards() {
    const changeMap = this.effectiveChangeMap;
    if (!changeMap) {
      return [];
    }
    return [
      {
        title: 'Ofertas',
        value: this.changeTripletLabel(
          Number(changeMap?.offers_created ?? 0),
          Number(changeMap?.offers_replaced ?? 0),
          Number(changeMap?.offers_deleted ?? 0),
          'creadas',
          'reemplazadas',
          'eliminadas',
        ),
      },
      {
        title: 'Secciones',
        value: this.changeTripletLabel(
          Number(changeMap?.sections_created ?? 0),
          Number(changeMap?.sections_updated ?? 0),
          Number(changeMap?.sections_deleted ?? 0),
          'creadas',
          'actualizadas',
          'eliminadas',
        ),
      },
      {
        title: 'Grupos',
        value: this.changeTripletLabel(
          Number(changeMap?.groups_created ?? 0),
          Number(changeMap?.groups_updated ?? 0),
          Number(changeMap?.groups_deleted ?? 0),
          'creados',
          'actualizados',
          'eliminados',
        ),
      },
      {
        title: 'Horarios',
        value: this.changeTripletLabel(
          Number(changeMap?.schedules_created ?? 0),
          Number(changeMap?.schedules_updated ?? 0),
          Number(changeMap?.schedules_deleted ?? 0),
          'creados',
          'actualizados',
          'eliminados',
        ),
      },
    ];
  }

  get syncScopesWithChanges() {
    const scopes = Array.isArray(this.batch?.scope_decisions) ? this.batch.scope_decisions : [];
    return scopes.filter((item: any) => this.scopeHasAnyChange(item?.change_map));
  }

  get syncDiffSummaryText() {
    const scopes = this.syncScopesWithChanges.length;
    const changeMap = this.effectiveChangeMap ?? {};
    return [
      scopes > 0 ? `${scopes} grupos detectados con cambios` : 'Sin cambios detectados',
      `${Number(changeMap?.offers_created ?? 0) + Number(changeMap?.offers_replaced ?? 0) + Number(changeMap?.offers_deleted ?? 0)} movimientos en ofertas`,
      `${Number(changeMap?.sections_created ?? 0) + Number(changeMap?.sections_updated ?? 0) + Number(changeMap?.sections_deleted ?? 0)} movimientos en secciones`,
      `${Number(changeMap?.groups_created ?? 0) + Number(changeMap?.groups_updated ?? 0) + Number(changeMap?.groups_deleted ?? 0)} movimientos en grupos`,
      `${Number(changeMap?.schedules_created ?? 0) + Number(changeMap?.schedules_updated ?? 0) + Number(changeMap?.schedules_deleted ?? 0)} movimientos en horarios`,
    ].join(' | ');
  }

  toggleSyncDiffDetail() {
    this.showSyncDiffDetail = !this.showSyncDiffDetail;
  }

  namespaceLabel(namespace: string) {
    return this.namespaceLabels[String(namespace ?? '').trim()] ?? this.humanizeCode(namespace);
  }

  countsLabel(counts: any) {
    return [
      `${Number(counts?.plan_rules ?? 0)} planes`,
      `${Number(counts?.offers ?? 0)} ofertas`,
      `${Number(counts?.sections ?? 0)} secciones`,
      `${Number(counts?.subsections ?? 0)} grupos`,
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
      `${existing.subsection_count ?? 0} grupos`,
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
      `${existing.subsection_count ?? 0} grupos`,
      `${existing.schedule_count ?? 0} horarios`,
    ].join(' | ');
  }

  scopeChangeLabel(scopeDecision: any, entity: 'offers' | 'sections' | 'groups' | 'schedules') {
    const changeMap = scopeDecision?.change_map ?? {};
    switch (entity) {
      case 'offers':
        return this.changeTripletLabel(
          Number(changeMap?.offers_created ?? 0),
          Number(changeMap?.offers_replaced ?? 0),
          Number(changeMap?.offers_deleted ?? 0),
          'creadas',
          'reemplazadas',
          'eliminadas',
        );
      case 'sections':
        return this.changeTripletLabel(
          Number(changeMap?.sections_created ?? 0),
          Number(changeMap?.sections_updated ?? 0),
          Number(changeMap?.sections_deleted ?? 0),
          'creadas',
          'actualizadas',
          'eliminadas',
        );
      case 'groups':
        return this.changeTripletLabel(
          Number(changeMap?.groups_created ?? 0),
          Number(changeMap?.groups_updated ?? 0),
          Number(changeMap?.groups_deleted ?? 0),
          'creados',
          'actualizados',
          'eliminados',
        );
      case 'schedules':
        return this.changeTripletLabel(
          Number(changeMap?.schedules_created ?? 0),
          Number(changeMap?.schedules_updated ?? 0),
          Number(changeMap?.schedules_deleted ?? 0),
          'creados',
          'actualizados',
          'eliminados',
        );
      default:
        return 'Sin cambios';
    }
  }

  scopeChangeExamples(scopeDecision: any, entity: 'offers' | 'sections' | 'groups' | 'schedules') {
    const changeMap = scopeDecision?.change_map ?? {};
    const prefix = entity;
    return {
      created: this.exampleListWithOverflow(
        changeMap?.[`${prefix}_created_items`] ?? [],
        Number(changeMap?.[`${prefix}_created`] ?? 0),
      ),
      updated: this.exampleListWithOverflow(
        changeMap?.[`${prefix === 'offers' ? 'offers_replaced_items' : `${prefix}_updated_items`}`] ?? [],
        Number(changeMap?.[`${prefix === 'offers' ? 'offers_replaced' : `${prefix}_updated`}`] ?? 0),
      ),
      deleted: this.exampleListWithOverflow(
        changeMap?.[`${prefix}_deleted_items`] ?? [],
        Number(changeMap?.[`${prefix}_deleted`] ?? 0),
      ),
    };
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
    if (result?.created_at) {
      const createdAtMs = Date.parse(String(result.created_at));
      this.previewStartedAtMs = Number.isFinite(createdAtMs) ? createdAtMs : this.previewStartedAtMs;
    }
    this.syncAkademicFormFromBatch(result);
    this.syncUploadProgressFromBatch(result);
    this.decisionDraftByScopeKey = {};
    for (const item of result?.scope_decisions ?? []) {
      this.decisionDraftByScopeKey[item.scope_key] = item.decision;
    }
    if (this.isBatchProcessingStatus(result?.status)) {
      if (result?.status === 'PREVIEW_PROCESSING') {
        this.error = '';
      }
      this.startBatchPolling(result.id);
    } else {
      this.stopBatchPolling();
    }
    this.syncPreviewDurationTimer();
    if (previousStatus === 'PREVIEW_PROCESSING' && result?.status === 'PREVIEW_READY') {
      this.error = '';
      this.completeUploadProgress();
      this.message = this.isAkademicSourceSelected
        ? 'Preview listo. Revisa el resultado y luego usa el boton para insertar en tablas.'
        : 'Revision previa generada correctamente.';
    }
    if (previousStatus === 'PREVIEW_PROCESSING' && result?.status === 'PREVIEW_FAILED') {
      this.message = '';
      this.error = result?.error_message ?? (
        this.isAkademicSourceSelected
          ? 'No se pudo preparar la sincronizacion desde Akademic.'
          : 'No se pudo generar el preview del archivo.'
      );
    }
    if (previousStatus === 'EXECUTING' && result?.status === 'EXECUTED') {
      this.error = '';
      this.message = this.executionReportSummary
        ? `${this.isAkademicSourceSelected ? 'Sincronizacion' : 'Carga masiva'} ejecutada correctamente. ${this.executionReportSummary}.`
        : `${this.isAkademicSourceSelected ? 'Sincronizacion' : 'Carga masiva'} ejecutada correctamente.`;
    }
    if (previousStatus === 'EXECUTING' && result?.status === 'FAILED') {
      this.message = '';
      this.error =
        result?.error_message ??
        result?.report?.error_message ??
        'La carga masiva fallo antes de completar el proceso.';
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

  private exampleListWithOverflow(values: any[], total: number) {
    const items = Array.isArray(values)
      ? values.map((item) => `${item ?? ''}`.trim()).filter(Boolean)
      : [];
    const overflow = Math.max(0, total - items.length);
    if (!items.length && overflow <= 0) {
      return '';
    }
    return overflow > 0 ? `${items.join(' | ')} | y ${overflow} mas` : items.join(' | ');
  }

  private startUploadProgress() {
    this.uploadProgress = 5;
    this.uploadStageLabel =
      this.isAkademicSourceSelected
        ? 'Preparando preview desde Akademic...'
        : 'Registrando archivo para generar el preview...';
    this.syncPreviewDurationTimer();
  }

  private completeUploadProgress() {
    this.uploadProgress = 100;
    this.uploadStageLabel = this.isAkademicSourceSelected
      ? 'Preview listo. Esperando confirmacion para insertar en tablas.'
      : 'Revision previa lista.';
  }

  private startBatchPolling(batchId: string) {
    if (this.batchPollingTimer && this.batchPollingId === batchId) {
      return;
    }
    this.stopBatchPolling();
    this.batchPollingId = batchId;
    this.batchPollingTimer = setInterval(() => {
      this.loadBatch(batchId, true);
    }, 800);
  }

  private stopBatchPolling() {
    if (this.batchPollingTimer) {
      clearInterval(this.batchPollingTimer);
      this.batchPollingTimer = null;
    }
    this.batchPollingId = '';
  }

  private syncPreviewDurationTimer() {
    const shouldRun = this.isPreviewProcessing || this.isExecutionProcessing || this.executing;
    if (shouldRun && !this.previewDurationTimer) {
      this.previewDurationTimer = setInterval(() => {
        this.cdr.detectChanges();
      }, 1000);
      return;
    }
    if (!shouldRun) {
      this.stopPreviewDurationTimer();
    }
  }

  private stopPreviewDurationTimer() {
    if (this.previewDurationTimer) {
      clearInterval(this.previewDurationTimer);
      this.previewDurationTimer = null;
    }
  }

  private syncUploadProgressFromBatch(result: any) {
    const progress = result?.progress ?? null;
    if (!progress) {
      return;
    }
    this.uploadProgress = Number(progress.percent ?? 0);
    this.uploadStageLabel =
      String(progress.message ?? '').trim() || (
        this.isAkademicSourceSelected ? 'Procesando preview desde Akademic...' : 'Procesando preview...'
      );
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

  private previewExcelImportRequest() {
    if (!this.selectedFile) {
      this.error = 'Selecciona un archivo Excel antes de generar el preview.';
      return null;
    }
    return this.api.previewPlanningImport(this.selectedFile);
  }

  private previewAkademicImportRequest() {
    if (!this.akademicForm.semester_id) {
      this.error = 'Selecciona el semestre de Akademic antes de sincronizar.';
      return null;
    }
    return this.api.previewPlanningAkademicImport({
      semester_id: this.akademicForm.semester_id,
    });
  }

  private previewDurationStartMs() {
    const createdAt = Date.parse(String(this.batch?.created_at ?? ''));
    if (Number.isFinite(createdAt)) {
      return createdAt;
    }
    return this.previewStartedAtMs;
  }

  private previewDurationEndMs() {
    if (this.isPreviewProcessing) {
      return Date.now();
    }
    const progressUpdatedAt = Date.parse(String(this.batch?.progress?.updated_at ?? ''));
    if (Number.isFinite(progressUpdatedAt)) {
      return progressUpdatedAt;
    }
    const updatedAt = Date.parse(String(this.batch?.updated_at ?? ''));
    if (Number.isFinite(updatedAt)) {
      return updatedAt;
    }
    return this.previewStartedAtMs;
  }

  private executionDurationStartMs() {
    const startedAt = Date.parse(String(this.batch?.report?.started_at ?? ''));
    if (Number.isFinite(startedAt)) {
      return startedAt;
    }
    const createdAt = Date.parse(String(this.batch?.created_at ?? ''));
    if (Number.isFinite(createdAt)) {
      return createdAt;
    }
    return null;
  }

  private executionDurationEndMs() {
    if (this.isExecutionProcessing || this.executing) {
      return Date.now();
    }
    const finishedAt = Date.parse(String(this.batch?.report?.finished_at ?? ''));
    if (Number.isFinite(finishedAt)) {
      return finishedAt;
    }
    const executedAt = Date.parse(String(this.batch?.executed_at ?? ''));
    if (Number.isFinite(executedAt)) {
      return executedAt;
    }
    const updatedAt = Date.parse(String(this.batch?.updated_at ?? ''));
    if (Number.isFinite(updatedAt)) {
      return updatedAt;
    }
    return this.executionDurationStartMs();
  }

  private executionProgressUpdatedAtMs() {
    const updatedAt = Date.parse(String(this.batch?.report?.updated_at ?? ''));
    return Number.isFinite(updatedAt) ? updatedAt : null;
  }

  private isBatchProcessingStatus(status: unknown) {
    const currentStatus = String(status ?? '').trim();
    return currentStatus === 'PREVIEW_PROCESSING' || currentStatus === 'EXECUTING';
  }

  private formatDuration(durationMs: number) {
    const totalSeconds = Math.max(0, Math.round(durationMs / 1000));
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (hours > 0) {
      return `${hours} h ${String(minutes).padStart(2, '0')} min`;
    }
    if (minutes > 0) {
      return `${minutes} min ${String(seconds).padStart(2, '0')} s`;
    }
    return `${seconds} s`;
  }

  private loadImportCatalog() {
    this.importCatalogLoading = true;
    this.api
      .getPlanningCatalogFilters()
      .pipe(finalize(() => (this.importCatalogLoading = false)))
      .subscribe({
        next: (catalog) => {
          const catalogSemesters = Array.isArray((catalog as any)?.semesters) ? (catalog as any).semesters : [];
          this.importCatalog = {
            ...this.importCatalog,
            ...(catalog ?? {}),
            semesters: this.mergeSemesterOptions(catalogSemesters, this.importCatalog.semesters),
          };
          this.ensureAkademicSemesterSelection();
          this.ensureCompareSemesterSelection();
          this.cdr.detectChanges();
        },
        error: () => {
          if (!(this.importCatalog.semesters || []).length) {
            this.error = 'No se pudo cargar el catalogo para importar desde Akademic.';
          }
          this.cdr.detectChanges();
        },
      });
  }

  private mergeSemesterOptions(primary: any[], fallback: any[]) {
    const merged = [...primary, ...fallback];
    const byId = new Map<string, any>();
    for (const item of merged) {
      const id = String(item?.id ?? '').trim();
      if (!id) {
        continue;
      }
      byId.set(id, item);
    }
    return [...byId.values()].sort((left: any, right: any) =>
      this.normalizePeriodToken(right?.name).localeCompare(this.normalizePeriodToken(left?.name)),
    );
  }

  private loadSemesterOptions() {
    this.semesterOptionsLoading = true;
    this.semesterOptionsLoaded = false;
    this.api.listSemesters().subscribe({
      next: (semesters) => {
        this.importCatalog = {
          ...this.importCatalog,
          semesters: this.mergeSemesterOptions(semesters, this.importCatalog.semesters),
        };
        this.semesterOptionsLoading = false;
        this.semesterOptionsLoaded = true;
        this.ensureAkademicSemesterSelection();
        this.ensureCompareSemesterSelection();
        this.cdr.detectChanges();
      },
      error: () => {
        this.semesterOptionsLoading = false;
        this.semesterOptionsLoaded = true;
        if (!this.importCatalogLoading && !(this.importCatalog.semesters || []).length) {
          this.error = 'No se pudo cargar la lista de semestres.';
        }
        this.cdr.detectChanges();
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

  private syncAkademicFormFromBatch(batch: any) {
    const semesterId = String(batch?.source_scope?.semester_id ?? '').trim();
    if (!semesterId) {
      this.ensureAkademicSemesterSelection();
      return;
    }
    this.akademicForm.semester_id = semesterId;
  }

  private ensureAkademicSemesterSelection() {
    const semesters = Array.isArray(this.importCatalog.semesters) ? this.importCatalog.semesters : [];
    if (!semesters.length) {
      this.akademicForm.semester_id = '';
      return;
    }
    const selectedSemesterId = String(this.akademicForm.semester_id ?? '').trim();
    if (selectedSemesterId && semesters.some((item: any) => item.id === selectedSemesterId)) {
      return;
    }
    this.akademicForm.semester_id = String(semesters[0]?.id ?? '').trim();
  }

  private ensureCompareSemesterSelection() {
    const semesters = Array.isArray(this.importCatalog.semesters) ? this.importCatalog.semesters : [];
    if (!semesters.length) {
      this.compareSemesterId = '';
      return;
    }
    const selectedSemesterId = String(this.compareSemesterId ?? '').trim();
    if (selectedSemesterId && semesters.some((item: any) => item.id === selectedSemesterId)) {
      return;
    }
    this.compareSemesterId = String(this.akademicForm.semester_id || semesters[0]?.id || '').trim();
  }

  private fileNameFromDisposition(contentDisposition: string | null) {
    const value = `${contentDisposition ?? ''}`;
    const match = value.match(/filename=\"?([^\";]+)\"?/i);
    return match?.[1] ?? '';
  }

  private resolveSemesterName(value: unknown) {
    const semesterId = String(value ?? '').trim();
    if (!semesterId) {
      return '';
    }
    const semesters = Array.isArray(this.importCatalog.semesters) ? this.importCatalog.semesters : [];
    const semester = semesters.find((item: any) => String(item?.id ?? '').trim() === semesterId);
    return String(semester?.name ?? '').trim();
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

  private mappingDependentSourceValues(
    item: any,
    key: 'dependent_campuses',
  ) {
    const values = (Array.isArray(item?.[key]) ? item[key] : [])
      .map((entry: any) => String(entry?.source_value ?? entry?.target_label ?? '').trim())
      .filter(Boolean);
    return [...new Set(values)];
  }

  private normalizeLoose(value: unknown) {
    return String(value ?? '')
      .normalize('NFD')
      .replace(/\p{Diacritic}/gu, '')
      .toLowerCase()
      .trim();
  }

  private mappingSampleLabel(sample: any) {
    const courseCode = String(sample?.course_code ?? '').trim();
    const courseName = String(sample?.course_name ?? '').trim();
    const section = String(sample?.section ?? '').trim();
    const rowNumber = Number(sample?.row_number ?? 0);

    const courseLabel = [courseCode, courseName].filter(Boolean).join(' - ');
    const context = [
      rowNumber > 0 ? `#${rowNumber}` : '',
      section ? `seccion ${section}` : '',
    ]
      .filter(Boolean)
      .join(' | ');

    return [courseLabel || 'Curso sin identificar', context].filter(Boolean).join(' | ');
  }

  private mappingContextLabel(item: any) {
    const campus = String(item?.campus ?? '').trim();
    const academicProgram = String(item?.academic_program ?? '').trim();
    const cycle = Number(item?.cycle ?? 0);

    return [
      campus || '',
      academicProgram || '',
      Number.isFinite(cycle) && cycle > 0 ? `Ciclo ${cycle}` : '',
    ]
      .filter(Boolean)
      .join(' | ');
  }

  private normalizePeriodToken(value: unknown) {
    const normalized = String(value ?? '').trim().toUpperCase().replace(/\s+/g, '');
    const match = normalized.match(/\d{4}-\d/);
    return match ? match[0] : normalized;
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
        title: 'El mismo grupo tiene mas de un horario',
        description: 'Conviene revisar esa fila antes de crear horarios duplicados para un solo grupo.',
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

  private scopeHasAnyChange(changeMap: any) {
    if (!changeMap || typeof changeMap !== 'object') {
      return false;
    }
    return [
      'offers_created',
      'offers_replaced',
      'offers_deleted',
      'sections_created',
      'sections_updated',
      'sections_deleted',
      'groups_created',
      'groups_updated',
      'groups_deleted',
      'schedules_created',
      'schedules_updated',
      'schedules_deleted',
    ].some((key) => Number(changeMap?.[key] ?? 0) > 0);
  }

  private changeTripletLabel(
    created: number,
    updated: number,
    deleted: number,
    createdLabel: string,
    updatedLabel: string,
    deletedLabel: string,
  ) {
    return [
      `${created} ${createdLabel}`,
      `${updated} ${updatedLabel}`,
      `${deleted} ${deletedLabel}`,
    ].join(' | ');
  }
}
