import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';

type PlanningSummaryFilters = {
  vc_period_id: string;
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
};

type WorkflowActionType = 'SUBMIT_REVIEW' | 'APPROVE' | 'REQUEST_CORRECTION';

type WorkflowTimelineEntry = {
  id: string;
  title: string;
  status: string;
  actor: string;
  changed_at: string;
  comment: string | null;
};

@Component({
  selector: 'app-planning-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './planning.page.html',
  styleUrl: './planning.page.css',
})
export class PlanningPageComponent implements OnInit {
  private readonly filtersStorageKey = 'planning.summary.filters';

  loading = true;
  error = '';

  filters: PlanningSummaryFilters = {
    vc_period_id: '',
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
  };

  catalog: any = {
    vc_periods: [],
    semesters: [],
    campuses: [],
    faculties: [],
    academic_programs: [],
  };

  rows: any[] = [];
  workflowDialog = {
    open: false,
    action: 'SUBMIT_REVIEW' as WorkflowActionType,
    row: null as any,
    rows: [] as any[],
    comment: '',
  };
  timelineDialog = {
    open: false,
    loading: false,
    row: null as any,
    entries: [] as WorkflowTimelineEntry[],
    error: '',
  };

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
    readonly auth: AuthService,
  ) {}

  ngOnInit(): void {
    this.restoreFilters();
    this.loadBootstrap();
  }

  get filteredPrograms() {
    if (!this.filters.faculty_id) {
      return this.catalog.academic_programs;
    }
    return this.catalog.academic_programs.filter(
      (item: any) => item.faculty_id === this.filters.faculty_id,
    );
  }

  get semesterOptions() {
    return Array.isArray(this.catalog.semesters) ? this.catalog.semesters : [];
  }

  loadBootstrap() {
    this.loading = true;
    this.api.getPlanningCatalogFilters().subscribe({
      next: (catalog) => {
        this.catalog = catalog;
        if (this.filters.campus_id && !catalog.campuses?.some((item: any) => item.id === this.filters.campus_id)) {
          this.filters.campus_id = '';
        }
        if (this.filters.faculty_id && !catalog.faculties?.some((item: any) => item.id === this.filters.faculty_id)) {
          this.filters.faculty_id = '';
        }
        if (
          this.filters.academic_program_id &&
          !catalog.academic_programs?.some((item: any) => item.id === this.filters.academic_program_id)
        ) {
          this.filters.academic_program_id = '';
        }
        this.syncPeriodFiltersFromCatalog();
        this.syncProgramSelection();
        this.persistFilters();
        this.cdr.detectChanges();
        this.loadRows();
      },
      error: () => {
        this.loading = false;
        this.error = 'No se pudo cargar el resumen de planificacion.';
        this.cdr.detectChanges();
      },
    });
  }

  onFacultyChange() {
    this.syncProgramSelection();
    this.persistFilters();
    this.loadRows();
  }

  onSemesterChange() {
    this.syncPeriodFiltersFromCatalog();
    this.persistFilters();
    this.loadRows();
  }

  loadRows() {
    this.persistFilters();
    this.loading = true;
    this.error = '';
    this.cdr.detectChanges();
    this.api.listPlanningConfiguredCycles(this.filters).subscribe({
      next: (rows) => {
        this.rows = rows;
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: () => {
        this.loading = false;
        this.error = 'No se pudieron cargar los ciclos configurados.';
        this.cdr.detectChanges();
      },
    });
  }

  openNewOffer() {
    this.router.navigate(['/planning/cycle-editor'], {
      queryParams: this.summaryQueryParams(),
    });
  }

  openChangeLog() {
    this.router.navigate(['/planning/change-log'], {
      queryParams: this.summaryQueryParams(),
    });
  }

  openVcMatch() {
    this.router.navigate(['/planning/vc-match'], {
      queryParams: this.summaryQueryParams(),
    });
  }

  openWorkspace() {
    this.router.navigate(['/planning/workspace'], {
      queryParams: this.summaryQueryParams(),
    });
  }

  openCycleDetail(row: any) {
    this.router.navigate(['/planning/cycle-editor'], {
      queryParams: {
        ...this.summaryQueryParams(),
        vc_period_id: row.vc_period_id ?? this.filters.vc_period_id ?? '',
        semester_id: row.semester_id ?? this.filters.semester_id ?? '',
        campus_id: this.filters.campus_id || row.campus_id || row.primary_campus_id || '',
        faculty_id: row.faculty_id ?? this.filters.faculty_id ?? '',
        academic_program_id: row.academic_program_id ?? this.filters.academic_program_id ?? '',
        cycle: row.cycle ? String(row.cycle) : '',
        study_plan_id: row.study_plan_id ?? '',
      },
    });
  }

  workflowStatusLabel(status: string | null | undefined) {
    switch (status) {
      case 'IN_REVIEW':
        return 'En revision';
      case 'APPROVED':
        return 'Aprobado';
      case 'IN_CORRECTION':
        return 'En correccion';
      default:
        return 'Borrador';
    }
  }

  workflowStatusClass(status: string | null | undefined) {
    switch (status) {
      case 'IN_REVIEW':
        return 'status-review';
      case 'APPROVED':
        return 'status-approved';
      case 'IN_CORRECTION':
        return 'status-correction';
      default:
        return 'status-draft';
    }
  }

  canSubmitReview(row: any) {
    return (
      this.auth.hasPermission('action.planning.plan.submit_review') &&
      ['DRAFT', 'IN_CORRECTION'].includes(row?.workflow_status ?? 'DRAFT') &&
      Boolean(row?.review_ready)
    );
  }

  canReviewDecide(row: any) {
    return (
      this.auth.hasPermission('action.planning.plan.review_decide') &&
      row?.workflow_status === 'IN_REVIEW'
    );
  }

  canRequestCorrection(row: any) {
    return (
      this.auth.hasPermission('action.planning.plan.review_decide') &&
      ['IN_REVIEW', 'APPROVED'].includes(row?.workflow_status ?? '')
    );
  }

  get bulkSubmittableRows() {
    return this.rows.filter((row) => this.canSubmitReview(row));
  }

  get canBulkSubmitReview() {
    return this.bulkSubmittableRows.length > 0;
  }

  openWorkflowDialog(event: Event, row: any, action: WorkflowActionType) {
    event.stopPropagation();
    this.workflowDialog = {
      open: true,
      action,
      row,
      rows: [],
      comment: '',
    };
  }

  openBulkSubmitReviewDialog() {
    if (!this.canBulkSubmitReview) {
      return;
    }
    this.workflowDialog = {
      open: true,
      action: 'SUBMIT_REVIEW',
      row: null,
      rows: this.bulkSubmittableRows,
      comment: '',
    };
  }

  closeWorkflowDialog() {
    this.workflowDialog = {
      open: false,
      action: 'SUBMIT_REVIEW',
      row: null,
      rows: [],
      comment: '',
    };
  }

  openTimelineDialog(event: Event, row: any) {
    event.stopPropagation();
    if (!row?.id) {
      return;
    }
    this.timelineDialog = {
      open: true,
      loading: true,
      row,
      entries: [],
      error: '',
    };
    this.api
      .listPlanningChangeLog({
        entity_type: 'planning_cycle_plan_rule',
        entity_id: row.id,
        limit: 30,
      })
      .subscribe({
        next: (rows) => {
          this.timelineDialog = {
            ...this.timelineDialog,
            loading: false,
            entries: this.buildWorkflowTimeline(rows),
          };
          this.cdr.detectChanges();
        },
        error: (err: any) => {
          this.timelineDialog = {
            ...this.timelineDialog,
            loading: false,
            error: err?.error?.message ?? 'No se pudo cargar la linea de tiempo del plan.',
          };
          this.cdr.detectChanges();
        },
      });
  }

  closeTimelineDialog() {
    this.timelineDialog = {
      open: false,
      loading: false,
      row: null,
      entries: [],
      error: '',
    };
  }

  confirmWorkflowAction() {
    const row = this.workflowDialog.row;
    const rows = this.workflowDialog.rows;
    if (this.workflowDialog.action === 'SUBMIT_REVIEW' && rows.length > 0) {
      const comment = this.workflowDialog.comment?.trim() || undefined;
      this.loading = true;
      this.error = '';
      this.api
        .submitPlanningPlanRulesReviewBulk({
          ids: rows.map((item) => item.id),
          review_comment: comment,
        })
        .subscribe({
          next: () => {
            this.closeWorkflowDialog();
            this.loadRows();
          },
          error: (err: any) => {
            this.loading = false;
            this.error = err?.error?.message ?? 'No se pudieron enviar los planes visibles a revision.';
            this.cdr.detectChanges();
          },
        });
      return;
    }
    if (!row?.id) {
      return;
    }
    const comment = this.workflowDialog.comment?.trim() || undefined;
    if (this.workflowDialog.action === 'REQUEST_CORRECTION' && !comment) {
      this.error = 'Debes ingresar un comentario para mandar a correccion.';
      this.cdr.detectChanges();
      return;
    }

    this.loading = true;
    this.error = '';
    const request =
      this.workflowDialog.action === 'APPROVE'
        ? this.api.approvePlanningPlanRule(row.id, { review_comment: comment })
        : this.workflowDialog.action === 'REQUEST_CORRECTION'
          ? this.api.requestPlanningPlanRuleCorrection(row.id, { review_comment: comment })
          : this.api.submitPlanningPlanRuleReview(row.id, { review_comment: comment });

    request.subscribe({
      next: () => {
        this.closeWorkflowDialog();
        this.loadRows();
      },
      error: (err: any) => {
        this.loading = false;
        this.error = err?.error?.message ?? 'No se pudo actualizar el workflow del plan.';
        this.cdr.detectChanges();
      },
    });
  }

  workflowActionTitle() {
    switch (this.workflowDialog.action) {
      case 'APPROVE':
        return 'Aprobar plan';
      case 'REQUEST_CORRECTION':
        return 'Mandar a correccion';
      default:
        return 'Enviar a revision';
    }
  }

  workflowActionDescription() {
    switch (this.workflowDialog.action) {
      case 'APPROVE':
        return 'El plan quedara aprobado. Un revisor con permiso podra reabrirlo a correccion si hace falta.';
      case 'REQUEST_CORRECTION':
        return 'El comentario sera visible para que el plan vuelva a edicion.';
      default:
        return this.workflowDialog.rows.length > 0
          ? 'Todos los planes visibles y elegibles se bloquearan para edicion mientras esten en revision.'
          : 'El plan se bloqueara para edicion mientras este en revision.';
    }
  }

  workflowTargetLabel() {
    if (this.workflowDialog.rows.length > 0) {
      const cycles = this.workflowDialog.rows
        .map((row) => row?.cycle)
        .filter((value) => value !== null && value !== undefined)
        .sort((left, right) => Number(left) - Number(right))
        .join(', ');
      return `${this.workflowDialog.rows.length} planes visibles${cycles ? ` · Ciclos ${cycles}` : ''}`;
    }
    const row = this.workflowDialog.row;
    if (!row) {
      return '';
    }
    return `${row.academic_program?.name || row.career_name || 'Programa'} · Ciclo ${row.cycle} · ${
      row.study_plan?.name || row.study_plan_id
    }`;
  }

  reviewReadinessLabel(row: any) {
    const expected = Number(row?.expected_course_count ?? 0);
    const ready = Number(row?.ready_course_count ?? 0);
    if (expected <= 0) {
      return 'Sin cursos listos todavia';
    }
    if (ready >= expected) {
      return 'Todos los cursos listos';
    }
    return `${ready}/${expected} cursos listos`;
  }

  timelineTargetLabel() {
    const row = this.timelineDialog.row;
    if (!row) {
      return '';
    }
    return `${row.academic_program?.name || row.career_name || 'Programa'} · Ciclo ${row.cycle} · ${
      row.study_plan?.name || row.study_plan_id
    }`;
  }

  timelineStatusLabel(status: string | null | undefined) {
    return this.workflowStatusLabel(status);
  }

  formatTimelineDate(value: string | null | undefined) {
    if (!value) {
      return 'Sin fecha';
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return value;
    }
    return parsed.toLocaleString('es-PE', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }

  private buildWorkflowTimeline(rows: any[]): WorkflowTimelineEntry[] {
    return (Array.isArray(rows) ? rows : [])
      .filter((row) => this.isWorkflowTimelineRow(row))
      .map((row) => {
        const workflowAction = row?.context_json?.workflow_action ?? null;
        const status = row?.after_json?.workflow_status ?? row?.before_json?.workflow_status ?? 'DRAFT';
        return {
          id: row.id,
          title: this.workflowTimelineTitle(row?.action, workflowAction, status),
          status,
          actor: row?.changed_by || 'Sistema',
          changed_at: row?.changed_at,
          comment: row?.after_json?.review_comment ?? row?.before_json?.review_comment ?? null,
        };
      })
      .sort((left, right) => new Date(right.changed_at).getTime() - new Date(left.changed_at).getTime());
  }

  private isWorkflowTimelineRow(row: any) {
    if (!row) {
      return false;
    }
    if (row.action === 'CREATE') {
      return true;
    }
    if (row?.context_json?.workflow_action) {
      return true;
    }
    return (row?.changes ?? []).some((change: any) => change?.field === 'workflow_status');
  }

  private workflowTimelineTitle(
    action: string | null | undefined,
    workflowAction: string | null | undefined,
    status: string | null | undefined,
  ) {
    if (action === 'CREATE') {
      return 'Plan creado';
    }
    switch (workflowAction) {
      case 'submit_review':
        return 'Enviado a revision';
      case 'approve':
        return 'Plan aprobado';
      case 'request_correction':
        return 'Mandado a correccion';
      default:
        return `Workflow actualizado a ${this.timelineStatusLabel(status).toLowerCase()}`;
    }
  }

  private restoreFilters() {
    const query = this.route.snapshot.queryParamMap;
    const queryFilters: PlanningSummaryFilters = {
      vc_period_id: query.get('vc_period_id') ?? '',
      semester_id: query.get('semester_id') ?? '',
      campus_id: query.get('campus_id') ?? '',
      faculty_id: query.get('faculty_id') ?? '',
      academic_program_id: query.get('academic_program_id') ?? '',
    };
    if (Object.values(queryFilters).some((value) => Boolean(value))) {
      this.filters = queryFilters;
      this.persistFilters();
      return;
    }
    try {
      const raw = localStorage.getItem(this.filtersStorageKey);
      if (!raw) {
        return;
      }
      const stored = JSON.parse(raw) as Partial<PlanningSummaryFilters>;
      this.filters = {
        vc_period_id: stored.vc_period_id ?? '',
        semester_id: stored.semester_id ?? '',
        campus_id: stored.campus_id ?? '',
        faculty_id: stored.faculty_id ?? '',
        academic_program_id: stored.academic_program_id ?? '',
      };
    } catch {
      this.filters = {
        vc_period_id: '',
        semester_id: '',
        campus_id: '',
        faculty_id: '',
        academic_program_id: '',
      };
    }
  }

  private persistFilters() {
    localStorage.setItem(this.filtersStorageKey, JSON.stringify(this.summaryQueryParams()));
  }

  private syncProgramSelection() {
    if (!this.filteredPrograms.some((item: any) => item.id === this.filters.academic_program_id)) {
      this.filters.academic_program_id = '';
    }
  }

  private syncPeriodFiltersFromCatalog() {
    const periods = Array.isArray(this.catalog?.vc_periods) ? this.catalog.vc_periods : [];
    const semesters = Array.isArray(this.catalog?.semesters) ? this.catalog.semesters : [];

    if (
      this.filters.vc_period_id &&
      !periods.some((item: any) => item.id === this.filters.vc_period_id)
    ) {
      this.filters.vc_period_id = '';
    }
    if (
      this.filters.semester_id &&
      !semesters.some((item: any) => item.id === this.filters.semester_id)
    ) {
      this.filters.semester_id = '';
    }

    if (!this.filters.semester_id && this.filters.vc_period_id) {
      this.filters.semester_id = this.resolveSemesterIdFromVcPeriodId(this.filters.vc_period_id) ?? '';
    }

    if (!this.filters.semester_id && semesters.length > 0) {
      this.filters.semester_id =
        semesters.find((item: any) => item.selected)?.id ?? semesters[0].id;
    }

    if (!this.filters.semester_id) {
      this.filters.vc_period_id = '';
      return;
    }

    const vcPeriod = this.findVcPeriodBySemesterId(this.filters.semester_id);
    if (vcPeriod?.id) {
      this.filters.vc_period_id = vcPeriod.id;
      return;
    }

    if (this.vcPeriodMatchesSemester(this.filters.vc_period_id, this.filters.semester_id)) {
      return;
    }

    this.filters.vc_period_id = '';
  }

  private resolveSemesterIdFromVcPeriodId(vcPeriodId: string) {
    const periods = Array.isArray(this.catalog?.vc_periods) ? this.catalog.vc_periods : [];
    const semesters = Array.isArray(this.catalog?.semesters) ? this.catalog.semesters : [];
    const period = periods.find((item: any) => item.id === vcPeriodId);
    const token = this.normalizePeriodToken(period?.text);
    if (!token) {
      return '';
    }
    return semesters.find((item: any) => this.normalizePeriodToken(item.name) === token)?.id ?? '';
  }

  private findVcPeriodBySemesterId(semesterId: string) {
    const periods = Array.isArray(this.catalog?.vc_periods) ? this.catalog.vc_periods : [];
    const semesters = Array.isArray(this.catalog?.semesters) ? this.catalog.semesters : [];
    const semester = semesters.find((item: any) => item.id === semesterId);
    const token = this.normalizePeriodToken(semester?.name);
    if (!token) {
      return null;
    }
    return periods.find((item: any) => this.normalizePeriodToken(item.text) === token) ?? null;
  }

  private vcPeriodMatchesSemester(vcPeriodId: string, semesterId: string) {
    if (!vcPeriodId || !semesterId) {
      return false;
    }
    return this.resolveSemesterIdFromVcPeriodId(vcPeriodId) === semesterId;
  }

  private normalizePeriodToken(value: string | null | undefined) {
    const normalized = String(value ?? '').trim().toUpperCase().replace(/\s+/g, '');
    if (!normalized) {
      return '';
    }
    const match = normalized.match(/\d{4}-\d/);
    return match ? match[0] : normalized;
  }

  private summaryQueryParams() {
    return {
      vc_period_id: this.filters.vc_period_id || null,
      semester_id: this.filters.semester_id || null,
      campus_id: this.filters.campus_id || null,
      faculty_id: this.filters.faculty_id || null,
      academic_program_id: this.filters.academic_program_id || null,
    };
  }
}
