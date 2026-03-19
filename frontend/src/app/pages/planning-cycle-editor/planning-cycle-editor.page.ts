import { CommonModule, NgClass } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { finalize, forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';

type PlanningEditorFilters = {
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
  cycle: string;
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
  selector: 'app-planning-cycle-editor-page',
  standalone: true,
  imports: [CommonModule, FormsModule, NgClass],
  templateUrl: './planning-cycle-editor.page.html',
  styleUrl: './planning-cycle-editor.page.css',
})
export class PlanningCycleEditorPageComponent implements OnInit {
  loading = true;
  saving = false;
  message = '';
  error = '';
  showRules = true;
  workflowDialog = {
    open: false,
    action: 'SUBMIT_REVIEW' as WorkflowActionType,
    comment: '',
  };
  timelineDialog = {
    open: false,
    loading: false,
    entries: [] as WorkflowTimelineEntry[],
    error: '',
  };

  filters: PlanningEditorFilters = {
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
    cycle: '',
  };

  catalog: any = {
    semesters: [],
    campuses: [],
    faculties: [],
    academic_programs: [],
    cycles: [],
    study_plan_cycles: [],
    study_plans: [],
    study_types: [],
    course_modalities: [],
  };

  teachers: any[] = [];
  classrooms: any[] = [];
  buildings: any[] = [];

  planRules: any[] = [];
  candidates: any[] = [];
  currentPlanRule: any = null;
  currentStudyPlan: any = null;

  selectedOffer: any = null;
  selectedSectionId = '';
  selectedSubsectionId = '';

  ruleForm = {
    study_plan_id: '',
  };

  sectionForm = {
    code: 'A',
    teacher_id: '',
    course_modality_id: '',
    has_subsections: true,
  };

  subsectionForm = {
    kind: 'THEORY',
    responsible_teacher_id: '',
    building_id: '',
    classroom_id: '',
    shift: '',
    denomination: '',
  };

  scheduleForm = {
    day_of_week: 'LUNES',
    start_time: '07:00',
    end_time: '07:50',
  };

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
    readonly auth: AuthService,
  ) {}

  ngOnInit(): void {
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

  get availableStudyPlansByProgram() {
    if (!this.filters.academic_program_id) {
      return [];
    }
    return this.catalog.study_plans.filter((item: any) => {
      if (this.filters.faculty_id && item.faculty_id !== this.filters.faculty_id) {
        return false;
      }
      return item.academic_program_id === this.filters.academic_program_id;
    });
  }

  get availableCycles() {
    if (!this.filters.academic_program_id) {
      return [];
    }
    const allowedStudyPlanIds = new Set(
      this.availableStudyPlansByProgram.map((item: any) => item.id),
    );
    const uniqueCycles = new Map<string, any>();
    for (const cycle of this.catalog.study_plan_cycles ?? []) {
      if (!allowedStudyPlanIds.has(cycle.study_plan_id)) {
        continue;
      }
      const key = String(cycle.value);
      if (!uniqueCycles.has(key)) {
        uniqueCycles.set(key, cycle);
      }
    }
    // Filter out cycles that already have a plan rule configured
    // but keep the currently selected cycle visible (for viewing existing configs)
    const configuredCycleValues = new Set(
      this.planRules
        .filter((rule: any) => {
          const matchesSemester = !this.filters.semester_id || rule.semester_id === this.filters.semester_id;
          const matchesCampus = !this.filters.campus_id || rule.campus_id === this.filters.campus_id;
          const matchesProgram = !this.filters.academic_program_id || rule.academic_program_id === this.filters.academic_program_id;
          return matchesSemester && matchesCampus && matchesProgram;
        })
        .map((rule: any) => String(rule.cycle)),
    );
    return [...uniqueCycles.values()]
      .filter((cycle) => {
        const val = String(cycle.value);
        // Keep currently selected cycle visible even if already configured
        if (this.filters.cycle && val === this.filters.cycle) {
          return true;
        }
        return !configuredCycleValues.has(val);
      })
      .sort((a, b) => Number(a.value) - Number(b.value));
  }

  get availableStudyPlans() {
    if (!this.filters.cycle) {
      return [];
    }
    const cycleStudyPlanIds = new Set(
      (this.catalog.study_plan_cycles ?? [])
        .filter((item: any) => String(item.value) === this.filters.cycle)
        .map((item: any) => item.study_plan_id),
    );
    return this.availableStudyPlansByProgram.filter((item: any) => cycleStudyPlanIds.has(item.id));
  }

  get hasRequiredContext() {
    return Boolean(
      this.filters.semester_id &&
        this.filters.campus_id &&
        this.filters.academic_program_id &&
        this.filters.cycle,
    );
  }

  get canSelectProgram() {
    return Boolean(this.filters.faculty_id);
  }

  get canSelectCycle() {
    return Boolean(this.filters.academic_program_id);
  }

  get canSelectStudyPlan() {
    return this.hasRequiredContext && this.availableStudyPlans.length > 0;
  }

  get canManageOffers() {
    return Boolean(this.currentPlanRule?.id);
  }

  get isWorkflowReadOnly() {
    return ['IN_REVIEW', 'APPROVED'].includes(this.currentPlanRule?.workflow_status ?? '');
  }

  get canSubmitReview() {
    return (
      Boolean(this.currentPlanRule?.id) &&
      this.auth.hasPermission('action.planning.plan.submit_review') &&
      ['DRAFT', 'IN_CORRECTION'].includes(this.currentPlanRule?.workflow_status ?? 'DRAFT') &&
      this.planReviewReady
    );
  }

  get canReviewDecide() {
    return (
      Boolean(this.currentPlanRule?.id) &&
      this.auth.hasPermission('action.planning.plan.review_decide') &&
      this.currentPlanRule?.workflow_status === 'IN_REVIEW'
    );
  }

  get selectedSection() {
    return this.selectedOffer?.sections?.find((item: any) => item.id === this.selectedSectionId) ?? null;
  }

  get selectedSubsection() {
    return (
      this.selectedSection?.subsections?.find((item: any) => item.id === this.selectedSubsectionId) ??
      null
    );
  }

  get selectedProgram() {
    return (
      this.catalog.academic_programs.find((item: any) => item.id === this.filters.academic_program_id) ??
      null
    );
  }

  get selectedCycleLabel() {
    return (
      this.availableCycles.find((item: any) => String(item.value) === this.filters.cycle)?.label ??
      this.filters.cycle ??
      'Selecciona ciclo'
    );
  }

  get planReviewReady() {
    return (
      this.candidates.length > 0 &&
      this.candidates.every((candidate) => candidate?.has_offer && candidate?.review_ready)
    );
  }

  get planReviewProgressLabel() {
    const total = this.candidates.length;
    if (total === 0) {
      return 'Este plan aun no tiene cursos listos para revision.';
    }
    const ready = this.candidates.filter(
      (candidate) => candidate?.has_offer && candidate?.review_ready,
    ).length;
    if (ready === total) {
      return 'Todos los cursos del plan estan listos para revision.';
    }
    return `${ready}/${total} cursos listos. Cada curso debe tener al menos una seccion y todas sus subsecciones configuradas.`;
  }

  get visiblePlanRules() {
    if (!this.filters.cycle) {
      return this.planRules;
    }
    return this.planRules.filter(
      (item: any) => String(item.cycle) === String(this.filters.cycle),
    );
  }

  loadBootstrap() {
    this.loading = true;
    forkJoin({
      catalog: this.api.getPlanningCatalogFilters(),
      teachers: this.api.listTeachers(),
      classrooms: this.api.listClassrooms(),
      buildings: this.api.listBuildings(),
    }).subscribe({
      next: ({ catalog, teachers, classrooms, buildings }) => {
        this.catalog = catalog;
        this.teachers = teachers;
        this.classrooms = classrooms;
        this.buildings = buildings;
        this.sectionForm.course_modality_id = catalog.course_modalities?.[0]?.id ?? '';
        this.applyQueryState();
        this.loading = false;
        this.cdr.detectChanges();
        this.reloadPlanningView();
      },
      error: () => {
        this.loading = false;
        this.error = 'No se pudo cargar el editor de planificacion.';
        this.cdr.detectChanges();
      },
    });
  }

  applyQueryState() {
    const query = this.route.snapshot.queryParamMap;
    this.filters.semester_id = query.get('semester_id') ?? '';
    this.filters.campus_id = query.get('campus_id') ?? '';
    this.filters.faculty_id = query.get('faculty_id') ?? '';
    this.filters.academic_program_id = query.get('academic_program_id') ?? '';
    this.filters.cycle = query.get('cycle') ?? '';
    this.ruleForm.study_plan_id = query.get('study_plan_id') ?? '';
    this.syncAcademicProgramSelection();
    this.syncCycleSelection();
    this.syncStudyPlanSelection();
  }

  onSemesterChange() {
    this.syncRouteState();
    this.resetOfferDetail();
    this.reloadPlanningView();
  }

  onCampusChange() {
    this.syncRouteState();
    this.resetOfferDetail();
    this.reloadPlanningView();
  }

  onFacultyChange() {
    this.syncAcademicProgramSelection();
    this.filters.cycle = '';
    this.ruleForm.study_plan_id = '';
    this.syncRouteState();
    this.resetOfferDetail();
    this.reloadPlanningView();
  }

  onAcademicProgramChange() {
    this.syncCycleSelection();
    this.ruleForm.study_plan_id = '';
    this.syncRouteState();
    this.resetOfferDetail();
    this.reloadPlanningView();
  }

  onCycleChange() {
    this.syncStudyPlanSelection();
    this.syncRouteState();
    this.resetOfferDetail();
    this.loadCandidates();
  }

  onStudyPlanChange() {
    this.syncRouteState();
    this.resetOfferDetail();
    this.loadCandidates();
  }

  reloadPlanningView() {
    this.message = '';
    this.error = '';
    this.loadPlanRules();
  }

  loadPlanRules() {
    if (!this.filters.semester_id || !this.filters.academic_program_id) {
      this.planRules = [];
      this.syncCycleSelection();
      this.syncStudyPlanSelection();
      this.cdr.detectChanges();
      this.loadCandidates();
      return;
    }
    this.api
      .listPlanningPlanRules(
        this.filters.semester_id,
        this.filters.campus_id,
        this.filters.academic_program_id,
      )
      .subscribe((rows) => {
        this.planRules = rows;
        // Re-sync selections: configured cycles are now filtered out
        this.syncCycleSelection();
        this.syncStudyPlanSelection();
        this.cdr.detectChanges();
        this.loadCandidates();
      });
  }

  loadCandidates() {
    if (!this.hasRequiredContext) {
      this.currentPlanRule = null;
      this.currentStudyPlan = null;
      this.candidates = [];
      this.loading = false;
      this.cdr.detectChanges();
      return;
    }

    this.loading = true;
    this.cdr.detectChanges();
    this.api
      .listPlanningCourseCandidates({
        ...this.filters,
        study_plan_id: this.ruleForm.study_plan_id,
      })
      .subscribe({
        next: (response) => {
          this.candidates = response.candidates ?? [];
          this.currentPlanRule = response.plan_rule ?? null;
          this.currentStudyPlan = response.study_plan ?? null;
          if (!this.ruleForm.study_plan_id && response.study_plan?.id) {
            this.ruleForm.study_plan_id = response.study_plan.id;
            this.syncRouteState();
          }
          this.loading = false;
          this.cdr.detectChanges();
        },
        error: () => {
          this.currentPlanRule = null;
          this.currentStudyPlan = null;
          this.candidates = [];
          this.loading = false;
          this.error = 'No se pudieron cargar los cursos del ciclo seleccionado.';
          this.cdr.detectChanges();
        },
      });
  }

  createPlanRule() {
    if (
      this.canManageOffers ||
      !this.filters.semester_id ||
      !this.filters.campus_id ||
      !this.filters.faculty_id ||
      !this.filters.academic_program_id ||
      !this.filters.cycle ||
      !this.ruleForm.study_plan_id
    ) {
      return;
    }
    this.saving = true;
    const payload: any = {
      semester_id: this.filters.semester_id,
      campus_id: this.filters.campus_id,
      academic_program_id: this.filters.academic_program_id,
      faculty_id: this.filters.faculty_id || undefined,
      career_name: this.selectedProgram?.name ?? undefined,
      cycle: Number(this.filters.cycle),
      study_plan_id: this.ruleForm.study_plan_id,
      is_active: true,
    };

    this.api
      .createPlanningPlanRule(payload)
      .pipe(
        finalize(() => {
          this.saving = false;
        }),
      )
      .subscribe({
        next: (response) => {
          const createdOffers = Number(response?.created_offer_count ?? 0);
          const totalOffers = Number(response?.total_offer_count ?? 0);
          this.message =
            totalOffers > 0
              ? `Plan guardado. ${createdOffers} ofertas nuevas creadas de ${totalOffers} cursos del ciclo.`
              : 'Plan guardado.';
          this.cdr.detectChanges();
          this.reloadPlanningView();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo guardar la configuracion del plan.';
          this.cdr.detectChanges();
        },
      });
  }

  deletePlanRule(ruleId: string) {
    if (this.isWorkflowReadOnly) {
      return;
    }
    this.api.deletePlanningPlanRule(ruleId).subscribe({
      next: (response) => {
        const deletedOffers = Number(response?.deleted_offer_count ?? 0);
        const deletedSections = Number(response?.deleted_section_count ?? 0);
        const deletedSubsections = Number(response?.deleted_subsection_count ?? 0);
        const deletedSchedules = Number(response?.deleted_schedule_count ?? 0);
        this.message =
          deletedOffers > 0 || deletedSections > 0 || deletedSubsections > 0 || deletedSchedules > 0
            ? `Plan eliminado. Tambien se borraron ${deletedOffers} ofertas, ${deletedSections} secciones, ${deletedSubsections} subsecciones y ${deletedSchedules} horarios.`
            : 'Plan eliminado.';
        this.cdr.detectChanges();
        this.loadPlanRules();
        this.loadCandidates();
      },
      error: () => {
        this.error = 'No se pudo eliminar la regla.';
        this.cdr.detectChanges();
      },
    });
  }

  createOffer(candidate: any) {
    if (
      !this.canManageOffers ||
      this.isWorkflowReadOnly ||
      !this.filters.semester_id ||
      !this.filters.campus_id ||
      !this.filters.cycle
    ) {
      return;
    }
    this.saving = true;
    const payload: any = {
      semester_id: this.filters.semester_id,
      campus_id: this.filters.campus_id,
      faculty_id: this.filters.faculty_id || undefined,
      academic_program_id: this.filters.academic_program_id || undefined,
      study_plan_id: candidate.study_plan_id,
      cycle: Number(this.filters.cycle),
      study_plan_course_id: candidate.study_plan_course_id,
      study_type_id: this.catalog.study_types?.[0]?.id ?? undefined,
    };

    this.api
      .createPlanningOffer(payload)
      .pipe(
        finalize(() => {
          this.saving = false;
        }),
      )
      .subscribe({
        next: () => {
          this.message = 'Oferta guardada.';
          this.cdr.detectChanges();
          this.loadCandidates();
        },
        error: () => {
          this.error = 'No se pudo guardar la oferta.';
          this.cdr.detectChanges();
        },
      });
  }

  openOffer(offerId: string) {
    if (!offerId || !this.canManageOffers) {
      return;
    }
    this.router.navigate(['/planning/offers', offerId, 'sections']);
  }

  bindSelectedOffer(offer: any) {
    this.selectedOffer = offer;
    this.selectedSectionId = offer?.sections?.[0]?.id ?? '';
    this.selectedSubsectionId = offer?.sections?.[0]?.subsections?.[0]?.id ?? '';
    this.sectionForm.code = this.nextSectionCode();
    this.subsectionForm.kind = this.defaultSubsectionKind();
  }

  createSection() {
    if (!this.selectedOffer) {
      return;
    }
    this.saving = true;
    this.api
      .createPlanningSection(this.selectedOffer.id, this.sectionForm)
      .subscribe({
        next: (section) => {
          this.saving = false;
          this.message = 'Seccion creada.';
          this.cdr.detectChanges();
          this.openOffer(this.selectedOffer.id);
          this.selectedSectionId = section.id;
        },
        error: (err) => {
          this.saving = false;
          this.error = err?.error?.message ?? 'No se pudo crear la seccion.';
          this.cdr.detectChanges();
        },
      });
  }

  createSubsection() {
    if (!this.selectedSection) {
      return;
    }
    this.saving = true;
    this.api
      .createPlanningSubsection(this.selectedSection.id, this.subsectionForm)
      .subscribe({
        next: (subsection) => {
          this.saving = false;
          this.message = 'Subseccion creada.';
          this.cdr.detectChanges();
          this.openOffer(this.selectedOffer.id);
          this.selectedSubsectionId = subsection.id;
        },
        error: (err) => {
          this.saving = false;
          this.error = err?.error?.message ?? 'No se pudo crear la subseccion.';
          this.cdr.detectChanges();
        },
      });
  }

  createSchedule() {
    if (!this.selectedSubsection) {
      return;
    }
    this.saving = true;
    this.api
      .createPlanningSubsectionSchedule(this.selectedSubsection.id, this.scheduleForm)
      .subscribe({
        next: (subsection) => {
          this.saving = false;
          this.message = 'Horario agregado.';
          this.selectedOffer = {
            ...this.selectedOffer,
            sections: this.selectedOffer.sections.map((section: any) =>
              section.id === this.selectedSection.id
                ? {
                    ...section,
                    subsections: section.subsections.map((item: any) =>
                      item.id === subsection.id ? subsection : item,
                    ),
                  }
                : section,
            ),
          };
          this.cdr.detectChanges();
          this.openOffer(this.selectedOffer.id);
        },
        error: (err) => {
          this.saving = false;
          this.error = err?.error?.message ?? 'No se pudo guardar el horario.';
          this.cdr.detectChanges();
        },
      });
  }

  deleteSchedule(scheduleId: string) {
    if (!scheduleId) {
      return;
    }
    this.api.deletePlanningSubsectionSchedule(scheduleId).subscribe({
      next: () => {
        this.message = 'Horario eliminado.';
        this.cdr.detectChanges();
        this.openOffer(this.selectedOffer.id);
      },
      error: () => {
        this.error = 'No se pudo eliminar el horario.';
        this.cdr.detectChanges();
      },
    });
  }

  selectSection(sectionId: string) {
    this.selectedSectionId = sectionId;
    this.selectedSubsectionId = this.selectedSection?.subsections?.[0]?.id ?? '';
    this.subsectionForm.kind = this.defaultSubsectionKind();
  }

  selectSubsection(subsectionId: string) {
    this.selectedSubsectionId = subsectionId;
  }

  nextSectionCode() {
    const sections = this.selectedOffer?.sections ?? [];
    const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    for (const letter of alphabet.split('')) {
      if (!sections.some((item: any) => item.code === letter)) {
        return letter;
      }
    }
    return `S${sections.length + 1}`;
  }

  defaultSubsectionKind() {
    const courseType = this.selectedOffer?.course_type ?? '';
    if (courseType === 'PRACTICO') {
      return 'PRACTICE';
    }
    if (courseType === 'TEORICO') {
      return 'THEORY';
    }
    return 'MIXED';
  }

  formatCourseType(value: string | null | undefined) {
    switch (value) {
      case 'TEORICO':
        return 'Teorico';
      case 'PRACTICO':
        return 'Practico';
      case 'TEORICO_PRACTICO':
        return 'Teorico practico';
      default:
        return value || '---';
    }
  }

  formatKind(value: string | null | undefined) {
    switch (value) {
      case 'THEORY':
        return 'Teorica';
      case 'PRACTICE':
        return 'Practica';
      case 'MIXED':
        return 'Mixta';
      default:
        return value || '---';
    }
  }

  formatRuleCycle(rule: any) {
    return String(rule?.cycle ?? '---');
  }

  trackOfferStatusClass(status: string | null | undefined) {
    return {
      'status-draft': status === 'DRAFT',
      'status-active': status === 'ACTIVE',
      'status-observed': status === 'OBSERVED',
      'status-closed': status === 'CLOSED',
    };
  }

  offerStatusLabel(status: string | null | undefined) {
    switch (status) {
      case 'ACTIVE':
        return 'Configurado';
      case 'OBSERVED':
        return 'Observado';
      case 'CLOSED':
        return 'Cerrado';
      case 'DRAFT':
        return 'Borrador';
      case 'SIN_OFERTA':
        return 'Sin oferta';
      default:
        return 'Borrador';
    }
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
    return {
      'status-draft': status === 'DRAFT' || !status,
      'status-review': status === 'IN_REVIEW',
      'status-approved': status === 'APPROVED',
      'status-correction': status === 'IN_CORRECTION',
    };
  }

  openWorkflowDialog(action: WorkflowActionType) {
    this.workflowDialog = {
      open: true,
      action,
      comment: '',
    };
  }

  closeWorkflowDialog() {
    this.workflowDialog = {
      open: false,
      action: 'SUBMIT_REVIEW',
      comment: '',
    };
  }

  openTimelineDialog() {
    if (!this.currentPlanRule?.id) {
      return;
    }
    this.timelineDialog = {
      open: true,
      loading: true,
      entries: [],
      error: '',
    };
    this.api
      .listPlanningChangeLog({
        entity_type: 'planning_cycle_plan_rule',
        entity_id: this.currentPlanRule.id,
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
      entries: [],
      error: '',
    };
  }

  confirmWorkflowAction() {
    if (!this.currentPlanRule?.id) {
      return;
    }
    const comment = this.workflowDialog.comment?.trim() || undefined;
    if (this.workflowDialog.action === 'REQUEST_CORRECTION' && !comment) {
      this.error = 'Debes ingresar un comentario para mandar a correccion.';
      this.cdr.detectChanges();
      return;
    }

    this.saving = true;
    const request =
      this.workflowDialog.action === 'APPROVE'
        ? this.api.approvePlanningPlanRule(this.currentPlanRule.id, { review_comment: comment })
        : this.workflowDialog.action === 'REQUEST_CORRECTION'
          ? this.api.requestPlanningPlanRuleCorrection(this.currentPlanRule.id, { review_comment: comment })
          : this.api.submitPlanningPlanRuleReview(this.currentPlanRule.id, { review_comment: comment });

    request
      .pipe(
        finalize(() => {
          this.saving = false;
        }),
      )
      .subscribe({
        next: () => {
          this.closeWorkflowDialog();
          this.message = 'Workflow del plan actualizado.';
          this.error = '';
          this.reloadPlanningView();
          this.cdr.detectChanges();
        },
        error: (err) => {
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
        return 'El plan quedara cerrado y solo se mostrara en modo lectura.';
      case 'REQUEST_CORRECTION':
        return 'El comentario sera visible y el plan volvera a edicion.';
      default:
        return 'El plan se bloqueara para edicion mientras este en revision.';
    }
  }

  goBackToSummary() {
    this.router.navigate(['/planning'], {
      queryParams: this.summaryQueryParams(),
    });
  }

  timelineTargetLabel() {
    if (!this.currentPlanRule) {
      return '';
    }
    return `${this.selectedProgram?.name || this.currentPlanRule?.career_name || 'Programa'} · Ciclo ${
      this.currentPlanRule?.cycle
    } · ${this.currentPlanRule?.study_plan?.name || this.currentPlanRule?.study_plan_id}`;
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

  private syncAcademicProgramSelection() {
    if (
      this.filters.academic_program_id &&
      !this.filteredPrograms.some((item: any) => item.id === this.filters.academic_program_id)
    ) {
      this.filters.academic_program_id = '';
    }
  }

  private syncCycleSelection() {
    if (
      this.filters.cycle &&
      !this.availableCycles.some((item: any) => String(item.value) === this.filters.cycle)
    ) {
      this.filters.cycle = '';
    }
  }

  private syncStudyPlanSelection() {
    if (
      this.ruleForm.study_plan_id &&
      !this.availableStudyPlans.some((item: any) => item.id === this.ruleForm.study_plan_id)
    ) {
      this.ruleForm.study_plan_id = '';
    }
  }

  syncRouteState() {
    this.router.navigate([], {
      relativeTo: this.route,
      replaceUrl: true,
      queryParams: {
        semester_id: this.filters.semester_id || null,
        campus_id: this.filters.campus_id || null,
        faculty_id: this.filters.faculty_id || null,
        academic_program_id: this.filters.academic_program_id || null,
        cycle: this.filters.cycle || null,
        study_plan_id: this.ruleForm.study_plan_id || null,
      },
    });
  }

  private summaryQueryParams() {
    return {
      semester_id: this.filters.semester_id || null,
      campus_id: this.filters.campus_id || null,
      faculty_id: this.filters.faculty_id || null,
      academic_program_id: this.filters.academic_program_id || null,
    };
  }

  private resetOfferDetail() {
    this.selectedOffer = null;
    this.selectedSectionId = '';
    this.selectedSubsectionId = '';
  }
}
