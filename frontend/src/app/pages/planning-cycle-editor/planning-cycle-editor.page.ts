import { CommonModule, NgClass } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { finalize, forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';

type PlanningEditorFilters = {
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
  cycle: string;
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
    this.api.deletePlanningPlanRule(ruleId).subscribe({
      next: () => {
        this.message = 'Regla eliminada.';
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

  goBackToSummary() {
    this.router.navigate(['/planning']);
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

  private resetOfferDetail() {
    this.selectedOffer = null;
    this.selectedSectionId = '';
    this.selectedSubsectionId = '';
  }
}
