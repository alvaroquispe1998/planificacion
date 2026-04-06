import { CommonModule, NgClass } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { catchError, finalize, forkJoin, of } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { DialogService } from '../../core/dialog.service';

type CreateSectionMode = 'SINGLE' | 'MULTIPLE';

type ScheduleDraft = {
  day_of_week: string;
  start_time: string;
  end_time: string;
  session_type: string;
};

@Component({
  selector: 'app-planning-offer-sections-page',
  standalone: true,
  imports: [CommonModule, FormsModule, NgClass],
  templateUrl: './planning-offer-sections.page.html',
  styleUrl: './planning-offer-sections.page.css',
})
export class PlanningOfferSectionsPageComponent implements OnInit {
  private readonly scheduleGridStartMinutes = 7 * 60 + 40;
  private readonly scheduleGridEndMinutes = 23 * 60 + 30;

  loading = true;
  supportDataLoading = false;
  saving = false;
  error = '';
  message = '';
  showCreateModal = false;
  showCreateTeacherOptions = false;
  offerId = '';
  createTeacherQuery = '';

  offer: any = null;
  catalog: any = this.emptyCatalog();
  teachers: any[] = [];
  buildings: any[] = [];
  classrooms: any[] = [];

  sectionTeacherQueryById: Record<string, string> = {};
  sectionTeacherMenuOpenById: Record<string, boolean> = {};
  subsectionTeacherQueryById: Record<string, string> = {};
  subsectionTeacherMenuOpenById: Record<string, boolean> = {};
  scheduleDraftsBySubsectionId: Record<string, ScheduleDraft> = {};
  expandedSectionId = '';
  drawerOpen = false;
  activeSectionId = '';
  activeSubsectionId = '';

  createSectionForm = {
    mode: 'SINGLE' as CreateSectionMode,
    subsection_count: 2,
    code: 'A',
    is_cepea: false,
    teacher_id: '',
    projected_vacancies: 0,
  };

  readonly shiftOptions = [
    { value: 'MANANA', label: 'Ma\u00f1ana' },
    { value: 'TARDE', label: 'Tarde' },
    { value: 'NOCHE', label: 'Noche' },
  ];

  readonly dayOptions = [
    { value: 'LUNES', label: 'Lunes' },
    { value: 'MARTES', label: 'Martes' },
    { value: 'MIERCOLES', label: 'Mi\u00e9rcoles' },
    { value: 'JUEVES', label: 'Jueves' },
    { value: 'VIERNES', label: 'Viernes' },
    { value: 'SABADO', label: 'S\u00e1bado' },
    { value: 'DOMINGO', label: 'Domingo' },
  ];

  readonly scheduleTimeOptions = this.buildScheduleTimeOptions();

  private readonly dayOrder: Record<string, number> = {
    LUNES: 1,
    MARTES: 2,
    MIERCOLES: 3,
    JUEVES: 4,
    VIERNES: 5,
    SABADO: 6,
    DOMINGO: 7,
  };

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
    private readonly dialog: DialogService,
    readonly auth: AuthService,
  ) {}

  ngOnInit(): void {
    this.offerId = this.route.snapshot.paramMap.get('offerId') ?? '';
    this.loadPage();
  }

  get semesterLabel() {
    return this.catalog.semesters.find((item: any) => item.id === this.offer?.semester_id)?.name ?? '---';
  }

  get campusLabel() {
    return this.catalog.campuses.find((item: any) => item.id === this.offer?.campus_id)?.name ?? '---';
  }

  get facultyLabel() {
    return this.catalog.faculties.find((item: any) => item.id === this.offer?.faculty_id)?.name ?? '---';
  }

  get programLabel() {
    return (
      this.catalog.academic_programs.find((item: any) => item.id === this.offer?.academic_program_id)?.name ??
      '---'
    );
  }

  get studyPlanLabel() {
    return this.catalog.study_plans.find((item: any) => item.id === this.offer?.study_plan_id)?.name ?? '---';
  }

  get canCreateSection() {
    return Boolean(this.offer) && !this.saving && !this.isWorkflowReadOnly;
  }

  get courseUsesBaseGroupOnly() {
    return this.offer?.course_type === 'TEORICO';
  }

  get createSectionSingleModeLabel() {
    switch (this.offer?.course_type) {
      case 'PRACTICO':
        return 'Grupo unico';
      case 'TEORICO_PRACTICO':
        return 'Grupo base unico';
      default:
        return 'Seccion unica';
    }
  }

  get createSectionMultipleModeLabel() {
    switch (this.offer?.course_type) {
      case 'PRACTICO':
        return 'Dividir en grupos practicos';
      case 'TEORICO_PRACTICO':
        return 'Dividir practica en grupos';
      default:
        return 'Con grupos';
    }
  }

  get createSectionGroupCountLabel() {
    return this.offer?.course_type === 'TEORICO_PRACTICO'
      ? 'Cantidad de grupos practicos'
      : 'Cantidad de grupos';
  }

  get createSectionModeHelper() {
    switch (this.offer?.course_type) {
      case 'TEORICO':
        return 'Este curso trabaja con un solo grupo base teorico.';
      case 'PRACTICO':
        return 'Puedes dejar un grupo unico o dividir la seccion en varios grupos practicos con vacantes repartidas.';
      case 'TEORICO_PRACTICO':
        return 'Si divides la practica, la seccion conserva un grupo base para teoria y crea grupos practicos adicionales para repartir vacantes.';
      default:
        return 'La seccion puede trabajar con un grupo base unico o con varios grupos segun el tipo del curso.';
    }
  }

  get createSectionGroupCountHelper() {
    if (this.createSectionForm.mode !== 'MULTIPLE') {
      return '';
    }
    if (this.offer?.course_type === 'TEORICO_PRACTICO') {
      return `Se crearan ${this.effectiveCreateSectionGroupCount()} grupos en total: 1 grupo base y ${this.createSectionForm.subsection_count} grupos practicos.`;
    }
    return `Se crearan ${this.effectiveCreateSectionGroupCount()} grupos con reparto inicial automatico de vacantes.`;
  }

  get planRule() {
    return this.offer?.plan_rule ?? null;
  }

  get canEditApprovedPlan() {
    return (
      this.planRule?.workflow_status === 'APPROVED' &&
      this.auth.hasPermission('action.planning.plan.review_decide')
    );
  }

  get isWorkflowReadOnly() {
    const status = this.planRule?.workflow_status ?? '';
    if (status === 'IN_REVIEW') {
      return true;
    }
    if (status === 'APPROVED') {
      return !this.canEditApprovedPlan;
    }
    return false;
  }

  get activeSection() {
    if (!this.activeSectionId) {
      return null;
    }
    return this.findSection(this.activeSectionId);
  }

  get activeSubsection() {
    if (!this.activeSubsectionId) {
      return null;
    }
    return this.findSubsection(this.activeSubsectionId);
  }

  get filteredCreateTeachers() {
    return this.filterTeachers(this.createTeacherQuery);
  }

  get subsectionModalityOptions() {
    const preferredCodes = new Set([
      'HIBRIDO_VIRTUAL',
      'HIBRIDO_PRESENCIAL',
      'VIRTUAL',
      'PRESENCIAL',
    ]);
    const items = this.catalog.course_modalities ?? [];
    const filtered = items.filter((item: any) => preferredCodes.has(item.code));
    return filtered.length > 0 ? filtered : items;
  }

  get campusBuildings() {
    if (!this.offer?.campus_id) {
      return [];
    }
    return this.buildings.filter((item: any) => item.campus_id === this.offer.campus_id);
  }

  loadPage() {
    if (!this.offerId) {
      this.error = 'No se encontro la oferta para configurar secciones.';
      this.loading = false;
      this.supportDataLoading = false;
      this.cdr.detectChanges();
      return;
    }

    this.loading = true;
    this.supportDataLoading = true;
    this.error = '';
    this.loadSupportData();
    this.api.getPlanningOffer(this.offerId).subscribe({
      next: (offer) => {
        this.offer = this.normalizeOffer(offer);
        this.syncUiStateFromOffer();
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: () => {
        this.error = 'No se pudo cargar la oferta para configurar secciones.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  reloadOffer() {
    if (!this.offerId) {
      return;
    }
    this.api.getPlanningOffer(this.offerId).subscribe({
      next: (offer) => {
        this.offer = this.normalizeOffer(offer);
        this.syncUiStateFromOffer();
        this.cdr.detectChanges();
      },
      error: () => {
        this.error = 'No se pudo refrescar la oferta.';
        this.cdr.detectChanges();
      },
    });
  }

  goBack() {
    if (!this.offer) {
      this.router.navigate(['/planning']);
      return;
    }
    this.router.navigate(['/planning/cycle-editor'], {
      queryParams: {
        semester_id: this.offer.semester_id,
        campus_id: this.offer.campus_id,
        faculty_id: this.offer.faculty_id,
        academic_program_id: this.offer.academic_program_id,
        cycle: String(this.offer.cycle ?? ''),
        study_plan_id: this.offer.study_plan_id,
      },
    });
  }

  openChangeLog() {
    if (!this.offerId) {
      return;
    }
    this.router.navigate(['/planning/change-log'], {
      queryParams: {
        offer_id: this.offerId,
        semester_id: this.offer?.semester_id ?? null,
        campus_id: this.offer?.campus_id ?? null,
        faculty_id: this.offer?.faculty_id ?? null,
        academic_program_id: this.offer?.academic_program_id ?? null,
      },
    });
  }

  openCreateSectionModal() {
    if (this.isWorkflowReadOnly) {
      return;
    }
    this.createSectionForm = {
      mode: 'SINGLE',
      subsection_count: 2,
      code: this.nextSectionCodeSuggestion(),
      is_cepea: false,
      teacher_id: '',
      projected_vacancies: 0,
    };
    this.createTeacherQuery = '';
    this.showCreateTeacherOptions = false;
    this.showCreateModal = true;
  }

  closeCreateSectionModal() {
    this.showCreateModal = false;
    this.showCreateTeacherOptions = false;
    this.createTeacherQuery = '';
  }

  isSectionExpanded(sectionId: string) {
    return this.expandedSectionId === sectionId;
  }

  toggleSection(sectionId: string) {
    const nextExpanded = this.expandedSectionId === sectionId ? '' : sectionId;
    this.expandedSectionId = nextExpanded;
    if (!nextExpanded && this.activeSectionId === sectionId) {
      this.closeSubsectionDrawer();
    }
  }

  openSubsectionDrawer(section: any, subsection: any) {
    this.expandedSectionId = section.id;
    this.activeSectionId = section.id;
    this.activeSubsectionId = subsection.id;
    this.scheduleDraftsBySubsectionId[subsection.id] = this.scheduleDraftFromSubsection(subsection);
    this.drawerOpen = true;
  }

  closeSubsectionDrawer() {
    this.drawerOpen = false;
    this.activeSectionId = '';
    this.activeSubsectionId = '';
  }

  onCreateSectionModeChange() {
    if (this.createSectionForm.mode === 'SINGLE') {
      return;
    }
    this.createSectionForm.subsection_count =
      this.createSectionForm.subsection_count <= 1
        ? 2
        : Math.max(2, Math.trunc(this.createSectionForm.subsection_count || 2));
  }

  onCreateSubsectionCountChange(value: number | string) {
    const numericValue = Math.trunc(Number(value || 0));
    this.createSectionForm.subsection_count = numericValue <= 1 ? 2 : numericValue;
  }

  onCreateSectionCodeChange(value: string) {
    this.createSectionForm.code = this.normalizeSectionCode(value);
  }

  onCreateTeacherFocus() {
    this.showCreateTeacherOptions = true;
  }

  onCreateTeacherBlur() {
    setTimeout(() => {
      this.showCreateTeacherOptions = false;
      this.restoreCreateTeacherQuery();
      this.cdr.detectChanges();
    }, 150);
  }

  onCreateTeacherQueryChange(value: string) {
    this.createTeacherQuery = value;
    this.createSectionForm.teacher_id = '';
    this.showCreateTeacherOptions = true;
  }

  selectCreateTeacher(teacher: any | null) {
    this.createSectionForm.teacher_id = teacher?.id ?? '';
    this.createTeacherQuery = teacher ? this.teacherDisplay(teacher) : '';
    this.showCreateTeacherOptions = false;
  }

  createSection() {
    if (!this.offerId || this.isWorkflowReadOnly) {
      return;
    }
    const sectionCode = this.normalizeSectionCode(this.createSectionForm.code) || this.nextSectionCodeSuggestion();
    if (!sectionCode) {
      this.error = 'Debes indicar un codigo valido para la seccion.';
      this.message = '';
      this.cdr.detectChanges();
      return;
    }
    const subsectionCount = this.effectiveCreateSectionGroupCount();
    const projectedVacancies = Math.max(0, Math.trunc(Number(this.createSectionForm.projected_vacancies || 0)));

    this.saving = true;
    this.api
      .createPlanningSection(this.offerId, {
        code: sectionCode,
        is_cepea: this.createSectionForm.is_cepea,
        teacher_id: this.createSectionForm.teacher_id || undefined,
        subsection_count: subsectionCount,
        projected_vacancies: projectedVacancies,
      })
      .subscribe({
        next: (section) => {
          this.message = `Seccion ${this.sectionPrimaryCode(section)} creada.`;
          this.error = '';
          this.showCreateModal = false;
          this.saving = false;
          this.upsertSection(section);
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo crear la seccion.';
          this.saving = false;
          this.cdr.detectChanges();
        },
      });
  }

  sectionTeacherQuery(sectionId: string) {
    return this.sectionTeacherQueryById[sectionId] ?? '';
  }

  subsectionTeacherQuery(subsectionId: string) {
    return this.subsectionTeacherQueryById[subsectionId] ?? '';
  }

  filteredSectionTeachers(sectionId: string) {
    return this.filterTeachers(this.sectionTeacherQuery(sectionId));
  }

  filteredSubsectionTeachers(subsectionId: string) {
    return this.filterTeachers(this.subsectionTeacherQuery(subsectionId));
  }

  onSectionTeacherFocus(sectionId: string) {
    this.sectionTeacherMenuOpenById[sectionId] = true;
  }

  onSectionTeacherBlur(sectionId: string) {
    setTimeout(() => {
      this.sectionTeacherMenuOpenById[sectionId] = false;
      this.restoreSectionTeacherQuery(sectionId);
      this.cdr.detectChanges();
    }, 150);
  }

  onSectionTeacherQueryChange(sectionId: string, value: string) {
    this.sectionTeacherQueryById[sectionId] = value;
    this.sectionTeacherMenuOpenById[sectionId] = true;
  }

  selectSectionTeacher(section: any, teacher: any | null) {
    this.sectionTeacherQueryById[section.id] = teacher ? this.teacherDisplay(teacher) : '';
    this.sectionTeacherMenuOpenById[section.id] = false;
    if ((section.teacher_id ?? '') === (teacher?.id ?? '')) {
      return;
    }
    this.updateSectionTeacher(section, teacher?.id ?? '');
  }

  onSubsectionTeacherFocus(subsectionId: string) {
    this.subsectionTeacherMenuOpenById[subsectionId] = true;
  }

  onSubsectionTeacherBlur(subsectionId: string) {
    setTimeout(() => {
      this.subsectionTeacherMenuOpenById[subsectionId] = false;
      this.restoreSubsectionTeacherQuery(subsectionId);
      this.cdr.detectChanges();
    }, 150);
  }

  onSubsectionTeacherQueryChange(subsectionId: string, value: string) {
    this.subsectionTeacherQueryById[subsectionId] = value;
    this.subsectionTeacherMenuOpenById[subsectionId] = true;
  }

  selectSubsectionTeacher(subsection: any, teacher: any | null) {
    this.subsectionTeacherQueryById[subsection.id] = teacher ? this.teacherDisplay(teacher) : '';
    this.subsectionTeacherMenuOpenById[subsection.id] = false;
    if ((subsection.responsible_teacher_id ?? '') === (teacher?.id ?? '')) {
      return;
    }
    this.updateSubsectionField(
      subsection,
      { responsible_teacher_id: teacher?.id ?? '' },
      `Grupo ${subsection.code} actualizado.`,
    );
  }

  updateSectionTeacher(section: any, teacherId: string) {
    if (this.isWorkflowReadOnly) {
      return;
    }
    this.saving = true;
    this.api
      .updatePlanningSection(section.id, {
        teacher_id: teacherId,
      })
      .subscribe({
        next: (updatedSection) => {
          this.message = `Seccion ${this.sectionPrimaryCode(updatedSection)} actualizada.`;
          this.error = '';
          this.saving = false;
          this.upsertSection(updatedSection);
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo actualizar la seccion.';
          this.saving = false;
          this.reloadOffer();
          this.cdr.detectChanges();
        },
      });
  }

  updateSectionVacancies(section: any, value: number | string) {
    if (this.isWorkflowReadOnly) {
      return;
    }
    const parsed = Math.max(0, Math.trunc(Number(value || 0)));
    if (Number(section.projected_vacancies ?? 0) === parsed) {
      section.projected_vacancies = parsed;
      return;
    }
    section.projected_vacancies = parsed;
    this.saving = true;
    this.api
      .updatePlanningSection(section.id, {
        projected_vacancies: parsed,
      })
      .subscribe({
        next: (updatedSection) => {
          this.message = `Vacantes actualizadas para la seccion ${this.sectionPrimaryCode(updatedSection)}.`;
          this.error = '';
          this.saving = false;
          this.upsertSection(updatedSection);
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudieron actualizar las vacantes de la seccion.';
          this.saving = false;
          this.reloadOffer();
          this.cdr.detectChanges();
        },
      });
  }

  async deleteSection(section: any) {
    if (!section?.id || this.isWorkflowReadOnly || this.saving) {
      return;
    }
    if (!this.canDeleteSection(section)) {
      this.error = this.sectionDeleteHelper(section);
      this.message = '';
      this.cdr.detectChanges();
      return;
    }
    const confirmation = await this.dialog.confirm({
      title: `Eliminar seccion ${this.sectionPrimaryCode(section)}`,
      message: `Se eliminara la seccion ${this.sectionPrimaryCode(section)} y todos sus grupos.\n\nEsta accion no se puede deshacer. Deseas continuar?`,
      confirmLabel: 'Eliminar seccion',
      cancelLabel: 'Cancelar',
      tone: 'danger',
    });
    if (!confirmation) {
      return;
    }

    this.saving = true;
    this.error = '';
    this.message = '';
    this.api.deletePlanningSection(section.id).subscribe({
      next: (response) => {
        const deletedSubsections = Number(response?.deleted_subsection_count ?? 0);
        const deletedSchedules = Number(response?.deleted_schedule_count ?? 0);
        this.message = `Seccion ${this.sectionPrimaryCode(section)} eliminada. ${deletedSubsections} grupos y ${deletedSchedules} horarios eliminados.`;
        if (this.expandedSectionId === section.id) {
          this.expandedSectionId = '';
        }
        if (this.activeSectionId === section.id) {
          this.closeSubsectionDrawer();
        }
        this.saving = false;
        this.reloadOffer();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo eliminar la seccion.';
        this.saving = false;
        this.cdr.detectChanges();
      },
    });
  }

  updateSubsectionVacancies(subsection: any, value: number | string) {
    const parsed = Math.max(0, Math.trunc(Number(value || 0)));
    const limit = this.subsectionVacancyLimit(subsection);
    const normalized = Math.min(parsed, limit);
    if (parsed > limit) {
      this.message = '';
      this.error = `La vacante de ${subsection.code} no puede superar la vacante de la seccion (${limit}).`;
    }
    if (Number(subsection.projected_vacancies ?? 0) === normalized) {
      subsection.projected_vacancies = normalized;
      return;
    }
    subsection.projected_vacancies = normalized;
    this.updateSubsectionField(
      subsection,
      { projected_vacancies: normalized },
      `Vacantes actualizadas para ${subsection.code}.`,
    );
  }

  updateSubsectionField(subsection: any, payload: Record<string, unknown>, successMessage: string) {
    if (this.isWorkflowReadOnly) {
      return;
    }
    this.saving = true;
    this.api.updatePlanningSubsection(subsection.id, payload).subscribe({
      next: (updatedSubsection) => {
        this.message = successMessage;
        this.error = '';
        this.saving = false;
        this.upsertSubsection(updatedSubsection);
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo actualizar el grupo.';
        this.saving = false;
        this.reloadOffer();
        this.cdr.detectChanges();
      },
    });
  }

  onSubsectionBuildingChange(subsection: any, value: string) {
    this.updateSubsectionField(
      subsection,
      {
        building_id: value,
        classroom_id: '',
      },
      `Pabellon actualizado para ${subsection.code}.`,
    );
  }

  onSubsectionClassroomChange(subsection: any, value: string) {
    const classroom = this.classrooms.find((item: any) => item.id === value) ?? null;
    const payload: Record<string, unknown> = {
      building_id: classroom?.building_id ?? subsection.building_id ?? '',
      classroom_id: value,
    };
    if (classroom?.capacity !== null && classroom?.capacity !== undefined) {
      payload['capacity_snapshot'] = classroom.capacity;
    }
    this.updateSubsectionField(subsection, payload, `Aula actualizada para ${subsection.code}.`);
  }

  onSubsectionShiftChange(subsection: any, value: string) {
    this.updateSubsectionField(subsection, { shift: value }, `Turno actualizado para ${subsection.code}.`);
  }

  onSubsectionModalityChange(subsection: any, value: string) {
    this.updateSubsectionField(
      subsection,
      { course_modality_id: value },
      `Modalidad actualizada para ${subsection.code}.`,
    );
  }

  onSubsectionKindChange(subsection: any, value: string) {
    if (!value || subsection.kind === value) {
      return;
    }
    const draft = this.scheduleDraftsBySubsectionId[subsection.id];
    if (draft) {
      const allowedTypes = this.scheduleSessionTypeOptionsForKind(value);
      if (!allowedTypes.some((option) => option.value === draft.session_type)) {
        draft.session_type = allowedTypes[0]?.value ?? this.defaultScheduleSessionType(value);
      }
    }
    this.updateSubsectionField(subsection, { kind: value }, `Tipo actualizado para ${subsection.code}.`);
  }

  subsectionKindOptions(subsection: any) {
    const section = this.findSectionBySubsection(subsection?.id ?? '');
    const subsectionCount = Math.max(1, Number(section?.subsections?.length ?? 0));
    switch (this.offer?.course_type) {
      case 'TEORICO':
        return [{ value: 'THEORY', label: 'Teorico' }];
      case 'PRACTICO':
        return [{ value: 'PRACTICE', label: 'Practico' }];
      default:
        if (subsectionCount <= 1) {
          return [
            { value: 'MIXED', label: 'Teorico practico' },
            { value: 'MIXED_PRACTICE_THEORY', label: 'Practico teorico' },
          ];
        }
        return [
          { value: 'THEORY', label: 'Teorico' },
          { value: 'PRACTICE', label: 'Practico' },
        ];
    }
  }

  canEditSubsectionKind(subsection: any) {
    return this.subsectionKindOptions(subsection).length > 1;
  }

  classroomsForBuilding(buildingId: string | null | undefined) {
    if (!buildingId) {
      return [];
    }
    return this.classrooms.filter(
      (item: any) => item.building_id === buildingId && item.campus_id === this.offer?.campus_id,
    );
  }

  subsectionVacancyLimit(subsection: any) {
    const section = this.findSectionBySubsection(subsection?.id ?? '');
    return Math.max(0, Math.trunc(Number(section?.projected_vacancies ?? 0)));
  }

  classroomOptionLabel(classroom: any) {
    if (!classroom) {
      return 'Sin aula';
    }
    return classroom.capacity ? `${classroom.name} - Aforo ${classroom.capacity}` : classroom.name;
  }

  teacherDisplay(teacher: any) {
    if (!teacher) {
      return 'Sin docente';
    }
    const name = teacher.full_name || teacher.name || 'Sin nombre';
    return teacher.dni ? `${teacher.dni} - ${name}` : name;
  }

  sectionPrimaryCode(section: any) {
    return section?.external_code || section?.code || 'Seccion';
  }

  sectionSecondaryCode(section: any) {
    if (section?.external_code && section?.code && section.external_code !== section.code) {
      return `Interna ${section.code}`;
    }
    return '';
  }

  scheduleSummary(subsection: any) {
    const schedules = subsection?.schedules ?? [];
    if (schedules.length === 0) {
      return 'Sin horarios';
    }
    const first = schedules[0];
    const summary = `${this.dayLabel(first.day_of_week)} ${this.timeLabel(first.start_time)}-${this.timeLabel(first.end_time)}`;
    return schedules.length === 1 ? summary : `${summary} +${schedules.length - 1}`;
  }

  scheduleConflictLabels(subsection: any) {
    const labels = new Set(
      (subsection?.conflicts ?? []).map((item: any) => this.conflictLabel(item?.conflict_type)),
    );
    return [...labels].filter(Boolean);
  }

  dayLabel(value: string | null | undefined) {
    return this.dayOptions.find((item) => item.value === value)?.label ?? value ?? '---';
  }

  timeLabel(value: string | null | undefined) {
    if (!value) {
      return '--:--';
    }
    return String(value).slice(0, 5);
  }

  formatAcademicHours(value: number | string | null | undefined) {
    const parsed = Number(value ?? 0);
    return Number.isFinite(parsed) ? parsed.toFixed(1) : '0.0';
  }

  scheduleSessionTypeLabel(schedule: any) {
    switch (schedule?.session_type) {
      case 'THEORY':
        return 'Teoria';
      case 'PRACTICE':
        return 'Practica';
      case 'LAB':
        return 'Laboratorio';
      default:
        return 'Otro';
    }
  }

  scheduleTeacherLabel(schedule: any, subsection: any) {
    return this.teacherDisplay(schedule?.teacher ?? subsection?.responsible_teacher ?? null);
  }

  scheduleLocationLabel(schedule: any, subsection: any) {
    const buildingName = schedule?.building?.name ?? subsection?.building?.name ?? '';
    const classroomName = schedule?.classroom?.name ?? subsection?.classroom?.name ?? '';
    if (buildingName && classroomName) {
      return `${buildingName} / ${classroomName}`;
    }
    return buildingName || classroomName || 'Sin ubicar';
  }

  subsectionKindLabel(kind: string | null | undefined) {
    switch (kind) {
      case 'THEORY':
        return 'Teorico';
      case 'PRACTICE':
        return 'Practico';
      case 'MIXED':
        return 'Teorico practico';
      case 'MIXED_PRACTICE_THEORY':
        return 'Practico teorico';
      default:
        return kind || '---';
    }
  }

  subsectionKindClass(kind: string | null | undefined) {
    switch (kind) {
      case 'THEORY':
        return 'kind-theory';
      case 'PRACTICE':
        return 'kind-practice';
      case 'MIXED':
      case 'MIXED_PRACTICE_THEORY':
        return 'kind-mixed';
      default:
        return 'kind-default';
    }
  }

  subsectionExpectedHoursLabel(subsection: any) {
    const expected = this.expectedAssignedHours(subsection?.kind);
    return `HT ${this.formatAcademicHours(expected.theoretical_hours)} · HP ${this.formatAcademicHours(expected.practical_hours)} · Total ${this.formatAcademicHours(expected.total_hours)}`;
  }

  subsectionCurrentHoursLabel(subsection: any) {
    return `Actual: HT ${this.formatAcademicHours(subsection?.assigned_theoretical_hours)} · HP ${this.formatAcademicHours(subsection?.assigned_practical_hours)} · Total ${this.formatAcademicHours(subsection?.assigned_total_hours)}`;
  }

  subsectionAssignedHoursMismatch(subsection: any) {
    const expected = this.expectedAssignedHours(subsection?.kind);
    return !(
      this.sameHourValue(subsection?.assigned_theoretical_hours, expected.theoretical_hours) &&
      this.sameHourValue(subsection?.assigned_practical_hours, expected.practical_hours) &&
      this.sameHourValue(subsection?.assigned_total_hours, expected.total_hours)
    );
  }

  subsectionKindGuidance(subsection: any) {
    const section = this.findSectionBySubsection(subsection?.id ?? '');
    const subsectionCount = Math.max(1, Number(section?.subsections?.length ?? 0));
    switch (this.offer?.course_type) {
      case 'TEORICO':
        return 'Este curso solo admite grupos teoricos.';
      case 'PRACTICO':
        return 'Este curso solo admite grupos practicos.';
      default:
        if (subsectionCount <= 1) {
          return 'Si solo existe un grupo, puedes definirlo como teorico practico o practico teorico.';
        }
        return 'En cursos teorico practico con varios grupos debe quedar al menos uno teorico y uno practico.';
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

  previewAcademicHours(subsectionId: string) {
    const minutes = this.scheduleMinutes(subsectionId);
    return minutes === null ? '---' : this.formatAcademicHours(minutes / 50);
  }

  scheduleDraftInvalid(subsectionId: string) {
    const draft = this.scheduleDraftsBySubsectionId[subsectionId];
    if (!draft?.start_time || !draft?.end_time) {
      return false;
    }
    return this.scheduleMinutes(subsectionId) === null;
  }

  canCreateSchedule(subsectionId: string) {
    const draft = this.scheduleDraftsBySubsectionId[subsectionId];
    if (!draft?.day_of_week || !draft?.start_time || !draft?.end_time) {
      return false;
    }
    return this.scheduleMinutes(subsectionId) !== null;
  }

  availableEndTimeOptions(subsectionId: string) {
    const draft = this.scheduleDraftsBySubsectionId[subsectionId];
    const startMinutes = this.toMinutes(draft?.start_time ?? '07:40');
    return this.scheduleTimeOptions.filter((option) => this.toMinutes(option.value) > startMinutes);
  }

  onScheduleStartTimeChange(subsectionId: string, value: string) {
    const draft = this.scheduleDraftsBySubsectionId[subsectionId];
    if (!draft) {
      return;
    }
    draft.start_time = this.normalizeScheduleTime(value, draft.start_time || '07:40');
    const endOptions = this.availableEndTimeOptions(subsectionId);
    const currentEndMinutes = this.toMinutes(draft.end_time);
    if (!endOptions.some((option) => option.value === draft.end_time) || currentEndMinutes <= this.toMinutes(value)) {
      draft.end_time = endOptions[0]?.value ?? value;
    }
  }

  onScheduleEndTimeChange(subsectionId: string, value: string) {
    const draft = this.scheduleDraftsBySubsectionId[subsectionId];
    if (!draft) {
      return;
    }
    draft.end_time = this.normalizeScheduleTime(value, draft.end_time || '08:30');
  }

  saveSchedule(subsection: any) {
    if (this.isWorkflowReadOnly) {
      return;
    }
    const draft = this.scheduleDraftsBySubsectionId[subsection.id];
    if (!this.canCreateSchedule(subsection.id)) {
      return;
    }
    const existingSchedule =
      (subsection?.schedules ?? []).find(
        (item: any) =>
          item.day_of_week === draft.day_of_week &&
          this.normalizeScheduleTime(item.start_time, draft.start_time) === draft.start_time &&
          this.normalizeScheduleTime(item.end_time, draft.end_time) === draft.end_time,
      ) ?? null;
    this.saving = true;
    const request = existingSchedule
      ? this.api.updatePlanningSubsectionSchedule(existingSchedule.id, draft)
      : this.api.createPlanningSubsectionSchedule(subsection.id, draft);
    request.subscribe({
      next: (updatedSubsection) => {
        this.message = existingSchedule
          ? `Horario actualizado en el grupo ${subsection.code}.`
          : `Horario agregado al grupo ${subsection.code}.`;
        this.error = '';
        this.saving = false;
        this.upsertSubsection(updatedSubsection);
        this.scheduleDraftsBySubsectionId[subsection.id] = this.defaultScheduleDraft(updatedSubsection?.kind);
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo guardar el horario.';
        this.saving = false;
        this.cdr.detectChanges();
      },
    });
  }

  deleteSchedule(subsection: any, scheduleId: string) {
    if (!scheduleId || this.isWorkflowReadOnly) {
      return;
    }
    this.saving = true;
    this.api.deletePlanningSubsectionSchedule(scheduleId).subscribe({
      next: (updatedSubsection) => {
        this.message = `Horario eliminado del grupo ${subsection.code}.`;
        this.error = '';
        this.saving = false;
        this.upsertSubsection(updatedSubsection);
        this.scheduleDraftsBySubsectionId[subsection.id] = this.scheduleDraftFromSubsection(updatedSubsection);
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo eliminar el horario.';
        this.saving = false;
        this.cdr.detectChanges();
      },
    });
  }

  private loadSupportData() {
    forkJoin({
      catalog: this.api.getPlanningCatalogFilters().pipe(catchError(() => of(this.emptyCatalog()))),
      teachers: this.api.listTeachers().pipe(catchError(() => of([]))),
      buildings: this.api.listBuildings().pipe(catchError(() => of([]))),
      classrooms: this.api.listClassrooms().pipe(catchError(() => of([]))),
    })
      .pipe(
        finalize(() => {
          this.supportDataLoading = false;
          this.cdr.detectChanges();
        }),
      )
      .subscribe({
        next: ({ catalog, teachers, buildings, classrooms }) => {
          this.catalog = catalog;
          this.teachers = teachers;
          this.buildings = buildings;
          this.classrooms = classrooms;
          this.syncUiStateFromOffer();
          this.restoreCreateTeacherQuery();
          this.cdr.detectChanges();
        },
      });
  }

  sectionTeacherSummary(section: any) {
    return this.currentSectionTeacherLabel(section) || 'Sin docente';
  }

  sectionConflictCount(section: any) {
    return (section?.subsections ?? []).reduce(
      (total: number, subsection: any) => total + (subsection?.conflicts?.length || 0),
      0,
    );
  }

  sectionStatusLabel(section: any) {
    const conflicts = this.sectionConflictCount(section);
    if (conflicts > 0) {
      return `${conflicts} cruces`;
    }
    const subsections = section?.subsections ?? [];
    if (subsections.length === 0) {
      return 'Sin grupos';
    }
    const readyCount = subsections.filter((item: any) => this.subsectionIsReady(item)).length;
    return readyCount === subsections.length ? 'Configurada' : 'En progreso';
  }

  sectionStatusClass(section: any) {
    return this.sectionConflictCount(section) > 0 ? 'status-observed' : 'status-ready';
  }

  sectionScheduleSummary(section: any) {
    const subsections = section?.subsections ?? [];
    const scheduled = subsections.filter((item: any) => (item?.schedules?.length || 0) > 0);
    if (scheduled.length === 0) {
      return 'Sin horarios';
    }
    if (scheduled.length === 1) {
      return `Grupo ${scheduled[0].code}: ${this.scheduleSummary(scheduled[0])}`;
    }
    return `${scheduled.length}/${subsections.length} grupos con horario`;
  }

  sectionScheduleCount(section: any) {
    return (section?.subsections ?? []).reduce(
      (total: number, subsection: any) => total + Number(subsection?.schedules?.length ?? 0),
      0,
    );
  }

  subsectionLocationSummary(subsection: any) {
    const buildingName = subsection?.building?.name ?? '';
    const classroomName = subsection?.classroom?.name ?? '';
    if (buildingName && classroomName) {
      return `${buildingName} / ${classroomName}`;
    }
    return buildingName || classroomName || 'Sin ubicar';
  }

  subsectionModalityLabel(subsection: any) {
    if (subsection?.modality?.name) {
      return subsection.modality.name;
    }
    return (
      this.subsectionModalityOptions.find((item: any) => item.id === subsection?.course_modality_id)?.name ??
      'Sin modalidad'
    );
  }

  subsectionStatusLabel(subsection: any) {
    if ((subsection?.conflicts?.length || 0) > 0) {
      return 'Con cruces';
    }
    return this.subsectionIsReady(subsection) ? 'Lista' : 'Pendiente';
  }

  subsectionStatusClass(subsection: any) {
    if ((subsection?.conflicts?.length || 0) > 0) {
      return 'status-observed';
    }
    return this.subsectionIsReady(subsection) ? 'status-ready' : 'status-pending';
  }

  private filterTeachers(query: string) {
    const normalizedQuery = this.normalizeSearchValue(query);
    const pool = !normalizedQuery
      ? this.teachers
      : this.teachers.filter((teacher: any) =>
          this.normalizeSearchValue(
            [teacher?.dni, teacher?.full_name, teacher?.name].filter(Boolean).join(' '),
          ).includes(normalizedQuery),
        );
    return pool.slice(0, 25);
  }

  private restoreCreateTeacherQuery() {
    const teacher = this.teachers.find((item: any) => item.id === this.createSectionForm.teacher_id) ?? null;
    this.createTeacherQuery = teacher ? this.teacherDisplay(teacher) : '';
  }

  private restoreSectionTeacherQuery(sectionId: string) {
    const section = this.findSection(sectionId);
    if (!section) {
      return;
    }
    this.sectionTeacherQueryById[sectionId] = this.currentSectionTeacherLabel(section);
  }

  private restoreSubsectionTeacherQuery(subsectionId: string) {
    const subsection = this.findSubsection(subsectionId);
    if (!subsection) {
      return;
    }
    this.subsectionTeacherQueryById[subsectionId] = this.currentSubsectionTeacherLabel(subsection);
  }

  private currentSectionTeacherLabel(section: any) {
    if (!section?.teacher_id) {
      return '';
    }
    const teacher = section.teacher ?? this.teachers.find((item: any) => item.id === section.teacher_id) ?? null;
    return teacher ? this.teacherDisplay(teacher) : '';
  }

  private currentSubsectionTeacherLabel(subsection: any) {
    if (!subsection?.responsible_teacher_id) {
      return '';
    }
    const teacher =
      subsection.responsible_teacher ??
      this.teachers.find((item: any) => item.id === subsection.responsible_teacher_id) ??
      null;
    return teacher ? this.teacherDisplay(teacher) : '';
  }

  private findSection(sectionId: string) {
    return this.offer?.sections?.find((item: any) => item.id === sectionId) ?? null;
  }

  canDeleteSection(section: any) {
    if (!section?.id) {
      return false;
    }
    const sections = this.offer?.sections ?? [];
    const highestIndex = sections.reduce((currentMax: number, item: any) => {
      const index = this.sectionCodeIndex(item?.code);
      return index === null ? currentMax : Math.max(currentMax, index);
    }, -1);
    const currentIndex = this.sectionCodeIndex(section.code);
    return currentIndex !== null && currentIndex === highestIndex;
  }

  sectionDeleteHelper(section: any) {
    if (this.canDeleteSection(section)) {
      return 'Eliminar esta seccion tambien eliminara sus grupos y horarios.';
    }
    return 'Solo puedes eliminar la ultima seccion para conservar el correlativo.';
  }

  private findSubsection(subsectionId: string) {
    for (const section of this.offer?.sections ?? []) {
      const found = section.subsections?.find((item: any) => item.id === subsectionId);
      if (found) {
        return found;
      }
    }
    return null;
  }

  private findSectionBySubsection(subsectionId: string) {
    for (const section of this.offer?.sections ?? []) {
      const found = section.subsections?.some((item: any) => item.id === subsectionId);
      if (found) {
        return section;
      }
    }
    return null;
  }

  private syncUiStateFromOffer() {
    if (!this.offer) {
      return;
    }

    const nextSectionQueries = { ...this.sectionTeacherQueryById };
    const nextSubsectionQueries = { ...this.subsectionTeacherQueryById };
    const nextScheduleDrafts = { ...this.scheduleDraftsBySubsectionId };

    for (const section of this.offer.sections ?? []) {
      if (!this.sectionTeacherMenuOpenById[section.id]) {
        nextSectionQueries[section.id] = this.currentSectionTeacherLabel(section);
      }
      this.sectionTeacherMenuOpenById[section.id] = this.sectionTeacherMenuOpenById[section.id] ?? false;

      for (const subsection of section.subsections ?? []) {
        if (!this.subsectionTeacherMenuOpenById[subsection.id]) {
          nextSubsectionQueries[subsection.id] = this.currentSubsectionTeacherLabel(subsection);
        }
        this.subsectionTeacherMenuOpenById[subsection.id] =
          this.subsectionTeacherMenuOpenById[subsection.id] ?? false;
        nextScheduleDrafts[subsection.id] =
          (subsection?.schedules?.length ?? 0) > 0
            ? this.scheduleDraftFromSubsection(subsection)
            : this.normalizeScheduleDraftForGroup(
                nextScheduleDrafts[subsection.id] ?? this.scheduleDraftFromSubsection(subsection),
                subsection,
              );
      }
    }

    this.sectionTeacherQueryById = nextSectionQueries;
    this.subsectionTeacherQueryById = nextSubsectionQueries;
    this.scheduleDraftsBySubsectionId = nextScheduleDrafts;

    if (this.activeSubsectionId && !this.findSubsection(this.activeSubsectionId)) {
      this.closeSubsectionDrawer();
    }
    if (this.expandedSectionId && !this.findSection(this.expandedSectionId)) {
      this.expandedSectionId = '';
    }
  }

  private emptyCatalog() {
    return {
      semesters: [],
      campuses: [],
      faculties: [],
      academic_programs: [],
      study_plans: [],
      course_modalities: [],
    };
  }

  private normalizeOffer(offer: any) {
    return {
      ...offer,
      theoretical_hours: Number(offer?.theoretical_hours ?? 0),
      practical_hours: Number(offer?.practical_hours ?? 0),
      total_hours: Number(offer?.total_hours ?? 0),
      sections: [...(offer?.sections ?? [])]
        .map((section: any) => this.normalizeSection(section))
        .sort((a: any, b: any) => String(a.code).localeCompare(String(b.code))),
    };
  }

  private normalizeSection(section: any) {
    return {
      ...section,
      external_code: section?.external_code ?? '',
      is_cepea: Boolean(section?.is_cepea),
      teacher_id: section.teacher_id ?? '',
      projected_vacancies: Number(section.projected_vacancies ?? 0),
      teacher: section.teacher ?? null,
      conflicts: [...(section.conflicts ?? [])],
      subsections: [...(section.subsections ?? [])]
        .map((subsection: any) => this.normalizeSubsection(subsection))
        .sort((a: any, b: any) => String(a.code).localeCompare(String(b.code))),
    };
  }

  private normalizeSubsection(subsection: any) {
    return {
      ...subsection,
      responsible_teacher_id: subsection.responsible_teacher_id ?? '',
      course_modality_id: subsection.course_modality_id ?? '',
      building_id: subsection.building_id ?? '',
      classroom_id: subsection.classroom_id ?? '',
      projected_vacancies: Number(subsection.projected_vacancies ?? 0),
      shift: subsection.shift ?? '',
      assigned_theoretical_hours: Number(subsection.assigned_theoretical_hours ?? 0),
      assigned_practical_hours: Number(subsection.assigned_practical_hours ?? 0),
      assigned_total_hours: Number(subsection.assigned_total_hours ?? 0),
      modality: subsection.modality ?? null,
      responsible_teacher: subsection.responsible_teacher ?? null,
      schedules: [...(subsection.schedules ?? [])]
        .map((schedule: any) => ({
          ...schedule,
          teacher: schedule?.teacher ?? null,
          building: schedule?.building ?? null,
          classroom: schedule?.classroom ?? null,
          session_type: schedule?.session_type ?? 'OTHER',
        }))
        .sort((a: any, b: any) => this.compareSchedules(a, b)),
      conflicts: [...(subsection.conflicts ?? [])],
    };
  }

  private compareSchedules(left: any, right: any) {
    const dayDelta = (this.dayOrder[left?.day_of_week ?? ''] ?? 99) - (this.dayOrder[right?.day_of_week ?? ''] ?? 99);
    if (dayDelta !== 0) {
      return dayDelta;
    }
    return String(left?.start_time ?? '').localeCompare(String(right?.start_time ?? ''));
  }

  private sectionCodeIndex(code: string | null | undefined) {
    const normalized = `${code ?? ''}`.trim().toUpperCase();
    if (!/^[A-Z]+$/.test(normalized)) {
      return null;
    }
    let index = 0;
    for (const char of normalized) {
      index = index * 26 + (char.charCodeAt(0) - 64);
    }
    return index - 1;
  }

  nextSectionCodeSuggestion() {
    const highestIndex = (this.offer?.sections ?? []).reduce((currentMax: number, item: any) => {
      const index = this.sectionCodeIndex(item?.code);
      return index === null ? currentMax : Math.max(currentMax, index);
    }, -1);
    return this.sectionCodeFromIndex(highestIndex + 1);
  }

  private sectionCodeFromIndex(index: number) {
    let value = Math.max(0, Math.trunc(index));
    let code = '';
    do {
      code = String.fromCharCode(65 + (value % 26)) + code;
      value = Math.floor(value / 26) - 1;
    } while (value >= 0);
    return code;
  }

  private normalizeSectionCode(value: string | null | undefined) {
    return `${value ?? ''}`
      .trim()
      .toUpperCase()
      .replace(/\s+/g, '')
      .replace(/[^A-Z0-9_-]/g, '');
  }

  private expectedAssignedHours(kind: string | null | undefined) {
    const theoreticalHours = Number(this.offer?.theoretical_hours ?? 0);
    const practicalHours = Number(this.offer?.practical_hours ?? 0);
    const totalHours = Number(this.offer?.total_hours ?? theoreticalHours + practicalHours);
    if (kind === 'THEORY') {
      return {
        theoretical_hours: theoreticalHours,
        practical_hours: 0,
        total_hours: theoreticalHours,
      };
    }
    if (kind === 'PRACTICE') {
      return {
        theoretical_hours: 0,
        practical_hours: practicalHours,
        total_hours: practicalHours,
      };
    }
    return {
      theoretical_hours: theoreticalHours,
      practical_hours: practicalHours,
      total_hours: totalHours,
    };
  }

  private sameHourValue(left: unknown, right: unknown) {
    return Math.abs(Number(left ?? 0) - Number(right ?? 0)) < 0.01;
  }

  private upsertSection(section: any) {
    const currentSections = [...(this.offer?.sections ?? [])].filter((item: any) => item.id !== section.id);
    currentSections.push(this.normalizeSection(section));
    this.offer = {
      ...this.offer,
      sections: currentSections.sort((a: any, b: any) => String(a.code).localeCompare(String(b.code))),
    };
    this.syncUiStateFromOffer();
  }

  private upsertSubsection(subsection: any) {
    const normalizedSubsection = this.normalizeSubsection(subsection);
    const nextSections = [...(this.offer?.sections ?? [])].map((section: any) => {
      if (section.id !== subsection.section?.id && section.id !== subsection.planning_section_id) {
        return section;
      }
      const nextSubsections = [...(section.subsections ?? [])].filter((item: any) => item.id !== subsection.id);
      nextSubsections.push(normalizedSubsection);
      return {
        ...section,
        subsections: nextSubsections.sort((a: any, b: any) => String(a.code).localeCompare(String(b.code))),
      };
    });
    this.offer = {
      ...this.offer,
      sections: nextSections.sort((a: any, b: any) => String(a.code).localeCompare(String(b.code))),
    };
    this.syncUiStateFromOffer();
  }

  private defaultScheduleDraft(kind: string | null | undefined = null): ScheduleDraft {
    return {
      day_of_week: 'LUNES',
      start_time: '07:40',
      end_time: '08:30',
      session_type: this.defaultScheduleSessionType(kind),
    };
  }

  private scheduleDraftFromSubsection(subsection: any): ScheduleDraft {
    const schedule = subsection?.schedules?.[0] ?? null;
    if (!schedule) {
      return this.defaultScheduleDraft(subsection?.kind);
    }
    return {
      day_of_week: schedule.day_of_week ?? 'LUNES',
      start_time: this.normalizeScheduleTime(schedule.start_time, '07:40'),
      end_time: this.normalizeScheduleTime(schedule.end_time, '08:30'),
      session_type: schedule.session_type ?? this.defaultScheduleSessionType(subsection?.kind),
    };
  }

  private effectiveCreateSectionGroupCount() {
    if (this.createSectionForm.mode === 'SINGLE' || this.courseUsesBaseGroupOnly) {
      return 1;
    }
    const requested = Math.max(2, Math.trunc(Number(this.createSectionForm.subsection_count || 2)));
    if (this.offer?.course_type === 'TEORICO_PRACTICO') {
      return requested + 1;
    }
    return requested;
  }

  private scheduleSessionTypeOptionsForKind(kind: string | null | undefined) {
    switch (kind) {
      case 'THEORY':
        return [{ value: 'THEORY', label: 'Teoria' }];
      case 'PRACTICE':
        return [
          { value: 'PRACTICE', label: 'Practica' },
          { value: 'LAB', label: 'Laboratorio' },
        ];
      default:
        return [
          { value: 'THEORY', label: 'Teoria' },
          { value: 'PRACTICE', label: 'Practica' },
          { value: 'LAB', label: 'Laboratorio' },
          { value: 'OTHER', label: 'Otro' },
        ];
    }
  }

  scheduleSessionTypeOptions(subsection: any) {
    return this.scheduleSessionTypeOptionsForKind(subsection?.kind);
  }

  canEditScheduleSessionType(subsection: any) {
    return this.scheduleSessionTypeOptions(subsection).length > 1;
  }

  private defaultScheduleSessionType(kind: string | null | undefined) {
    if (kind === 'THEORY') {
      return 'THEORY';
    }
    if (kind === 'PRACTICE') {
      return 'PRACTICE';
    }
    return 'THEORY';
  }

  private normalizeScheduleDraftForGroup(draft: ScheduleDraft, subsection: any) {
    const allowed = this.scheduleSessionTypeOptions(subsection);
    if (allowed.some((option) => option.value === draft.session_type)) {
      return draft;
    }
    return {
      ...draft,
      session_type: allowed[0]?.value ?? this.defaultScheduleSessionType(subsection?.kind),
    };
  }

  private scheduleMinutes(subsectionId: string) {
    const draft = this.scheduleDraftsBySubsectionId[subsectionId];
    if (!draft?.start_time || !draft?.end_time) {
      return null;
    }
    const start = this.toMinutes(draft.start_time);
    const end = this.toMinutes(draft.end_time);
    if (
      !Number.isFinite(start) ||
      !Number.isFinite(end) ||
      end <= start ||
      !this.isAcademicBlockAligned(start) ||
      !this.isAcademicBlockAligned(end)
    ) {
      return null;
    }
    const duration = end - start;
    if (duration % 50 !== 0) {
      return null;
    }
    return duration;
  }

  private toMinutes(value: string) {
    const [hours = '0', minutes = '0'] = String(value).split(':');
    return Number(hours) * 60 + Number(minutes);
  }

  private normalizeScheduleTime(value: string | null | undefined, fallback: string) {
    const normalized = String(value ?? '').slice(0, 5);
    return this.scheduleTimeOptions.some((option) => option.value === normalized) ? normalized : fallback;
  }

  private isAcademicBlockAligned(minutes: number) {
    return (
      minutes >= this.scheduleGridStartMinutes &&
      minutes <= this.scheduleGridEndMinutes &&
      (minutes - this.scheduleGridStartMinutes) % 50 === 0
    );
  }

  private buildScheduleTimeOptions() {
    const options: Array<{ value: string; label: string }> = [];
    for (
      let minutes = this.scheduleGridStartMinutes;
      minutes <= this.scheduleGridEndMinutes;
      minutes += 50
    ) {
      const value = this.minutesToTime(minutes);
      options.push({ value, label: value });
    }
    return options;
  }

  private minutesToTime(totalMinutes: number) {
    const hours = Math.floor(totalMinutes / 60)
      .toString()
      .padStart(2, '0');
    const minutes = Math.floor(totalMinutes % 60)
      .toString()
      .padStart(2, '0');
    return `${hours}:${minutes}`;
  }

  private conflictLabel(type: string | null | undefined) {
    switch (type) {
      case 'TEACHER_OVERLAP':
        return 'Cruce docente';
      case 'CLASSROOM_OVERLAP':
        return 'Cruce aula';
      case 'SUBSECTION_OVERLAP':
        return 'Cruce horario';
      case 'SECTION_OVERLAP':
        return 'Cruce seccion';
      default:
        return type ?? '';
    }
  }

  private normalizeSearchValue(value: string | null | undefined) {
    return `${value ?? ''}`
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .trim();
  }

  private subsectionIsReady(subsection: any) {
    return Boolean(
      subsection &&
        subsection.responsible_teacher_id &&
        subsection.shift &&
        subsection.course_modality_id &&
        (subsection.schedules?.length || 0) > 0,
    );
  }
}
