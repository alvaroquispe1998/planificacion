import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, ElementRef, HostListener, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { forkJoin } from 'rxjs';
import {
  FilterCatalogOption,
  FilterOptionsDto,
  VideoconferenceApiService,
  VideoconferenceInheritanceCandidateItem,
  VideoconferenceInheritanceCatalogSchedule,
  VideoconferenceInheritanceItem,
} from '../../services/videoconference-api.service';

@Component({
  selector: 'app-videoconference-inheritances-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './videoconference-inheritances.page.html',
  styleUrl: './videoconference-inheritances.page.css',
})
export class VideoconferenceInheritancesPageComponent implements OnInit {
  loading = true;
  saving = false;
  catalogLoading = false;
  filtersLoading = false;
  error = '';
  message = '';

  periodOptions: FilterCatalogOption[] = [];
  campusOptions: FilterCatalogOption[] = [];
  facultyOptions: FilterCatalogOption[] = [];
  programOptions: FilterCatalogOption[] = [];

  mappings: VideoconferenceInheritanceItem[] = [];
  schedules: VideoconferenceInheritanceCatalogSchedule[] = [];
  inheritanceCandidates: VideoconferenceInheritanceCandidateItem[] = [];
  /** Set of candidate IDs where the user has manually swapped parent ↔ first child */
  invertedCandidateIds = new Set<string>();

  filters = {
    semesterId: '',
    facultyId: '',
  };

  private filterRequestId = 0;

  form = {
    id: '',
    parentCampusId: '',
    parentProgramId: '',
    parentCourseSearch: '',
    parentCourseId: '',
    parentSectionId: '',
    parentScheduleId: '',
    childCampusId: '',
    childProgramId: '',
    childCourseId: '',
    childSectionId: '',
    childScheduleId: '',
    notes: '',
    isActive: true,
  };

  candidateLoading = false;
  cleaningLegacy = false;
  candidateSavingId = '';
  parentCourseDropdownOpen = false;
  private suppressCascadeReset = false;

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly cdr: ChangeDetectorRef,
    private readonly elementRef: ElementRef,
  ) {}

  ngOnInit() {
    this.loadMappings();
    this.refreshFilters(true);
  }

  get canLoadCatalog() {
    return Boolean(this.filters.semesterId && this.filters.facultyId);
  }

  get parentCourseOptions() {
    const query = this.normalizeSearch(this.form.parentCourseSearch);
    let schedules = this.schedules;
    if (this.form.parentCampusId) {
      schedules = schedules.filter((s) => s.campus_id === this.form.parentCampusId);
    }
    if (this.form.parentProgramId) {
      schedules = schedules.filter((s) => s.program_id === this.form.parentProgramId);
    }
    const options = this.buildCourseOptions(schedules);
    if (!query) {
      return options;
    }
    return options.filter((item) => this.normalizeSearch(item.label).includes(query));
  }

  get selectedParentCourseLabel() {
    if (!this.form.parentCourseId) {
      return 'Selecciona un curso';
    }
    return (
      this.buildCourseOptions(this.schedules).find((item) => item.id === this.form.parentCourseId)?.label ??
      'Selecciona un curso'
    );
  }

  get parentSectionOptions() {
    if (!this.form.parentCourseId) {
      return [] as Array<{ id: string; label: string }>;
    }
    return this.buildSectionOptions(
      this.schedules.filter(
        (item) =>
          item.course_id === this.form.parentCourseId &&
          (!this.form.parentCampusId || item.campus_id === this.form.parentCampusId) &&
          (!this.form.parentProgramId || item.program_id === this.form.parentProgramId),
      ),
    );
  }

  get parentScheduleOptions() {
    if (!this.form.parentSectionId) {
      return [] as VideoconferenceInheritanceCatalogSchedule[];
    }
    return this.schedules.filter(
      (item) =>
        item.course_id === this.form.parentCourseId &&
        item.section_id === this.form.parentSectionId &&
        (!this.form.parentCampusId || item.campus_id === this.form.parentCampusId) &&
        (!this.form.parentProgramId || item.program_id === this.form.parentProgramId),
    );
  }

  get selectedParentSchedule() {
    return this.schedules.find((item) => item.schedule_id === this.form.parentScheduleId) ?? null;
  }

  get childCandidateSchedules() {
    const parent = this.selectedParentSchedule;
    if (!parent) {
      return [] as VideoconferenceInheritanceCatalogSchedule[];
    }
    const blockMap = this.buildContinuousBlockMap(this.schedules);
    const parentBlock = blockMap.get(parent.schedule_id) ?? {
      start_time: parent.start_time,
      end_time: parent.end_time,
    };
    let schedules = this.schedules.filter((item) => {
      if (item.schedule_id === parent.schedule_id) {
        return false;
      }
      if (
        item.day_of_week !== parent.day_of_week
      ) {
        return false;
      }
      const itemBlock = blockMap.get(item.schedule_id) ?? {
        start_time: item.start_time,
        end_time: item.end_time,
      };
      if (itemBlock.start_time !== parentBlock.start_time || itemBlock.end_time !== parentBlock.end_time) {
        return false;
      }
      if (!item.is_child_inherited) {
        return true;
      }
      return item.schedule_id === this.form.childScheduleId;
    });

    if (this.form.childCampusId) {
      schedules = schedules.filter((item) => item.campus_id === this.form.childCampusId);
    }
    if (this.form.childProgramId) {
      schedules = schedules.filter((item) => item.program_id === this.form.childProgramId);
    }
    return schedules;
  }

  get childCourseOptions() {
    return this.buildCourseOptions(this.childCandidateSchedules);
  }

  get childSectionOptions() {
    if (!this.form.childCourseId) {
      return [] as Array<{ id: string; label: string }>;
    }
    return this.buildSectionOptions(
      this.childCandidateSchedules.filter((item) => item.course_id === this.form.childCourseId),
    );
  }

  get childScheduleOptions() {
    if (!this.form.childSectionId) {
      return [] as VideoconferenceInheritanceCatalogSchedule[];
    }
    return this.childCandidateSchedules.filter(
      (item) =>
        item.course_id === this.form.childCourseId &&
        item.section_id === this.form.childSectionId,
    );
  }

  get hasSchedules() {
    return this.schedules.length > 0;
  }

  get catalogHint() {
    if (this.catalogLoading) {
      return 'Buscando horarios disponibles para este contexto...';
    }
    if (!this.canLoadCatalog) {
      return 'Selecciona periodo y facultad para cargar el catalogo.';
    }
    if (!this.hasSchedules) {
      return 'No hay horarios disponibles para la combinacion seleccionada.';
    }
    return `${this.schedules.length} horario(s) disponibles para mapear.`;
  }

  refreshFilters(initialLoad = false) {
    const requestId = ++this.filterRequestId;
    this.filtersLoading = true;
    forkJoin({
      periods: this.api.getFilterOptions({}),
      campuses: this.api.getFilterOptions(this.buildCampusFilterRequest()),
      faculties: this.api.getFilterOptions(this.buildFacultyFilterRequest()),
      programs: this.api.getFilterOptions(this.buildProgramFilterRequest()),
    }).subscribe({
      next: (result) => {
        if (requestId !== this.filterRequestId) {
          return;
        }
        this.periodOptions = result.periods.periods ?? [];
        this.campusOptions = result.campuses.campuses ?? [];
        this.facultyOptions = result.faculties.faculties ?? [];
        this.programOptions = result.programs.programs ?? [];
        this.retainContextSelections();
        this.filtersLoading = false;
        this.loading = false;
        if (!initialLoad && this.canLoadCatalog) {
          this.loadCatalogSchedules();
          return;
        }
        if (!this.canLoadCatalog) {
          this.schedules = [];
        }
        this.cdr.detectChanges();
      },
      error: (err) => {
        if (requestId !== this.filterRequestId) {
          return;
        }
        this.error = err?.error?.message ?? 'No se pudieron cargar los filtros.';
        this.filtersLoading = false;
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  loadMappings() {
    this.api.listInheritances().subscribe({
      next: (rows) => {
        this.mappings = Array.isArray(rows) ? rows : [];
        this.cdr.detectChanges();
      },
      error: () => {
        this.cdr.detectChanges();
      },
    });
  }

  onFiltersChanged() {
    this.resetFormSelections();
    this.inheritanceCandidates = [];
    this.error = '';
    this.message = '';
    this.refreshFilters();
  }

  loadCatalogSchedules() {
    if (!this.canLoadCatalog) {
      this.schedules = [];
      this.catalogLoading = false;
      this.cdr.detectChanges();
      return;
    }
    this.catalogLoading = true;
    this.api
      .getInheritanceCatalog({
        semesterId: this.filters.semesterId,
        facultyId: this.filters.facultyId,
        campusId: '',
        programId: '',
      })
      .subscribe({
        next: (result) => {
          this.schedules = Array.isArray(result?.schedules) ? result.schedules : [];
          this.catalogLoading = false;
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo cargar el catalogo de horarios.';
          this.schedules = [];
          this.catalogLoading = false;
          this.cdr.detectChanges();
        },
      });
  }

  onFormContextChange() {
    if (this.suppressCascadeReset) {
      return;
    }
    this.resetFormSelections();
    this.inheritanceCandidates = [];
    this.cdr.detectChanges();
  }

  onParentCourseChange() {
    this.parentCourseDropdownOpen = false;
    this.form.parentCourseSearch = '';
    this.form.parentSectionId = '';
    this.form.parentScheduleId = '';
    this.form.childCampusId = '';
    this.form.childProgramId = '';
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onParentSectionChange() {
    if (this.suppressCascadeReset) {
      return;
    }
    this.form.parentScheduleId = '';
    this.form.childCampusId = '';
    this.form.childProgramId = '';
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onParentScheduleChange() {
    if (this.suppressCascadeReset) {
      return;
    }
    this.form.childCampusId = '';
    this.form.childProgramId = '';
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onChildContextChange() {
    if (this.suppressCascadeReset) {
      return;
    }
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onChildCourseChange() {
    if (this.suppressCascadeReset) {
      return;
    }
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onChildSectionChange() {
    if (this.suppressCascadeReset) {
      return;
    }
    this.form.childScheduleId = '';
  }

  exportToExcel() {
    const header = [
      'Padre - Curso',
      'Padre - Seccion',
      'Padre - Grupo',
      'Padre - Horario',
      'Hijo - Curso',
      'Hijo - Seccion',
      'Hijo - Grupo',
      'Hijo - Horario',
      'Creada el',
      'Estado',
    ];

    const rows = this.mappings.map((item) => [
      item.parent?.course_label ?? '',
      item.parent?.section_label ?? '',
      item.parent?.subsection_label ?? '',
      item.parent?.schedule_label ?? '',
      item.child?.course_label ?? '',
      item.child?.section_label ?? '',
      item.child?.subsection_label ?? '',
      item.child?.schedule_label ?? '',
      item.created_at ? new Date(item.created_at).toLocaleString('es-PE') : '',
      item.is_active ? 'Activa' : 'Inactiva',
    ]);

    const escape = (value: string) => {
      if (value.includes(',') || value.includes('"') || value.includes('\n')) {
        return '"' + value.replace(/"/g, '""') + '"';
      }
      return value;
    };

    const csv = [header, ...rows]
      .map((line) => line.map(escape).join(','))
      .join('\r\n');

    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    const ts = new Date().toISOString().slice(0, 10);
    link.href = url;
    link.download = `herencias-zoom-${ts}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  edit(item: VideoconferenceInheritanceItem) {
    const parent = this.schedules.find((row) => row.schedule_id === item.parent_schedule_id) ?? null;
    const child = this.schedules.find((row) => row.schedule_id === item.child_schedule_id) ?? null;
    const parentOption = parent ? this.buildCourseOptions([parent])[0] : null;
    this.suppressCascadeReset = true;
    this.form = {
      id: item.id,
      parentCampusId: parent?.campus_id ?? '',
      parentProgramId: parent?.program_id ?? '',
      parentCourseSearch: parentOption?.label ?? '',
      parentCourseId: parent?.course_id ?? '',
      parentSectionId: parent?.section_id ?? '',
      parentScheduleId: item.parent_schedule_id,
      childCampusId: child?.campus_id ?? '',
      childProgramId: child?.program_id ?? '',
      childCourseId: child?.course_id ?? '',
      childSectionId: child?.section_id ?? '',
      childScheduleId: item.child_schedule_id,
      notes: item.notes ?? '',
      isActive: item.is_active,
    };
    setTimeout(() => {
      this.suppressCascadeReset = false;
      this.cdr.detectChanges();
    }, 0);
    this.inheritanceCandidates = [];
    this.message = '';
    this.error = '';
  }

  resetForm() {
    this.form = {
      id: '',
      parentCampusId: '',
      parentProgramId: '',
      parentCourseSearch: '',
      parentCourseId: '',
      parentSectionId: '',
      parentScheduleId: '',
      childCampusId: '',
      childProgramId: '',
      childCourseId: '',
      childSectionId: '',
      childScheduleId: '',
      notes: '',
      isActive: true,
    };
  }

  startNewMapping() {
    this.resetForm();
    this.inheritanceCandidates = [];
    this.parentCourseDropdownOpen = false;
    this.message = '';
    this.error = '';
    this.cdr.detectChanges();
  }

  toggleParentCourseDropdown() {
    if (!this.hasSchedules) {
      return;
    }
    this.parentCourseDropdownOpen = !this.parentCourseDropdownOpen;
    if (!this.parentCourseDropdownOpen) {
      this.form.parentCourseSearch = '';
    }
  }

  selectParentCourse(courseId: string) {
    this.form.parentCourseId = courseId;
    this.onParentCourseChange();
    this.cdr.detectChanges();
  }

  save() {
    if (!this.form.parentScheduleId || !this.form.childScheduleId) {
      this.error = 'Selecciona horario padre y horario hijo.';
      this.cdr.detectChanges();
      return;
    }
    this.saving = true;
    this.error = '';
    this.message = '';
    const request = this.form.id
      ? this.api.updateInheritance(this.form.id, {
          parentScheduleId: this.form.parentScheduleId,
          childScheduleId: this.form.childScheduleId,
          notes: this.form.notes,
          isActive: this.form.isActive,
        })
      : this.api.createInheritance({
          parentScheduleId: this.form.parentScheduleId,
          childScheduleId: this.form.childScheduleId,
          notes: this.form.notes,
          isActive: this.form.isActive,
        });

    request.subscribe({
      next: () => {
        this.saving = false;
        this.message = this.form.id ? 'Herencia actualizada.' : 'Herencia creada.';
        this.resetForm();
        this.inheritanceCandidates = [];
        this.loadMappings();
        this.loadCatalogSchedules();
      },
      error: (err) => {
        this.saving = false;
        this.error = err?.error?.message ?? 'No se pudo guardar la herencia.';
        this.cdr.detectChanges();
      },
    });
  }

  previewCandidates() {
    if (!this.filters.semesterId || !this.filters.facultyId) {
      return;
    }
    this.candidateLoading = true;
    this.error = '';
    this.message = '';

    this.api
      .previewInheritanceCandidates({
        semesterId: this.filters.semesterId,
        facultyId: this.filters.facultyId,
      })
      .subscribe({
        next: (res) => {
          this.candidateLoading = false;
          this.inheritanceCandidates = Array.isArray(res?.items) ? res.items : [];
          this.message = res.count
            ? `Se encontraron ${res.count} posible(s) herencia(s) para revisar.`
            : 'No se encontraron posibles herencias con la regla actual.';
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.candidateLoading = false;
          this.inheritanceCandidates = [];
          this.error = err?.error?.message ?? 'No se pudieron buscar posibles herencias.';
          this.cdr.detectChanges();
        },
      });
  }

  applyCandidate(item: VideoconferenceInheritanceCandidateItem) {
    if (this.saving || this.candidateSavingId) {
      return;
    }
    const childSchedules = (item.children ?? [])
      .map((child) => child.schedule_id)
      .filter((value) => Boolean(value));
    if (!childSchedules.length) {
      this.error = 'La sugerencia no tiene horarios hijo para guardar.';
      this.cdr.detectChanges();
      return;
    }
    this.candidateSavingId = item.id;
    this.error = '';
    this.message = '';
    forkJoin(
      childSchedules.map((childScheduleId) =>
        this.api.createInheritance({
          parentScheduleId: item.parent.schedule_id,
          childScheduleId,
          notes: 'Sugerencia aplicada desde preview de herencias.',
          isActive: true,
        }),
      ),
    ).subscribe({
        next: () => {
          this.candidateSavingId = '';
          this.message = 'Sugerencia guardada como herencia activa.';
          this.inheritanceCandidates = this.inheritanceCandidates.filter(
            (candidate) => candidate.id !== item.id,
          );
          this.resetForm();
          this.parentCourseDropdownOpen = false;
          this.loadMappings();
          this.loadCatalogSchedules();
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.candidateSavingId = '';
          this.error = err?.error?.message ?? 'No se pudo guardar la sugerencia.';
          this.cdr.detectChanges();
        },
      });
  }

  dismissCandidate(item: VideoconferenceInheritanceCandidateItem) {
    this.inheritanceCandidates = this.inheritanceCandidates.filter(
      (candidate) => candidate.id !== item.id,
    );
    this.cdr.detectChanges();
  }

  /**
   * Swaps parent ↔ first child for a candidate so the user can choose which
   * schedule should be the actual parent before applying.
   */
  invertCandidate(item: VideoconferenceInheritanceCandidateItem) {
    const firstChild = item.children[0];
    if (!firstChild) {
      return;
    }
    const originalParent = item.parent;
    // Swap in-place
    (item as any).parent = firstChild;
    item.children[0] = originalParent as any;

    if (this.invertedCandidateIds.has(item.id)) {
      this.invertedCandidateIds.delete(item.id);
    } else {
      this.invertedCandidateIds.add(item.id);
    }
    this.cdr.detectChanges();
  }

  cleanupLegacyMappings() {
    if (!window.confirm('Se eliminaran solo las herencias automaticas antiguas. Deseas continuar?')) {
      return;
    }
    this.cleaningLegacy = true;
    this.error = '';
    this.message = '';

    this.api
      .cleanupLegacyInheritances({
        semesterId: this.filters.semesterId || undefined,
        facultyId: this.filters.facultyId || undefined,
      })
      .subscribe({
        next: (res) => {
          this.cleaningLegacy = false;
          this.message = res.count
            ? `Se eliminaron ${res.count} herencia(s) automatica(s) antigua(s).`
            : 'No se encontraron herencias automaticas antiguas para limpiar.';
          this.loadMappings();
          if (this.canLoadCatalog) {
            this.loadCatalogSchedules();
          }
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.cleaningLegacy = false;
          this.error = err?.error?.message ?? 'No se pudieron limpiar las herencias automaticas antiguas.';
          this.cdr.detectChanges();
        },
      });
  }

  toggleActive(item: VideoconferenceInheritanceItem) {
    this.api
      .updateInheritance(item.id, {
        isActive: !item.is_active,
      })
      .subscribe({
        next: () => {
          this.message = !item.is_active ? 'Herencia activada.' : 'Herencia desactivada.';
          this.loadMappings();
          this.loadCatalogSchedules();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo actualizar el estado.';
          this.cdr.detectChanges();
        },
      });
  }

  remove(item: VideoconferenceInheritanceItem) {
    if (!window.confirm('Se eliminara este mapeo de herencia. Deseas continuar?')) {
      return;
    }
    this.api.deleteInheritance(item.id).subscribe({
      next: () => {
        this.message = 'Herencia eliminada.';
        if (this.form.id === item.id) {
          this.resetForm();
        }
        this.loadMappings();
        this.loadCatalogSchedules();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo eliminar la herencia.';
        this.cdr.detectChanges();
      },
    });
  }

  private resetFormSelections() {
    this.form.parentCourseSearch = '';
    this.form.parentCourseId = '';
    this.form.parentSectionId = '';
    this.form.parentScheduleId = '';
    this.form.childCampusId = '';
    this.form.childProgramId = '';
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  private retainContextSelections() {
    this.filters.semesterId = this.retainSingleSelection(this.filters.semesterId, this.periodOptions);
    this.filters.facultyId = this.retainSingleSelection(this.filters.facultyId, this.facultyOptions);
  }

  private retainSingleSelection(current: string, options: FilterCatalogOption[]) {
    if (!current) {
      return '';
    }
    return options.some((item) => item.id === current) ? current : '';
  }

  private buildCampusFilterRequest(): FilterOptionsDto {
    return {
      semesterId: this.filters.semesterId || undefined,
    };
  }

  private buildFacultyFilterRequest(): FilterOptionsDto {
    return {
      semesterId: this.filters.semesterId || undefined,
    };
  }

  private buildProgramFilterRequest(): FilterOptionsDto {
    return {
      semesterId: this.filters.semesterId || undefined,
      facultyIds: this.filters.facultyId ? [this.filters.facultyId] : undefined,
    };
  }

  private buildCourseOptions(items: VideoconferenceInheritanceCatalogSchedule[]) {
    const map = new Map<string, string>();
    for (const item of items) {
      map.set(item.course_id, item.course_label);
    }
    return Array.from(map.entries())
      .map(([id, label]) => ({ id, label }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }

  private buildSectionOptions(items: VideoconferenceInheritanceCatalogSchedule[]) {
    const map = new Map<string, string>();
    for (const item of items) {
      map.set(
        item.section_id,
        `${this.formatSectionOnlyLabel(item)} | Vacantes: ${item.section_projected_vacancies ?? 0}`,
      );
    }
    return Array.from(map.entries())
      .map(([id, label]) => ({ id, label }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }

  private formatSectionOnlyLabel(item: Pick<VideoconferenceInheritanceCatalogSchedule, 'section_code' | 'section_label'>) {
    const sectionCode = String(item.section_code ?? '').trim();
    if (sectionCode) {
      return sectionCode;
    }
    const value = String(item.section_label ?? '').trim();
    const parts = value
      .split('|')
      .map((part) => part.trim())
      .filter(Boolean);
    return parts.length ? parts[parts.length - 1] : value;
  }

  private normalizeSearch(value: string) {
    return String(value ?? '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
  }

  private buildContinuousBlockMap(items: VideoconferenceInheritanceCatalogSchedule[]) {
    const groups = new Map<string, VideoconferenceInheritanceCatalogSchedule[]>();
    for (const item of items) {
      const teacherKey = String(item.teacher_name ?? '').trim().toUpperCase();
      const key = [
        item.section_id,
        item.course_id,
        item.day_of_week,
        item.campus_id ?? '',
        item.program_id ?? '',
        teacherKey,
      ].join('|');
      const current = groups.get(key) ?? [];
      current.push(item);
      groups.set(key, current);
    }

    const blockMap = new Map<string, { start_time: string; end_time: string }>();
    for (const groupRows of groups.values()) {
      const ordered = [...groupRows].sort((left, right) => left.start_time.localeCompare(right.start_time));
      let blockRows: VideoconferenceInheritanceCatalogSchedule[] = [ordered[0]];
      for (let index = 1; index < ordered.length; index += 1) {
        const previous = blockRows[blockRows.length - 1];
        const row = ordered[index];
        if (previous.end_time === row.start_time) {
          blockRows.push(row);
          continue;
        }
        this.assignBlockRows(blockMap, blockRows);
        blockRows = [row];
      }
      this.assignBlockRows(blockMap, blockRows);
    }
    return blockMap;
  }

  private assignBlockRows(
    blockMap: Map<string, { start_time: string; end_time: string }>,
    blockRows: VideoconferenceInheritanceCatalogSchedule[],
  ) {
    if (!blockRows.length) {
      return;
    }
    const blockStart = blockRows[0].start_time;
    const blockEnd = blockRows[blockRows.length - 1].end_time;
    for (const row of blockRows) {
      blockMap.set(row.schedule_id, {
        start_time: blockStart,
        end_time: blockEnd,
      });
    }
  }

  formatCandidateLine(
    side: Pick<
      VideoconferenceInheritanceCandidateItem['parent'],
      'campus_name' | 'program_name' | 'course_label' | 'vc_section_name' | 'section_label' | 'subsection_label'
    >,
    item: Pick<VideoconferenceInheritanceCandidateItem, 'cycle' | 'day_label' | 'start_time' | 'end_time'>,
    vacancies?: number | null,
  ) {
    const sectionOnly = String(side.section_label ?? '')
      .split('|')
      .map((part) => part.trim())
      .filter(Boolean)
      .pop() || side.section_label || '(Sin seccion)';
    const subsectionRaw = String(side.subsection_label ?? '')
      .replace(/grupo\s+/gi, '')
      .replace(/\s+/g, ' ')
      .trim();
    const sectionFull = subsectionRaw
      ? `${sectionOnly} (${subsectionRaw})`
      : sectionOnly;
    const vcSection = String(side.vc_section_name ?? '')
      .replace(/\s*-\s*/g, '-')
      .trim();
    const sectionToShow = vcSection || sectionFull;

    return [
      side.campus_name || '(Sin sede)',
      side.program_name || '(Sin programa)',
      side.course_label,
      `Ciclo ${item.cycle ?? '-'}`,
      sectionToShow,
      `Vacantes: ${vacancies ?? 0}`,
      item.day_label,
      `${item.start_time} - ${item.end_time}`,
    ]
      .map((part) => String(part ?? '').trim())
      .filter(Boolean)
      .join(' - ');
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: Event) {
    const target = event.target as Node | null;
    if (!target || this.elementRef.nativeElement.contains(target)) {
      return;
    }
    this.parentCourseDropdownOpen = false;
    this.form.parentCourseSearch = '';
  }
}
