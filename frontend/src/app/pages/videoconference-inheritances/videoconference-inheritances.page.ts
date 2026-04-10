import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, ElementRef, HostListener, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {
  FilterCatalogOption,
  FilterOptionsDto,
  VideoconferenceApiService,
  VideoconferenceInheritanceCatalogSchedule,
  VideoconferenceInheritanceItem,
} from '../../services/videoconference-api.service';
import { forkJoin } from 'rxjs';

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

  filters = {
    semesterId: '',
    campusId: '',
    facultyId: '',
    programId: '',
  };

  private filterRequestId = 0;

  form = {
    id: '',
    parentCourseSearch: '',
    parentCourseId: '',
    parentSectionId: '',
    parentScheduleId: '',
    childCourseId: '',
    childSectionId: '',
    childScheduleId: '',
    notes: '',
    isActive: true,
  };

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly cdr: ChangeDetectorRef,
    private readonly elementRef: ElementRef,
  ) {}

  parentCourseDropdownOpen = false;

  ngOnInit() {
    this.loadMappings();
    this.refreshFilters(true);
  }

  get canLoadCatalog() {
    return Boolean(
      this.filters.semesterId &&
        this.filters.campusId &&
        this.filters.facultyId &&
        this.filters.programId,
    );
  }

  get parentCourseOptions() {
    const query = this.normalizeSearch(this.form.parentCourseSearch);
    const options = this.buildCourseOptions(this.schedules);
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
      this.schedules.filter((item) => item.course_id === this.form.parentCourseId),
    );
  }

  get parentScheduleOptions() {
    if (!this.form.parentSectionId) {
      return [] as VideoconferenceInheritanceCatalogSchedule[];
    }
    return this.schedules.filter(
      (item) =>
        item.course_id === this.form.parentCourseId &&
        item.section_id === this.form.parentSectionId,
    );
  }

  get selectedParentSchedule() {
    return this.schedules.find((item) => item.schedule_id === this.form.parentScheduleId) ?? null;
  }

  get childCourseOptions() {
    return this.buildCourseOptions(this.childCandidateSchedules);
  }

  get childSectionOptions() {
    if (!this.form.childCourseId) {
      return [] as Array<{ id: string; label: string }>;
    }
    return this.buildSectionOptions(
      this.childCandidateSchedules.filter(
        (item) => item.course_id === this.form.childCourseId,
      ),
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

  get childCandidateSchedules() {
    const parent = this.selectedParentSchedule;
    if (!parent) {
      return [] as VideoconferenceInheritanceCatalogSchedule[];
    }
    return this.schedules.filter((item) => {
      if (item.schedule_id === parent.schedule_id) {
        return false;
      }
      if (
        item.day_of_week !== parent.day_of_week ||
        item.start_time !== parent.start_time ||
        item.end_time !== parent.end_time
      ) {
        return false;
      }
      if (!item.is_child_inherited) {
        return true;
      }
      return item.schedule_id === this.form.childScheduleId;
    });
  }

  get hasSchedules() {
    return this.schedules.length > 0;
  }

  get catalogHint() {
    if (this.catalogLoading) {
      return 'Buscando horarios disponibles para este contexto...';
    }
    if (!this.canLoadCatalog) {
      return 'Selecciona periodo, sede, facultad y programa para cargar el catalogo.';
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
    })
      .subscribe({
      next: (result) => {
        if (requestId !== this.filterRequestId) {
          return;
        }
        this.periodOptions = result.periods?.periods ?? [];
        this.campusOptions = result.campuses?.campuses ?? [];
        this.facultyOptions = result.faculties?.faculties ?? [];
        this.programOptions = result.programs?.programs ?? [];
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
        campusId: this.filters.campusId,
        facultyId: this.filters.facultyId,
        programId: this.filters.programId,
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

  private buildCampusFilterRequest(): FilterOptionsDto {
    return {
      semesterId: this.filters.semesterId || undefined,
    };
  }

  private buildFacultyFilterRequest(): FilterOptionsDto {
    return {
      semesterId: this.filters.semesterId || undefined,
      campusIds: this.filters.campusId ? [this.filters.campusId] : undefined,
    };
  }

  private buildProgramFilterRequest(): FilterOptionsDto {
    return {
      semesterId: this.filters.semesterId || undefined,
      campusIds: this.filters.campusId ? [this.filters.campusId] : undefined,
      facultyIds: this.filters.facultyId ? [this.filters.facultyId] : undefined,
    };
  }

  onParentCourseChange() {
    this.parentCourseDropdownOpen = false;
    this.form.parentCourseSearch = '';
    this.form.parentSectionId = '';
    this.form.parentScheduleId = '';
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onParentSectionChange() {
    this.form.parentScheduleId = '';
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onParentScheduleChange() {
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onChildCourseChange() {
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  onChildSectionChange() {
    this.form.childScheduleId = '';
  }

  edit(item: VideoconferenceInheritanceItem) {
    const parent = this.schedules.find((row) => row.schedule_id === item.parent_schedule_id) ?? null;
    const child = this.schedules.find((row) => row.schedule_id === item.child_schedule_id) ?? null;
    const parentOption = parent ? this.buildCourseOptions([parent])[0] : null;
    this.form = {
      id: item.id,
      parentCourseSearch: parentOption?.label ?? '',
      parentCourseId: parent?.course_id ?? '',
      parentSectionId: parent?.section_id ?? '',
      parentScheduleId: item.parent_schedule_id,
      childCourseId: child?.course_id ?? '',
      childSectionId: child?.section_id ?? '',
      childScheduleId: item.child_schedule_id,
      notes: item.notes ?? '',
      isActive: item.is_active,
    };
    this.message = '';
    this.error = '';
  }

  resetForm() {
    this.form = {
      id: '',
      parentCourseSearch: '',
      parentCourseId: '',
      parentSectionId: '',
      parentScheduleId: '',
      childCourseId: '',
      childSectionId: '',
      childScheduleId: '',
      notes: '',
      isActive: true,
    };
  }

  startNewMapping() {
    this.resetForm();
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
    this.form.childCourseId = '';
    this.form.childSectionId = '';
    this.form.childScheduleId = '';
  }

  private retainContextSelections() {
    this.filters.semesterId = this.retainSingleSelection(this.filters.semesterId, this.periodOptions);
    this.filters.campusId = this.retainSingleSelection(this.filters.campusId, this.campusOptions);
    this.filters.facultyId = this.retainSingleSelection(this.filters.facultyId, this.facultyOptions);
    this.filters.programId = this.retainSingleSelection(this.filters.programId, this.programOptions);
  }

  private retainSingleSelection(current: string, options: FilterCatalogOption[]) {
    if (!current) {
      return '';
    }
    return options.some((item) => item.id === current) ? current : '';
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
    const parts = value.split('|').map((item) => item.trim()).filter(Boolean);
    return parts.length ? parts[parts.length - 1] : value;
  }

  private normalizeSearch(value: string) {
    return String(value ?? '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
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
