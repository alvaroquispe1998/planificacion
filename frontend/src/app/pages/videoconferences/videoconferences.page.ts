import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit, QueryList, ViewChildren } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MultiSelectComponent, MultiSelectOption } from '../../components/multi-select/multi-select.component';
import { DialogService } from '../../core/dialog.service';
import {
  FilterOptionsDto,
  VideoconferenceApiService,
  VideoconferenceFilterOptionsResponse,
  VideoconferencePreviewItem,
} from '../../services/videoconference-api.service';

type PreviewSelectionItem = VideoconferencePreviewItem & {
  selected: boolean;
};

@Component({
  selector: 'app-videoconferences-page',
  standalone: true,
  imports: [CommonModule, FormsModule, MultiSelectComponent],
  templateUrl: './videoconferences.page.html',
  styleUrl: './videoconferences.page.css',
})
export class VideoconferencesPageComponent implements OnInit {
  @ViewChildren(MultiSelectComponent) multiSelectComponents!: QueryList<MultiSelectComponent>;

  selectedCampuses: string[] = [];
  selectedFaculties: string[] = [];
  selectedPrograms: string[] = [];
  selectedCourses: string[] = [];
  selectedModality = '';
  selectedDays: string[] = [];

  campusOptions: MultiSelectOption[] = [];
  facultyOptions: MultiSelectOption[] = [];
  programOptions: MultiSelectOption[] = [];
  courseOptions: MultiSelectOption[] = [];
  modalityOptions: MultiSelectOption[] = [];
  dayOptions: MultiSelectOption[] = [];

  previewData: PreviewSelectionItem[] = [];
  startDate = '';
  endDate = '';
  generationResult: any = null;
  loading = false;
  filterOptionsLoading = false;
  hasSearched = false;

  private filterOptionsRequestId = 0;

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly dialog: DialogService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit() {
    this.refreshFilterOptions();
  }

  get totalSchedules() {
    return this.previewData.length;
  }

  get selectedCount() {
    return this.previewData.filter((item) => item.selected).length;
  }

  get allSelected() {
    return this.previewData.length > 0 && this.previewData.every((item) => item.selected);
  }

  get activeFilterLabels() {
    return [
      this.buildSelectionLabel('Sedes', this.selectedCampuses, this.campusOptions),
      this.buildSelectionLabel('Facultades', this.selectedFaculties, this.facultyOptions),
      this.buildSelectionLabel('Programas', this.selectedPrograms, this.programOptions),
      this.buildSelectionLabel('Cursos', this.selectedCourses, this.courseOptions),
      this.buildSingleSelectionLabel('Modalidad', this.selectedModality, this.modalityOptions),
      this.buildSelectionLabel('Dias', this.selectedDays, this.dayOptions),
    ].filter((label): label is string => Boolean(label));
  }

  get hasActiveFilters() {
    return this.activeFilterLabels.length > 0;
  }

  onCampusChange(selectedIds: string[]) {
    this.selectedCampuses = selectedIds;
    this.handleFilterChange();
  }

  onFacultyChange(selectedIds: string[]) {
    this.selectedFaculties = selectedIds;
    this.handleFilterChange();
  }

  onProgramChange(selectedIds: string[]) {
    this.selectedPrograms = selectedIds;
    this.handleFilterChange();
  }

  onCourseChange(selectedIds: string[]) {
    this.selectedCourses = selectedIds;
    this.handleFilterChange();
  }

  onModalityChange(selectedModality: string) {
    this.selectedModality = selectedModality;
    this.handleFilterChange();
  }

  onDaysChange(selectedIds: string[]) {
    this.selectedDays = selectedIds;
    this.handleFilterChange();
  }

  clearFilters() {
    this.selectedCampuses = [];
    this.selectedFaculties = [];
    this.selectedPrograms = [];
    this.selectedCourses = [];
    this.selectedModality = '';
    this.selectedDays = [];
    this.startDate = '';
    this.endDate = '';
    this.resetPreviewState();
    this.closeFilterDropdowns();
    this.blurActiveElement();
    this.refreshFilterOptions();
  }

  onLoad() {
    this.closeFilterDropdowns();
    this.blurActiveElement();
    this.loading = true;
    this.hasSearched = true;
    this.generationResult = null;
    this.cdr.detectChanges();

    this.api.preview(this.buildFilters()).subscribe({
      next: (data) => {
        this.previewData = this.sortPreviewData(data).map((item) => ({ ...item, selected: false }));
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: async () => {
        this.loading = false;
        this.cdr.detectChanges();
        await this.dialog.alert({
          title: 'No se pudo cargar la previsualizacion',
          message: 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  toggleAll() {
    const nextValue = !this.allSelected;
    this.previewData.forEach((item) => {
      item.selected = nextValue;
    });
  }

  getSubsectionDisplay(item: VideoconferencePreviewItem) {
    const subsection = item.subsection_code?.trim() || item.subsection_label?.trim();
    return subsection ? `Subseccion ${subsection}` : 'Sin subseccion';
  }

  async generate() {
    const selected = this.previewData.filter((item) => item.selected);
    if (!selected.length) {
      await this.dialog.alert({
        title: 'Seleccion requerida',
        message: 'Seleccione al menos un horario.',
      });
      return;
    }

    if (!this.startDate || !this.endDate) {
      await this.dialog.alert({
        title: 'Fechas requeridas',
        message: 'Seleccione fechas de inicio y fin.',
      });
      return;
    }

    if (this.startDate > this.endDate) {
      await this.dialog.alert({
        title: 'Rango invalido',
        message: 'La fecha de inicio no puede ser mayor que la fecha fin.',
      });
      return;
    }

    const confirmed = await this.dialog.confirm({
      title: 'Generar videoconferencias',
      message: `Se generaran ocurrencias para ${selected.length} horarios entre ${this.startDate} y ${this.endDate}. Deseas continuar?`,
      confirmLabel: 'Generar',
      cancelLabel: 'Cancelar',
    });
    if (!confirmed) {
      return;
    }

    this.loading = true;
    this.api
      .generate({
        scheduleIds: selected.map((item) => item.schedule_id),
        startDate: this.startDate,
        endDate: this.endDate,
      })
      .subscribe({
        next: async (res) => {
          this.generationResult = res;
          this.loading = false;
          await this.dialog.alert({
            title: 'Proceso finalizado',
            message: res.message || 'Las videoconferencias fueron procesadas.',
            tone: 'success',
          });
        },
        error: async () => {
          this.loading = false;
          await this.dialog.alert({
            title: 'No se pudieron generar las videoconferencias',
            message: 'Error generando videoconferencias.',
            tone: 'danger',
          });
        },
      });
  }

  private handleFilterChange() {
    this.resetPreviewState();
    this.refreshFilterOptions();
  }

  private refreshFilterOptions() {
    const requestId = ++this.filterOptionsRequestId;
    this.filterOptionsLoading = true;
    this.cdr.detectChanges();

    this.api.getFilterOptions(this.buildFilters()).subscribe({
      next: (data) => {
        if (requestId !== this.filterOptionsRequestId) {
          return;
        }

        const selectionsChanged = this.applyFilterOptions(data);
        if (selectionsChanged) {
          this.refreshFilterOptions();
          return;
        }

        this.filterOptionsLoading = false;
        this.cdr.detectChanges();
      },
      error: (error) => {
        if (requestId !== this.filterOptionsRequestId) {
          return;
        }

        console.error('Error loading dependent filter options', error);
        this.filterOptionsLoading = false;
        this.cdr.detectChanges();
      },
    });
  }

  private applyFilterOptions(data: VideoconferenceFilterOptionsResponse) {
    this.campusOptions = data.campuses;
    this.facultyOptions = data.faculties;
    this.programOptions = data.programs;
    this.courseOptions = data.courses;
    this.modalityOptions = data.modalities;
    this.dayOptions = data.days;

    const changedSelections = [
      this.retainAvailableSelections('selectedCampuses', this.campusOptions),
      this.retainAvailableSelections('selectedFaculties', this.facultyOptions),
      this.retainAvailableSelections('selectedPrograms', this.programOptions),
      this.retainAvailableSelections('selectedCourses', this.courseOptions),
      this.retainAvailableSingleSelection('selectedModality', this.modalityOptions),
      this.retainAvailableSelections('selectedDays', this.dayOptions),
    ];

    return changedSelections.some(Boolean);
  }

  private retainAvailableSelections(
    property:
      | 'selectedCampuses'
      | 'selectedFaculties'
      | 'selectedPrograms'
      | 'selectedCourses'
      | 'selectedDays',
    options: MultiSelectOption[],
  ) {
    const availableIds = new Set(options.map((option) => option.id));
    const current = this[property];
    const next = current.filter((id) => availableIds.has(id));
    const changed = next.length !== current.length;
    if (changed) {
      this[property] = next;
    }
    return changed;
  }

  private retainAvailableSingleSelection(
    property: 'selectedModality',
    options: MultiSelectOption[],
  ) {
    const current = this[property];
    if (!current) {
      return false;
    }

    const availableIds = new Set(options.map((option) => option.id));
    if (availableIds.has(current)) {
      return false;
    }

    this[property] = '';
    return true;
  }

  private resetPreviewState() {
    this.previewData = [];
    this.generationResult = null;
    this.hasSearched = false;
  }

  private buildFilters(): FilterOptionsDto {
    return {
      campusIds: this.selectedCampuses.length ? this.selectedCampuses : undefined,
      facultyIds: this.selectedFaculties.length ? this.selectedFaculties : undefined,
      programIds: this.selectedPrograms.length ? this.selectedPrograms : undefined,
      courseIds: this.selectedCourses.length ? this.selectedCourses : undefined,
      modality: this.selectedModality || undefined,
      days: this.selectedDays.length ? this.selectedDays : undefined,
    };
  }

  private sortPreviewData(items: VideoconferencePreviewItem[]) {
    const dayOrder: Record<string, number> = {
      LUNES: 1,
      MARTES: 2,
      MIERCOLES: 3,
      JUEVES: 4,
      VIERNES: 5,
      SABADO: 6,
      DOMINGO: 7,
    };

    return [...items].sort((left, right) => {
      const comparisons = [
        (left.campus_name || '').localeCompare(right.campus_name || ''),
        (left.faculty_name || '').localeCompare(right.faculty_name || ''),
        (left.program_name || '').localeCompare(right.program_name || ''),
        left.course_label.localeCompare(right.course_label),
        (left.section_code || '').localeCompare(right.section_code || ''),
        (left.subsection_code || '').localeCompare(right.subsection_code || ''),
        (dayOrder[left.day_of_week] || 99) - (dayOrder[right.day_of_week] || 99),
        left.start_time.localeCompare(right.start_time),
      ];

      return comparisons.find((value) => value !== 0) || 0;
    });
  }

  private closeFilterDropdowns() {
    this.multiSelectComponents?.forEach((component) => component.closeDropdown());
  }

  private blurActiveElement() {
    const active = document.activeElement;
    if (active instanceof HTMLElement) {
      active.blur();
    }
  }

  private buildSelectionLabel(label: string, selectedIds: string[], options: MultiSelectOption[]) {
    if (!selectedIds.length) {
      return null;
    }

    const selectedLabels = options
      .filter((option) => selectedIds.includes(option.id))
      .map((option) => option.label);

    if (!selectedLabels.length) {
      return `${label}: ${selectedIds.length} seleccionados`;
    }

    if (selectedLabels.length <= 2) {
      return `${label}: ${selectedLabels.join(', ')}`;
    }

    return `${label}: ${selectedLabels.length} seleccionados`;
  }

  private buildSingleSelectionLabel(label: string, selectedId: string, options: MultiSelectOption[]) {
    if (!selectedId) {
      return null;
    }

    const selectedOption = options.find((option) => option.id === selectedId);
    return `${label}: ${selectedOption?.label || selectedId}`;
  }
}
