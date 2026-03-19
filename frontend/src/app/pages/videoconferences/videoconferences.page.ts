import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MultiSelectComponent, MultiSelectOption } from '../../components/multi-select/multi-select.component';
import { DialogService } from '../../core/dialog.service';
import {
  FilterOptionsDto,
  VideoconferenceApiService,
  VideoconferencePreviewItem,
} from '../../services/videoconference-api.service';

@Component({
  selector: 'app-videoconferences-page',
  standalone: true,
  imports: [CommonModule, FormsModule, MultiSelectComponent],
  templateUrl: './videoconferences.page.html',
  styleUrl: './videoconferences.page.css',
})
export class VideoconferencesPageComponent implements OnInit {
  selectedFaculties: string[] = [];
  selectedPrograms: string[] = [];
  selectedCourses: string[] = [];
  selectedSections: string[] = [];
  selectedModality = '';
  selectedDays: string[] = [];

  faculties: any[] = [];
  programs: any[] = [];
  courses: any[] = [];
  sections: any[] = [];

  facultyOptions: MultiSelectOption[] = [];
  programOptions: MultiSelectOption[] = [];
  courseOptions: MultiSelectOption[] = [];
  sectionOptions: MultiSelectOption[] = [];
  dayOptions: MultiSelectOption[] = [
    { id: 'LUNES', label: 'Lunes' },
    { id: 'MARTES', label: 'Martes' },
    { id: 'MIERCOLES', label: 'Miercoles' },
    { id: 'JUEVES', label: 'Jueves' },
    { id: 'VIERNES', label: 'Viernes' },
    { id: 'SABADO', label: 'Sabado' },
    { id: 'DOMINGO', label: 'Domingo' },
  ];

  previewData: VideoconferencePreviewItem[] = [];
  allSelected = false;

  startDate = '';
  endDate = '';
  generationResult: any = null;

  loading = false;

  private programsSub: any;
  private coursesSub: any;
  private sectionsSub: any;

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly dialog: DialogService,
  ) {}

  ngOnInit() {
    this.loadFaculties();
  }

  loadFaculties() {
    this.api.getFaculties().subscribe({
      next: (data) => {
        this.faculties = data;
        this.facultyOptions = data.map((faculty) => ({
          id: faculty.id,
          label: faculty.name || '(Sin nombre)',
        }));
      },
      error: (err) => console.error('Error loading faculties', err),
    });
  }

  onFacultyChange(selectedIds: string[]) {
    this.selectedFaculties = selectedIds;
    this.selectedPrograms = [];
    this.selectedCourses = [];
    this.selectedSections = [];
    this.programs = [];
    this.courses = [];
    this.sections = [];
    this.programOptions = [];
    this.courseOptions = [];
    this.sectionOptions = [];

    if (this.programsSub) {
      this.programsSub.unsubscribe();
    }

    if (this.selectedFaculties.length > 0) {
      this.programsSub = this.api.getPrograms(this.selectedFaculties).subscribe({
        next: (data) => {
          this.programs = data;
          this.programOptions = data.map((program) => ({ id: program.id, label: program.name }));
          if (data.length === 0) {
            console.warn('No programs found for selected faculties');
          }
        },
        error: (err) => {
          console.error('Error loading programs', err);
          void this.dialog.alert({
            title: 'No se pudieron cargar los programas',
            message: 'Verifica la conexion e intenta nuevamente.',
            tone: 'danger',
          });
        },
      });
    }
  }

  onProgramChange(selectedIds: string[]) {
    this.selectedPrograms = selectedIds;
    this.selectedCourses = [];
    this.selectedSections = [];
    this.courses = [];
    this.sections = [];
    this.courseOptions = [];
    this.sectionOptions = [];

    if (this.coursesSub) {
      this.coursesSub.unsubscribe();
    }

    if (this.selectedPrograms.length > 0) {
      this.coursesSub = this.api.getCourses(this.selectedPrograms).subscribe({
        next: (data) => {
          this.courses = data;
          this.courseOptions = data.map((course) => ({
            id: course.id,
            label: `${course.name} (${course.code})`,
          }));
        },
        error: (err) => console.error('Error loading courses', err),
      });
    }
  }

  onCourseChange(selectedIds: string[]) {
    this.selectedCourses = selectedIds;
    this.selectedSections = [];
    this.sections = [];
    this.sectionOptions = [];

    if (this.sectionsSub) {
      this.sectionsSub.unsubscribe();
    }

    if (this.selectedCourses.length > 0) {
      this.sectionsSub = this.api.getSections(this.selectedCourses).subscribe({
        next: (data) => {
          this.sections = data;
          this.sectionOptions = data.map((section) => ({ id: section.id, label: section.name }));
        },
        error: (err) => console.error('Error loading sections', err),
      });
    }
  }

  onLoad() {
    this.loading = true;
    const filters: FilterOptionsDto = {
      facultyIds: this.selectedFaculties.length ? this.selectedFaculties : undefined,
      programIds: this.selectedPrograms.length ? this.selectedPrograms : undefined,
      courseIds: this.selectedCourses.length ? this.selectedCourses : undefined,
      sectionIds: this.selectedSections.length ? this.selectedSections : undefined,
      modality: this.selectedModality || undefined,
      days: this.selectedDays.length ? this.selectedDays : undefined,
    };

    this.api.preview(filters).subscribe({
      next: (data) => {
        this.previewData = data.map((item) => ({ ...item, selected: false }));
        this.loading = false;
        this.allSelected = false;
      },
      error: (err) => {
        console.error(err);
        this.loading = false;
        void this.dialog.alert({
          title: 'No se pudo cargar la previsualizacion',
          message: 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  toggleAll() {
    this.allSelected = !this.allSelected;
    this.previewData.forEach((item) => (item.selected = this.allSelected));
  }

  assignZoomAuto() {
    void this.dialog.alert({
      title: 'Asignacion automatica',
      message: 'Asignacion automatica pendiente de definicion de reglas.',
    });
  }

  async generate() {
    const selected = this.previewData.filter((item) => item.selected);
    if (!selected.length) {
      await this.dialog.alert({
        title: 'Seleccion requerida',
        message: 'Seleccione al menos una clase.',
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

    const confirmed = await this.dialog.confirm({
      title: 'Generar videoconferencias',
      message: `Se generaran videoconferencias para ${selected.length} clases entre ${this.startDate} y ${this.endDate}. Deseas continuar?`,
      confirmLabel: 'Generar',
      cancelLabel: 'Cancelar',
    });
    if (!confirmed) {
      return;
    }

    this.loading = true;
    const payload = {
      meetings: selected.map((item) => item.id),
      startDate: this.startDate,
      endDate: this.endDate,
    };

    this.api.generate(payload).subscribe({
      next: (res) => {
        void this.dialog.alert({
          title: 'Videoconferencias generadas',
          message: res.message,
          tone: 'success',
        });
        this.loading = false;
      },
      error: (err) => {
        console.error(err);
        void this.dialog.alert({
          title: 'No se pudieron generar las videoconferencias',
          message: 'Error generando videoconferencias.',
          tone: 'danger',
        });
        this.loading = false;
      },
    });
  }
}
