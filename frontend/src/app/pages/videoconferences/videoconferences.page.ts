import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { VideoconferenceApiService, FilterOptionsDto, VideoconferencePreviewItem } from '../../services/videoconference-api.service';
import { MultiSelectComponent, MultiSelectOption } from '../../components/multi-select/multi-select.component';

@Component({
    selector: 'app-videoconferences-page',
    standalone: true,
    imports: [CommonModule, FormsModule, MultiSelectComponent],
    templateUrl: './videoconferences.page.html',
    styleUrl: './videoconferences.page.css'
})
export class VideoconferencesPageComponent implements OnInit {
    // Filters
    selectedFaculties: string[] = [];
    selectedPrograms: string[] = [];
    selectedCourses: string[] = [];
    selectedSections: string[] = []; // Group IDs really
    selectedModality: string = '';
    selectedDays: string[] = []; // 'LUNES', 'MARTES'...

    // Data for filters
    faculties: any[] = [];
    programs: any[] = [];
    courses: any[] = [];
    sections: any[] = [];

    // Options for MultiSelect
    facultyOptions: MultiSelectOption[] = [];
    programOptions: MultiSelectOption[] = [];
    courseOptions: MultiSelectOption[] = [];
    sectionOptions: MultiSelectOption[] = [];
    dayOptions: MultiSelectOption[] = [
        { id: 'LUNES', label: 'Lunes' },
        { id: 'MARTES', label: 'Martes' },
        { id: 'MIERCOLES', label: 'Miércoles' },
        { id: 'JUEVES', label: 'Jueves' },
        { id: 'VIERNES', label: 'Viernes' },
        { id: 'SABADO', label: 'Sábado' },
        { id: 'DOMINGO', label: 'Domingo' }
    ];

    // Results
    previewData: VideoconferencePreviewItem[] = [];
    allSelected = false;

    // Generation
    startDate: string = '';
    endDate: string = '';
    generationResult: any = null;

    loading = false;

    // Subscriptions
    private programsSub: any;
    private coursesSub: any;
    private sectionsSub: any;

    constructor(private api: VideoconferenceApiService) { }

    ngOnInit() {
        this.loadFaculties();
    }

    loadFaculties() {
        this.api.getFaculties().subscribe({
            next: (data) => {
                this.faculties = data;
                this.facultyOptions = data.map(f => ({ id: f.id, label: f.name || '(Sin Nombre)' }));
            },
            error: (err) => console.error('Error loading faculties', err)
        });
    }

    onFacultyChange(selectedIds: string[]) {
        this.selectedFaculties = selectedIds;

        // Reset downstream
        this.selectedPrograms = [];
        this.selectedCourses = [];
        this.selectedSections = [];
        this.programs = [];
        this.courses = [];
        this.sections = [];
        this.programOptions = [];
        this.courseOptions = [];
        this.sectionOptions = [];

        // Cancel previous request
        if (this.programsSub) {
            this.programsSub.unsubscribe();
        }

        if (this.selectedFaculties.length > 0) {
            this.programsSub = this.api.getPrograms(this.selectedFaculties).subscribe({
                next: (data) => {
                    this.programs = data;
                    this.programOptions = data.map(p => ({ id: p.id, label: p.name }));
                    if (data.length === 0) {
                        console.warn('No programs found for selected faculties');
                    }
                },
                error: (err) => {
                    console.error('Error loading programs', err);
                    alert('Error cargando programas. Verifica la conexión.');
                }
            });
        }
    }

    onProgramChange(selectedIds: string[]) {
        this.selectedPrograms = selectedIds;

        // Reset downstream
        this.selectedCourses = [];
        this.selectedSections = [];
        this.courses = [];
        this.sections = [];
        this.courseOptions = [];
        this.sectionOptions = [];

        // Cancel previous request
        if (this.coursesSub) {
            this.coursesSub.unsubscribe();
        }

        if (this.selectedPrograms.length > 0) {
            this.coursesSub = this.api.getCourses(this.selectedPrograms).subscribe({
                next: (data) => {
                    this.courses = data;
                    this.courseOptions = data.map(c => ({ id: c.id, label: `${c.name} (${c.code})` }));
                },
                error: (err) => console.error('Error loading courses', err)
            });
        }
    }

    onCourseChange(selectedIds: string[]) {
        this.selectedCourses = selectedIds;

        // Reset downstream
        this.selectedSections = [];
        this.sections = [];
        this.sectionOptions = [];

        // Cancel previous request
        if (this.sectionsSub) {
            this.sectionsSub.unsubscribe();
        }

        if (this.selectedCourses.length > 0) {
            this.sectionsSub = this.api.getSections(this.selectedCourses).subscribe({
                next: (data) => {
                    this.sections = data;
                    this.sectionOptions = data.map(s => ({ id: s.id, label: s.name })); // Use name instead of code for new VcEntity
                },
                error: (err) => console.error('Error loading sections', err)
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
            days: this.selectedDays.length ? this.selectedDays : undefined
        };

        this.api.preview(filters).subscribe({
            next: (data) => {
                this.previewData = data.map(item => ({ ...item, selected: false }));
                this.loading = false;
                this.allSelected = false;
            },
            error: (err) => {
                console.error(err);
                this.loading = false;
                alert('Error cargando previsualización');
            }
        });
    }

    toggleAll() {
        this.allSelected = !this.allSelected;
        this.previewData.forEach(item => item.selected = this.allSelected);
    }

    // Assign Zoom Automatically Logic (Stub for now)
    assignZoomAuto() {
        // This logic needs clarification on "regla configurable".
        // For now, maybe assign random user or first available?
        // I'll leave a placeholder alert.
        alert('Asignación automática pendiente de definición de reglas.');
    }

    generate() {
        const selected = this.previewData.filter(item => item.selected);
        if (!selected.length) {
            alert('Seleccione al menos una clase.');
            return;
        }
        if (!this.startDate || !this.endDate) {
            alert('Seleccione fechas de inicio y fin.');
            return;
        }

        if (!confirm(`Se generarán videoconferencias para ${selected.length} clases entre ${this.startDate} y ${this.endDate}. ¿Continuar?`)) {
            return;
        }

        this.loading = true;
        const payload = {
            meetings: selected.map(item => item.id), // ClassMeeting IDs
            startDate: this.startDate,
            endDate: this.endDate
        };

        this.api.generate(payload).subscribe({
            next: (res) => {
                alert(res.message);
                this.loading = false;
                // Maybe refresh or go to detail page?
            },
            error: (err) => {
                console.error(err);
                alert('Error generando videoconferencias');
                this.loading = false;
            }
        });
    }
}
