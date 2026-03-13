import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { ApiService } from '../../core/api.service';

type PlanningSummaryFilters = {
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
};

@Component({
  selector: 'app-planning-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './planning.page.html',
  styleUrl: './planning.page.css',
})
export class PlanningPageComponent implements OnInit {
  loading = true;
  error = '';

  filters: PlanningSummaryFilters = {
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
  };

  catalog: any = {
    semesters: [],
    campuses: [],
    faculties: [],
    academic_programs: [],
  };

  rows: any[] = [];

  constructor(
    private readonly api: ApiService,
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

  loadBootstrap() {
    this.loading = true;
    this.api.getPlanningCatalogFilters().subscribe({
      next: (catalog) => {
        this.catalog = catalog;
        // Auto-select first semester if available
        if (catalog.semesters?.length > 0 && !this.filters.semester_id) {
          this.filters.semester_id = catalog.semesters[0].id;
        }
        this.cdr.detectChanges();
        // Now load rows with the (possibly updated) filters
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
    if (!this.filteredPrograms.some((item: any) => item.id === this.filters.academic_program_id)) {
      this.filters.academic_program_id = '';
    }
    this.loadRows();
  }

  loadRows() {
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
    this.router.navigate(['/planning/cycle-editor']);
  }

  openCycleDetail(row: any) {
    this.router.navigate(['/planning/cycle-editor'], {
      queryParams: {
        semester_id: row.semester_id ?? '',
        campus_id: this.filters.campus_id || row.campus_id || row.primary_campus_id || '',
        faculty_id: row.faculty_id ?? '',
        academic_program_id: row.academic_program_id ?? '',
        cycle: row.cycle ? String(row.cycle) : '',
        study_plan_id: row.study_plan_id ?? '',
      },
    });
  }
}
