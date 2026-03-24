import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { forkJoin } from 'rxjs';
import { ApiService } from '../../core/api.service';

type PlanningCycleDetailFilters = {
  vc_period_id: string;
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
  cycle: string;
  study_plan_id: string;
};

@Component({
  selector: 'app-planning-cycle-detail-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './planning-cycle-detail.page.html',
  styleUrl: './planning-cycle-detail.page.css',
})
export class PlanningCycleDetailPageComponent implements OnInit {
  loading = false;
  error = '';

  filters: PlanningCycleDetailFilters = {
    vc_period_id: '',
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
    cycle: '',
    study_plan_id: '',
  };

  catalog: any = {
    vc_periods: [],
    semesters: [],
    campuses: [],
    faculties: [],
    academic_programs: [],
    study_plans: [],
  };

  offers: any[] = [];

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
  ) {}

  ngOnInit(): void {
    const query = this.route.snapshot.queryParamMap;
    this.filters.vc_period_id = query.get('vc_period_id') ?? '';
    this.filters.semester_id = query.get('semester_id') ?? '';
    this.filters.campus_id = query.get('campus_id') ?? '';
    this.filters.faculty_id = query.get('faculty_id') ?? '';
    this.filters.academic_program_id = query.get('academic_program_id') ?? '';
    this.filters.cycle = query.get('cycle') ?? '';
    this.filters.study_plan_id = query.get('study_plan_id') ?? '';
    this.loadDetail();
  }

  get semesterLabel() {
    return this.catalog.semesters.find((item: any) => item.id === this.filters.semester_id)?.name ?? '---';
  }

  get vcPeriodLabel() {
    return this.catalog.vc_periods.find((item: any) => item.id === this.filters.vc_period_id)?.text ?? '---';
  }

  get campusLabel() {
    return this.catalog.campuses.find((item: any) => item.id === this.filters.campus_id)?.name ?? 'Sin local';
  }

  get facultyLabel() {
    return this.catalog.faculties.find((item: any) => item.id === this.filters.faculty_id)?.name ?? '---';
  }

  get programLabel() {
    return (
      this.catalog.academic_programs.find((item: any) => item.id === this.filters.academic_program_id)?.name ??
      '---'
    );
  }

  get studyPlanLabel() {
    return this.catalog.study_plans.find((item: any) => item.id === this.filters.study_plan_id)?.name ?? '---';
  }

  loadDetail() {
    if (
      !this.filters.semester_id ||
      !this.filters.faculty_id ||
      !this.filters.academic_program_id ||
      !this.filters.cycle ||
      !this.filters.study_plan_id
    ) {
      this.error = 'Falta contexto para mostrar los cursos guardados.';
      return;
    }

    this.loading = true;
    this.error = '';
    forkJoin({
      catalog: this.api.getPlanningCatalogFilters(),
      offers: this.api.listPlanningOffers({
        vc_period_id: this.filters.vc_period_id,
        semester_id: this.filters.semester_id,
        campus_id: this.filters.campus_id,
        faculty_id: this.filters.faculty_id,
        academic_program_id: this.filters.academic_program_id,
        cycle: this.filters.cycle,
        study_plan_id: this.filters.study_plan_id,
      }),
    }).subscribe({
      next: ({ catalog, offers }) => {
        this.catalog = catalog;
        this.offers = offers;
        this.loading = false;
      },
      error: () => {
        this.error = 'No se pudieron cargar los cursos guardados.';
        this.loading = false;
      },
    });
  }

  goBack() {
    this.router.navigate(['/planning'], {
      queryParams: {
        vc_period_id: this.filters.vc_period_id || null,
        semester_id: this.filters.semester_id || null,
        campus_id: this.filters.campus_id || null,
        faculty_id: this.filters.faculty_id || null,
        academic_program_id: this.filters.academic_program_id || null,
      },
    });
  }

  editCycle() {
    this.router.navigate(['/planning/cycle-editor'], {
      queryParams: {
        ...this.filters,
      },
    });
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
}
