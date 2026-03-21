import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { ApiService } from '../../core/api.service';

type ChangeLogFilters = {
  entity_type: string;
  action: string;
  changed_by: string;
  offer_id: string;
  from: string;
  to: string;
  limit: number;
};

type PlanningReturnFilters = {
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
};

@Component({
  selector: 'app-planning-change-log-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './planning-change-log.page.html',
  styleUrl: './planning-change-log.page.css',
})
export class PlanningChangeLogPageComponent implements OnInit {
  loading = false;
  error = '';
  rows: any[] = [];
  expandedId = '';
  returnFilters: PlanningReturnFilters = {
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
  };

  filters: ChangeLogFilters = {
    entity_type: '',
    action: '',
    changed_by: '',
    offer_id: '',
    from: '',
    to: '',
    limit: 100,
  };

  readonly entityTypeOptions = [
    { value: '', label: 'Todas las entidades' },
    { value: 'planning_cycle_plan_rule', label: 'Regla de ciclo' },
    { value: 'planning_offer', label: 'Oferta' },
    { value: 'planning_section', label: 'Seccion' },
    { value: 'planning_subsection', label: 'Subseccion' },
    { value: 'planning_subsection_schedule', label: 'Horario de subseccion' },
  ];

  readonly actionOptions = [
    { value: '', label: 'Todas las acciones' },
    { value: 'CREATE', label: 'Creacion' },
    { value: 'UPDATE', label: 'Edicion' },
    { value: 'DELETE', label: 'Eliminacion' },
  ];

  readonly limitOptions = [50, 100, 150, 200];

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.route.queryParamMap.subscribe((params) => {
      this.returnFilters = {
        semester_id: params.get('semester_id') ?? '',
        campus_id: params.get('campus_id') ?? '',
        faculty_id: params.get('faculty_id') ?? '',
        academic_program_id: params.get('academic_program_id') ?? '',
      };
      this.filters.offer_id = params.get('offer_id') ?? '';
      this.loadRows();
    });
  }

  loadRows() {
    this.loading = true;
    this.error = '';
    this.cdr.detectChanges();
    this.api.listPlanningChangeLog(this.filters).subscribe({
      next: (rows) => {
        this.rows = Array.isArray(rows) ? rows : [];
        if (this.expandedId && !this.rows.some((row) => row.id === this.expandedId)) {
          this.expandedId = '';
        }
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (error) => {
        this.rows = [];
        this.loading = false;
        this.error = error?.error?.message ?? 'No se pudo cargar el historial de cambios.';
        this.cdr.detectChanges();
      },
    });
  }

  clearFilters() {
    this.filters = {
      entity_type: '',
      action: '',
      changed_by: '',
      offer_id: '',
      from: '',
      to: '',
      limit: 100,
    };
    this.expandedId = '';
    this.loadRows();
  }

  goBack() {
    this.router.navigate(['/planning'], {
      queryParams: {
        semester_id: this.returnFilters.semester_id || null,
        campus_id: this.returnFilters.campus_id || null,
        faculty_id: this.returnFilters.faculty_id || null,
        academic_program_id: this.returnFilters.academic_program_id || null,
      },
    });
  }

  toggleRow(id: string) {
    this.expandedId = this.expandedId === id ? '' : id;
  }

  formatEntityType(value: string) {
    return this.entityTypeOptions.find((item) => item.value === value)?.label ?? value ?? '---';
  }

  formatAction(value: string) {
    switch (value) {
      case 'CREATE':
        return 'Creacion';
      case 'UPDATE':
        return 'Edicion';
      case 'DELETE':
        return 'Eliminacion';
      default:
        return value || '---';
    }
  }

  actionClass(value: string) {
    switch (value) {
      case 'CREATE':
        return 'pill success';
      case 'DELETE':
        return 'pill danger';
      default:
        return 'pill info';
    }
  }

  actorLabel(row: any) {
    return row?.changed_by || row?.changed_by_user_id || 'SYSTEM';
  }

  ipLabel(row: any) {
    return row?.changed_from_ip || 'Sin IP registrada';
  }

  contextLabel(row: any) {
    if (row?.reference_label) {
      return row.reference_label;
    }
    const context = row?.context_json ?? {};
    if (context.subsection_id) {
      return 'Oferta · Seccion · Subseccion';
    }
    if (context.section_id) {
      return 'Oferta · Seccion';
    }
    if (context.offer_id) {
      return 'Oferta';
    }
    if (context.study_plan_id && context.cycle) {
      return `Plan · Ciclo ${context.cycle}`;
    }
    return 'Sin contexto adicional';
  }

  detailTitle(row: any) {
    return row?.reference_label || this.formatEntityType(row?.entity_type) || 'Detalle del evento';
  }

  displayValue(value: unknown) {
    if (value === null || value === undefined) {
      return '—';
    }
    if (typeof value === 'boolean') {
      return value ? 'Si' : 'No';
    }
    if (typeof value === 'object') {
      return JSON.stringify(value);
    }
    const normalized = `${value}`;
    return normalized.trim() ? normalized : '(vacio)';
  }
}
