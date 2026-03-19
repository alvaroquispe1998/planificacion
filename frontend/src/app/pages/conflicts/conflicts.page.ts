import { CommonModule, NgClass } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { ApiService } from '../../core/api.service';

type ConflictView = 'TEACHER_OVERLAP' | 'SECTION_OVERLAP';

@Component({
  selector: 'app-conflicts-page',
  standalone: true,
  imports: [CommonModule, FormsModule, NgClass],
  templateUrl: './conflicts.page.html',
  styleUrl: './conflicts.page.css',
})
export class ConflictsPageComponent implements OnInit {
  loading = true;
  refreshing = false;
  error = '';
  message = '';
  semesterId = '';
  semesters: any[] = [];
  rows: any[] = [];
  activeView: ConflictView = 'TEACHER_OVERLAP';
  lastRun: any | null = null;
  lastUpdatedAt = '';

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadBootstrap();
  }

  get selectedSemesterLabel() {
    return this.semesters.find((item: any) => item.id === this.semesterId)?.name ?? 'Sin periodo';
  }

  get teacherConflicts() {
    return this.rows.filter((item: any) => item.conflict_type === 'TEACHER_OVERLAP');
  }

  get sectionConflicts() {
    return this.rows.filter((item: any) => item.conflict_type === 'SECTION_OVERLAP');
  }

  get activeRows() {
    return this.activeView === 'TEACHER_OVERLAP' ? this.teacherConflicts : this.sectionConflicts;
  }

  get activeViewTitle() {
    return this.activeView === 'TEACHER_OVERLAP' ? 'Cruce de horario de docente' : 'Cruce de horario de seccion';
  }

  get activeViewDescription() {
    return this.activeView === 'TEACHER_OVERLAP'
      ? 'Docentes asignados en dos reuniones que se pisan en el mismo periodo.'
      : 'Secciones con reuniones superpuestas dentro del mismo curso/oferta.';
  }

  loadBootstrap() {
    this.loading = true;
    this.api.getPlanningCatalogFilters().subscribe({
      next: (catalog) => {
        this.semesters = [...(catalog?.semesters ?? [])];
        const querySemesterId = this.route.snapshot.queryParamMap.get('semester_id') ?? '';
        const initialSemesterId =
          querySemesterId && this.semesters.some((item: any) => item.id === querySemesterId)
            ? querySemesterId
            : this.semesters[0]?.id ?? '';
        this.semesterId = initialSemesterId;
        this.syncRouteState();
        if (!this.semesterId) {
          this.loading = false;
          this.error = 'No hay periodos disponibles para revisar cruces.';
          this.cdr.detectChanges();
          return;
        }
        this.refreshConflicts();
      },
      error: () => {
        this.loading = false;
        this.error = 'No se pudo cargar el catalogo de periodos.';
        this.cdr.detectChanges();
      },
    });
  }

  onSemesterChange() {
    this.syncRouteState();
    this.refreshConflicts();
  }

  setActiveView(view: ConflictView) {
    this.activeView = view;
  }

  refreshConflicts() {
    if (!this.semesterId) {
      this.rows = [];
      this.loading = false;
      this.refreshing = false;
      this.cdr.detectChanges();
      return;
    }

    this.error = '';
    this.message = '';
    this.refreshing = true;
    this.cdr.detectChanges();

    this.api.listPlanningConflicts(this.semesterId).subscribe({
      next: (rows) => {
        this.lastRun = {
          semester_id: this.semesterId,
          source: 'planning_manual',
          total: Array.isArray(rows) ? rows.length : 0,
        };
        this.rows = this.dedupeConflictRows(
          (Array.isArray(rows) ? rows : []).filter(
            (item: any) => item.conflict_type === 'TEACHER_OVERLAP' || item.conflict_type === 'SECTION_OVERLAP',
          ),
        );
        this.lastUpdatedAt = new Date().toISOString();
        this.message = `Cruces cargados para ${this.selectedSemesterLabel}.`;
        if (this.activeView === 'TEACHER_OVERLAP' && this.teacherConflicts.length === 0 && this.sectionConflicts.length > 0) {
          this.activeView = 'SECTION_OVERLAP';
        }
        if (this.activeView === 'SECTION_OVERLAP' && this.sectionConflicts.length === 0 && this.teacherConflicts.length > 0) {
          this.activeView = 'TEACHER_OVERLAP';
        }
        this.loading = false;
        this.refreshing = false;
        this.cdr.detectChanges();
      },
      error: () => {
        this.rows = [];
        this.loading = false;
        this.refreshing = false;
        this.error = 'No se pudieron cargar los cruces de planificacion para el periodo seleccionado.';
        this.cdr.detectChanges();
      },
    });
  }

  overlapWindowLabel(row: any) {
    const day = this.dayLabel(row?.overlap_day);
    const start = this.timeLabel(row?.overlap_start);
    const end = this.timeLabel(row?.overlap_end);
    if (row?.overlap_day && row?.overlap_start && row?.overlap_end) {
      return `${day} ${start}-${end}`;
    }
    return day !== 'Sin dia' ? day : 'Cruce detectado';
  }

  formatSeverity(value: string | null | undefined) {
    switch (value) {
      case 'CRITICAL':
        return 'Critico';
      case 'WARNING':
        return 'Advertencia';
      case 'INFO':
        return 'Informativo';
      default:
        return value || 'Sin nivel';
    }
  }

  severityClass(value: string | null | undefined) {
    return {
      'pill-danger': value === 'CRITICAL',
      'pill-warning': value === 'WARNING',
      'pill-neutral': !value || value === 'INFO',
    };
  }

  formatUpdatedAt(value: string | null | undefined) {
    if (!value) {
      return 'Sin actualizacion reciente';
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return value;
    }
    return parsed.toLocaleString('es-PE', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }

  dayLabel(value: string | null | undefined) {
    switch (value) {
      case 'LUNES':
        return 'Lunes';
      case 'MARTES':
        return 'Martes';
      case 'MIERCOLES':
        return 'Miercoles';
      case 'JUEVES':
        return 'Jueves';
      case 'VIERNES':
        return 'Viernes';
      case 'SABADO':
        return 'Sabado';
      case 'DOMINGO':
        return 'Domingo';
      default:
        return value || 'Sin dia';
    }
  }

  timeLabel(value: string | null | undefined) {
    if (!value) {
      return '--:--';
    }
    return String(value).slice(0, 5);
  }

  meetingScheduleLabel(meeting: any) {
    if (!meeting) {
      return 'Sin bloque';
    }
    return `${this.dayLabel(meeting.day_of_week)} ${this.timeLabel(meeting.start_time)}-${this.timeLabel(meeting.end_time)}`;
  }

  private syncRouteState() {
    this.router.navigate([], {
      relativeTo: this.route,
      replaceUrl: true,
      queryParams: {
        semester_id: this.semesterId || null,
      },
    });
  }

  private dedupeConflictRows(rows: any[]) {
    const deduped = new Map<string, any>();
    for (const row of rows) {
      const pair = [row?.schedule_a_id ?? '', row?.schedule_b_id ?? ''].sort().join(':');
      const key = [
        row?.conflict_type ?? '',
        pair,
        row?.teacher_id ?? '',
        row?.classroom_id ?? '',
      ].join('|');
      if (!deduped.has(key)) {
        deduped.set(key, this.normalizeConflictRow(row));
      }
    }
    return [...deduped.values()];
  }

  private normalizeConflictRow(row: any) {
    if (row?.conflict_type !== 'SECTION_OVERLAP') {
      return row;
    }
    const left = [row?.meeting_a?.section_name, row?.meeting_a?.group_code].filter(Boolean).join(' · ');
    const right = [row?.meeting_b?.section_name, row?.meeting_b?.group_code].filter(Boolean).join(' · ');
    return {
      ...row,
      affected_label: [left, right].filter(Boolean).join(' vs ') || row?.affected_label || 'Cruce de seccion',
    };
  }
}
