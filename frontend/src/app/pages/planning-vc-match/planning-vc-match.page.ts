import { CommonModule, NgClass } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { finalize } from 'rxjs';
import { ApiService } from '../../core/api.service';

type PlanningVcMatchFilters = {
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
  cycle: string;
  query: string;
};

@Component({
  selector: 'app-planning-vc-match-page',
  standalone: true,
  imports: [CommonModule, FormsModule, NgClass],
  templateUrl: './planning-vc-match.page.html',
  styleUrl: './planning-vc-match.page.css',
})
export class PlanningVcMatchPageComponent implements OnInit {
  loading = true;
  saving = false;
  error = '';
  message = '';

  filters: PlanningVcMatchFilters = {
    semester_id: '',
    campus_id: '',
    faculty_id: '',
    academic_program_id: '',
    cycle: '',
    query: '',
  };

  catalog: any = {
    semesters: [],
    campuses: [],
    faculties: [],
    academic_programs: [],
    study_plan_cycles: [],
    campus_vc_locations: [],
  };

  rows: any[] = [];
  vcSectionSelectionBySubsectionId: Record<string, string> = {};
  campusVcLocationDraftByCampusId: Record<string, string> = {};
  isMappingPanelOpen = false;

  readonly vcLocationCodes = ['CH', 'HU', 'SU', 'IC'];

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.applyQueryState();
    this.loadBootstrap();
  }

  get filteredPrograms() {
    if (!this.filters.faculty_id) {
      return this.catalog.academic_programs ?? [];
    }
    return (this.catalog.academic_programs ?? []).filter(
      (item: any) => item.faculty_id === this.filters.faculty_id,
    );
  }

  get availableCycles() {
    const unique = new Map<string, any>();
    for (const item of this.catalog.study_plan_cycles ?? []) {
      const value = String(item.value ?? '');
      if (!value || unique.has(value)) {
        continue;
      }
      unique.set(value, item);
    }
    return [...unique.values()].sort((left, right) => Number(left.value) - Number(right.value));
  }

  loadBootstrap() {
    this.loading = true;
    this.api.getPlanningCatalogFilters().subscribe({
      next: (catalog) => {
        this.catalog = catalog;
        if (
          this.filters.semester_id &&
          !catalog.semesters?.some((item: any) => item.id === this.filters.semester_id)
        ) {
          this.filters.semester_id = '';
        }
        if (catalog.semesters?.length > 0 && !this.filters.semester_id) {
          this.filters.semester_id = catalog.semesters[0].id;
        }
        this.syncProgramSelection();
        this.hydrateCampusMappingDrafts();
        this.syncRouteState();
        this.loadRows();
      },
      error: () => {
        this.loading = false;
        this.error = 'No se pudo cargar la pantalla de match VC.';
        this.cdr.detectChanges();
      },
    });
  }

  loadRows() {
    this.loading = true;
    this.error = '';
    this.message = '';
    this.syncRouteState();
    this.api.listPlanningVcMatchRows(this.filters).subscribe({
      next: (rows) => {
        this.rows = rows ?? [];
        this.hydrateRowSelections();
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.loading = false;
        this.error = err?.error?.message ?? 'No se pudieron cargar los matches VC.';
        this.cdr.detectChanges();
      },
    });
  }

  onFacultyChange() {
    this.syncProgramSelection();
    this.loadRows();
  }

  recalculateVisible() {
    this.saving = true;
    this.api
      .recalculatePlanningVcMatches({
        ...this.filters,
        cycle: this.filters.cycle ? Number(this.filters.cycle) : undefined,
      })
      .pipe(finalize(() => (this.saving = false)))
      .subscribe({
        next: (response) => {
          this.message = `Recalculo ejecutado. ${response?.updated_subsection_count ?? 0} subsecciones actualizadas.`;
          this.loadRows();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo recalcular el match VC.';
          this.cdr.detectChanges();
        },
      });
  }

  recalculateRow(row: any) {
    this.saving = true;
    this.api
      .recalculatePlanningVcMatches({
        subsection_ids: [row.id],
      })
      .pipe(finalize(() => (this.saving = false)))
      .subscribe({
        next: () => {
          this.message = `Sugerencias recalculadas para ${row.subsection?.code}.`;
          this.loadRows();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo recalcular la subseccion.';
          this.cdr.detectChanges();
        },
      });
  }

  saveRowMatch(row: any) {
    const selectedVcSectionId = this.vcSectionSelectionBySubsectionId[row.id] ?? '';
    this.saving = true;
    this.api
      .updatePlanningSubsectionVcMatch(row.id, {
        vc_section_id: selectedVcSectionId || undefined,
      })
      .pipe(finalize(() => (this.saving = false)))
      .subscribe({
        next: () => {
          this.message = `Match VC actualizado para ${row.subsection?.code}.`;
          this.loadRows();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo guardar el match VC.';
          this.cdr.detectChanges();
        },
      });
  }

  clearRowMatch(row: any) {
    this.vcSectionSelectionBySubsectionId[row.id] = '';
    this.saveRowMatch(row);
  }

  saveCampusMapping(campusId: string) {
    const vcLocationCode = this.campusVcLocationDraftByCampusId[campusId] ?? '';
    if (!vcLocationCode) {
      this.error = 'Selecciona un codigo VC antes de guardar el local.';
      this.cdr.detectChanges();
      return;
    }
    this.saving = true;
    this.api
      .updatePlanningCampusVcLocation(campusId, { vc_location_code: vcLocationCode })
      .pipe(finalize(() => (this.saving = false)))
      .subscribe({
        next: () => {
          const existing = (this.catalog.campus_vc_locations ?? []).filter(
            (item: any) => item.campus_id !== campusId,
          );
          this.catalog.campus_vc_locations = [
            ...existing,
            {
              campus_id: campusId,
              vc_location_code: vcLocationCode,
            },
          ];
          this.message = 'Codigo VC de local actualizado.';
          this.loadRows();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo guardar el codigo VC del local.';
          this.cdr.detectChanges();
        },
      });
  }

  openOffer(row: any) {
    if (!row?.offer?.id) {
      return;
    }
    this.router.navigate(['/planning/offers', row.offer.id, 'sections']);
  }

  toggleMappingPanel() {
    this.isMappingPanelOpen = !this.isMappingPanelOpen;
  }

  matchStatusLabel(status: string | null | undefined) {
    switch (status) {
      case 'MATCHED':
        return 'Match';
      case 'AMBIGUOUS':
        return 'Ambiguo';
      default:
        return 'Pendiente';
    }
  }

  matchStatusClass(status: string | null | undefined) {
    return {
      'status-matched': status === 'MATCHED',
      'status-ambiguous': status === 'AMBIGUOUS',
      'status-unmatched': !status || status === 'UNMATCHED',
    };
  }

  campusVcLocationValue(campusId: string) {
    return this.campusVcLocationDraftByCampusId[campusId] ?? '';
  }

  currentVcSectionLabel(row: any) {
    return row?.vc_section?.name ?? 'Sin match';
  }

  candidateLabel(candidate: any) {
    return candidate?.name ?? candidate?.id ?? 'Seccion VC';
  }

  vcSourceLabel(value: string | null | undefined) {
    switch (value) {
      case 'sync_source':
        return 'Sincronizado';
      case 'manual_override':
        return 'Manual';
      case 'fallback_match':
        return 'Fallback';
      default:
        return 'Sin definir';
    }
  }

  private applyQueryState() {
    const query = this.route.snapshot.queryParamMap;
    this.filters = {
      semester_id: query.get('semester_id') ?? '',
      campus_id: query.get('campus_id') ?? '',
      faculty_id: query.get('faculty_id') ?? '',
      academic_program_id: query.get('academic_program_id') ?? '',
      cycle: query.get('cycle') ?? '',
      query: query.get('query') ?? '',
    };
  }

  private syncRouteState() {
    this.router.navigate([], {
      relativeTo: this.route,
      replaceUrl: true,
      queryParams: {
        semester_id: this.filters.semester_id || null,
        campus_id: this.filters.campus_id || null,
        faculty_id: this.filters.faculty_id || null,
        academic_program_id: this.filters.academic_program_id || null,
        cycle: this.filters.cycle || null,
        query: this.filters.query || null,
      },
    });
  }

  private syncProgramSelection() {
    if (
      this.filters.academic_program_id &&
      !this.filteredPrograms.some((item: any) => item.id === this.filters.academic_program_id)
    ) {
      this.filters.academic_program_id = '';
    }
  }

  private hydrateRowSelections() {
    this.vcSectionSelectionBySubsectionId = {};
    for (const row of this.rows) {
      this.vcSectionSelectionBySubsectionId[row.id] = row?.subsection?.vc_section_id ?? '';
    }
  }

  private hydrateCampusMappingDrafts() {
    const draftByCampusId: Record<string, string> = {};
    for (const item of this.catalog.campus_vc_locations ?? []) {
      draftByCampusId[item.campus_id] = item.vc_location_code;
    }
    for (const campus of this.catalog.campuses ?? []) {
      draftByCampusId[campus.id] = draftByCampusId[campus.id] ?? '';
    }
    this.campusVcLocationDraftByCampusId = draftByCampusId;
  }
}
