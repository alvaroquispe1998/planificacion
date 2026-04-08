import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { catchError, forkJoin, of } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { AuthService } from '../../core/auth.service';
import { DialogService } from '../../core/dialog.service';

type PlanningWorkspaceFilters = {
  semester_id: string;
  campus_id: string;
  faculty_id: string;
  academic_program_id: string;
  study_plan_id: string;
  delivery_modality_id: string;
  shift_id: string;
  search: string;
};

type WorkspaceQuickFilter = 'ALL' | 'BLOCKED' | 'ALERTS' | 'NO_TEACHER' | 'NO_SCHEDULE' | 'COMPLETE';

type PlanningWorkspaceAlert = {
  code: string;
  severity: string;
  message: string;
  blocking: boolean;
};

type PlanningWorkspaceTeacher = {
  assignment_id: string | null;
  teacher_id: string;
  full_name: string | null;
  role: string;
  is_primary: boolean;
};

type PlanningWorkspaceRow = {
  row_id: string;
  row_kind: 'GROUP' | 'MEETING';
  offering_id: string;
  offer_source_system: string | null;
  section_id: string;
  section_source_section_id: string | null;
  group_id: string;
  meeting_id: string | null;
  semester_id: string;
  semester_name: string | null;
  campus_id: string;
  campus_name: string | null;
  faculty_id: string;
  faculty_name: string | null;
  academic_program_id: string;
  academic_program_code: string | null;
  academic_program_name: string | null;
  study_plan_id: string;
  study_plan_name: string | null;
  course_id: string;
  course_code: string | null;
  course_name: string | null;
  course_section_id: string;
  course_section_name: string | null;
  external_section_code: string | null;
  internal_section_code: string | null;
  delivery_modality_id: string;
  shift_id: string;
  projected_vacancies: number | null;
  offering_status: boolean;
  source_status: string;
  group_type: string;
  group_code: string;
  group_capacity: number | null;
  building_id: string | null;
  building_name: string | null;
  group_note: string | null;
  teachers: PlanningWorkspaceTeacher[];
  primary_teacher_name: string | null;
  session_type: string | null;
  day_of_week: string | null;
  start_time: string | null;
  end_time: string | null;
  minutes: number;
  academic_hours: number | null;
  classroom_id: string | null;
  classroom_name: string | null;
  classroom_code: string | null;
  assigned_theoretical_hours: number | null;
  assigned_practical_hours: number | null;
  assigned_total_hours: number | null;
  alerts: PlanningWorkspaceAlert[];
  alert_count: number;
  blocking_alert_count: number;
};

type PlanningWorkspaceSummary = {
  offering_id: string;
  semester_id: string;
  course_id: string;
  course_code: string | null;
  course_name: string | null;
  academic_program_name: string | null;
  study_plan_name: string | null;
  campus_name: string | null;
  group_count: number;
  row_count: number;
  scheduled_row_count: number;
  teacher_count: number;
  total_alerts: number;
  blocking_alerts: number;
  hours_required: {
    theory: number;
    practice: number;
    lab: number;
  };
  hours_planned: {
    theory: number;
    practice: number;
    lab: number;
  };
  state: 'DRAFT' | 'IN_PROGRESS' | 'COMPLETE' | 'BLOCKED';
};

type PlanningWorkspaceTotals = {
  offerings: number;
  rows: number;
  alerts: number;
  blocking_alerts: number;
};

type PlanningWorkspaceResponse = {
  filters: Record<string, string>;
  summaries: PlanningWorkspaceSummary[];
  rows: PlanningWorkspaceRow[];
  totals: PlanningWorkspaceTotals;
};

type WorkspaceDrawerForm = {
  teacher_id: string;
  building_id: string;
  classroom_id: string;
  session_type: string;
  day_of_week: string;
  start_time: string;
  end_time: string;
  academic_hours: string;
  capacity: string;
  group_note: string;
  projected_vacancies: string;
  offering_status: boolean;
};

type WorkspaceCatalog = {
  semesters: any[];
  campuses: any[];
  faculties: any[];
  academic_programs: any[];
  study_plans: any[];
  course_modalities: any[];
  shift_options: Array<{ id: string; label: string }>;
};

@Component({
  selector: 'app-planning-workspace-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './planning-workspace.page.html',
  styleUrl: './planning-workspace.page.css',
})
export class PlanningWorkspacePageComponent implements OnInit {
  private readonly filtersStorageKey = 'planning.workspace.filters';
  private readonly dayOrder: Record<string, number> = {
    LUNES: 1,
    MARTES: 2,
    MIERCOLES: 3,
    JUEVES: 4,
    VIERNES: 5,
    SABADO: 6,
    DOMINGO: 7,
  };

  readonly dayOptions = [
    { value: 'LUNES', label: 'Lunes' },
    { value: 'MARTES', label: 'Martes' },
    { value: 'MIERCOLES', label: 'Miercoles' },
    { value: 'JUEVES', label: 'Jueves' },
    { value: 'VIERNES', label: 'Viernes' },
    { value: 'SABADO', label: 'Sabado' },
    { value: 'DOMINGO', label: 'Domingo' },
  ];

  readonly sessionTypeOptions = [
    { value: 'THEORY', label: 'Teoria' },
    { value: 'PRACTICE', label: 'Practica' },
    { value: 'LAB', label: 'Laboratorio' },
    { value: 'OTHER', label: 'Otro' },
  ];

  readonly fallbackShiftOptions = [
    { id: 'DIURNO', label: 'Diurno' },
    { id: 'MANANA', label: 'Manana' },
    { id: 'TARDE', label: 'Tarde' },
    { id: 'NOCHE', label: 'Noche' },
    { id: 'NOCTURNO', label: 'Nocturno' },
    { id: 'TARDE/NOCHE', label: 'Tarde/Noche' },
  ];

  readonly quickFilterKeys: WorkspaceQuickFilter[] = [
    'ALL',
    'BLOCKED',
    'ALERTS',
    'NO_TEACHER',
    'NO_SCHEDULE',
    'COMPLETE',
  ];
  readonly pageSizeOptions = [20, 50, 100];

  loading = true;
  saving = false;
  error = '';
  message = '';
  quickFilter: WorkspaceQuickFilter = 'ALL';
  currentPage = 1;
  pageSize = 20;

  filters: PlanningWorkspaceFilters = this.emptyFilters();
  searchDraft = '';
  catalog: WorkspaceCatalog = this.emptyCatalog();
  workspace: PlanningWorkspaceResponse = this.emptyWorkspace();
  offerFilterUniverse: any[] = [];
  offerDetails: any[] = [];
  teachers: any[] = [];
  buildings: any[] = [];
  classrooms: any[] = [];
  selectedRowIds: string[] = [];
  private offerFilterUniverseSemesterId = '';
  private rowSearchIndex = new Map<string, string>();
  private serverScopedRowsCache: PlanningWorkspaceRow[] = [];
  private baseRowsCache: PlanningWorkspaceRow[] = [];
  private visibleRowsCache: PlanningWorkspaceRow[] = [];
  private modalityOptionsCache: any[] = [];
  private shiftOptionsCache: Array<{ id: string; label: string }> = [];
  private visibleTotalsCache: PlanningWorkspaceTotals = {
    offerings: 0,
    rows: 0,
    alerts: 0,
    blocking_alerts: 0,
  };
  private visibleGroupCountCache = 0;

  drawer = {
    open: false,
    row_id: '',
    form: this.emptyDrawerForm(),
  };

  teacherModal = {
    open: false,
    teacher_id: '',
    query: '',
  };

  classroomModal = {
    open: false,
    classroom_id: '',
    query: '',
  };

  replicateModal = {
    open: false,
    source_row_id: '',
  };

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
    private readonly dialog: DialogService,
    readonly auth: AuthService,
  ) {}

  ngOnInit(): void {
    this.restoreFilters();
    this.searchDraft = this.filters.search;
    this.loadBootstrap();
  }

  get periodOptions() {
    return this.availableSemesters;
  }

  get availableSemesters() {
    return [...(Array.isArray(this.catalog.semesters) ? this.catalog.semesters : [])].sort((left, right) =>
      `${right?.name ?? ''}`.localeCompare(`${left?.name ?? ''}`),
    );
  }

  get availableCampuses() {
    const map = new Map<string, any>();
    this.filteredOfferUniverseBySemester.forEach((offer: any) => {
      if (!offer?.campus_id) {
        return;
      }
      map.set(offer.campus_id, {
        id: offer.campus_id,
        name:
          this.findCatalogName(this.catalog.campuses, offer.campus_id) ??
          offer?.campus?.name ??
          offer?.campus_name ??
          offer.campus_id,
      });
    });
    return [...map.values()].sort((left, right) => `${left.name ?? ''}`.localeCompare(`${right.name ?? ''}`));
  }

  get availableFaculties() {
    const map = new Map<string, any>();
    this.filteredOfferUniverseByCampus.forEach((offer: any) => {
      if (!offer?.faculty_id) {
        return;
      }
      map.set(offer.faculty_id, {
        id: offer.faculty_id,
        name:
          this.findCatalogName(this.catalog.faculties, offer.faculty_id) ??
          offer?.faculty?.name ??
          offer?.faculty_name ??
          offer.faculty_id,
      });
    });
    return [...map.values()].sort((left, right) => `${left.name ?? ''}`.localeCompare(`${right.name ?? ''}`));
  }

  get filteredPrograms() {
    const map = new Map<string, any>();
    this.filteredOfferUniverseByFaculty.forEach((offer: any) => {
      if (!offer?.academic_program_id) {
        return;
      }
      map.set(offer.academic_program_id, {
        id: offer.academic_program_id,
        name:
          this.findCatalogName(this.catalog.academic_programs, offer.academic_program_id) ??
          offer?.academic_program?.name ??
          offer?.academic_program_name ??
          offer.academic_program_id,
        faculty_id: offer?.faculty_id ?? null,
      });
    });
    return [...map.values()].sort((left, right) => `${left.name ?? ''}`.localeCompare(`${right.name ?? ''}`));
  }

  get filteredStudyPlans() {
    const map = new Map<string, any>();
    this.filteredOfferUniverseByProgram.forEach((offer: any) => {
      if (!offer?.study_plan_id) {
        return;
      }
      map.set(offer.study_plan_id, {
        id: offer.study_plan_id,
        name:
          this.findCatalogName(this.catalog.study_plans, offer.study_plan_id) ??
          offer?.study_plan?.name ??
          offer?.study_plan_name ??
          offer.study_plan_id,
        faculty_id: offer?.faculty_id ?? null,
        academic_program_id: offer?.academic_program_id ?? null,
      });
    });
    return [...map.values()].sort((left, right) => `${left.name ?? ''}`.localeCompare(`${right.name ?? ''}`));
  }

  get modalityOptions() {
    return this.modalityOptionsCache;
  }

  get shiftOptions() {
    return this.shiftOptionsCache;
  }

  get filteredOfferUniverseBySemester() {
    return (Array.isArray(this.offerFilterUniverse) ? this.offerFilterUniverse : []).filter((offer: any) =>
      !this.filters.semester_id || offer?.semester_id === this.filters.semester_id,
    );
  }

  get filteredOfferUniverseByCampus() {
    return this.filteredOfferUniverseBySemester.filter((offer: any) =>
      !this.filters.campus_id || offer?.campus_id === this.filters.campus_id,
    );
  }

  get filteredOfferUniverseByFaculty() {
    return this.filteredOfferUniverseByCampus.filter((offer: any) =>
      !this.filters.faculty_id || offer?.faculty_id === this.filters.faculty_id,
    );
  }

  get filteredOfferUniverseByProgram() {
    return this.filteredOfferUniverseByFaculty.filter((offer: any) =>
      !this.filters.academic_program_id || offer?.academic_program_id === this.filters.academic_program_id,
    );
  }

  get serverScopedRows() {
    return this.serverScopedRowsCache;
  }

  get baseRows() {
    return this.baseRowsCache;
  }

  get visibleRows() {
    return this.visibleRowsCache;
  }

  get totalVisibleRows() {
    return this.visibleRows.length;
  }

  get totalPages() {
    return Math.max(1, Math.ceil(this.totalVisibleRows / this.pageSize));
  }

  get paginatedRows() {
    const page = Math.min(this.currentPage, this.totalPages);
    const start = (page - 1) * this.pageSize;
    return this.visibleRows.slice(start, start + this.pageSize);
  }

  get currentPageStart() {
    if (this.totalVisibleRows === 0) {
      return 0;
    }
    return (Math.min(this.currentPage, this.totalPages) - 1) * this.pageSize + 1;
  }

  get currentPageEnd() {
    if (this.totalVisibleRows === 0) {
      return 0;
    }
    return Math.min(this.currentPageStart + this.paginatedRows.length - 1, this.totalVisibleRows);
  }

  get visibleTotals() {
    return this.visibleTotalsCache;
  }

  get visibleGroupCount() {
    return this.visibleGroupCountCache;
  }

  get selectedRows() {
    const selected = new Set(this.selectedRowIds);
    return this.workspace.rows.filter((row) => selected.has(row.row_id));
  }

  get allVisibleSelected() {
    return this.paginatedRows.length > 0 && this.paginatedRows.every((row) => this.selectedRowIds.includes(row.row_id));
  }

  get activeDrawerRow() {
    return this.workspace.rows.find((row) => row.row_id === this.drawer.row_id) ?? null;
  }

  get activeDrawerBlockingAlerts() {
    return (this.activeDrawerRow?.alerts ?? []).filter((item) => item.blocking);
  }

  get activeDrawerObservedAlerts() {
    return (this.activeDrawerRow?.alerts ?? []).filter((item) => !item.blocking);
  }

  get selectedCampusId() {
    const campusIds = [...new Set(this.selectedRows.map((row) => row.campus_id).filter(Boolean))];
    return campusIds.length === 1 ? campusIds[0] : '';
  }

  get selectedMeetingRows() {
    return this.selectedRows.filter((row) => Boolean(row.meeting_id));
  }

  get canOpenTeacherModal() {
    return this.selectedRows.length > 0 && !this.loading && !this.saving;
  }

  get canOpenClassroomModal() {
    return (
      this.selectedRows.length > 0 &&
      Boolean(this.selectedCampusId) &&
      !this.loading &&
      !this.saving
    );
  }

  get bulkTeacherOptions() {
    return this.filteredTeachers(this.teacherModal.query);
  }

  get bulkClassroomOptions() {
    return this.filteredClassrooms(this.classroomModal.query, this.selectedCampusId);
  }

  get drawerTeacherOptions() {
    return this.teachers;
  }

  get drawerCampusBuildings() {
    const campusId = this.activeDrawerRow?.campus_id ?? '';
    if (!campusId) {
      return [];
    }
    return this.buildings.filter((item: any) => item.campus_id === campusId);
  }

  get drawerClassroomOptions() {
    const campusId = this.activeDrawerRow?.campus_id ?? '';
    const buildingId = this.drawer.form.building_id ?? '';
    if (!campusId || !buildingId) {
      return [];
    }
    return this.classrooms.filter(
      (item: any) => item.campus_id === campusId && item.building_id === buildingId,
    );
  }

  loadBootstrap() {
    this.loading = true;
    this.error = '';
    forkJoin({
      catalog: this.api.getPlanningCatalogFilters(),
      teachers: this.api.listTeachers().pipe(catchError(() => of([]))),
      buildings: this.api.listBuildings().pipe(catchError(() => of([]))),
      classrooms: this.api.listClassrooms().pipe(catchError(() => of([]))),
    }).subscribe({
      next: ({ catalog, teachers, buildings, classrooms }) => {
        this.catalog = this.normalizeCatalog(catalog);
        this.teachers = Array.isArray(teachers) ? teachers : [];
        this.buildings = Array.isArray(buildings) ? buildings : [];
        this.classrooms = Array.isArray(classrooms) ? classrooms : [];
        this.syncCatalogSelections();
        this.persistFilters();
        this.loadWorkspace(undefined, true);
      },
      error: () => {
        this.loading = false;
        this.error = 'No se pudo cargar el panel operativo.';
        this.cdr.detectChanges();
      },
    });
  }

  loadWorkspace(focusRowId?: string, refreshUniverse = this.shouldRefreshOfferUniverse()) {
    this.loading = true;
    this.error = '';
    this.cdr.detectChanges();

    const loadExpandedOffers = () => {
      this.persistFilters();
      this.api.listPlanningExpandedOffers(this.manualOfferFilters()).subscribe({
        next: (offers) => {
          this.offerDetails = Array.isArray(offers) ? offers : [];
          if (this.offerDetails.length === 0) {
            this.workspace = this.emptyWorkspace();
            this.rebuildRowSearchIndex();
            this.refreshWorkspaceView();
            this.resetPagination();
            this.loading = false;
            this.syncSelectedRows();
            this.syncDrawerAfterReload(focusRowId);
            this.cdr.detectChanges();
            return;
          }
          this.workspace = this.buildManualWorkspace(this.offerDetails);
          this.rebuildRowSearchIndex();
          this.refreshWorkspaceView();
          this.resetPagination();
          this.loading = false;
          this.syncSelectedRows();
          this.syncDrawerAfterReload(focusRowId);
          this.cdr.detectChanges();
        },
        error: (err: any) => {
          this.loading = false;
          this.error = err?.error?.message ?? 'No se pudo cargar el panel operativo.';
          this.cdr.detectChanges();
        },
      });
    };

    if (!refreshUniverse) {
      loadExpandedOffers();
      return;
    }

    this.api.listPlanningOffers(this.offerUniverseFilters()).pipe(catchError(() => of([]))).subscribe({
      next: (offers) => {
        this.offerFilterUniverse = Array.isArray(offers) ? offers : [];
        this.offerFilterUniverseSemesterId = this.filters.semester_id ?? '';
        this.syncCatalogSelections();
        loadExpandedOffers();
      },
    });
  }

  goBack() {
    this.router.navigate(['/planning'], {
      queryParams: this.summaryQueryParams(),
    });
  }

  reload() {
    this.clearSelection();
    this.closeDrawer();
    this.loadBootstrap();
  }

  onFacultyChange() {
    this.resetPagination();
    this.syncProgramSelection();
    this.syncStudyPlanSelection();
    this.quickFilter = 'ALL';
    this.clearSelection();
    this.closeDrawer();
    this.persistFilters();
    this.refreshWorkspaceView();
  }

  onSemesterChange() {
    this.resetPagination();
    this.quickFilter = 'ALL';
    this.clearSelection();
    this.closeDrawer();
    this.loadWorkspace(undefined, true);
  }

  onProgramChange() {
    this.resetPagination();
    this.syncStudyPlanSelection();
    this.quickFilter = 'ALL';
    this.clearSelection();
    this.closeDrawer();
    this.loadWorkspace();
  }

  onServerFilterChange() {
    this.resetPagination();
    this.syncCatalogSelections();
    this.quickFilter = 'ALL';
    this.clearSelection();
    this.closeDrawer();
    this.persistFilters();
    this.loadWorkspace();
  }

  onLocalFilterChange() {
    this.resetPagination();
    this.quickFilter = 'ALL';
    this.clearSelection();
    this.closeDrawer();
    this.persistFilters();
    this.refreshWorkspaceView();
  }

  applySearch() {
    this.filters.search = this.searchDraft.trim();
    this.resetPagination();
    this.quickFilter = 'ALL';
    this.clearSelection();
    this.closeDrawer();
    this.persistFilters();
    this.refreshWorkspaceView();
  }

  onSearchChange() {
    this.searchDraft = this.searchDraft ?? '';
  }

  clearFilters() {
    this.resetPagination();
    this.filters = this.emptyFilters();
    this.searchDraft = '';
    this.syncCatalogSelections();
    this.quickFilter = 'ALL';
    this.message = '';
    this.error = '';
    this.clearSelection();
    this.closeDrawer();
    this.loadWorkspace(undefined, true);
  }

  setQuickFilter(filter: WorkspaceQuickFilter) {
    this.resetPagination();
    this.quickFilter = filter;
    this.clearSelection();
    this.closeDrawer();
    this.refreshWorkspaceView();
  }

  quickFilterLabel(filter: WorkspaceQuickFilter) {
    switch (filter) {
      case 'BLOCKED':
        return 'Con bloqueo';
      case 'ALERTS':
        return 'Con alertas';
      case 'NO_TEACHER':
        return 'Sin docente';
      case 'NO_SCHEDULE':
        return 'Sin horario';
      case 'COMPLETE':
        return 'Completos';
      default:
        return 'Todas';
    }
  }

  quickFilterCount(filter: WorkspaceQuickFilter) {
    return this.baseRows.filter((row) => this.matchesQuickFilter(row, filter)).length;
  }

  toggleSelectAllVisible(checked: boolean) {
    if (checked) {
      const selected = new Set(this.selectedRowIds);
      this.paginatedRows.forEach((row) => selected.add(row.row_id));
      this.selectedRowIds = [...selected];
      return;
    }
    const visibleIds = new Set(this.paginatedRows.map((row) => row.row_id));
    this.selectedRowIds = this.selectedRowIds.filter((id) => !visibleIds.has(id));
  }

  onPageSizeChange(value: string | number) {
    const parsed = Number(value);
    if (!this.pageSizeOptions.includes(parsed)) {
      return;
    }
    this.pageSize = parsed;
    this.currentPage = 1;
  }

  goToFirstPage() {
    this.currentPage = 1;
  }

  goToPreviousPage() {
    this.currentPage = Math.max(1, this.currentPage - 1);
  }

  goToNextPage() {
    this.currentPage = Math.min(this.totalPages, this.currentPage + 1);
  }

  goToLastPage() {
    this.currentPage = this.totalPages;
  }

  private resetPagination() {
    this.currentPage = 1;
  }

  toggleRowSelection(rowId: string, checked: boolean) {
    if (checked) {
      if (!this.selectedRowIds.includes(rowId)) {
        this.selectedRowIds = [...this.selectedRowIds, rowId];
      }
      return;
    }
    this.selectedRowIds = this.selectedRowIds.filter((id) => id !== rowId);
  }

  clearSelection() {
    this.selectedRowIds = [];
  }

  onDrawerBuildingChange(value: string) {
    this.drawer.form.building_id = value;
    if (!value) {
      this.drawer.form.classroom_id = '';
      return;
    }
    const currentClassroom = this.classrooms.find((item: any) => item.id === this.drawer.form.classroom_id) ?? null;
    if (currentClassroom?.building_id !== value) {
      this.drawer.form.classroom_id = '';
    }
  }

  openDrawer(row: PlanningWorkspaceRow) {
    this.drawer = {
      open: true,
      row_id: row.row_id,
      form: this.buildDrawerForm(row),
    };
    this.message = '';
  }

  closeDrawer() {
    this.drawer = {
      open: false,
      row_id: '',
      form: this.emptyDrawerForm(),
    };
  }

  saveDrawer() {
    const row = this.activeDrawerRow;
    if (!row) {
      return;
    }

    const sectionPayload: Record<string, unknown> = {};
    const subsectionPayload: Record<string, unknown> = {};
    const nextTeacherId = this.drawer.form.teacher_id.trim();
    if (!row.meeting_id && nextTeacherId !== (this.primaryTeacher(row)?.teacher_id ?? '')) {
      subsectionPayload['responsible_teacher_id'] = nextTeacherId || '';
    }

    const nextBuildingId = this.drawer.form.building_id.trim();
    const nextClassroomId = this.drawer.form.classroom_id.trim();
    if (
      !row.meeting_id &&
      (nextBuildingId !== (row.building_id ?? '') || nextClassroomId !== (row.classroom_id ?? ''))
    ) {
      const classroom = nextClassroomId
        ? this.classrooms.find((item: any) => item.id === nextClassroomId) ?? null
        : null;
      subsectionPayload['building_id'] = (classroom?.building_id ?? nextBuildingId) || '';
      subsectionPayload['classroom_id'] = nextClassroomId || '';
      if (classroom?.capacity !== null && classroom?.capacity !== undefined) {
        subsectionPayload['capacity_snapshot'] = classroom.capacity;
      }
    }

    const academicHours = this.parseOptionalInt(this.drawer.form.academic_hours);
    if (academicHours !== null && academicHours !== row.academic_hours) {
      Object.assign(subsectionPayload, this.buildAssignedHoursPayload(row, academicHours));
    }

    const capacity = this.parseOptionalInt(this.drawer.form.capacity);
    if (capacity !== null && capacity !== row.group_capacity) {
      subsectionPayload['capacity_snapshot'] = capacity;
    }

    if (this.drawer.form.group_note !== (row.group_note ?? '')) {
      subsectionPayload['denomination'] = this.drawer.form.group_note;
    }

    const projectedVacancies = this.parseOptionalInt(this.drawer.form.projected_vacancies);
    if (projectedVacancies !== null && projectedVacancies !== row.projected_vacancies) {
      sectionPayload['projected_vacancies'] = projectedVacancies;
    }

    if (this.drawer.form.offering_status !== row.offering_status) {
      subsectionPayload['status'] = this.drawer.form.offering_status ? 'ACTIVE' : 'CLOSED';
    }

    const schedulePayload: Record<string, unknown> = {};
    const nextDay = this.drawer.form.day_of_week.trim();
    const nextStart = this.drawer.form.start_time.trim();
    const nextEnd = this.drawer.form.end_time.trim();
    const nextSessionType = this.normalizeSessionTypeForRow(
      row,
      this.drawer.form.session_type.trim() || this.defaultSessionTypeForGroupType(row.group_type),
    );
    this.drawer.form.session_type = nextSessionType;
    const scheduleTouched =
      nextDay !== (row.day_of_week ?? '') ||
      nextStart !== (row.start_time ?? '') ||
      nextEnd !== (row.end_time ?? '') ||
      nextSessionType !== (row.session_type ?? 'OTHER');

    if (nextTeacherId !== (this.primaryTeacher(row)?.teacher_id ?? '')) {
      schedulePayload['teacher_id'] = nextTeacherId || '';
    }
    if (nextBuildingId !== (row.building_id ?? '') || nextClassroomId !== (row.classroom_id ?? '')) {
      const classroom = nextClassroomId
        ? this.classrooms.find((item: any) => item.id === nextClassroomId) ?? null
        : null;
      schedulePayload['building_id'] = (classroom?.building_id ?? nextBuildingId) || '';
      schedulePayload['classroom_id'] = nextClassroomId || '';
    }
    if (nextSessionType !== (row.session_type ?? 'OTHER')) {
      schedulePayload['session_type'] = nextSessionType;
    }
    if (scheduleTouched) {
      schedulePayload['day_of_week'] = nextDay;
      schedulePayload['start_time'] = nextStart;
      schedulePayload['end_time'] = nextEnd;
    }

    if (!row.meeting_id && (nextDay || nextStart || nextEnd) && (!nextDay || !nextStart || !nextEnd)) {
      this.error = 'Para crear el primer horario debes indicar dia, hora inicio y hora fin.';
      this.cdr.detectChanges();
      return;
    }

    if (
      !row.meeting_id &&
      (scheduleTouched || nextTeacherId || nextBuildingId || nextClassroomId) &&
      (!nextDay || !nextStart || !nextEnd)
    ) {
      this.error = 'Para crear el primer horario debes completar dia y rango horario.';
      this.cdr.detectChanges();
      return;
    }

    const requests = [] as any[];
    if (Object.keys(sectionPayload).length > 0) {
      requests.push(this.api.updatePlanningSection(row.section_id, sectionPayload));
    }
    if (Object.keys(subsectionPayload).length > 0) {
      requests.push(this.api.updatePlanningSubsection(row.group_id, subsectionPayload));
    }
    if (row.meeting_id && Object.keys(schedulePayload).length > 0) {
      if (this.drawer.form.academic_hours.trim()) {
        schedulePayload['academic_hours'] = this.parseOptionalInt(this.drawer.form.academic_hours);
      }
      requests.push(
        this.api.updatePlanningSubsectionSchedule(row.meeting_id, schedulePayload),
      );
    } else if (!row.meeting_id && nextDay && nextStart && nextEnd) {
      if (this.drawer.form.academic_hours.trim()) {
        schedulePayload['academic_hours'] = this.parseOptionalInt(this.drawer.form.academic_hours);
      }
      requests.push(
        this.api.createPlanningSubsectionSchedule(row.group_id, {
          ...schedulePayload,
          day_of_week: nextDay,
          start_time: nextStart,
          end_time: nextEnd,
          session_type: nextSessionType,
        }),
      );
    }

    if (requests.length === 0) {
      this.message = 'No hay cambios pendientes en el grupo u horario seleccionado.';
      this.error = '';
      this.cdr.detectChanges();
      return;
    }

    this.saving = true;
    this.error = '';
    forkJoin(requests).subscribe({
      next: () => {
        this.message = 'Grupo u horario actualizado.';
        this.clearSelection();
        this.saving = false;
        this.loadWorkspace(row.row_id);
      },
      error: (err: any) => {
        this.saving = false;
        this.error = err?.error?.message ?? 'No se pudo actualizar la fila operativa.';
        this.cdr.detectChanges();
      },
    });
  }

  openTeacherModal() {
    if (!this.canOpenTeacherModal) {
      return;
    }
    this.teacherModal = {
      open: true,
      teacher_id: '',
      query: '',
    };
  }

  closeTeacherModal() {
    this.teacherModal = {
      open: false,
      teacher_id: '',
      query: '',
    };
  }

  confirmBulkTeacher() {
    if (!this.teacherModal.teacher_id) {
      this.error = 'Selecciona un docente para la asignacion masiva.';
      this.cdr.detectChanges();
      return;
    }

    this.saving = true;
    this.error = '';
    const meetingRequests = this.selectedRows
      .filter((row) => Boolean(row.meeting_id))
      .map((row) =>
        this.api.updatePlanningSubsectionSchedule(row.meeting_id as string, {
          teacher_id: this.teacherModal.teacher_id,
        }),
      );
    const groupIdsWithoutMeeting = [
      ...new Set(this.selectedRows.filter((row) => !row.meeting_id).map((row) => row.group_id)),
    ];
    const groupRequests = groupIdsWithoutMeeting.map((subsectionId) =>
      this.api.updatePlanningSubsection(subsectionId, {
        responsible_teacher_id: this.teacherModal.teacher_id,
      }),
    );

    forkJoin([...meetingRequests, ...groupRequests]).subscribe({
      next: () => {
        this.message = `Docente asignado en ${this.selectedRows.length} filas seleccionadas.`;
        this.closeTeacherModal();
        this.clearSelection();
        this.closeDrawer();
        this.saving = false;
        this.loadWorkspace();
      },
      error: (err: any) => {
        this.saving = false;
        this.error = err?.error?.message ?? 'No se pudo asignar el docente.';
        this.cdr.detectChanges();
      },
    });
  }

  openClassroomModal() {
    if (!this.canOpenClassroomModal) {
      return;
    }
    this.classroomModal = {
      open: true,
      classroom_id: '',
      query: '',
    };
  }

  closeClassroomModal() {
    this.classroomModal = {
      open: false,
      classroom_id: '',
      query: '',
    };
  }

  confirmBulkClassroom() {
    if (!this.classroomModal.classroom_id) {
      this.error = 'Selecciona un aula para la asignacion masiva.';
      this.cdr.detectChanges();
      return;
    }

    this.saving = true;
    this.error = '';
    const classroom = this.classrooms.find((item: any) => item.id === this.classroomModal.classroom_id) ?? null;
    const meetingRequests = this.selectedRows
      .filter((row) => Boolean(row.meeting_id))
      .map((row) =>
        this.api.updatePlanningSubsectionSchedule(row.meeting_id as string, {
          classroom_id: this.classroomModal.classroom_id,
          building_id: classroom?.building_id ?? '',
        }),
      );
    const groupIdsWithoutMeeting = [
      ...new Set(this.selectedRows.filter((row) => !row.meeting_id).map((row) => row.group_id)),
    ];
    const groupRequests = groupIdsWithoutMeeting.map((subsectionId) =>
      this.api.updatePlanningSubsection(subsectionId, {
        classroom_id: this.classroomModal.classroom_id,
        building_id: classroom?.building_id ?? '',
        capacity_snapshot: classroom?.capacity ?? null,
      }),
    );

    forkJoin([...meetingRequests, ...groupRequests]).subscribe({
      next: () => {
        this.message = `Aula asignada en ${this.selectedRows.length} filas seleccionadas.`;
        this.closeClassroomModal();
        this.clearSelection();
        this.closeDrawer();
        this.saving = false;
        this.loadWorkspace();
      },
      error: (err: any) => {
        this.saving = false;
        this.error = err?.error?.message ?? 'No se pudo asignar el aula.';
        this.cdr.detectChanges();
      },
    });
  }

  openReplicateModal() {
    const source = this.replicateSourceRow();
    if (!source || this.compatibleReplicateTargetGroups(source).length === 0) {
      this.error = 'Selecciona al menos una fila con horario y otro grupo compatible del mismo curso.';
      this.cdr.detectChanges();
      return;
    }
    this.replicateModal = {
      open: true,
      source_row_id: source.row_id,
    };
  }

  closeReplicateModal() {
    this.replicateModal = {
      open: false,
      source_row_id: '',
    };
  }

  confirmReplicate() {
    const sourceRow = this.replicateSourceRow();
    const targetGroups = this.compatibleReplicateTargetGroups(sourceRow);
    if (!sourceRow || !sourceRow.meeting_id || targetGroups.length === 0) {
      this.error = 'No hay grupos destino compatibles para replicar el horario.';
      this.cdr.detectChanges();
      return;
    }

    this.saving = true;
    this.error = '';
    forkJoin(
      targetGroups.map((row) =>
        this.api.createPlanningSubsectionSchedule(row.group_id, {
          day_of_week: sourceRow.day_of_week,
          start_time: sourceRow.start_time,
          end_time: sourceRow.end_time,
          academic_hours: sourceRow.academic_hours ?? undefined,
          teacher_id: this.primaryTeacher(sourceRow)?.teacher_id || undefined,
          building_id: sourceRow.building_id || undefined,
          classroom_id: sourceRow.classroom_id || undefined,
          session_type: sourceRow.session_type || undefined,
        }),
      ),
    ).subscribe({
      next: () => {
        this.message = `Horario replicado hacia ${targetGroups.length} grupos del mismo curso.`;
        this.closeReplicateModal();
        this.clearSelection();
        this.closeDrawer();
        this.saving = false;
        this.loadWorkspace();
      },
      error: (err: any) => {
        this.saving = false;
        this.error = err?.error?.message ?? 'No se pudo replicar el horario.';
        this.cdr.detectChanges();
      },
    });
  }

  canSyncSectionFromAkademic(row: PlanningWorkspaceRow) {
    return row.offer_source_system === 'AKADEMIC' && Boolean(row.section_source_section_id);
  }

  async syncSectionFromAkademic(event: Event, row: PlanningWorkspaceRow) {
    event.stopPropagation();
    if (!this.canSyncSectionFromAkademic(row)) {
      return;
    }
    const accepted = await this.dialog.confirm({
      title: 'Sincronizar con Akademic',
      message: `Se reemplazaran grupos, docentes y horarios de la seccion ${row.external_section_code || row.internal_section_code || row.group_code} del curso ${this.courseLabel(row)} con lo que venga hoy desde Akademic.`,
      confirmLabel: 'Sincronizar',
    });
    if (!accepted) {
      return;
    }

    this.saving = true;
    this.error = '';
    this.api.syncPlanningSectionFromAkademic(row.section_id).subscribe({
      next: (result: any) => {
        const summary = result?.summary ?? {};
        this.message = result?.section_deleted
          ? `La seccion ${row.external_section_code || row.internal_section_code || row.group_code} ya no existe en Akademic y fue retirada localmente.`
          : `Seccion sincronizada. Grupos +${summary.subsections_created ?? 0} / ~${summary.subsections_updated ?? 0} / -${summary.subsections_deleted ?? 0}. Horarios +${summary.schedules_created ?? 0} / ~${summary.schedules_updated ?? 0} / -${summary.schedules_deleted ?? 0}.`;
        this.clearSelection();
        this.closeDrawer();
        this.saving = false;
        this.loadWorkspace();
      },
      error: (err: any) => {
        this.saving = false;
        this.error = err?.error?.message ?? 'No se pudo sincronizar la seccion con Akademic.';
        this.cdr.detectChanges();
      },
    });
  }

  rowState(row: PlanningWorkspaceRow) {
    if (!row.meeting_id) {
      return 'DRAFT';
    }
    if (row.blocking_alert_count > 0) {
      return 'BLOCKED';
    }
    if (row.alert_count > 0) {
      return 'IN_PROGRESS';
    }
    return 'COMPLETE';
  }

  rowStateLabel(row: PlanningWorkspaceRow) {
    switch (this.rowState(row)) {
      case 'BLOCKED':
        return 'Bloqueado';
      case 'IN_PROGRESS':
        return 'En progreso';
      case 'COMPLETE':
        return 'Completo';
      default:
        return 'Borrador';
    }
  }

  rowStateClass(row: PlanningWorkspaceRow) {
    switch (this.rowState(row)) {
      case 'BLOCKED':
        return 'status-blocked';
      case 'IN_PROGRESS':
        return 'status-progress';
      case 'COMPLETE':
        return 'status-complete';
      default:
        return 'status-draft';
    }
  }

  trackRow(_: number, row: PlanningWorkspaceRow) {
    return row.row_id;
  }

  alertPills(row: PlanningWorkspaceRow) {
    return (row.alerts ?? []).slice(0, 2);
  }

  remainingAlertCount(row: PlanningWorkspaceRow) {
    return Math.max(0, (row.alerts ?? []).length - this.alertPills(row).length);
  }

  courseLabel(row: PlanningWorkspaceRow) {
    if (row.course_code && row.course_name) {
      return `${row.course_code} - ${row.course_name}`;
    }
    return row.course_code || row.course_name || 'Curso sin referencia';
  }

  groupLabel(row: PlanningWorkspaceRow) {
    return row.external_section_code || row.course_section_name || 'Seccion sin referencia';
  }

  groupMetaLabel(row: PlanningWorkspaceRow) {
    const subsection = row.group_code ? `Grupo ${row.group_code}` : null;
    const internal = row.internal_section_code ? `Interna ${row.internal_section_code}` : null;
    return [subsection, internal, this.groupTypeLabel(row.group_type)].filter(Boolean).join(' | ');
  }

  groupTypeLabel(value: string | null | undefined) {
    switch (value) {
      case 'THEORY':
        return 'Teoria';
      case 'PRACTICE':
        return 'Practica';
      case 'MIXED':
        return 'Teorico practico';
      case 'MIXED_PRACTICE_THEORY':
        return 'Practico teorico';
      case 'LAB':
        return 'Laboratorio';
      default:
        return value || 'Grupo';
    }
  }

  modalityLabel(row: PlanningWorkspaceRow) {
    return (
      this.modalityOptions.find((item: any) => item.id === row.delivery_modality_id)?.name ??
      row.delivery_modality_id ??
      'Sin modalidad'
    );
  }

  shiftLabel(value: string | null | undefined) {
    return this.shiftOptions.find((item) => item.id === value)?.label ?? value ?? 'Sin turno';
  }

  dayLabel(value: string | null | undefined) {
    return this.dayOptions.find((item) => item.value === value)?.label ?? value ?? 'Sin dia';
  }

  timeLabel(value: string | null | undefined) {
    return value ? String(value).slice(0, 5) : '--:--';
  }

  scheduleLabel(row: PlanningWorkspaceRow) {
    if (!row.meeting_id) {
      return 'Sin horario';
    }
    return `${this.dayLabel(row.day_of_week)} ${this.timeLabel(row.start_time)}-${this.timeLabel(row.end_time)}`;
  }

  sessionTypeLabel(value: string | null | undefined) {
    switch (value) {
      case 'THEORY':
        return 'Teoria';
      case 'PRACTICE':
        return 'Practica';
      case 'LAB':
        return 'Laboratorio';
      case 'OTHER':
        return 'Otro';
      default:
        return value || 'Sin tipo';
    }
  }

  sessionTypeOptionsForRow(row: PlanningWorkspaceRow | null | undefined) {
    switch (row?.group_type) {
      case 'THEORY':
        return this.sessionTypeOptions.filter((item) => item.value === 'THEORY');
      case 'PRACTICE':
        return this.sessionTypeOptions.filter((item) => item.value === 'PRACTICE' || item.value === 'LAB');
      default:
        return this.sessionTypeOptions;
    }
  }

  canEditSessionTypeForRow(row: PlanningWorkspaceRow | null | undefined) {
    return this.sessionTypeOptionsForRow(row).length > 1;
  }

  capacityLabel(row: PlanningWorkspaceRow) {
    return `${row.projected_vacancies ?? 0} / ${row.group_capacity ?? 0}`;
  }

  classroomLabel(row: PlanningWorkspaceRow) {
    if (row.classroom_name && row.classroom_code) {
      return `${row.classroom_code} - ${row.classroom_name}`;
    }
    return row.classroom_name || row.classroom_code || 'Sin aula';
  }

  buildingLabel(row: PlanningWorkspaceRow) {
    return row.building_name || 'Sin pabellon';
  }

  teacherDisplay(teacher: any | null | undefined) {
    if (!teacher) {
      return 'Sin docente';
    }
    const name = teacher.full_name || teacher.name || 'Sin nombre';
    return teacher.dni ? `${teacher.dni} - ${name}` : name;
  }

  primaryTeacher(row: PlanningWorkspaceRow) {
    return row.teachers.find((teacher) => teacher.is_primary) ?? row.teachers[0] ?? null;
  }

  classroomDisplay(classroom: any | null | undefined) {
    if (!classroom) {
      return 'Sin aula';
    }
    const buildingName =
      classroom.building_name ||
      this.buildings.find((item: any) => item.id === classroom.building_id)?.name ||
      '';
    const classroomName = [classroom.code, classroom.name].filter(Boolean).join(' - ');
    const head = [buildingName, classroomName].filter(Boolean).join(' / ');
    return classroom.capacity ? `${head} - Aforo ${classroom.capacity}` : head || classroom.id;
  }

  rowClasses(row: PlanningWorkspaceRow) {
    return {
      'is-selected': this.selectedRowIds.includes(row.row_id),
      'is-blocked': row.blocking_alert_count > 0,
      'is-no-teacher': row.teachers.length === 0,
      'is-no-schedule': !row.meeting_id,
    };
  }

  stopRowClick(event: Event) {
    event.stopPropagation();
  }

  replicateSourceOptions() {
    return this.selectedMeetingRows.sort((left, right) => this.compareRows(left, right));
  }

  replicateSourceRow() {
    const sourceId = this.replicateModal.source_row_id;
    if (sourceId) {
      return this.selectedMeetingRows.find((row) => row.row_id === sourceId) ?? null;
    }
    return this.replicateSourceOptions()[0] ?? null;
  }

  replicateTargetRows() {
    return this.compatibleReplicateTargetGroups(this.replicateSourceRow());
  }

  private emptyFilters(): PlanningWorkspaceFilters {
    return {
      semester_id: '',
      campus_id: '',
      faculty_id: '',
      academic_program_id: '',
      study_plan_id: '',
      delivery_modality_id: '',
      shift_id: '',
      search: '',
    };
  }

  private emptyCatalog(): WorkspaceCatalog {
    return {
      semesters: [],
      campuses: [],
      faculties: [],
      academic_programs: [],
      study_plans: [],
      course_modalities: [],
      shift_options: [],
    };
  }

  private emptyWorkspace(): PlanningWorkspaceResponse {
    return {
      filters: {},
      summaries: [],
      rows: [],
      totals: {
        offerings: 0,
        rows: 0,
        alerts: 0,
        blocking_alerts: 0,
      },
    };
  }

  private emptyDrawerForm(): WorkspaceDrawerForm {
    return {
      teacher_id: '',
      building_id: '',
      classroom_id: '',
      session_type: 'THEORY',
      day_of_week: '',
      start_time: '',
      end_time: '',
      academic_hours: '',
      capacity: '',
      group_note: '',
      projected_vacancies: '',
      offering_status: true,
    };
  }

  private buildDrawerForm(row: PlanningWorkspaceRow): WorkspaceDrawerForm {
    const classroom = this.classrooms.find((item: any) => item.id === row.classroom_id) ?? null;
    const sessionType = this.normalizeSessionTypeForRow(
      row,
      row.session_type ?? this.defaultSessionTypeForGroupType(row.group_type),
    );
    return {
      teacher_id: this.primaryTeacher(row)?.teacher_id ?? '',
      building_id: row.building_id ?? classroom?.building_id ?? '',
      classroom_id: row.classroom_id ?? '',
      session_type: sessionType,
      day_of_week: row.day_of_week ?? '',
      start_time: row.start_time ?? '',
      end_time: row.end_time ?? '',
      academic_hours: row.academic_hours === null || row.academic_hours === undefined ? '' : String(row.academic_hours),
      capacity: row.group_capacity === null || row.group_capacity === undefined ? '' : String(row.group_capacity),
      group_note: row.group_note ?? '',
      projected_vacancies:
        row.projected_vacancies === null || row.projected_vacancies === undefined
          ? ''
          : String(row.projected_vacancies),
      offering_status: Boolean(row.offering_status),
    };
  }

  private defaultSessionTypeForGroupType(groupType: string | null | undefined) {
    if (groupType === 'THEORY') {
      return 'THEORY';
    }
    if (groupType === 'PRACTICE') {
      return 'PRACTICE';
    }
    return 'THEORY';
  }

  private normalizeSessionTypeForRow(
    row: PlanningWorkspaceRow | null | undefined,
    value: string | null | undefined,
  ) {
    const allowed = this.sessionTypeOptionsForRow(row);
    const candidate = (value ?? '').trim() || this.defaultSessionTypeForGroupType(row?.group_type);
    return allowed.some((item) => item.value === candidate)
      ? candidate
      : (allowed[0]?.value ?? 'OTHER');
  }

  private normalizeCatalog(catalog: any): WorkspaceCatalog {
    return {
      semesters: Array.isArray(catalog?.semesters) ? catalog.semesters : [],
      campuses: Array.isArray(catalog?.campuses) ? catalog.campuses : [],
      faculties: Array.isArray(catalog?.faculties) ? catalog.faculties : [],
      academic_programs: Array.isArray(catalog?.academic_programs) ? catalog.academic_programs : [],
      study_plans: Array.isArray(catalog?.study_plans) ? catalog.study_plans : [],
      course_modalities: Array.isArray(catalog?.course_modalities) ? catalog.course_modalities : [],
      shift_options: Array.isArray(catalog?.shift_options) ? catalog.shift_options : [],
    };
  }

  private findCatalogName(items: any[], id: string | null | undefined) {
    if (!id || !Array.isArray(items)) {
      return null;
    }
    return items.find((item: any) => item?.id === id)?.name ?? null;
  }

  private normalizeWorkspace(response: any): PlanningWorkspaceResponse {
    return {
      filters: response?.filters ?? {},
      summaries: Array.isArray(response?.summaries) ? response.summaries : [],
      rows: Array.isArray(response?.rows) ? response.rows : [],
      totals: {
        offerings: Number(response?.totals?.offerings ?? 0),
        rows: Number(response?.totals?.rows ?? 0),
        alerts: Number(response?.totals?.alerts ?? 0),
        blocking_alerts: Number(response?.totals?.blocking_alerts ?? 0),
      },
    };
  }

  private rebuildRowSearchIndex() {
    this.rowSearchIndex = new Map(
      this.workspace.rows.map((row) => [
        row.row_id,
        this.normalizeSearchValue(
          [
            row.course_code,
            row.course_name,
            row.academic_program_code,
            row.academic_program_name,
            row.course_section_name,
            row.external_section_code,
            row.internal_section_code,
            row.group_code,
            row.primary_teacher_name,
            row.classroom_code,
            row.classroom_name,
          ]
            .filter(Boolean)
            .join(' '),
        ),
      ] as const),
    );
  }

  private refreshWorkspaceView() {
    this.serverScopedRowsCache = this.workspace.rows.filter(
      (row) => this.matchesFacultyFilter(row) && this.matchesStudyPlanFilter(row),
    );
    this.modalityOptionsCache = this.buildModalityOptions(this.serverScopedRowsCache);
    this.shiftOptionsCache = this.buildShiftOptions(this.serverScopedRowsCache);
    this.baseRowsCache = this.serverScopedRowsCache.filter(
      (row) =>
        this.matchesModalityFilter(row) &&
        this.matchesShiftFilter(row) &&
        this.matchesSearchFilter(row),
    );
    this.visibleRowsCache = [...this.baseRowsCache]
      .filter((row) => this.matchesQuickFilter(row))
      .sort((left, right) => this.compareRows(left, right));
    this.visibleTotalsCache = {
      offerings: new Set(this.visibleRowsCache.map((row) => row.offering_id)).size,
      rows: this.visibleRowsCache.length,
      alerts: this.visibleRowsCache.reduce((sum, row) => sum + row.alert_count, 0),
      blocking_alerts: this.visibleRowsCache.reduce((sum, row) => sum + row.blocking_alert_count, 0),
    };
    this.visibleGroupCountCache = new Set(this.visibleRowsCache.map((row) => row.group_id)).size;
  }

  private buildModalityOptions(rows: PlanningWorkspaceRow[]) {
    const map = new Map<string, any>();
    const preferredCodes = new Set([
      'HIBRIDO_VIRTUAL',
      'HIBRIDO_PRESENCIAL',
      'VIRTUAL',
      'PRESENCIAL',
    ]);
    const catalogModalities = Array.isArray(this.catalog.course_modalities) ? this.catalog.course_modalities : [];
    
    catalogModalities.forEach((item: any) => {
      if (preferredCodes.has(item.code)) {
        map.set(item.id, {
          id: item.id,
          name: item.name,
        });
      }
    });

    rows.forEach((row) => {
      if (!row.delivery_modality_id) {
        return;
      }
      if (map.has(row.delivery_modality_id)) {
        return;
      }
      const match = catalogModalities.find((item: any) => item.id === row.delivery_modality_id) ?? null;
      map.set(row.delivery_modality_id, {
        id: row.delivery_modality_id,
        name: match?.name ?? row.delivery_modality_id,
      });
    });
    return [...map.values()].sort((left, right) => `${left.name ?? ''}`.localeCompare(`${right.name ?? ''}`));
  }

  private buildShiftOptions(rows: PlanningWorkspaceRow[]) {
    const map = new Map<string, { id: string; label: string }>();
    rows.forEach((row) => {
      if (!row.shift_id) {
        return;
      }
      const known =
        (Array.isArray(this.catalog.shift_options) ? this.catalog.shift_options : []).find(
          (item: any) => item.id === row.shift_id,
        ) ??
        this.fallbackShiftOptions.find((item) => item.id === row.shift_id) ??
        { id: row.shift_id, label: row.shift_id };
      map.set(row.shift_id, known);
    });
    return [...map.values()];
  }

  private restoreFilters() {
    const query = this.route.snapshot.queryParamMap;
    const queryFilters: PlanningWorkspaceFilters = {
      semester_id: query.get('semester_id') ?? '',
      campus_id: query.get('campus_id') ?? '',
      faculty_id: query.get('faculty_id') ?? '',
      academic_program_id: query.get('academic_program_id') ?? '',
      study_plan_id: query.get('study_plan_id') ?? '',
      delivery_modality_id: query.get('delivery_modality_id') ?? '',
      shift_id: query.get('shift_id') ?? '',
      search: query.get('search') ?? '',
    };
    if (Object.values(queryFilters).some((value) => Boolean(value))) {
      this.filters = queryFilters;
      this.persistFilters();
      return;
    }

    try {
      const raw = localStorage.getItem(this.filtersStorageKey);
      if (!raw) {
        return;
      }
      const stored = JSON.parse(raw) as Partial<PlanningWorkspaceFilters>;
      this.filters = {
        semester_id: stored.semester_id ?? '',
        campus_id: stored.campus_id ?? '',
        faculty_id: stored.faculty_id ?? '',
        academic_program_id: stored.academic_program_id ?? '',
        study_plan_id: stored.study_plan_id ?? '',
        delivery_modality_id: stored.delivery_modality_id ?? '',
        shift_id: stored.shift_id ?? '',
        search: stored.search ?? '',
      };
    } catch {
      this.filters = this.emptyFilters();
    }
  }

  private persistFilters() {
    localStorage.setItem(this.filtersStorageKey, JSON.stringify(this.filters));
  }

  private syncCatalogSelections() {
    if (this.filters.semester_id && !this.availableSemesters.some((item: any) => item.id === this.filters.semester_id)) {
      this.filters.semester_id = '';
    }
    if (!this.filters.semester_id && this.availableSemesters.length > 0) {
      this.filters.semester_id = this.availableSemesters[0].id;
    }
    if (this.filters.campus_id && !this.availableCampuses.some((item: any) => item.id === this.filters.campus_id)) {
      this.filters.campus_id = '';
    }
    if (this.filters.faculty_id && !this.availableFaculties.some((item: any) => item.id === this.filters.faculty_id)) {
      this.filters.faculty_id = '';
    }
    this.syncProgramSelection();
    this.syncStudyPlanSelection();
    if (
      this.filters.delivery_modality_id &&
      !this.modalityOptions.some((item: any) => item.id === this.filters.delivery_modality_id)
    ) {
      this.filters.delivery_modality_id = '';
    }
    if (this.filters.shift_id && !this.shiftOptions.some((item) => item.id === this.filters.shift_id)) {
      this.filters.shift_id = '';
    }
  }

  private syncProgramSelection() {
    if (!this.filteredPrograms.some((item: any) => item.id === this.filters.academic_program_id)) {
      this.filters.academic_program_id = '';
    }
  }

  private syncStudyPlanSelection() {
    if (!this.filteredStudyPlans.some((item: any) => item.id === this.filters.study_plan_id)) {
      this.filters.study_plan_id = '';
    }
  }

  private apiFilters() {
    return {
      semester_id: this.filters.semester_id,
      campus_id: this.filters.campus_id,
      academic_program_id: this.filters.academic_program_id,
      study_plan_id: this.filters.study_plan_id,
      delivery_modality_id: this.filters.delivery_modality_id,
      shift_id: this.filters.shift_id,
      search: this.filters.search.trim(),
    };
  }

  private summaryQueryParams() {
    return {
      semester_id: this.filters.semester_id || null,
      campus_id: this.filters.campus_id || null,
      faculty_id: this.filters.faculty_id || null,
      academic_program_id: this.filters.academic_program_id || null,
    };
  }

  private manualOfferFilters() {
    return {
      semester_id: this.filters.semester_id,
      campus_id: this.filters.campus_id,
      faculty_id: this.filters.faculty_id,
      academic_program_id: this.filters.academic_program_id,
      study_plan_id: this.filters.study_plan_id,
    };
  }

  private offerUniverseFilters() {
    return {
      semester_id: this.filters.semester_id,
    };
  }

  private shouldRefreshOfferUniverse() {
    return (this.filters.semester_id ?? '') !== this.offerFilterUniverseSemesterId || this.offerFilterUniverse.length === 0;
  }

  private buildManualWorkspace(offers: any[]): PlanningWorkspaceResponse {
    const rows = offers.flatMap((offer) => this.buildRowsFromOffer(offer));
    const rowsByOffer = new Map<string, PlanningWorkspaceRow[]>();
    rows.forEach((row) => {
      const bucket = rowsByOffer.get(row.offering_id) ?? [];
      bucket.push(row);
      rowsByOffer.set(row.offering_id, bucket);
    });
    const summaries = offers.map((offer) => this.buildSummaryFromOffer(offer, rowsByOffer.get(offer.id) ?? []));
    return {
      filters: this.apiFilters(),
      summaries,
      rows,
      totals: {
        offerings: summaries.length,
        rows: rows.length,
        alerts: rows.reduce((sum, row) => sum + row.alert_count, 0),
        blocking_alerts: rows.reduce((sum, row) => sum + row.blocking_alert_count, 0),
      },
    };
  }

  private buildRowsFromOffer(offer: any) {
    const sections = Array.isArray(offer?.sections) ? offer.sections : [];
    return sections.flatMap((section: any) =>
      (Array.isArray(section?.subsections) ? section.subsections : []).flatMap((subsection: any) => {
        const schedules = Array.isArray(subsection?.schedules) ? subsection.schedules : [];
        if (schedules.length === 0) {
          return [this.makeManualRow(offer, section, subsection, null)];
        }
        return schedules.map((schedule: any) => this.makeManualRow(offer, section, subsection, schedule));
      }),
    );
  }

  private makeManualRow(offer: any, section: any, subsection: any, schedule: any | null): PlanningWorkspaceRow {
    const effectiveTeacher = schedule?.teacher ?? subsection?.responsible_teacher ?? null;
    const effectiveTeacherId =
      schedule?.teacher_id ?? subsection?.responsible_teacher_id ?? effectiveTeacher?.id ?? null;
    const teachers = effectiveTeacherId
      ? [
          {
            assignment_id: schedule?.id ?? subsection?.id ?? effectiveTeacherId,
            teacher_id: effectiveTeacherId,
            full_name: effectiveTeacher?.full_name ?? effectiveTeacher?.name ?? null,
            role: 'PRIMARY',
            is_primary: true,
          } satisfies PlanningWorkspaceTeacher,
        ]
      : [];
    const effectiveBuilding = schedule?.building ?? subsection?.building ?? null;
    const effectiveClassroom = schedule?.classroom ?? subsection?.classroom ?? null;
    const alerts = this.buildManualAlerts(subsection, schedule);
    return {
      row_id: schedule?.id ?? `subsection:${subsection.id}`,
      row_kind: schedule ? 'MEETING' : 'GROUP',
      offering_id: offer.id,
      offer_source_system: offer.source_system ?? null,
      section_id: section.id,
      section_source_section_id: section.source_section_id ?? null,
      group_id: subsection.id,
      meeting_id: schedule?.id ?? null,
      semester_id: offer.semester_id,
      semester_name: offer.semester?.name ?? this.catalog.semesters.find((item: any) => item.id === offer.semester_id)?.name ?? null,
      campus_id: offer.campus_id,
      campus_name: offer.campus?.name ?? this.catalog.campuses.find((item: any) => item.id === offer.campus_id)?.name ?? null,
      faculty_id: offer.faculty_id,
      faculty_name: offer.faculty?.name ?? this.availableFaculties.find((item: any) => item.id === offer.faculty_id)?.name ?? null,
      academic_program_id: offer.academic_program_id,
      academic_program_code: offer.academic_program?.code ?? offer.academic_program_code ?? null,
      academic_program_name:
        offer.academic_program?.name ??
        this.catalog.academic_programs.find((item: any) => item.id === offer.academic_program_id)?.name ??
        null,
      study_plan_id: offer.study_plan_id,
      study_plan_name:
        offer.study_plan?.name ??
        this.catalog.study_plans.find((item: any) => item.id === offer.study_plan_id)?.name ??
        null,
      course_id: offer.study_plan_course_id ?? offer.id,
      course_code: offer.course_code ?? null,
      course_name: offer.course_name ?? null,
      course_section_id: section.id,
      course_section_name: section.external_code || section.code || null,
      external_section_code: section.external_code ?? null,
      internal_section_code: section.code ?? null,
      delivery_modality_id: subsection.course_modality_id ?? '',
      shift_id: subsection.shift ?? '',
      projected_vacancies: section.projected_vacancies ?? null,
      offering_status: (subsection.status ?? offer.status) !== 'CLOSED',
      source_status: subsection.status ?? offer.status ?? 'DRAFT',
      group_type: subsection.kind ?? 'MIXED',
      group_code: subsection.code ?? '',
      group_capacity: subsection.capacity_snapshot ?? null,
      building_id: schedule?.building_id ?? subsection.building_id ?? effectiveBuilding?.id ?? null,
      building_name: effectiveBuilding?.name ?? null,
      group_note: subsection.denomination ?? null,
      teachers,
      primary_teacher_name: teachers[0]?.full_name ?? null,
      session_type: schedule?.session_type ?? null,
      day_of_week: schedule?.day_of_week ?? null,
      start_time: schedule?.start_time ?? null,
      end_time: schedule?.end_time ?? null,
      minutes: schedule ? this.computeMinutes(schedule.start_time, schedule.end_time) : 0,
      academic_hours: schedule?.academic_hours ?? subsection.assigned_total_hours ?? null,
      classroom_id: schedule?.classroom_id ?? subsection.classroom_id ?? effectiveClassroom?.id ?? null,
      classroom_name: effectiveClassroom?.name ?? null,
      classroom_code: effectiveClassroom?.code ?? null,
      assigned_theoretical_hours: subsection.assigned_theoretical_hours ?? null,
      assigned_practical_hours: subsection.assigned_practical_hours ?? null,
      assigned_total_hours: subsection.assigned_total_hours ?? null,
      alerts,
      alert_count: alerts.length,
      blocking_alert_count: alerts.filter((item) => item.blocking).length,
    };
  }

  private buildManualAlerts(subsection: any, schedule: any | null) {
    const alerts = [] as PlanningWorkspaceAlert[];
    const conflicts = Array.isArray(subsection?.conflicts) ? subsection.conflicts : [];
    conflicts.forEach((conflict: any) => {
      alerts.push({
        code: conflict.conflict_type ?? 'CONFLICT',
        severity: conflict.severity ?? 'WARNING',
        message: this.conflictLabel(conflict.conflict_type),
        blocking: (conflict.severity ?? 'WARNING') !== 'INFO',
      });
    });

    const effectiveTeacherId = schedule?.teacher_id ?? subsection?.responsible_teacher_id;
    const effectiveClassroomId = schedule?.classroom_id ?? subsection?.classroom_id;
    if (!effectiveTeacherId) {
      alerts.push({
        code: 'NO_TEACHER',
        severity: 'WARNING',
        message: 'Falta docente asignado',
        blocking: true,
      });
    }
    if (!subsection?.course_modality_id) {
      alerts.push({
        code: 'NO_MODALITY',
        severity: 'WARNING',
        message: 'Falta modalidad',
        blocking: true,
      });
    }
    if (!subsection?.shift) {
      alerts.push({
        code: 'NO_SHIFT',
        severity: 'WARNING',
        message: 'Falta turno',
        blocking: true,
      });
    }
    if (!schedule) {
      alerts.push({
        code: 'NO_SCHEDULE',
        severity: 'WARNING',
        message: 'Falta horario',
        blocking: true,
      });
    }
    if (schedule && !effectiveClassroomId) {
      alerts.push({
        code: 'NO_CLASSROOM',
        severity: 'WARNING',
        message: 'Falta aula asignada',
        blocking: true,
      });
    }

    const deduped = new Map<string, PlanningWorkspaceAlert>();
    alerts.forEach((alert) => deduped.set(`${alert.code}:${alert.message}`, alert));
    return [...deduped.values()];
  }

  private buildSummaryFromOffer(offer: any, rows: PlanningWorkspaceRow[]): PlanningWorkspaceSummary {
    const state =
      rows.length === 0
        ? 'DRAFT'
        : rows.some((row) => row.blocking_alert_count > 0)
          ? 'BLOCKED'
          : rows.some((row) => row.alert_count > 0)
            ? 'IN_PROGRESS'
            : 'COMPLETE';
    return {
      offering_id: offer.id,
      semester_id: offer.semester_id,
      course_id: offer.study_plan_course_id ?? offer.id,
      course_code: offer.course_code ?? null,
      course_name: offer.course_name ?? null,
      academic_program_name: offer.academic_program?.name ?? null,
      study_plan_name: offer.study_plan?.name ?? null,
      campus_name: offer.campus?.name ?? null,
      group_count: new Set(rows.map((row) => row.group_id)).size,
      row_count: rows.length,
      scheduled_row_count: rows.filter((row) => Boolean(row.meeting_id)).length,
      teacher_count: rows.filter((row) => row.teachers.length > 0).length,
      total_alerts: rows.reduce((sum, row) => sum + row.alert_count, 0),
      blocking_alerts: rows.reduce((sum, row) => sum + row.blocking_alert_count, 0),
      hours_required: {
        theory: Number(offer.theoretical_hours ?? 0),
        practice: Number(offer.practical_hours ?? 0),
        lab: 0,
      },
      hours_planned: {
        theory: rows.reduce((sum, row) => sum + Number(row.assigned_theoretical_hours ?? 0), 0),
        practice: rows.reduce((sum, row) => sum + Number(row.assigned_practical_hours ?? 0), 0),
        lab: 0,
      },
      state,
    };
  }

  private buildAssignedHoursPayload(row: PlanningWorkspaceRow, total: number) {
    if (row.group_type === 'THEORY') {
      return {
        assigned_theoretical_hours: total,
        assigned_practical_hours: 0,
        assigned_total_hours: total,
      };
    }
    if (row.group_type === 'PRACTICE') {
      return {
        assigned_theoretical_hours: 0,
        assigned_practical_hours: total,
        assigned_total_hours: total,
      };
    }

    const currentTheory = Number(row.assigned_theoretical_hours ?? 0);
    const currentPractice = Number(row.assigned_practical_hours ?? 0);
    const currentTotal = Math.max(1, currentTheory + currentPractice);
    const nextTheory = Math.round((currentTheory / currentTotal) * total);
    return {
      assigned_theoretical_hours: nextTheory,
      assigned_practical_hours: Math.max(0, total - nextTheory),
      assigned_total_hours: total,
    };
  }

  private computeMinutes(start: string | null | undefined, end: string | null | undefined) {
    if (!start || !end) {
      return 0;
    }
    return Math.max(0, this.toMinutes(end) - this.toMinutes(start));
  }

  private toMinutes(value: string) {
    const [hours = '0', minutes = '0'] = String(value).split(':');
    return Number(hours) * 60 + Number(minutes);
  }

  private conflictLabel(type: string | null | undefined) {
    switch (type) {
      case 'TEACHER_OVERLAP':
        return 'Cruce docente';
      case 'CLASSROOM_OVERLAP':
        return 'Cruce aula';
      case 'SUBSECTION_OVERLAP':
        return 'Cruce horario';
      case 'SECTION_OVERLAP':
        return 'Cruce seccion';
      default:
        return type ?? 'Cruce detectado';
    }
  }

  private findSubsectionById(subsectionId: string) {
    for (const offer of this.offerDetails) {
      for (const section of offer?.sections ?? []) {
        const found = (section?.subsections ?? []).find((item: any) => item.id === subsectionId);
        if (found) {
          return found;
        }
      }
    }
    return null;
  }

  private matchesFacultyFilter(row: PlanningWorkspaceRow) {
    if (!this.filters.faculty_id) {
      return true;
    }
    return row.faculty_id === this.filters.faculty_id;
  }

  private matchesStudyPlanFilter(row: PlanningWorkspaceRow) {
    if (!this.filters.study_plan_id) {
      return true;
    }
    return row.study_plan_id === this.filters.study_plan_id;
  }

  private matchesModalityFilter(row: PlanningWorkspaceRow) {
    if (!this.filters.delivery_modality_id) {
      return true;
    }
    return row.delivery_modality_id === this.filters.delivery_modality_id;
  }

  private matchesShiftFilter(row: PlanningWorkspaceRow) {
    if (!this.filters.shift_id) {
      return true;
    }
    return row.shift_id === this.filters.shift_id;
  }

  private matchesSearchFilter(row: PlanningWorkspaceRow) {
    const query = this.normalizeSearchValue(this.filters.search);
    if (!query) {
      return true;
    }
    return (this.rowSearchIndex.get(row.row_id) ?? '').includes(query);
  }

  private matchesQuickFilter(row: PlanningWorkspaceRow, filter: WorkspaceQuickFilter = this.quickFilter) {
    switch (filter) {
      case 'BLOCKED':
        return row.blocking_alert_count > 0;
      case 'ALERTS':
        return row.alert_count > 0;
      case 'NO_TEACHER':
        return row.teachers.length === 0;
      case 'NO_SCHEDULE':
        return !row.meeting_id;
      case 'COMPLETE':
        return row.alert_count === 0 && row.teachers.length > 0 && Boolean(row.meeting_id);
      default:
        return true;
    }
  }

  private compareRows(left: PlanningWorkspaceRow, right: PlanningWorkspaceRow) {
    const courseDelta = this.courseLabel(left).localeCompare(this.courseLabel(right));
    if (courseDelta !== 0) {
      return courseDelta;
    }

    const sectionDelta = this.groupLabel(left).localeCompare(this.groupLabel(right));
    if (sectionDelta !== 0) {
      return sectionDelta;
    }

    const groupDelta = String(left.group_code ?? '').localeCompare(String(right.group_code ?? ''));
    if (groupDelta !== 0) {
      return groupDelta;
    }

    const dayDelta = (this.dayOrder[left.day_of_week ?? ''] ?? 99) - (this.dayOrder[right.day_of_week ?? ''] ?? 99);
    if (dayDelta !== 0) {
      return dayDelta;
    }

    return String(left.start_time ?? '').localeCompare(String(right.start_time ?? ''));
  }

  private filteredTeachers(query: string) {
    const normalized = this.normalizeSearchValue(query);
    const pool = !normalized
      ? this.teachers
      : this.teachers.filter((teacher: any) =>
          this.normalizeSearchValue([teacher?.dni, teacher?.full_name, teacher?.name].filter(Boolean).join(' ')).includes(normalized),
        );
    return pool.slice(0, 200);
  }

  private filteredClassrooms(query: string, campusId: string) {
    const normalized = this.normalizeSearchValue(query);
    const byCampus = campusId ? this.classrooms.filter((classroom: any) => classroom.campus_id === campusId) : this.classrooms;
    const pool = !normalized
      ? byCampus
      : byCampus.filter((classroom: any) =>
          this.normalizeSearchValue(
            [classroom?.code, classroom?.name, classroom?.building_name, classroom?.capacity].filter(Boolean).join(' '),
          ).includes(normalized),
        );
    return pool.slice(0, 200);
  }

  private normalizeSearchValue(value: string | number | null | undefined) {
    return `${value ?? ''}`
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .trim();
  }

  private parseOptionalInt(value: string) {
    const normalized = String(value ?? '').trim();
    if (!normalized) {
      return null;
    }
    const parsed = Number(normalized);
    if (!Number.isFinite(parsed)) {
      return null;
    }
    return Math.max(0, Math.trunc(parsed));
  }

  private syncSelectedRows() {
    const available = new Set(this.workspace.rows.map((row) => row.row_id));
    this.selectedRowIds = this.selectedRowIds.filter((id) => available.has(id));
  }

  private syncDrawerAfterReload(focusRowId?: string) {
    if (focusRowId) {
      const focused = this.workspace.rows.find((row) => row.row_id === focusRowId) ?? null;
      if (focused) {
        this.openDrawer(focused);
        return;
      }
      this.closeDrawer();
      return;
    }

    if (!this.drawer.open) {
      return;
    }
    const refreshed = this.workspace.rows.find((row) => row.row_id === this.drawer.row_id) ?? null;
    if (!refreshed) {
      this.closeDrawer();
      return;
    }
    this.openDrawer(refreshed);
  }

  private compatibleReplicateTargetGroups(sourceRow: PlanningWorkspaceRow | null) {
    if (!sourceRow) {
      return [] as PlanningWorkspaceRow[];
    }

    const uniqueTargets = new Map<string, PlanningWorkspaceRow>();
    for (const row of this.selectedRows) {
      if (row.offering_id !== sourceRow.offering_id) {
        continue;
      }
      if (row.group_id === sourceRow.group_id) {
        continue;
      }
      if (!uniqueTargets.has(row.group_id)) {
        uniqueTargets.set(row.group_id, row);
      }
    }
    return [...uniqueTargets.values()].sort((left, right) => this.compareRows(left, right));
  }
}
