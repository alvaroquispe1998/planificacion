import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit, QueryList, ViewChildren } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { MultiSelectComponent, MultiSelectOption } from '../../components/multi-select/multi-select.component';
import { DialogService } from '../../core/dialog.service';
import {
  VideoconferenceAssignmentPreviewDaySummary,
  VideoconferenceAssignmentPreviewItem,
  VideoconferenceAssignmentPreviewResponse,
  FilterOptionsDto,
  VideoconferenceGenerationResponse,
  VideoconferenceGenerationResultItem,
  VideoconferenceApiService,
  VideoconferenceFilterOptionsResponse,
  VideoconferenceOverridePayload,
  VideoconferencePreviewDto,
  VideoconferencePreviewItem,
} from '../../services/videoconference-api.service';

type PreviewSelectionItem = VideoconferencePreviewItem & {
  selected: boolean;
};

type ResolvedGenerationResultItem = VideoconferenceGenerationResultItem & {
  preview: PreviewSelectionItem | null;
};

type ResolvedAssignmentPreviewItem = VideoconferenceAssignmentPreviewItem & {
  preview: PreviewSelectionItem | null;
};

type AssignmentUserUsageSummary = {
  host_key: string;
  host_label: string;
  total_sessions: number;
  inherited_rows: number;
  license_label: string | null;
  is_licensed: boolean | null;
  session_lines: string[];
};

type AppliedFilterSnapshot = {
  selectedPeriod: string;
  selectedCampuses: string[];
  selectedFaculties: string[];
  selectedPrograms: string[];
  selectedCourses: string[];
  selectedModalities: string[];
  selectedDays: string[];
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
  selectedPeriod = '';
  selectedModalities: string[] = [];
  selectedDays: string[] = [];

  periodOptions: MultiSelectOption[] = [];
  campusOptions: MultiSelectOption[] = [];
  facultyOptions: MultiSelectOption[] = [];
  programOptions: MultiSelectOption[] = [];
  courseOptions: MultiSelectOption[] = [];
  modalityOptions: MultiSelectOption[] = [];
  dayOptions: MultiSelectOption[] = [];

  previewData: PreviewSelectionItem[] = [];
  startDate = '';
  endDate = '';
  assignmentPreview: VideoconferenceAssignmentPreviewResponse | null = null;
  generationResult: VideoconferenceGenerationResponse | null = null;
  loading = false;
  filterOptionsLoading = false;
  overrideSaving = false;
  assignmentUsersModalOpen = false;
  expandedAssignmentHostKey = '';
  hasSearched = false;
  retryingRecordId = '';
  appliedFilterSnapshot: AppliedFilterSnapshot | null = null;

  overrideEditorOpen = false;
  overrideTarget: PreviewSelectionItem | null = null;
  overrideForm = {
    overrideDate: '',
    overrideStartTime: '',
    overrideEndTime: '',
    reasonCode: 'OTHER' as 'HOLIDAY' | 'WEATHER' | 'OTHER',
    notes: '',
  };

  private filterOptionsRequestId = 0;

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly dialog: DialogService,
    private readonly cdr: ChangeDetectorRef,
    private readonly router: Router,
  ) {}

  ngOnInit() {
    this.refreshFilterOptions();
  }

  get totalSchedules() {
    return this.previewData.length;
  }

  get selectableRows() {
    return this.previewData.filter((item) => item.selectable);
  }

  get selectedCount() {
    return this.previewData.filter((item) => item.selectable && item.selected).length;
  }

  get allSelected() {
    return this.selectableRows.length > 0 && this.selectableRows.every((item) => item.selected);
  }

  get selectionSummaryLabel() {
    const unitLabel = this.usesOccurrenceRows ? 'ocurrencias generables' : 'horarios base';
    if (!this.totalSchedules) {
      return this.usesOccurrenceRows ? 'Sin ocurrencias cargadas' : 'Sin horarios cargados';
    }
    if (!this.selectableRows.length) {
      return this.usesOccurrenceRows
        ? `${this.totalSchedules} ocurrencias en rango, pero todas estan omitidas`
        : `${this.totalSchedules} horarios base cargados`;
    }
    if (this.selectedCount === this.selectableRows.length) {
      return `Todos los ${this.selectableRows.length} ${unitLabel} estan seleccionados`;
    }
    return `${this.selectedCount} de ${this.selectableRows.length} ${unitLabel} seleccionados`;
  }

  get activeFilterLabels() {
    return this.buildFilterLabels(this.appliedFilterSnapshot ?? this.captureFilterSnapshot());
  }

  get hasActiveFilters() {
    return this.activeFilterLabels.length > 0;
  }

  get hasOperationalRange() {
    return Boolean(this.startDate && this.endDate);
  }

  get usesOccurrenceRows() {
    return this.previewData.some((item) => Boolean(item.effective_conference_date));
  }

  get generationRows(): ResolvedGenerationResultItem[] {
    const rows = this.generationResult?.results ?? [];
    return rows.map((item) => ({
      ...item,
      preview: this.findPreviewItemForResult(item),
    }));
  }

  get validationResultRows() {
    return this.generationRows.filter((item) => item.status === 'VALIDATION_ERROR');
  }

  get unmatchedResultRows() {
    return this.generationRows.filter((item) => item.status === 'CREATED_UNMATCHED');
  }

  get attentionResultRows() {
    return this.generationRows.filter((item) =>
      ['ERROR', 'NO_AVAILABLE_ZOOM_USER', 'BLOCKED_EXISTING'].includes(item.status),
    );
  }

  get successfulResultRows() {
    return this.generationRows.filter((item) => item.status === 'MATCHED');
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

  onPeriodChange(selectedPeriod: string) {
    this.selectedPeriod = selectedPeriod;
    this.handleFilterChange();
  }

  onModalityChange(selectedIds: string[]) {
    this.selectedModalities = selectedIds;
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
    this.selectedPeriod = '';
    this.selectedModalities = [];
    this.selectedDays = [];
    this.closeOverrideEditor();
    this.resetPreviewState();
    this.closeFilterDropdowns();
    this.blurActiveElement();
    this.refreshFilterOptions();
  }

  onLoadBase() {
    this.closeFilterDropdowns();
    this.blurActiveElement();
    this.hasSearched = true;
    this.generationResult = null;
    this.closeOverrideEditor();
    this.loadPreview(new Set<string>(), false);
  }

  async applyOperationalRange() {
    if (!this.startDate || !this.endDate) {
      await this.dialog.alert({
        title: 'Rango requerido',
        message: 'Define fecha inicio y fecha fin para ver ocurrencias por fecha.',
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

    this.closeOverrideEditor();
    this.generationResult = null;
    this.hasSearched = true;
    this.loadPreview();
  }

  toggleAll() {
    const nextValue = !this.allSelected;
    this.previewData.forEach((item) => {
      if (item.selectable) {
        item.selected = nextValue;
      }
    });
  }

  get assignmentPreviewRows(): ResolvedAssignmentPreviewItem[] {
    const rows = this.assignmentPreview?.items ?? [];
    return rows.map((item) => ({
      ...item,
      preview: this.findPreviewItemForAssignment(item),
    }));
  }

  get assignmentSummary() {
    return this.assignmentPreview?.summary ?? null;
  }

  get assignmentDaySummaries(): VideoconferenceAssignmentPreviewDaySummary[] {
    return this.assignmentPreview?.summary?.licenses_by_day ?? [];
  }

  get assignmentUserSummaries(): AssignmentUserUsageSummary[] {
    const usageMap = new Map<string, AssignmentUserUsageSummary>();
    for (const item of this.assignmentPreviewRows) {
      const hostKey = `${item.zoom_user_id || item.zoom_user_email || item.zoom_user_name || ''}`.trim();
      if (!hostKey) {
        continue;
      }

      const current = usageMap.get(hostKey) ?? {
        host_key: hostKey,
        host_label: item.zoom_user_email || item.zoom_user_name || item.zoom_user_id || hostKey,
        total_sessions: 0,
        inherited_rows: 0,
        license_label: item.license_label,
        is_licensed: item.is_licensed,
        session_lines: [],
      };

      if (item.preview_status === 'INHERITED') {
        current.inherited_rows += 1;
      }

      current.session_lines.push(this.buildAssignmentSessionLine(item));

      if (item.consumes_capacity || item.preview_status === 'BLOCKED_EXISTING') {
        current.total_sessions += 1;
      }

      usageMap.set(hostKey, current);
    }

    return [...usageMap.values()]
      .filter((item) => item.total_sessions > 0 || item.inherited_rows > 0)
      .sort((left, right) => {
        const bySessions = right.total_sessions - left.total_sessions;
        if (bySessions !== 0) {
          return bySessions;
        }
        const byInherited = right.inherited_rows - left.inherited_rows;
        if (byInherited !== 0) {
          return byInherited;
        }
        return left.host_label.localeCompare(right.host_label);
      });
  }

  openAssignmentUsersModal() {
    if (!this.assignmentUserSummaries.length) {
      return;
    }
    this.expandedAssignmentHostKey = '';
    this.assignmentUsersModalOpen = true;
  }

  closeAssignmentUsersModal() {
    this.assignmentUsersModalOpen = false;
    this.expandedAssignmentHostKey = '';
  }

  toggleAssignmentUserDetails(hostKey: string) {
    this.expandedAssignmentHostKey = this.expandedAssignmentHostKey === hostKey ? '' : hostKey;
  }

  isAssignmentUserExpanded(hostKey: string) {
    return this.expandedAssignmentHostKey === hostKey;
  }

  togglePreviewSelection(item: PreviewSelectionItem) {
    if (!item.selectable) {
      return;
    }
    item.selected = !item.selected;
  }

  getSectionDisplay(item: VideoconferencePreviewItem) {
    const section = item.section_code?.trim();
    return section ? `Seccion ${section}` : 'Seccion sin codigo';
  }

  getSubsectionDisplay(item: VideoconferencePreviewItem) {
    const subsection = item.subsection_code?.trim() || item.subsection_label?.trim();
    return subsection ? `Grupo ${subsection}` : 'Sin grupo';
  }

  getTeacherMeta(item: VideoconferencePreviewItem) {
    return [
      `DNI: ${item.teacher_dni || 'Sin DNI'}`,
      item.modality_name || 'Sin modalidad',
    ].join(' | ');
  }

  getContextPrimary(item: VideoconferencePreviewItem) {
    return item.faculty_name || 'Sin facultad';
  }

  getContextSecondary(item: VideoconferencePreviewItem) {
    return [
      item.program_name || 'Sin programa',
      item.campus_name || 'Sin sede',
      item.cycle ? `Ciclo ${item.cycle}` : '',
    ]
      .filter(Boolean)
      .join(' | ');
  }

  getAvContextPrimary(item: VideoconferencePreviewItem) {
    return item.vc_faculty_name || 'Sin facultad AV';
  }

  getAvContextSecondary(item: VideoconferencePreviewItem) {
    return [
      item.vc_academic_program_name || 'Sin programa AV',
      item.vc_course_name || item.course_name || 'Sin curso AV',
    ]
      .filter(Boolean)
      .join(' | ');
  }

  hasDifferentAvContext(item: VideoconferencePreviewItem) {
    const planningFaculty = (item.faculty_name || '').trim().toUpperCase();
    const planningProgram = (item.program_name || '').trim().toUpperCase();
    const avFaculty = (item.vc_faculty_name || '').trim().toUpperCase();
    const avProgram = (item.vc_academic_program_name || '').trim().toUpperCase();

    return Boolean(avFaculty || avProgram) && (planningFaculty !== avFaculty || planningProgram !== avProgram);
  }

  getVcContext(item: VideoconferencePreviewItem) {
    return item.vc_section_name ? `VC: ${item.vc_section_name}` : 'VC sin seccion';
  }

  getVcSourceLabel(item: VideoconferencePreviewItem) {
    switch (item.vc_source) {
      case 'sync_source':
        return 'AV sincronizado';
      case 'manual_override':
        return 'AV manual';
      case 'fallback_match':
        return 'AV fallback';
      default:
        return '';
    }
  }

  inheritanceLabel(item: VideoconferencePreviewItem) {
    return item.inheritance?.is_inherited
      ? `Hereda de: ${item.inheritance.parent_label || item.inheritance.parent_schedule_id || 'Horario padre'}`
      : 'Owner Zoom';
  }

  getOccurrenceBadgeLabel(item: VideoconferencePreviewItem) {
    switch (item.occurrence_type) {
      case 'RESCHEDULED':
        return 'Reprogramada';
      case 'SKIPPED':
        return 'Omitida';
      default:
        return 'Base';
    }
  }

  getOccurrenceBadgeClass(item: VideoconferencePreviewItem) {
    switch (item.occurrence_type) {
      case 'RESCHEDULED':
        return 'occurrence-rescheduled';
      case 'SKIPPED':
        return 'occurrence-skipped';
      default:
        return 'occurrence-base';
    }
  }

  getOccurrenceDateLabel(item: VideoconferencePreviewItem) {
    if (!item.effective_conference_date) {
      return 'Horario semanal';
    }
    if (item.occurrence_type === 'RESCHEDULED') {
      return `${item.base_conference_date} -> ${item.effective_conference_date}`;
    }
    return item.effective_conference_date;
  }

  canManageOccurrence(item: VideoconferencePreviewItem) {
    return Boolean(item.base_conference_date) && !item.inheritance?.is_inherited;
  }

  openOverrideEditor(item: PreviewSelectionItem) {
    this.overrideTarget = item;
    this.overrideEditorOpen = true;
    this.overrideForm = {
      overrideDate: item.effective_conference_date,
      overrideStartTime: item.effective_start_time,
      overrideEndTime: item.effective_end_time,
      reasonCode: (item.override_reason_code as 'HOLIDAY' | 'WEATHER' | 'OTHER') || 'OTHER',
      notes: item.override_notes || '',
    };
  }

  closeOverrideEditor() {
    this.overrideEditorOpen = false;
    this.overrideTarget = null;
    this.overrideSaving = false;
    this.overrideForm = {
      overrideDate: '',
      overrideStartTime: '',
      overrideEndTime: '',
      reasonCode: 'OTHER',
      notes: '',
    };
  }

  async saveOverride() {
    if (!this.overrideTarget) {
      return;
    }
    if (!this.overrideForm.overrideDate || !this.overrideForm.overrideStartTime || !this.overrideForm.overrideEndTime) {
      await this.dialog.alert({
        title: 'Datos incompletos',
        message: 'La reprogramacion requiere fecha, hora inicio y hora fin.',
      });
      return;
    }
    if (this.overrideForm.overrideStartTime >= this.overrideForm.overrideEndTime) {
      await this.dialog.alert({
        title: 'Horario invalido',
        message: 'La hora inicio debe ser menor que la hora fin.',
      });
      return;
    }

    this.overrideSaving = true;
    const payload: VideoconferenceOverridePayload = {
      scheduleId: this.overrideTarget.schedule_id,
      conferenceDate: this.overrideTarget.base_conference_date,
      action: 'RESCHEDULE',
      overrideDate: this.overrideForm.overrideDate,
      overrideStartTime: this.overrideForm.overrideStartTime,
      overrideEndTime: this.overrideForm.overrideEndTime,
      reasonCode: this.overrideForm.reasonCode,
      notes: this.overrideForm.notes,
    };

    this.api.upsertOverride(payload).subscribe({
      next: async () => {
        this.overrideSaving = false;
        this.closeOverrideEditor();
        await this.reloadPreviewPreservingSelection();
      },
      error: async () => {
        this.overrideSaving = false;
        await this.dialog.alert({
          title: 'No se pudo reprogramar',
          message: 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  async skipOccurrence(item: PreviewSelectionItem) {
    const confirmed = await this.dialog.confirm({
      title: 'Omitir ocurrencia',
      message: `La clase del ${item.base_conference_date} no se generara. Deseas continuar?`,
      confirmLabel: 'Omitir',
      cancelLabel: 'Cancelar',
      tone: 'danger',
    });
    if (!confirmed) {
      return;
    }

    this.loading = true;
    this.api.upsertOverride({
      scheduleId: item.schedule_id,
      conferenceDate: item.base_conference_date,
      action: 'SKIP',
      reasonCode: 'OTHER',
    }).subscribe({
      next: async () => {
        this.loading = false;
        await this.reloadPreviewPreservingSelection();
      },
      error: async () => {
        this.loading = false;
        await this.dialog.alert({
          title: 'No se pudo omitir',
          message: 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  async resetOccurrence(item: PreviewSelectionItem) {
    this.loading = true;
    this.api.deleteOverride(item.schedule_id, item.base_conference_date).subscribe({
      next: async () => {
        this.loading = false;
        await this.reloadPreviewPreservingSelection();
      },
      error: async () => {
        this.loading = false;
        await this.dialog.alert({
          title: 'No se pudo restablecer',
          message: 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  async generate() {
    const selected = this.previewData.filter((item) => item.selectable && item.selected);
    if (!selected.length) {
      await this.dialog.alert({
        title: 'Seleccion requerida',
        message: 'Selecciona al menos una ocurrencia generable.',
      });
      return;
    }

    if (!this.startDate || !this.endDate) {
      await this.dialog.alert({
        title: 'Fechas requeridas',
        message: 'Selecciona fechas de inicio y fin.',
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
      message: this.usesOccurrenceRows
        ? `Se generaran ${selected.length} ocurrencias entre ${this.startDate} y ${this.endDate}. Deseas continuar?`
        : `Se generaran videoconferencias para ${selected.length} horarios entre ${this.startDate} y ${this.endDate}. Deseas continuar?`,
      confirmLabel: 'Generar',
      cancelLabel: 'Cancelar',
    });
    if (!confirmed) {
      return;
    }

    const payload = this.usesOccurrenceRows
      ? {
          occurrenceKeys: selected.map((item) => item.occurrence_key),
          startDate: this.startDate,
          endDate: this.endDate,
        }
      : {
          scheduleIds: selected.map((item) => item.schedule_id),
          startDate: this.startDate,
          endDate: this.endDate,
        };
    this.executeGeneration(payload, false);
  }

  async previewAssignments() {
    const selected = this.previewData.filter((item) => item.selectable && item.selected);
    if (!selected.length) {
      await this.dialog.alert({
        title: 'Seleccion requerida',
        message: 'Selecciona al menos un horario u ocurrencia para previsualizar la asignacion.',
      });
      return;
    }

    const payload = this.usesOccurrenceRows
      ? selected.length === this.selectableRows.length
        ? {
            selectAllVisible: true,
            ...this.buildCatalogFilters(),
            startDate: this.startDate,
            endDate: this.endDate,
          }
        : {
            occurrenceKeys: selected.map((item) => item.occurrence_key),
            startDate: this.startDate,
            endDate: this.endDate,
          }
      : selected.length === this.selectableRows.length
        ? {
            selectAllVisible: true,
            ...this.buildCatalogFilters(),
          }
        : {
            scheduleIds: selected.map((item) => item.schedule_id),
          };

    this.loading = true;
    this.api.assignmentPreview(payload).subscribe({
      next: (response) => {
        this.assignmentPreview = response;
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: async (error) => {
        this.loading = false;
        this.cdr.detectChanges();
        await this.dialog.alert({
          title: 'No se pudo calcular la asignacion Zoom',
          message: error?.error?.message ?? 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  getAssignmentPreview(preview: PreviewSelectionItem) {
    return (
      this.assignmentPreview?.items.find((item) => item.id === preview.occurrence_key)
      ?? null
    );
  }

  assignmentStatusLabel(item: VideoconferenceAssignmentPreviewItem | null) {
    switch (item?.preview_status) {
      case 'ASSIGNED_LICENSED':
        return 'Asignado';
      case 'ASSIGNED_RISK':
        return 'Asignado con riesgo';
      case 'INHERITED':
        return 'Heredado';
      case 'BLOCKED_EXISTING':
        return 'Bloqueado: Ya fue generada';
      case 'VALIDATION_ERROR':
        return 'Validacion pendiente';
      case 'NO_AVAILABLE_ZOOM_USER':
        return 'Sin host';
      default:
        return 'Sin preview';
    }
  }

  assignmentStatusClass(item: VideoconferenceAssignmentPreviewItem | null) {
    switch (item?.preview_status) {
      case 'ASSIGNED_LICENSED':
      case 'INHERITED':
        return 'assignment-pill assignment-pill-success';
      case 'ASSIGNED_RISK':
      case 'BLOCKED_EXISTING':
        return 'assignment-pill assignment-pill-warning';
      case 'VALIDATION_ERROR':
      case 'NO_AVAILABLE_ZOOM_USER':
        return 'assignment-pill assignment-pill-danger';
      default:
        return 'assignment-pill';
    }
  }

  assignmentLicenseClass(item: VideoconferenceAssignmentPreviewItem | null) {
    switch (item?.license_status) {
      case 'LICENSED':
      case 'ON_PREM':
        return 'assignment-license assignment-license-ok';
      case 'BASIC':
        return 'assignment-license assignment-license-warning';
      case 'UNKNOWN':
        return 'assignment-license assignment-license-neutral';
      default:
        return 'assignment-license';
    }
  }

  assignmentHostLabel(item: VideoconferenceAssignmentPreviewItem | null) {
    if (!item) {
      return 'Sin simulacion';
    }
    return item.zoom_user_email || item.zoom_user_name || 'Sin host sugerido';
  }

  resultStatusLabel(item: VideoconferenceGenerationResultItem) {
    if (item.link_mode === 'INHERITED' && item.status === 'MATCHED') {
      return 'Heredada';
    }
    if (item.link_mode === 'INHERITED' && item.status === 'CREATED_UNMATCHED') {
      return 'Heredada pendiente';
    }
    switch (item.status) {
      case 'MATCHED':
        return 'Conciliada';
      case 'CREATED_UNMATCHED':
        return 'Creada sin match';
      case 'BLOCKED_EXISTING':
        return 'Ya existente';
      case 'NO_AVAILABLE_ZOOM_USER':
        return 'Sin host disponible';
      case 'VALIDATION_ERROR':
        return 'Validacion pendiente';
      default:
        return 'Error';
    }
  }

  resultToneClass(item: VideoconferenceGenerationResultItem) {
    switch (item.status) {
      case 'MATCHED':
        return 'result-tone-success';
      case 'CREATED_UNMATCHED':
        return 'result-tone-warning';
      case 'BLOCKED_EXISTING':
        return 'result-tone-neutral';
      case 'NO_AVAILABLE_ZOOM_USER':
      case 'VALIDATION_ERROR':
        return 'result-tone-warning';
      default:
        return 'result-tone-danger';
    }
  }

  resultCourseLabel(item: ResolvedGenerationResultItem) {
    return item.preview?.course_label || `Horario ${item.schedule_id}`;
  }

  resultContextLabel(item: ResolvedGenerationResultItem) {
    if (!item.preview) {
      return item.conference_date || 'Sin contexto local';
    }
    const parts = [
      this.getSectionDisplay(item.preview),
      this.getSubsectionDisplay(item.preview),
      item.conference_date || item.preview.effective_conference_date || item.preview.base_conference_date,
    ];
    if (item.preview.inheritance?.is_inherited) {
      parts.push(item.preview.inheritance.parent_label || 'Hereda Zoom');
    }
    return parts
      .filter(Boolean)
      .join(' | ');
  }

  canRetryReconcile(item: VideoconferenceGenerationResultItem) {
    return item.status === 'CREATED_UNMATCHED' && Boolean(item.record_id);
  }

  isRetrying(item: VideoconferenceGenerationResultItem) {
    return Boolean(item.record_id) && this.retryingRecordId === item.record_id;
  }

  openAuditDetail(recordId: string | null) {
    if (!recordId) {
      return;
    }
    const url = this.router.serializeUrl(
      this.router.createUrlTree(['/videoconferences/audit', recordId]),
    );
    window.open(url, '_blank', 'noopener');
  }

  retryReconcile(item: VideoconferenceGenerationResultItem) {
    if (!item.record_id || this.retryingRecordId) {
      return;
    }

    this.retryingRecordId = item.record_id;
    this.api.reconcile(item.record_id).subscribe({
      next: async (response) => {
        this.retryingRecordId = '';
        this.replaceGenerationResultItem(response.result);
        await this.dialog.alert({
          title: response.matched ? 'Conciliacion completada' : 'Sin coincidencia unica',
          message: response.message,
          tone: response.matched ? 'success' : 'default',
        });
      },
      error: async (error) => {
        this.retryingRecordId = '';
        await this.dialog.alert({
          title: 'No se pudo reintentar la conciliacion',
          message: error?.error?.message ?? 'Intenta nuevamente en unos segundos.',
          tone: 'danger',
        });
      },
    });
  }

  private handleFilterChange() {
    this.closeOverrideEditor();
    this.resetPreviewState();
    this.refreshFilterOptions();
  }

  private captureFilterSnapshot(): AppliedFilterSnapshot {
    return {
      selectedPeriod: this.selectedPeriod,
      selectedCampuses: [...this.selectedCampuses],
      selectedFaculties: [...this.selectedFaculties],
      selectedPrograms: [...this.selectedPrograms],
      selectedCourses: [...this.selectedCourses],
      selectedModalities: [...this.selectedModalities],
      selectedDays: [...this.selectedDays],
    };
  }

  private buildFilterLabels(snapshot: AppliedFilterSnapshot) {
    return [
      this.buildSingleSelectionLabel('Periodo', snapshot.selectedPeriod, this.periodOptions),
      this.buildSelectionLabel('Sedes', snapshot.selectedCampuses, this.campusOptions),
      this.buildSelectionLabel('Facultades', snapshot.selectedFaculties, this.facultyOptions),
      this.buildSelectionLabel('Programas', snapshot.selectedPrograms, this.programOptions),
      this.buildSelectionLabel('Cursos', snapshot.selectedCourses, this.courseOptions),
      this.buildSelectionLabel('Modalidades', snapshot.selectedModalities, this.modalityOptions),
      this.buildSelectionLabel('Dias', snapshot.selectedDays, this.dayOptions),
    ].filter((label): label is string => Boolean(label));
  }

  private refreshFilterOptions() {
    const requestId = ++this.filterOptionsRequestId;
    this.filterOptionsLoading = true;
    this.cdr.detectChanges();

    this.api.getFilterOptions(this.buildCatalogFilters()).subscribe({
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
    this.periodOptions = data.periods;
    this.campusOptions = data.campuses;
    this.facultyOptions = data.faculties;
    this.programOptions = data.programs;
    this.courseOptions = data.courses;
    this.modalityOptions = data.modalities;
    this.dayOptions = data.days;

    const changedSelections = [
      this.retainAvailableSingleSelection('selectedPeriod', this.periodOptions),
      this.retainAvailableSelections('selectedCampuses', this.campusOptions),
      this.retainAvailableSelections('selectedFaculties', this.facultyOptions),
      this.retainAvailableSelections('selectedPrograms', this.programOptions),
      this.retainAvailableSelections('selectedCourses', this.courseOptions),
      this.retainAvailableSelections('selectedModalities', this.modalityOptions),
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
      | 'selectedModalities'
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
    property: 'selectedPeriod',
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
    this.assignmentPreview = null;
    this.assignmentUsersModalOpen = false;
    this.generationResult = null;
    this.hasSearched = false;
    this.retryingRecordId = '';
    this.appliedFilterSnapshot = null;
  }

  private buildCatalogFilters(): FilterOptionsDto {
    return {
      semesterId: this.selectedPeriod || undefined,
      campusIds: this.selectedCampuses.length ? this.selectedCampuses : undefined,
      facultyIds: this.selectedFaculties.length ? this.selectedFaculties : undefined,
      programIds: this.selectedPrograms.length ? this.selectedPrograms : undefined,
      courseIds: this.selectedCourses.length ? this.selectedCourses : undefined,
      modalities: this.selectedModalities.length ? this.selectedModalities : undefined,
      days: this.selectedDays.length ? this.selectedDays : undefined,
    };
  }

  private buildPreviewFilters(includeOperationalRange = true): VideoconferencePreviewDto {
    return includeOperationalRange
      ? {
          ...this.buildCatalogFilters(),
          startDate: this.startDate || undefined,
          endDate: this.endDate || undefined,
        }
      : {
          ...this.buildCatalogFilters(),
        };
  }

  private async reloadPreviewPreservingSelection() {
    const selectedKeys = new Set(
      this.previewData.filter((item) => item.selectable && item.selected).map((item) => item.occurrence_key),
    );
    this.assignmentPreview = null;
    await this.loadPreview(selectedKeys, this.usesOccurrenceRows);
  }

  private loadPreview(selectedKeys = new Set<string>(), includeOperationalRange = true) {
    this.loading = true;
    const appliedSnapshot = this.captureFilterSnapshot();
    this.cdr.detectChanges();

    this.api.preview(this.buildPreviewFilters(includeOperationalRange)).subscribe({
      next: (data) => {
        this.previewData = this.sortPreviewData(data).map((item) => ({
          ...item,
          selected: item.selectable && (selectedKeys.size ? selectedKeys.has(item.occurrence_key) : false),
        }));
        this.assignmentPreview = null;
        this.appliedFilterSnapshot = appliedSnapshot;
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

  private sortPreviewData(items: VideoconferencePreviewItem[]) {
    return [...items].sort((left, right) => {
      const comparisons = [
        left.effective_conference_date.localeCompare(right.effective_conference_date),
        (left.campus_name || '').localeCompare(right.campus_name || ''),
        (left.faculty_name || '').localeCompare(right.faculty_name || ''),
        (left.program_name || '').localeCompare(right.program_name || ''),
        left.course_label.localeCompare(right.course_label),
        (left.section_code || '').localeCompare(right.section_code || ''),
        (left.subsection_code || '').localeCompare(right.subsection_code || ''),
        left.start_time.localeCompare(right.start_time),
      ];

      return comparisons.find((value) => value !== 0) || 0;
    });
  }

  private closeFilterDropdowns() {
    this.multiSelectComponents?.forEach((component) => component.closeDropdown());
  }

  private findPreviewItemForResult(item: VideoconferenceGenerationResultItem) {
    const occurrenceKey = item.occurrence_key?.trim();
    if (occurrenceKey) {
      const byOccurrence = this.previewData.find((preview) => preview.occurrence_key === occurrenceKey);
      if (byOccurrence) {
        return byOccurrence;
      }
    }

    return this.previewData.find((preview) => {
      if (preview.schedule_id !== item.schedule_id) {
        return false;
      }
      if (!item.conference_date) {
        return true;
      }
      return (
        preview.effective_conference_date === item.conference_date
        || preview.base_conference_date === item.conference_date
      );
    }) ?? null;
  }

  private findPreviewItemForAssignment(item: VideoconferenceAssignmentPreviewItem) {
    const occurrenceKey = item.occurrence_key?.trim();
    if (occurrenceKey) {
      const byOccurrence = this.previewData.find((preview) => preview.occurrence_key === occurrenceKey);
      if (byOccurrence) {
        return byOccurrence;
      }
    }

    return this.previewData.find((preview) => preview.occurrence_key === item.id) ?? null;
  }

  private buildAssignmentSessionLine(item: ResolvedAssignmentPreviewItem) {
    const preview = item.preview;
    const courseLabel = preview?.course_label || `Horario ${item.schedule_id}`;
    const sectionLabel = preview ? this.getSectionDisplay(preview) : '';
    const subsectionLabel = preview ? this.getSubsectionDisplay(preview) : '';
    const whenLabel =
      item.mode === 'OCCURRENCE'
        ? item.conference_date || preview?.effective_conference_date || preview?.base_conference_date || item.day_label
        : item.day_label || preview?.day_label || '';
    const startTime = preview?.effective_start_time || preview?.start_time || item.start_time;
    const endTime = preview?.effective_end_time || preview?.end_time || item.end_time;
    const statusPrefix =
      item.preview_status === 'INHERITED'
        ? '[Heredada] '
        : item.preview_status === 'BLOCKED_EXISTING'
          ? '[Existente] '
          : '';

    return [
      `${statusPrefix}${courseLabel}`,
      [sectionLabel, subsectionLabel].filter(Boolean).join(' / '),
      [whenLabel, startTime && endTime ? `${startTime}-${endTime}` : ''].filter(Boolean).join(' '),
    ]
      .filter(Boolean)
      .join(' | ');
  }

  private replaceGenerationResultItem(nextItem: VideoconferenceGenerationResultItem) {
    if (!this.generationResult) {
      return;
    }

    const results = this.generationResult.results.map((item) =>
      item.record_id === nextItem.record_id ? nextItem : item,
    );
    this.generationResult = {
      ...this.generationResult,
      results,
      summary: this.rebuildGenerationSummary(results),
    };
    this.cdr.detectChanges();
  }

  private rebuildGenerationSummary(results: VideoconferenceGenerationResultItem[]) {
    return {
      requestedSchedules: this.generationResult?.summary?.requestedSchedules ?? 0,
      requestedOccurrences: this.generationResult?.summary?.requestedOccurrences ?? results.length,
      matched: results.filter((item) => item.status === 'MATCHED').length,
      createdUnmatched: results.filter((item) => item.status === 'CREATED_UNMATCHED').length,
      blockedExisting: results.filter((item) => item.status === 'BLOCKED_EXISTING').length,
      noAvailableZoomUser: results.filter((item) => item.status === 'NO_AVAILABLE_ZOOM_USER').length,
      validationErrors: results.filter((item) => item.status === 'VALIDATION_ERROR').length,
      errors: results.filter((item) => item.status === 'ERROR').length,
    };
  }

  private executeGeneration(
    payload: {
      scheduleIds?: string[];
      occurrenceKeys?: string[];
      startDate: string;
      endDate: string;
      allowPoolWarnings?: boolean;
    },
    hasConfirmedWarnings: boolean,
  ) {
    this.loading = true;
    this.api.generate(payload).subscribe({
      next: async (res) => {
        this.generationResult = res;
        this.loading = false;
        await this.dialog.alert({
          title: 'Proceso finalizado',
          message: res.message || 'Las videoconferencias fueron procesadas.',
          tone: 'success',
        });
      },
      error: async (error) => {
        this.loading = false;
        const message = error?.error?.message ?? 'Error generando videoconferencias.';
        const shouldConfirmPoolWarning =
          !hasConfirmedWarnings &&
          typeof message === 'string' &&
          message.includes('Confirma si deseas continuar');
        if (shouldConfirmPoolWarning) {
          const confirmed = await this.dialog.confirm({
            title: 'Advertencia del pool Zoom',
            message,
            confirmLabel: 'Continuar',
            cancelLabel: 'Cancelar',
          });
          if (confirmed) {
            this.executeGeneration({ ...payload, allowPoolWarnings: true }, true);
          }
          return;
        }
        await this.dialog.alert({
          title: 'No se pudieron generar las videoconferencias',
          message,
          tone: 'danger',
        });
      },
    });
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

  trackPreviewItem(_index: number, item: PreviewSelectionItem) {
    return item.occurrence_key;
  }
}
