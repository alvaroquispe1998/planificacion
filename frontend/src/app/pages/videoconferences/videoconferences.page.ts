import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnDestroy, OnInit, QueryList, ViewChildren } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, RouterLink } from '@angular/router';
import { firstValueFrom } from 'rxjs';
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
  ZoomGroupItem,
} from '../../services/videoconference-api.service';

type PreviewSelectionItem = VideoconferencePreviewItem & {
  selected: boolean;
  manualZoomUserId?: string;
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
  imports: [CommonModule, FormsModule, MultiSelectComponent, RouterLink],
  templateUrl: './videoconferences.page.html',
  styleUrl: './videoconferences.page.css',
})
export class VideoconferencesPageComponent implements OnInit, OnDestroy {
  @ViewChildren(MultiSelectComponent) multiSelectComponents!: QueryList<MultiSelectComponent>;

  selectedCampuses: string[] = [];
  selectedFaculties: string[] = [];
  selectedPrograms: string[] = [];
  selectedCourses: string[] = [];
  selectedPeriod = '';
  selectedModalities: string[] = [];
  selectedDays: string[] = [];
  selectedZoomGroupId = '';
  zoomGroups: ZoomGroupItem[] = [];
  hybridZoomUsers: { id: string; label: string }[] = [];

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
  generating = false;
  generationProgress = 0;
  /** Total occurrences sent to the backend for the current run, used to show "X / N" in progress bar. */
  generationTotal = 0;
  private generationStartTime = 0;
  filterOptionsLoading = false;
  overrideSaving = false;
  checkingExisting = false;

  /** occurrence_key -> existing record summary for already-created conferences */
  existingByOccurrenceKey = new Map<string, {
    status: string;
    zoom_meeting_id: string | null;
    zoom_user_email: string | null;
    record_id: string;
  }>();

  private generationProgressInterval: ReturnType<typeof setInterval> | null = null;
  private generationPollInterval: ReturnType<typeof setInterval> | null = null;
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

  payloadPreviewOpen = false;
  payloadPreviewTitle = '';
  payloadPreviewJson = '';

  // ─── Split / Cursos Especiales ──────────────────────────────────────────────
  /** Count of active host rules — displayed as compact badge on the VC page */
  splitRulesCount = 0;

  private filterOptionsRequestId = 0;

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly dialog: DialogService,
    private readonly cdr: ChangeDetectorRef,
    private readonly router: Router,
  ) {}

  ngOnInit() {
    this.loadZoomGroups();
    this.loadSplitRulesCount();
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

  get selectedZoomGroup() {
    return this.zoomGroups.find((item) => item.id === this.selectedZoomGroupId) ?? null;
  }

  get isHybridGroup() {
    const group = this.selectedZoomGroup;
    return group ? group.code.toUpperCase().includes('HIBRI') : false;
  }

  get selectedZoomGroupLabel() {
    const group = this.selectedZoomGroup;
    return group ? `${group.name} (${group.code})` : 'Sin grupo Zoom';
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
    return this.generationRows.filter(
      (item) =>
        item.status === 'NO_AVAILABLE_ZOOM_USER' ||
        (item.status === 'BLOCKED_EXISTING' && !item.zoom_meeting_id),
    );
  }

  get errorResultRows() {
    return this.generationRows.filter((item) => item.status === 'ERROR');
  }

  get retryableFailedRows() {
    return this.generationRows.filter((item) =>
      ['ERROR', 'NO_AVAILABLE_ZOOM_USER'].includes(item.status) && Boolean(item.occurrence_key),
    );
  }

  get canRetryFailed() {
    return this.retryableFailedRows.length > 0 && !this.generating;
  }

  isAlreadyCreated(item: PreviewSelectionItem): boolean {
    return this.existingByOccurrenceKey.has(item.occurrence_key);
  }

  getExistingRecord(item: PreviewSelectionItem) {
    return this.existingByOccurrenceKey.get(item.occurrence_key) ?? null;
  }

  get successfulResultRows() {
    return this.generationRows.filter(
      (item) =>
        item.status === 'MATCHED' ||
        item.status === 'CREATED_UNMATCHED' ||
        (item.status === 'BLOCKED_EXISTING' && Boolean(item.zoom_meeting_id)),
    );
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

  onZoomGroupChange(value: string) {
    this.selectedZoomGroupId = value;
    this.assignmentPreview = null;
    this.hybridZoomUsers = [];
    if (value && this.isHybridGroup) {
      this.api.getZoomGroupPool(value).subscribe({
        next: (res) => {
          this.hybridZoomUsers = (res.users ?? res.items ?? []).map((u) => ({
            id: u.zoom_user_id ?? u.id ?? '',
            label: u.email ? `${u.name ?? ''} (${u.email})`.trim() : (u.name ?? u.zoom_user_id ?? u.id ?? ''),
          })).filter((u) => u.id);
          this.cdr.markForCheck();
        },
      });
    }
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
    // If dates are set, check existing conferences in that range even on base load
    const afterLoad = (this.startDate && this.endDate) ? () => this.refreshExistingCheck() : undefined;
    this.loadPreview(new Set<string>(), false, afterLoad);
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
    this.loadPreview(undefined, undefined, () => this.refreshExistingCheck());
  }

  async exportCurrentRows() {
    if (!this.previewData.length) {
      await this.dialog.alert({
        title: 'Sin datos',
        message: 'No hay filas cargadas para exportar.',
      });
      return;
    }

    const header = [
      'Curso',
      'Curso / Nombre',
      'Seccion',
      'Grupo',
      'Dia',
      'Fecha',
      'Hora inicio',
      'Hora fin',
      'Duracion (min)',
      'Docente',
      'DNI docente',
      'Sede',
      'Facultad',
      'Programa',
      'Ciclo',
      'Modo fila',
      'Estado herencia',
      'Contexto AV',
    ];

    const rows = this.previewData.map((item) => [
      item.course_code ?? '',
      item.course_name ?? '',
      item.section_code ?? '',
      item.subsection_code ?? '',
      item.day_label ?? '',
      item.effective_conference_date || '',
      item.start_time ?? '',
      item.end_time ?? '',
      `${item.duration_minutes ?? ''}`,
      item.teacher_name ?? '',
      item.teacher_dni ?? '',
      item.campus_name ?? '',
      item.faculty_name ?? '',
      item.program_name ?? '',
      `${item.cycle ?? ''}`,
      item.effective_conference_date ? 'Ocurrencia' : 'Horario base',
      item.inheritance?.is_inherited ? 'Hijo' : 'Padre',
      item.vc_context_message ?? '',
    ]);

    const csv = [header, ...rows].map((line) => line.map((value) => this.escapeCsv(value)).join(',')).join('\r\n');
    const content = `\uFEFF${csv}`;
    const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    const timestamp = this.buildExportTimestamp();
    link.href = url;
    link.download = `videoconferencias-${timestamp}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
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

  getHostRuleChipLabel(item: VideoconferencePreviewItem): string {
    const rule = item.host_rule;
    if (!rule) return '';
    if (rule.zoom_user_name) return rule.lock_host ? `🔒 ${rule.zoom_user_name}` : rule.zoom_user_name;
    if (rule.zoom_user_email) return rule.lock_host ? `🔒 ${rule.zoom_user_email}` : rule.zoom_user_email;
    if (rule.zoom_group_name) return `Grupo: ${rule.zoom_group_name}`;
    return 'Auto-asignar';
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
    if (!this.selectedZoomGroupId) {
      await this.dialog.alert({
        title: 'Grupo Zoom requerido',
        message: 'Selecciona un grupo Zoom antes de generar videoconferencias.',
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

    const preferredHosts = this.buildPreferredHostsForGeneration(selected);
    const payload = this.usesOccurrenceRows
      ? {
          zoomGroupId: this.selectedZoomGroupId,
          occurrenceKeys: selected.map((item) => item.occurrence_key),
          startDate: this.startDate,
          endDate: this.endDate,
          preferredHosts,
        }
      : {
          zoomGroupId: this.selectedZoomGroupId,
          scheduleIds: Array.from(
            new Set(
              selected.flatMap((item) =>
                item.grouped_schedule_ids?.length
                  ? item.grouped_schedule_ids
                  : [item.schedule_id],
              ),
            ),
          ),
          startDate: this.startDate,
          endDate: this.endDate,
          preferredHosts,
        };
    this.executeGeneration(payload, false);
  }

  openPayloadPreview(item: PreviewSelectionItem) {
    const payload = this.buildCreationPayloadPreview(item);
    this.payloadPreviewTitle = item.course_label || 'Payload de reunion';
    this.payloadPreviewJson = JSON.stringify(payload, null, 2);
    this.payloadPreviewOpen = true;
  }

  closePayloadPreview() {
    this.payloadPreviewOpen = false;
    this.payloadPreviewTitle = '';
    this.payloadPreviewJson = '';
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
    if (!this.selectedZoomGroupId) {
      await this.dialog.alert({
        title: 'Grupo Zoom requerido',
        message: 'Selecciona un grupo Zoom antes de previsualizar la asignacion.',
      });
      return;
    }

    const payload = this.usesOccurrenceRows
      ? selected.length === this.selectableRows.length
        ? {
            zoomGroupId: this.selectedZoomGroupId,
            selectAllVisible: true,
            ...this.buildCatalogFilters(),
            startDate: this.startDate,
            endDate: this.endDate,
          }
        : {
            zoomGroupId: this.selectedZoomGroupId,
            occurrenceKeys: selected.map((item) => item.occurrence_key),
            startDate: this.startDate,
            endDate: this.endDate,
          }
      : selected.length === this.selectableRows.length
        ? {
            zoomGroupId: this.selectedZoomGroupId,
            selectAllVisible: true,
            ...this.buildCatalogFilters(),
            startDate: this.startDate || undefined,
            endDate: this.endDate || undefined,
          }
        : {
            zoomGroupId: this.selectedZoomGroupId,
            scheduleIds: selected.map((item) => item.schedule_id),
            startDate: this.startDate || undefined,
            endDate: this.endDate || undefined,
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
    if (!this.assignmentPreview?.items?.length) return null;
    // Exact match (both in same mode)
    const exact = this.assignmentPreview.items.find((item) => item.id === preview.occurrence_key);
    if (exact) return exact;
    // Fallback: base preview row ("schedule:UUID") ↔ occurrence assignment item ("UUID::date")
    return this.assignmentPreview.items.find((item) =>
      item.schedule_id === preview.schedule_id &&
      (!preview.effective_conference_date || item.conference_date === preview.effective_conference_date)
    ) ?? null;
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
        return 'Sin host disponible en grupo';
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
    if (item.status === 'BLOCKED_EXISTING') {
      return item.zoom_meeting_id ? 'Ya conciliada' : 'Ya creada (sin match)';
    }
    switch (item.status) {
      case 'MATCHED':
        return 'Conciliada';
      case 'CREATED_UNMATCHED':
        return 'Creada sin match';
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
        return item.zoom_meeting_id ? 'result-tone-success' : 'result-tone-neutral';
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
    const preview = item.preview;
    const date = item.conference_date || preview.effective_conference_date || preview.base_conference_date;
    const startTime = preview.effective_start_time || preview.start_time;
    const endTime = preview.effective_end_time || preview.end_time;
    const timeRange = startTime && endTime ? `${startTime} - ${endTime}` : null;
    const dayTime = [preview.day_label, timeRange].filter(Boolean).join(' ');
    const parts: (string | null | undefined)[] = [
      this.getSectionDisplay(preview),
      this.getSubsectionDisplay(preview),
      date || null,
      dayTime || null,
      preview.teacher_name || null,
    ];
    if (preview.inheritance?.is_inherited) {
      parts.push(preview.inheritance.parent_label || 'Hereda Zoom');
    }
    return parts.filter(Boolean).join(' | ');
  }

  canRetryReconcile(item: VideoconferenceGenerationResultItem) {
    return (
      (item.status === 'CREATED_UNMATCHED' || item.status === 'BLOCKED_EXISTING') &&
      Boolean(item.record_id) &&
      !item.zoom_meeting_id
    );
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

  async retryAllFailed() {
    const rows = this.errorResultRows.filter((item) => Boolean(item.occurrence_key));
    if (!rows.length || this.generating) {
      return;
    }
    const confirmed = await this.dialog.confirm({
      title: 'Reintentar reuniones con error',
      message: `Se reintentara la creacion de ${rows.length} reunion(es) que fallaron. Deseas continuar?`,
      confirmLabel: 'Reintentar',
      cancelLabel: 'Cancelar',
    });
    if (!confirmed) {
      return;
    }
    const occurrenceKeys = rows.map((item) => item.occurrence_key).filter((k): k is string => Boolean(k));
    const payload = {
      zoomGroupId: this.selectedZoomGroupId,
      occurrenceKeys,
      startDate: this.startDate,
      endDate: this.endDate,
    };
    this.executeGeneration(payload, false);
  }

  copyFailedList() {
    const rows = this.errorResultRows;
    if (!rows.length) {
      return;
    }
    const lines = rows.map((item) => {
      const label = this.resultCourseLabel(item);
      const ctx = this.resultContextLabel(item);
      const msg = item.message || '';
      return `${label} | ${ctx} | ${msg}`;
    });
    const text = `Reuniones con error (${rows.length}):\n\n${lines.join('\n')}`;
    navigator.clipboard.writeText(text).catch(() => {
      // fallback: create a temporary textarea
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    });
    this.dialog.alert({ title: 'Copiado', message: `${rows.length} error(es) copiados al portapapeles.`, tone: 'success' });
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

  private loadZoomGroups() {
    this.api.listZoomGroups().subscribe({
      next: (groups) => {
        this.zoomGroups = (Array.isArray(groups) ? groups : [])
          .filter((item) => item?.is_active !== false)
          .sort((left, right) => `${left.name}`.localeCompare(`${right.name}`));

        if (!this.selectedZoomGroupId || !this.zoomGroups.some((item) => item.id === this.selectedZoomGroupId)) {
          // Prefer the REGULAR group as default; fall back to first active group
          const regularGroup = this.zoomGroups.find((item) => item.code.toUpperCase().includes('REGULAR'));
          this.selectedZoomGroupId = (regularGroup ?? this.zoomGroups[0])?.id || '';
        }
        this.cdr.detectChanges();
      },
      error: async () => {
        this.zoomGroups = [];
        this.selectedZoomGroupId = '';
        this.cdr.detectChanges();
        await this.dialog.alert({
          title: 'No se pudieron cargar los grupos Zoom',
          message: 'Configura los grupos de usuarios Zoom antes de generar videoconferencias.',
          tone: 'danger',
        });
      },
    });
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
    this.closePayloadPreview();
    this.generationResult = null;
    this.hasSearched = false;
    this.retryingRecordId = '';
    this.appliedFilterSnapshot = null;
    this.existingByOccurrenceKey = new Map();
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

  private loadPreview(
    selectedKeys = new Set<string>(),
    includeOperationalRange = true,
    afterLoad?: () => void,
  ) {
    this.loading = true;
    const appliedSnapshot = this.captureFilterSnapshot();
    this.cdr.detectChanges();

    this.api.preview(this.buildPreviewFilters(includeOperationalRange)).subscribe({
      next: (data) => {
        this.previewData = this.sortPreviewData(data).map((item) => ({
          ...item,
          selected: item.selectable && (selectedKeys.size ? selectedKeys.has(item.occurrence_key) : false),
          // Pre-fill from host_rule config (Cursos Especiales): lock_host uses fixed user,
          // flexible rules let the user override from the pool.
          manualZoomUserId: item.host_rule?.zoom_user_id ?? undefined,
        }));
        this.assignmentPreview = null;
        this.appliedFilterSnapshot = appliedSnapshot;
        this.loading = false;
        this.cdr.detectChanges();
        afterLoad?.();
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

  private refreshExistingCheck() {
    // ── Occurrence mode: each row has a real conference_date in its occurrence_key ──
    if (this.usesOccurrenceRows) {
      const occurrenceKeys = this.previewData
        .filter((item) => item.selectable && item.occurrence_key && item.effective_conference_date)
        .map((item) => item.occurrence_key);

      if (!occurrenceKeys.length) {
        this.existingByOccurrenceKey = new Map();
        return;
      }

      this.checkingExisting = true;
      this.cdr.detectChanges();
      this.api.checkExisting({ occurrenceKeys }).subscribe({
        next: (res) => this.applyExistingCheckResult(res.existing),
        error: () => { this.checkingExisting = false; this.cdr.detectChanges(); },
      });
      return;
    }

    // ── Base schedule mode: check by schedule IDs + date range ────────────
    if (!this.startDate || !this.endDate) {
      this.existingByOccurrenceKey = new Map();
      return;
    }

    const scheduleIds = [...new Set(
      this.previewData
        .filter((item) => item.selectable)
        .flatMap((item) => item.grouped_schedule_ids?.length ? item.grouped_schedule_ids : [item.schedule_id]),
    )];

    if (!scheduleIds.length) {
      this.existingByOccurrenceKey = new Map();
      return;
    }

    this.checkingExisting = true;
    this.cdr.detectChanges();
    this.api.checkExisting({ scheduleIds, startDate: this.startDate, endDate: this.endDate }).subscribe({
      next: (res) => this.applyExistingCheckResult(res.existing),
      error: () => { this.checkingExisting = false; this.cdr.detectChanges(); },
    });
  }

  private applyExistingCheckResult(existing: Array<{
    occurrence_key: string;
    schedule_id: string;
    conference_date: string;
    status: string;
    zoom_meeting_id: string | null;
    zoom_user_email: string | null;
    record_id: string;
  }>) {
    const map = new Map<string, { status: string; zoom_meeting_id: string | null; zoom_user_email: string | null; record_id: string }>();
    for (const item of existing) {
      map.set(item.occurrence_key, {
        status: item.status,
        zoom_meeting_id: item.zoom_meeting_id,
        zoom_user_email: item.zoom_user_email,
        record_id: item.record_id,
      });
    }
    this.existingByOccurrenceKey = map;

    if (this.usesOccurrenceRows) {
      // Auto-deselect rows that are already created (occurrence mode)
      this.previewData.forEach((item) => {
        if (map.has(item.occurrence_key)) {
          item.selected = false;
        }
      });
    }
    this.checkingExisting = false;
    this.cdr.detectChanges();
  }

  /** Count conferences already created for a base schedule in the current date range */
  getExistingCountForSchedule(item: PreviewSelectionItem): number {
    const ids = new Set(item.grouped_schedule_ids?.length ? item.grouped_schedule_ids : [item.schedule_id]);
    let count = 0;
    for (const key of this.existingByOccurrenceKey.keys()) {
      const sep = key.lastIndexOf('::');
      if (sep >= 0 && ids.has(key.slice(0, sep))) {
        count++;
      }
    }
    return count;
  }

  get existingScheduleIds(): Set<string> {
    const ids = new Set<string>();
    for (const key of this.existingByOccurrenceKey.keys()) {
      const sep = key.lastIndexOf('::');
      if (sep >= 0) ids.add(key.slice(0, sep));
    }
    return ids;
  }

  isAnyCreatedForSchedule(item: PreviewSelectionItem): boolean {
    const ids = new Set(item.grouped_schedule_ids?.length ? item.grouped_schedule_ids : [item.schedule_id]);
    const existing = this.existingScheduleIds;
    return [...ids].some((id) => existing.has(id));
  }

  async retryFailed() {
    const failed = this.retryableFailedRows;
    if (!failed.length) return;
    if (!this.selectedZoomGroupId) {
      await this.dialog.alert({
        title: 'Grupo Zoom requerido',
        message: 'Selecciona un grupo Zoom antes de reintentar.',
      });
      return;
    }

    const confirmed = await this.dialog.confirm({
      title: 'Reintentar fallidos',
      message: `Se reintentara la creacion de ${failed.length} ocurrencia(s) con error. Deseas continuar?`,
      confirmLabel: 'Reintentar',
      cancelLabel: 'Cancelar',
    });
    if (!confirmed) return;

    const occurrenceKeys = failed
      .map((item) => item.occurrence_key)
      .filter((k): k is string => Boolean(k));

    const preferredHosts = this.buildPreferredHostsForGeneration(
      this.previewData.filter((item) => occurrenceKeys.includes(item.occurrence_key)),
    );

    this.executeGeneration({
      zoomGroupId: this.selectedZoomGroupId,
      occurrenceKeys,
      startDate: this.startDate,
      endDate: this.endDate,
      preferredHosts,
    }, false);
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

    const byScheduleAndDate = this.previewData.find((preview) => {
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
    });

    if (byScheduleAndDate) {
      return byScheduleAndDate;
    }

    // Last resort: match by schedule_id (or grouped_schedule_ids) only.
    // Needed for base schedule mode where previewData occurrence_key is "schedule:UUID"
    // but generation result occurrence_key is "UUID::date" — both exact lookups above fail.
    return (
      this.previewData.find(
        (preview) =>
          preview.schedule_id === item.schedule_id ||
          (preview.grouped_schedule_ids?.includes(item.schedule_id) ?? false),
      ) ?? null
    );
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

  private escapeCsv(value: unknown) {
    const normalized = String(value ?? '');
    if (/[",\r\n]/.test(normalized)) {
      return `"${normalized.replace(/"/g, '""')}"`;
    }
    return normalized;
  }

  private buildExportTimestamp() {
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = `${now.getMonth() + 1}`.padStart(2, '0');
    const dd = `${now.getDate()}`.padStart(2, '0');
    const hh = `${now.getHours()}`.padStart(2, '0');
    const mi = `${now.getMinutes()}`.padStart(2, '0');
    return `${yyyy}${mm}${dd}-${hh}${mi}`;
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

  // Max schedules/occurrences per single HTTP request to avoid proxy timeouts.
  // Each item: 500ms throttle + 1-8s Aula Virtual POST = up to ~8.5s worst case.
  // 6 items × 8.5s = ~51s, safely under a standard 60s Apache proxy timeout.
  private readonly GENERATION_CHUNK_SIZE = 6;

  private executeGeneration(
    payload: {
      zoomGroupId: string;
      scheduleIds?: string[];
      occurrenceKeys?: string[];
      startDate: string;
      endDate: string;
      allowPoolWarnings?: boolean;
      preferredHosts?: Array<{
        scheduleId: string;
        conferenceDate?: string;
        zoomUserId: string;
      }>;
    },
    hasConfirmedWarnings: boolean,
  ) {
    const ids = payload.occurrenceKeys?.length ? payload.occurrenceKeys : (payload.scheduleIds ?? []);
    if (ids.length > this.GENERATION_CHUNK_SIZE) {
      this.executeGenerationChunked(payload, hasConfirmedWarnings);
      return;
    }

    this.loading = true;
    const estimatedTotal =
      (payload.occurrenceKeys?.length ?? 0) ||
      (payload.scheduleIds?.length ?? 0) ||
      this.selectedCount;
    const displayTotal = this.selectedCount || estimatedTotal;
    this.startGenerationProgress(estimatedTotal, displayTotal);
    this.api.generate(payload).subscribe({
      next: async (res) => {
        this.finishGenerationProgress();
        this.generationResult = res;
        this.loading = false;
        await this.dialog.alert({
          title: 'Proceso finalizado',
          message: (res.message || 'Las videoconferencias fueron procesadas.') + ' Sincroniza el ID Zoom desde Auditoría.',
          tone: 'success',
        });
      },
      error: async (error) => {
        this.loading = false;

        // Gateway / network error (502, 503, 504, status 0, or the nginx error-page
        // redirect pattern where NestJS returns 404 { message: "Cannot GET /502.shtml" }):
        // The backend may still be processing. Keep the progress bar alive and
        // poll checkExisting until the count stabilises, then finish normally.
        const errorBody = error?.error?.message ?? error?.message ?? '';
        const isGatewayErrorPage =
          typeof errorBody === 'string' && /Cannot GET \//i.test(errorBody);
        const isGatewayError =
          error.status === 0 ||
          error.status === 502 ||
          error.status === 503 ||
          error.status === 504 ||
          isGatewayErrorPage;
        if (isGatewayError) {
          this.startGatewayPolling({
            scheduleIds: payload.scheduleIds,
            occurrenceKeys: payload.occurrenceKeys,
            startDate: payload.startDate,
            endDate: payload.endDate,
          });
          this.cdr.detectChanges();
          return;
        }

        this.finishGenerationProgress();

        // If the error body contains partial results (e.g. timeout after fail-fast),
        // display them so the user can see what was processed and retry the rest.
        const partialResults = error?.error?.results;
        if (Array.isArray(partialResults) && partialResults.length > 0) {
          this.generationResult = error.error;
        }

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
          title: Array.isArray(partialResults) && partialResults.length > 0
            ? 'Proceso terminado con errores — revisa los resultados'
            : 'No se pudieron generar las videoconferencias',
          message,
          tone: 'danger',
        });
      },
    });
  }

  /**
   * Sends large generation batches in chunks of GENERATION_CHUNK_SIZE to avoid
   * proxy timeouts on the production server. Progress is driven by real chunk
   * completion (not time estimation). Results from all chunks are merged.
   */
  private async executeGenerationChunked(
    payload: {
      zoomGroupId: string;
      scheduleIds?: string[];
      occurrenceKeys?: string[];
      startDate: string;
      endDate: string;
      allowPoolWarnings?: boolean;
      preferredHosts?: Array<{ scheduleId: string; conferenceDate?: string; zoomUserId: string }>;
    },
    hasConfirmedWarnings: boolean,
  ) {
    const useOccurrences = (payload.occurrenceKeys?.length ?? 0) > 0;
    const ids = useOccurrences ? (payload.occurrenceKeys ?? []) : (payload.scheduleIds ?? []);
    const chunkSize = this.GENERATION_CHUNK_SIZE;
    const chunks: string[][] = [];
    for (let i = 0; i < ids.length; i += chunkSize) {
      chunks.push(ids.slice(i, i + chunkSize));
    }

    this.loading = true;
    this.generating = true;
    this.generationProgress = 0;
    this.generationTotal = this.selectedCount || ids.length;
    this.cdr.detectChanges();

    // Stop the time-based animation — we'll drive progress from real chunk completion.
    if (this.generationProgressInterval) {
      clearInterval(this.generationProgressInterval);
      this.generationProgressInterval = null;
    }

    const allResults: VideoconferenceGenerationResultItem[] = [];
    let allowWarnings = hasConfirmedWarnings || Boolean(payload.allowPoolWarnings);

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      const chunkPayload = {
        ...payload,
        ...(useOccurrences
          ? { occurrenceKeys: chunk, scheduleIds: undefined }
          : { scheduleIds: chunk, occurrenceKeys: undefined }),
        allowPoolWarnings: allowWarnings || undefined,
        // Pass all preferredHosts — backend ignores non-matching ones
        preferredHosts: payload.preferredHosts,
      };

      try {
        const res = await firstValueFrom(this.api.generate(chunkPayload));
        allResults.push(...(res.results ?? []));
        allowWarnings = true; // pool already validated after first successful chunk
      } catch (error: any) {
        const errorBody = error?.error?.message ?? error?.message ?? '';
        const isGatewayErrorPage = typeof errorBody === 'string' && /Cannot GET \//i.test(errorBody);
        const isGatewayError =
          error.status === 0 || error.status === 502 ||
          error.status === 503 || error.status === 504 || isGatewayErrorPage;

        if (isGatewayError) {
          // Backend still processing this chunk — show partial results and poll.
          if (allResults.length) {
            this.generationResult = { results: allResults } as VideoconferenceGenerationResponse;
          }
          this.startGatewayPolling({ startDate: payload.startDate, endDate: payload.endDate });
          this.cdr.detectChanges();
          return;
        }

        // Pool warning on first chunk — prompt and retry from beginning
        const message = error?.error?.message ?? '';
        if (!allowWarnings && typeof message === 'string' && message.includes('Confirma si deseas continuar')) {
          const confirmed = await this.dialog.confirm({
            title: 'Advertencia del pool Zoom',
            message,
            confirmLabel: 'Continuar',
            cancelLabel: 'Cancelar',
          });
          if (confirmed) {
            allowWarnings = true;
            i--; // retry current chunk
            continue;
          }
          break;
        }

        // Real error — show partial results if any and stop
        if (allResults.length) {
          this.generationResult = { results: allResults } as VideoconferenceGenerationResponse;
        }
        this.finishGenerationProgress();
        this.loading = false;
        await this.dialog.alert({
          title: allResults.length
            ? `Error en lote ${i + 1} de ${chunks.length} — revisa los resultados parciales`
            : 'No se pudieron generar las videoconferencias',
          message: message || 'Error generando videoconferencias.',
          tone: 'danger',
        });
        return;
      }

      // Advance progress based on completed items
      const completedCount = Math.min((i + 1) * chunkSize, ids.length);
      this.generationProgress = Math.min(92, (completedCount / ids.length) * 100);
      this.cdr.detectChanges();
    }

    // All chunks done
    this.generationResult = { results: allResults } as VideoconferenceGenerationResponse;
    this.finishGenerationProgress();
    this.loading = false;

    const created = allResults.filter((r) => r.status === 'MATCHED' || r.status === 'CREATED_UNMATCHED').length;
    const errors = allResults.filter((r) =>
      ['ERROR', 'NO_AVAILABLE_ZOOM_USER', 'VALIDATION_ERROR'].includes(r.status),
    ).length;
    const blocked = allResults.filter((r) => r.status === 'BLOCKED_EXISTING').length;

    const parts: string[] = [];
    if (created > 0) parts.push(`${created} creadas`);
    if (blocked > 0) parts.push(`${blocked} ya existían`);
    if (errors > 0) parts.push(`${errors} con error`);

    await this.dialog.alert({
      title: 'Proceso finalizado',
      message: (parts.length ? parts.join(', ') + '. ' : '') + 'Sincroniza el ID Zoom desde Auditoría.',
      tone: errors > 0 && created === 0 ? 'danger' : 'success',
    });
  }

  private startGenerationProgress(total = 0, displayTotal?: number) {
    this.generationTotal = displayTotal ?? total;
    this.generationStartTime = Date.now();
    this.generating = true;
    this.cdr.detectChanges();
    if (this.generationProgressInterval) {
      clearInterval(this.generationProgressInterval);
    }
    // Estimate ~5 seconds per item (throttle + API calls + possible retry).
    // Progress advances linearly based on real elapsed time so the bar moves
    // at a steady, honest rate instead of rushing to 90% and freezing.
    const estimatedMs = Math.max(total * 5000, 8000);
    this.generationProgressInterval = setInterval(() => {
      const elapsed = Date.now() - this.generationStartTime;
      this.generationProgress = Math.min(92, (elapsed / estimatedMs) * 100);
      this.cdr.detectChanges();
    }, 500);
  }

  finishGenerationProgress() {
    if (this.generationProgressInterval) {
      clearInterval(this.generationProgressInterval);
      this.generationProgressInterval = null;
    }
    if (this.generationPollInterval) {
      clearInterval(this.generationPollInterval);
      this.generationPollInterval = null;
    }
    this.generationProgress = 100;
    this.cdr.detectChanges();
    setTimeout(() => {
      this.generating = false;
      this.generationProgress = 0;
      this.generationTotal = 0;
      this.cdr.detectChanges();
    }, 1200);
  }

  ngOnDestroy() {
    if (this.generationProgressInterval) clearInterval(this.generationProgressInterval);
    if (this.generationPollInterval) clearInterval(this.generationPollInterval);
  }

  /**
   * After a gateway error (502/0) the backend may still be creating records.
   * Poll checkExisting every 8 s; when the count is stable for 2 consecutive
   * rounds (≥ 3 polls minimum = ~24 s), consider the generation complete,
   * finish the progress bar and show a result summary.
   */
  private startGatewayPolling(payload: {
    scheduleIds?: string[];
    occurrenceKeys?: string[];
    startDate: string;
    endDate: string;
  }) {
    if (this.generationPollInterval) {
      clearInterval(this.generationPollInterval);
    }
    let lastCount = -1;
    let stableRounds = 0;
    let pollCount = 0;

    this.generationPollInterval = setInterval(() => {
      pollCount++;
      this.api.checkExisting(payload).subscribe({
        next: async (res) => {
          const count = res.existing.length;
          if (count === lastCount && pollCount >= 3) {
            stableRounds++;
            if (stableRounds >= 2) {
              clearInterval(this.generationPollInterval!);
              this.generationPollInterval = null;
              this.finishGenerationProgress();
              await this.dialog.alert({
                title: 'Proceso finalizado',
                message: count > 0
                  ? `Se detectaron ${count} clase(s) creadas en el rango ${payload.startDate} – ${payload.endDate}. Revisa el detalle en Auditoría.`
                  : 'La generación concluyó pero no se detectaron nuevas clases en el rango. Revisa Auditoría para confirmar.',
                tone: count > 0 ? 'success' : 'default',
              });
            }
          } else {
            lastCount = count;
            stableRounds = 0;
          }
        },
        error: () => {
          // Network still unstable — keep polling silently
        },
      });
    }, 8000);
  }

  private blurActiveElement() {
    const active = document.activeElement;
    if (active instanceof HTMLElement) {
      active.blur();
    }
  }

  private buildPreferredHostsForGeneration(selectedRows: PreviewSelectionItem[]) {
    const dedup = new Map<string, { scheduleId: string; conferenceDate?: string; zoomUserId: string }>();

    // Manual overrides from hybrid selector take priority
    for (const row of selectedRows) {
      if (row.manualZoomUserId) {
        const key = `${row.schedule_id}|${row.effective_conference_date || ''}`;
        dedup.set(key, {
          scheduleId: row.schedule_id,
          conferenceDate: row.effective_conference_date || undefined,
          zoomUserId: row.manualZoomUserId,
        });
      }
    }

    // Fill remaining from assignment preview (if available)
    if (this.assignmentPreview) {
      const selectedKeys = new Set(selectedRows.map((item) => item.occurrence_key));
      const preferred = this.assignmentPreviewRows
        .filter((item) => selectedKeys.has(item.preview?.occurrence_key || ''))
        .filter((item) => item.zoom_user_id)
        .filter((item) => item.preview_status === 'ASSIGNED_LICENSED' || item.preview_status === 'ASSIGNED_RISK')
        .map((item) => ({
          scheduleId: item.schedule_id,
          conferenceDate: item.mode === 'OCCURRENCE' ? (item.conference_date || undefined) : undefined,
          zoomUserId: item.zoom_user_id as string,
        }));
      for (const item of preferred) {
        const key = `${item.scheduleId}|${item.conferenceDate || ''}`;
        if (!dedup.has(key)) {
          dedup.set(key, item);
        }
      }
    }

    return Array.from(dedup.values());
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

  private buildCreationPayloadPreview(item: PreviewSelectionItem) {
    const assignment = this.getAssignmentPreview(item);
    const dayCode = this.resolveDayCode(item);
    const startTime = (item.effective_start_time || item.start_time || '').trim();
    const endTime = (item.effective_end_time || item.end_time || '').trim();
    const conferenceDate = (item.effective_conference_date || item.base_conference_date || '').trim();
    const topic = this.buildTopicPreview(item, dayCode, startTime, endTime);
    const minutes = this.calculateDurationMinutes(startTime, endTime);
    const formattedDate = conferenceDate ? this.formatDateForAulaVirtual(conferenceDate) : '';

    return {
      courseCode: item.course_code?.trim() || '',
      courseName: item.course_name?.trim() || '',
      section: item.vc_section_name?.trim() || '',
      dni: item.teacher_dni?.trim() || '',
      teacher: item.teacher_name?.trim() || '',
      day: dayCode,
      startTime,
      endTime,
      termId: item.vc_period_id?.trim() || '',
      facultyId: item.vc_faculty_id?.trim() || '',
      careerId: item.vc_academic_program_id?.trim() || '',
      courseId: item.vc_course_id?.trim() || '',
      name: topic,
      sectionId: item.vc_section_id?.trim() || '',
      start: formattedDate ? `${formattedDate} ${startTime}` : '',
      end: formattedDate ? `${formattedDate} ${endTime}` : '',
      minutes: String(minutes),
      'daysOfWeek[0]': String(this.dayToAulaVirtual(dayCode)),
      credentialId: assignment?.zoom_user_id || '',
      _meta: {
        scheduleId: item.schedule_id,
        occurrenceKey: item.occurrence_key,
        inherited: item.inheritance?.is_inherited ?? false,
        zoomGroupId: this.selectedZoomGroupId || null,
        zoomGroupLabel: this.selectedZoomGroupLabel,
        hostPreview: assignment?.zoom_user_email || assignment?.zoom_user_name || assignment?.zoom_user_id || null,
        hostFixedFromPreview: Boolean(assignment?.zoom_user_id),
      },
    };
  }

  private buildTopicPreview(
    item: PreviewSelectionItem,
    dayCode: string,
    startTime: string,
    endTime: string,
  ) {
    const parts = [
      item.course_name?.trim() || '',
      item.vc_section_name?.trim() || '',
      item.teacher_dni?.trim() || '',
      item.teacher_name?.trim() || '',
      `${dayCode} ${startTime}-${endTime}`,
    ];
    const courseCode = item.course_code?.trim() || '';
    if (courseCode) {
      parts.unshift(courseCode);
    }
    if (item.occurrence_type === 'RESCHEDULED') {
      parts.unshift('REP');
    }
    return parts.join('|');
  }

  private resolveDayCode(item: PreviewSelectionItem) {
    const day = (item.day_of_week || '').trim().toUpperCase();
    return day || 'LUNES';
  }

  private dayToAulaVirtual(dayCode: string) {
    switch (dayCode) {
      case 'LUNES':
        return 2;
      case 'MARTES':
        return 3;
      case 'MIERCOLES':
        return 4;
      case 'JUEVES':
        return 5;
      case 'VIERNES':
        return 6;
      case 'SABADO':
        return 7;
      case 'DOMINGO':
        return 1;
      default:
        return 2;
    }
  }

  private formatDateForAulaVirtual(value: string) {
    const [year, month, day] = (value || '').split('-');
    if (!year || !month || !day) {
      return value || '';
    }
    return `${day}/${month}/${year}`;
  }

  private calculateDurationMinutes(startTime: string, endTime: string) {
    const [startHour, startMinute] = (startTime || '00:00').split(':').map((part) => Number(part));
    const [endHour, endMinute] = (endTime || '00:00').split(':').map((part) => Number(part));
    return endHour * 60 + endMinute - (startHour * 60 + startMinute);
  }

  // ─── Split / Cursos Especiales ──────────────────────────────────────────────

  loadSplitRulesCount() {
    this.api.listHostRules().subscribe({
      next: (rules) => {
        this.splitRulesCount = rules.filter((r) => r.is_active).length;
        this.cdr.detectChanges();
      },
      error: (err) => console.error('[Split] loadSplitRulesCount error', err),
    });
  }
}
