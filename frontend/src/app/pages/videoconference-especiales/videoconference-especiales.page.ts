import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {
  VideoconferenceApiService,
  VideoconferencePreviewItem,
  VcScheduleHostRule,
  ZoomGroupItem,
  ZoomPoolLicenseAwareUser,
} from '../../services/videoconference-api.service';
import { DialogService } from '../../core/dialog.service';

// ─── Local types ─────────────────────────────────────────────────────────────

type CourseOption = { id: string; label: string };

type SectionSummary = {
  section_id: string;
  section_code: string;
  section_label: string;
  course_label: string;
  teacher_name: string | null;
  schedules: ScheduleRow[];
};

type HostRuleShape = {
  rule_id: string;
  zoom_user_id: string | null;
  zoom_user_email: string | null;
  zoom_user_name: string | null;
  zoom_group_id: string | null;
  zoom_group_name: string | null;
  lock_host: boolean;
  skip_zoom: boolean;
};

type ScheduleRow = Omit<VideoconferencePreviewItem, 'host_rule'> & {
  host_rule: HostRuleShape | null;
  draft: {
    zoomMode: 'default' | 'zoom' | 'skip';
    zoomGroupId: string;
    zoomUserId: string;
    lockHost: boolean;
    notes: string;
    poolUsers: ZoomPoolLicenseAwareUser[];
    poolLoading: boolean;
  };
};

@Component({
  selector: 'app-videoconference-especiales-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './videoconference-especiales.page.html',
  styleUrl: './videoconference-especiales.page.css',
})
export class VideoconferenceEspecialesPageComponent implements OnInit {
  // ─── Stage 1: Course search ────────────────────────────────────────────────
  allCourses: CourseOption[] = [];
  courseSearch = '';
  courseDropdownOpen = false;
  selectedCourse: CourseOption | null = null;
  catalogLoading = false;

  // ─── Stage 2: Sections ────────────────────────────────────────────────────
  sections: SectionSummary[] = [];
  sectionsLoading = false;
  selectedSection: SectionSummary | null = null;

  // ─── Stage 3: Configuration ───────────────────────────────────────────────
  assignMode: 'unified' | 'individual' = 'individual';
  zoomGroups: ZoomGroupItem[] = [];
  unifiedDraft = {
    zoomGroupId: '',
    zoomUserId: '',
    lockHost: false,
    notes: '',
    poolUsers: [] as ZoomPoolLicenseAwareUser[],
    poolLoading: false,
  };
  configSaving = false;
  configMessage = '';
  configError = '';

  // ─── Global saved rules ───────────────────────────────────────────────────
  allSavedRules: import('../../services/videoconference-api.service').VcScheduleHostRule[] = [];
  allSavedRulesLoading = false;

  constructor(
    private readonly api: VideoconferenceApiService,
    private readonly dialog: DialogService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit() {
    this.loadCatalog();
    this.loadZoomGroups();
    this.loadAllSavedRules();
  }

  loadAllSavedRules() {
    this.allSavedRulesLoading = true;
    this.api.listHostRules().subscribe({
      next: (rules) => { this.allSavedRules = rules; this.allSavedRulesLoading = false; this.cdr.markForCheck(); },
      error: () => { this.allSavedRulesLoading = false; this.cdr.markForCheck(); },
    });
  }

  get groupedSavedRules(): Array<{
    section_id: string; section_code: string; course_id: string | null; course_label: string | null;
    rules: VcScheduleHostRule[];
  }> {
    const map = new Map<string, { section_id: string; section_code: string; course_id: string | null; course_label: string | null; rules: VcScheduleHostRule[] }>();
    for (const r of this.allSavedRules) {
      if (!map.has(r.section_id)) {
        map.set(r.section_id, { section_id: r.section_id, section_code: r.section_code, course_id: r.course_id, course_label: r.course_label, rules: [] });
      }
      map.get(r.section_id)!.rules.push(r);
    }
    return Array.from(map.values());
  }

  editFromGlobalSummary(group: { section_id: string; section_code: string; course_id: string | null; course_label: string | null }) {
    const scrollToConfig = () => {
      setTimeout(() => {
        const el = document.querySelector('.workspace-config');
        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 80);
    };

    // If the course is already loaded, just select the section
    const existingSection = this.sections.find((s) => s.section_id === group.section_id);
    if (existingSection) {
      this.selectSection(existingSection);
      scrollToConfig();
      return;
    }
    // Otherwise load the course first
    const courseId = group.course_id;
    if (!courseId) {
      this.configError = 'No se pudo abrir la configuración: falta el curso asociado.';
      this.cdr.markForCheck();
      return;
    }
    // Set the search field so user sees the context
    this.selectedCourse = { id: courseId, label: group.course_label ?? courseId };
    this.courseSearch = this.selectedCourse.label;
    this.loadSections(courseId, group.section_id);
    scrollToConfig();
  }

  async deleteAllForSection(group: { section_id: string; section_code: string; rules: VcScheduleHostRule[] }) {
    const ok = await this.dialog.confirm(`¿Eliminar ${group.rules.length} regla(s) de la sección ${group.section_code}?`);
    if (!ok) return;
    const ops = group.rules.map((r) => new Promise<void>((res, rej) => {
      this.api.deleteHostRule(r.id).subscribe({ next: () => res(), error: rej });
    }));
    try {
      await Promise.all(ops);
      this.loadAllSavedRules();
      // If this section is currently selected, refresh it too
      if (this.selectedCourse && this.selectedSection?.section_id === group.section_id) {
        this.loadSections(this.selectedCourse.id, undefined);
      }
    } catch {
      // silent
    }
    this.cdr.markForCheck();
  }

  // ─── Stage 1 ─────────────────────────────────────────────────────────────

  loadCatalog() {
    this.catalogLoading = true;
    this.api.getFilterOptions({}).subscribe({
      next: (res) => {
        this.allCourses = res.courses ?? [];
        this.catalogLoading = false;
        this.cdr.markForCheck();
      },
      error: () => { this.catalogLoading = false; this.cdr.markForCheck(); },
    });
  }

  loadZoomGroups() {
    this.api.listZoomGroups().subscribe({
      next: (g) => { this.zoomGroups = g; this.cdr.markForCheck(); },
    });
  }

  get filteredCourses(): CourseOption[] {
    const q = this.courseSearch.trim().toLowerCase();
    if (!q) return this.allCourses.slice(0, 80);
    return this.allCourses.filter((c) => c.label.toLowerCase().includes(q)).slice(0, 80);
  }

  openDropdown() { this.courseDropdownOpen = true; }

  closeDropdown() {
    setTimeout(() => { this.courseDropdownOpen = false; this.cdr.markForCheck(); }, 180);
  }

  selectCourse(course: CourseOption) {
    this.selectedCourse = course;
    this.courseSearch = course.label;
    this.courseDropdownOpen = false;
    this.resetFromStage2();
    this.loadSections(course.id);
  }

  clearCourse() {
    this.selectedCourse = null;
    this.courseSearch = '';
    this.resetFromStage2();
  }

  // ─── Stage 2 ─────────────────────────────────────────────────────────────

  loadSections(courseId: string, reselectSectionId?: string) {
    this.sectionsLoading = true;
    this.sections = [];
    this.api.preview({ courseIds: [courseId], includeAll: true, expandGroups: true }).subscribe({
      next: (items) => {
        this.sections = this.buildSections(items);
        // Re-select the same section with fresh data so host_rule values stay in sync
        if (reselectSectionId) {
          const fresh = this.sections.find((s) => s.section_id === reselectSectionId);
          if (fresh) this.selectSection(fresh);
        }
        this.sectionsLoading = false;
        this.cdr.markForCheck();
      },
      error: () => { this.sectionsLoading = false; this.cdr.markForCheck(); },
    });
  }

  private buildSections(items: VideoconferencePreviewItem[]): SectionSummary[] {
    const map = new Map<string, SectionSummary>();
    for (const item of items) {
      if (!map.has(item.section_id)) {
        map.set(item.section_id, {
          section_id: item.section_id,
          section_code: item.section_code,
          section_label: item.section_label,
          course_label: item.course_label,
          teacher_name: item.teacher_name,
          schedules: [],
        });
      }
      const sec = map.get(item.section_id)!;
      if (!sec.schedules.some((s) => s.schedule_id === item.schedule_id)) {
        const hostRule = item.host_rule
          ? { ...item.host_rule, lock_host: (item.host_rule as any).lock_host ?? false, skip_zoom: (item.host_rule as any).skip_zoom ?? false }
          : null;
        sec.schedules.push({
          ...item,
          host_rule: hostRule,
          draft: {
            zoomMode: hostRule ? (hostRule.skip_zoom ? 'skip' : (hostRule.zoom_group_id ? 'zoom' : 'default')) : 'default',
            zoomGroupId: hostRule?.zoom_group_id ?? '',
            zoomUserId: hostRule?.zoom_user_id ?? '',
            lockHost: hostRule?.lock_host ?? false,
            notes: '',
            poolUsers: [],
            poolLoading: false,
          },
        });
      }
    }
    return Array.from(map.values());
  }

  selectSection(section: SectionSummary) {
    this.selectedSection = section;
    this.configMessage = '';
    this.configError = '';

    // Detect unified mode: ALL schedules have rules, none are skip_zoom, and same group/user config
    const allHaveRules = section.schedules.every((r) => r.host_rule !== null);
    const zoomRules = section.schedules.filter((r) => r.host_rule && !r.host_rule.skip_zoom);
    const uniqueConfig = new Set(zoomRules.map((r) => `${r.host_rule!.zoom_group_id}|${r.host_rule!.zoom_user_id}`));
    if (allHaveRules && section.schedules.length > 0 && uniqueConfig.size <= 1 && zoomRules.length === section.schedules.length) {
      this.assignMode = 'unified';
      const first = zoomRules[0];
      this.unifiedDraft = {
        zoomGroupId: first?.host_rule!.zoom_group_id ?? '',
        zoomUserId: first?.host_rule!.zoom_user_id ?? '',
        lockHost: first?.host_rule!.lock_host ?? false,
        notes: '',
        poolUsers: [],
        poolLoading: false,
      };
      if (this.unifiedDraft.zoomGroupId) {
        this.loadPoolForDraft(this.unifiedDraft, this.unifiedDraft.zoomGroupId);
      }
    } else {
      this.assignMode = 'individual';
      this.unifiedDraft = { zoomGroupId: '', zoomUserId: '', lockHost: false, notes: '', poolUsers: [], poolLoading: false };
    }

    // Load pools for individual rows that already have a group
    for (const row of section.schedules) {
      if (row.draft.zoomGroupId) {
        this.loadPoolForDraft(row.draft, row.draft.zoomGroupId);
      }
    }
  }

  // ─── Stage 3: Configuration ───────────────────────────────────────────────

  onUnifiedGroupChange() {
    this.unifiedDraft.zoomUserId = '';
    this.unifiedDraft.poolUsers = [];
    if (this.unifiedDraft.zoomGroupId) {
      this.loadPoolForDraft(this.unifiedDraft, this.unifiedDraft.zoomGroupId);
    }
  }

  onIndividualGroupChange(draft: ScheduleRow['draft']) {
    draft.zoomUserId = '';
    draft.poolUsers = [];
    if (draft.zoomGroupId) {
      this.loadPoolForDraft(draft, draft.zoomGroupId);
    }
  }

  onZoomModeChange(row: ScheduleRow, mode: 'default' | 'zoom' | 'skip') {
    row.draft.zoomMode = mode;
    if (mode !== 'zoom') {
      row.draft.zoomGroupId = '';
      row.draft.zoomUserId = '';
      row.draft.poolUsers = [];
    } else if (row.draft.zoomGroupId) {
      this.loadPoolForDraft(row.draft, row.draft.zoomGroupId);
    }
  }

  private loadPoolForDraft(draft: { poolUsers: ZoomPoolLicenseAwareUser[]; poolLoading: boolean }, groupId: string) {
    draft.poolLoading = true;
    this.api.getZoomGroupPool(groupId).subscribe({
      next: (res) => {
        // items = only pool members; users = all Zoom users (for management UI)
        draft.poolUsers = res.items ?? [];
        draft.poolLoading = false;
        this.cdr.markForCheck();
      },
      error: () => { draft.poolLoading = false; this.cdr.markForCheck(); },
    });
  }

  /** Groups schedules by day and merges adjacent/consecutive time blocks. */
  get unifiedScheduleSummary(): { groups: string[]; day_label: string; start_time: string; end_time: string; merged: boolean }[] {
    if (!this.selectedSection) return [];

    // Collect rows per day
    const byDay = new Map<string, { day_label: string; rows: { group: string; start_time: string; end_time: string }[] }>();
    for (const row of this.selectedSection.schedules) {
      const entry = byDay.get(row.day_of_week) ?? { day_label: row.day_label, rows: [] };
      entry.rows.push({ group: row.subsection_label, start_time: row.start_time, end_time: row.end_time });
      byDay.set(row.day_of_week, entry);
    }

    const result: { groups: string[]; day_label: string; start_time: string; end_time: string; merged: boolean }[] = [];
    for (const { day_label, rows } of byDay.values()) {
      const sorted = [...rows].sort((a, b) => a.start_time.localeCompare(b.start_time));
      const blocks: { groups: string[]; start_time: string; end_time: string }[] = [];
      for (const row of sorted) {
        const last = blocks[blocks.length - 1];
        // Merge when next slot starts at or before current block end (consecutive or overlapping)
        if (last && row.start_time <= last.end_time) {
          last.groups.push(row.group);
          if (row.end_time > last.end_time) last.end_time = row.end_time;
        } else {
          blocks.push({ groups: [row.group], start_time: row.start_time, end_time: row.end_time });
        }
      }
      for (const block of blocks) {
        result.push({ ...block, day_label, merged: block.groups.length > 1 });
      }
    }
    return result;
  }

  getPoolLabel(u: ZoomPoolLicenseAwareUser): string {
    const name = u.name && u.name.trim() !== '' && !/^\d+$/.test(u.name.trim()) ? u.name.trim() : null;
    const email = u.email?.trim() || null;
    const license = u.license_label?.trim() || null;
    const parts: string[] = [];
    if (name) parts.push(name);
    if (email) parts.push(name ? `(${email})` : email);
    if (license) parts.push(`· ${license}`);
    return parts.join(' ') || u.zoom_user_id || u.id || '—';
  }

  get canSave(): boolean {
    if (!this.selectedSection) return false;
    if (this.assignMode === 'unified') {
      return Boolean(this.unifiedDraft.zoomGroupId);
    }
    // In individual mode, always saveable (any row can be default/skip/zoom)
    return true;
  }

  async saveConfig() {
    if (!this.selectedSection) return;
    this.configSaving = true;
    this.configMessage = '';
    this.configError = '';

    const schedules = this.selectedSection.schedules;
    const ops: Promise<void>[] = [];

    if (this.assignMode === 'unified') {
      const { zoomGroupId, zoomUserId, lockHost, notes } = this.unifiedDraft;
      if (!zoomGroupId) {
        this.configError = 'Selecciona un grupo Zoom.';
        this.configSaving = false;
        return;
      }
      for (const row of schedules) {
        ops.push(this.upsertRule(row, zoomGroupId, zoomUserId || null, lockHost, notes, false));
      }
    } else {
      for (const row of schedules) {
        const d = row.draft;
        if (d.zoomMode === 'default') {
          // Auto: save rule with null group/user — system will auto-assign host at generation time
          ops.push(this.upsertRule(row, null, null, false, '', false));
        } else if (d.zoomMode === 'skip') {
          ops.push(this.upsertRule(row, null, null, false, '', true));
        } else {
          // zoom mode: group required; user optional
          if (!d.zoomGroupId) {
            // no group set in zoom mode → treat as auto
            ops.push(this.upsertRule(row, null, null, false, '', false));
          } else {
            ops.push(this.upsertRule(row, d.zoomGroupId, d.zoomUserId || null, d.lockHost, d.notes, false));
          }
        }
      }
    }

    try {
      await Promise.all(ops);
      this.configMessage = 'Configuracion guardada.';
      this.loadAllSavedRules();
      if (this.selectedCourse) this.loadSections(this.selectedCourse.id, this.selectedSection?.section_id);
    } catch (err: any) {
      this.configError = err?.error?.message ?? 'Error al guardar.';
    } finally {
      this.configSaving = false;
      this.cdr.markForCheck();
    }
  }

  private upsertRule(row: ScheduleRow, zoomGroupId: string | null, zoomUserId: string | null, lockHost: boolean, notes: string, skipZoom: boolean): Promise<void> {
    return new Promise((resolve, reject) => {
      if (row.host_rule) {
        this.api.updateHostRule(row.host_rule.rule_id, { zoomGroupId: zoomGroupId ?? undefined, zoomUserId: zoomUserId ?? undefined, lockHost, notes: notes || undefined, skipZoom })
          .subscribe({ next: () => resolve(), error: reject });
      } else {
        this.api.createHostRule({ scheduleId: row.schedule_id, zoomGroupId: zoomGroupId ?? undefined, zoomUserId: zoomUserId ?? undefined, lockHost, notes: notes || undefined, skipZoom })
          .subscribe({ next: () => resolve(), error: reject });
      }
    });
  }

  private deleteRuleForRow(row: ScheduleRow): Promise<void> {
    return new Promise((resolve, reject) => {
      this.api.deleteHostRule(row.host_rule!.rule_id).subscribe({ next: () => resolve(), error: reject });
    });
  }

  async removeRule(row: ScheduleRow) {
    if (!row.host_rule) return;
    const ok = await this.dialog.confirm(`Quitar host fijo de ${row.subsection_label}?`);
    if (!ok) return;
    this.api.deleteHostRule(row.host_rule.rule_id).subscribe({
      next: () => {
        row.host_rule = null;
        row.draft.zoomGroupId = '';
        row.draft.zoomUserId = '';
        this.cdr.markForCheck();
      },
    });
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  private resetFromStage2() {
    this.sections = [];
    this.selectedSection = null;
    this.assignMode = 'individual';
    this.unifiedDraft = { zoomGroupId: '', zoomUserId: '', lockHost: false, notes: '', poolUsers: [], poolLoading: false };
    this.configMessage = '';
    this.configError = '';
  }

  rulesCount(section: SectionSummary): number {
    return section.schedules.filter((s) => s.host_rule).length;
  }

  get savedRules(): Array<{ section: SectionSummary; row: ScheduleRow }> {
    const result: Array<{ section: SectionSummary; row: ScheduleRow }> = [];
    for (const sec of this.sections) {
      for (const row of sec.schedules) {
        result.push({ section: sec, row });
      }
    }
    return result;
  }

  deleteFromSummary(section: SectionSummary, row: ScheduleRow) {
    if (!row.host_rule) return;
    this.api.deleteHostRule(row.host_rule.rule_id).subscribe({
      next: () => {
        row.host_rule = null;
        row.draft.zoomMode = 'default';
        row.draft.zoomGroupId = '';
        row.draft.zoomUserId = '';
        this.loadAllSavedRules();
        if (this.selectedCourse) {
          this.loadSections(this.selectedCourse.id, this.selectedSection?.section_id);
        }
        this.cdr.markForCheck();
      },
    });
  }
}
