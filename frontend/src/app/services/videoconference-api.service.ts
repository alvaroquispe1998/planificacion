import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { API_BASE_URL } from '../core/api-base';

export interface FilterOptionsDto {
    semesterId?: string;
    campusIds?: string[];
    facultyIds?: string[];
    programIds?: string[];
    courseIds?: string[];
    modality?: string;
    modalities?: string[];
    days?: string[];
}

export interface VideoconferencePreviewDto extends FilterOptionsDto {
    startDate?: string;
    endDate?: string;
    includeSplit?: boolean;
    includeAll?: boolean;
    /** Return each schedule as its own independent row (no continuous-block grouping) */
    expandGroups?: boolean;
}

export interface FilterCatalogOption {
    id: string;
    label: string;
}

export interface VideoconferenceFilterOptionsResponse {
    periods: FilterCatalogOption[];
    campuses: FilterCatalogOption[];
    faculties: FilterCatalogOption[];
    programs: FilterCatalogOption[];
    courses: FilterCatalogOption[];
    modalities: FilterCatalogOption[];
    days: FilterCatalogOption[];
}

export interface VideoconferencePreviewItem {
    id: string;
    occurrence_key: string;
    schedule_id: string;
    grouped_schedule_ids?: string[];
    section_id: string;
    section_code: string;
    section_label: string;
    subsection_id: string;
    subsection_code: string;
    subsection_label: string;
    campus_id: string | null;
    campus_name: string | null;
    faculty_id: string | null;
    faculty_name: string | null;
    program_id: string | null;
    program_name: string | null;
    course_id: string;
    cycle: number | null;
    course_code: string | null;
    course_name: string | null;
    course_label: string;
    modality_code: string | null;
    modality_name: string | null;
    teacher_id: string | null;
    teacher_name: string | null;
    teacher_dni: string | null;
    day_of_week: string;
    day_label: string;
    start_time: string;
    end_time: string;
    duration_minutes: number;
    base_start_time?: string;
    base_end_time?: string;
    base_duration_minutes?: number;
    vc_period_id: string | null;
    vc_faculty_id: string | null;
    vc_faculty_name: string | null;
    vc_academic_program_id: string | null;
    vc_academic_program_name: string | null;
    vc_course_id: string | null;
    vc_course_name: string | null;
    vc_section_id: string | null;
    vc_section_name: string | null;
    vc_source: string | null;
    vc_context_message: string | null;
    occurrence_type: 'BASE' | 'RESCHEDULED' | 'SKIPPED';
    base_conference_date: string;
    effective_conference_date: string;
    effective_start_time: string;
    effective_end_time: string;
    override_id: string | null;
    override_reason_code: string | null;
    override_notes: string | null;
    selectable: boolean;
    inheritance: {
        is_inherited: boolean;
        mapping_id: string | null;
        parent_schedule_id: string | null;
        parent_occurrence_key: string | null;
        parent_label: string | null;
        family_owner_schedule_id: string;
    };
    section_projected_vacancies: number | null;
    host_rule?: {
        rule_id: string;
        zoom_user_id: string | null;
        zoom_user_email: string | null;
        zoom_user_name: string | null;
        zoom_group_id: string | null;
        zoom_group_name: string | null;
        lock_host: boolean;
        skip_zoom: boolean;
    } | null;
    selected?: boolean;
}

export type VideoconferenceAssignmentPreviewMode = 'BASE' | 'OCCURRENCE';

export type VideoconferenceAssignmentPreviewStatus =
    | 'ASSIGNED_LICENSED'
    | 'ASSIGNED_RISK'
    | 'INHERITED'
    | 'BLOCKED_EXISTING'
    | 'NO_AVAILABLE_ZOOM_USER'
    | 'VALIDATION_ERROR';

export interface VideoconferenceAssignmentPreviewDaySummary {
    day_of_week: string;
    day_label: string;
    required_hosts: number;
}

export interface VideoconferenceAssignmentPreviewSummary {
    requested_rows: number;
    assigned_rows: number;
    hosts_used: number;
    verified_hosts_used: number;
    risk_hosts_used: number;
    no_available_zoom_user: number;
    validation_errors: number;
    blocked_existing: number;
    virtual_hosts_needed: number;
    licenses_required_global: number;
    additional_licenses_needed: number | null;
    licenses_by_day: VideoconferenceAssignmentPreviewDaySummary[];
}

export interface VideoconferenceAssignmentPreviewItem {
    id: string;
    mode: VideoconferenceAssignmentPreviewMode;
    occurrence_key: string | null;
    schedule_id: string;
    conference_date: string | null;
    day_of_week: string;
    day_label: string;
    start_time: string;
    end_time: string;
    preview_status: VideoconferenceAssignmentPreviewStatus;
    message: string;
    zoom_user_id: string | null;
    zoom_user_email: string | null;
    zoom_user_name: string | null;
    license_status: ZoomPoolLicenseStatus | null;
    license_label: string | null;
    is_licensed: boolean | null;
    depends_on_unverified_license: boolean;
    consumes_capacity: boolean;
    inheritance: VideoconferencePreviewItem['inheritance'];
    owner_occurrence_key: string | null;
}

export interface VideoconferenceAssignmentPreviewResponse {
    mode: VideoconferenceAssignmentPreviewMode;
    summary: VideoconferenceAssignmentPreviewSummary;
    items: VideoconferenceAssignmentPreviewItem[];
    pool_warnings?: string[];
    license_sync_ok: boolean;
    license_sync_error: string | null;
}

export interface VideoconferenceOverridePayload {
    scheduleId: string;
    conferenceDate: string;
    action: 'KEEP' | 'SKIP' | 'RESCHEDULE';
    overrideDate?: string;
    overrideStartTime?: string;
    overrideEndTime?: string;
    reasonCode?: 'HOLIDAY' | 'WEATHER' | 'OTHER';
    notes?: string;
}

export interface VideoconferenceTemporaryOverridePayload {
    scheduleId: string;
    conferenceDate: string;
    overrideDate: string;
    overrideStartTime: string;
    overrideEndTime: string;
    reasonCode?: 'HOLIDAY' | 'WEATHER' | 'OTHER';
    notes?: string;
    topicOverride?: string;
}

export type VideoconferenceGenerationStatus =
    | 'MATCHED'
    | 'CREATED_UNMATCHED'
    | 'BLOCKED_EXISTING'
    | 'NO_AVAILABLE_ZOOM_USER'
    | 'VALIDATION_ERROR'
    | 'ERROR';

export interface VideoconferenceGenerationResultItem {
    schedule_id: string;
    occurrence_key: string | null;
    conference_date: string | null;
    status: VideoconferenceGenerationStatus;
    message: string;
    record_id: string | null;
    zoom_user_id: string | null;
    zoom_user_email: string | null;
    zoom_meeting_id: string | null;
    link_mode: 'OWNED' | 'INHERITED';
    owner_videoconference_id: string | null;
    inheritance: VideoconferencePreviewItem['inheritance'];
}

export interface VideoconferenceGenerationSummary {
    requestedSchedules: number;
    requestedOccurrences: number;
    matched: number;
    createdUnmatched: number;
    blockedExisting: number;
    noAvailableZoomUser: number;
    validationErrors: number;
    errors: number;
}

export interface VideoconferenceGenerationResponse {
    success: boolean;
    message: string;
    summary: VideoconferenceGenerationSummary;
    results: VideoconferenceGenerationResultItem[];
    pool_warnings?: string[];
}

export interface VideoconferenceReconcileResponse {
    success: boolean;
    matched: boolean;
    message: string;
    result: VideoconferenceGenerationResultItem;
}

export interface VideoconferenceRescheduleItem {
    id: string;
    schedule_id: string;
    conference_date: string;
    override_date: string | null;
    override_start_time: string | null;
    override_end_time: string | null;
    reason_code: string | null;
    notes: string | null;
    created_at: string | null;
    updated_at: string | null;
    semester_id: string | null;
    semester_name: string | null;
    course_label: string;
    section_label: string;
    subsection_label: string;
    teacher_name: string | null;
    teacher_dni: string | null;
    campus_name: string | null;
    faculty_name: string | null;
    program_name: string | null;
    cycle: number | null;
    topic: string | null;
    payload_json: Record<string, unknown> | null;
    record_id: string | null;
    conference_status: string | null;
    audit_sync_status: string | null;
    zoom_meeting_id: string | null;
    zoom_user_email: string | null;
    zoom_user_name: string | null;
    can_reset: boolean;
}

export interface VideoconferenceRescheduleListResponse {
    totals: {
        total: number;
        created: number;
        pending: number;
    };
    items: VideoconferenceRescheduleItem[];
}

export type ZoomPoolLicenseStatus = 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN';

export interface ZoomPoolLicenseAwareUser {
    id?: string;
    zoom_user_id?: string;
    name: string | null;
    email: string | null;
    in_pool?: boolean;
    sort_order?: number;
    is_active?: boolean;
    license_status: ZoomPoolLicenseStatus;
    license_label: string;
    is_licensed: boolean | null;
}

export interface ZoomPoolResponse {
    items: ZoomPoolLicenseAwareUser[];
    users: ZoomPoolLicenseAwareUser[];
    license_sync_ok: boolean;
    license_sync_error: string | null;
}

export interface ZoomGroupItem {
    id: string;
    name: string;
    code: string;
    is_active: boolean;
    is_default?: boolean;
    members_count?: number;
    active_members_count?: number;
}

export interface VideoconferenceInheritanceCatalogSchedule {
    schedule_id: string;
    campus_id: string | null;
    program_id: string | null;
    subsection_id: string;
    section_id: string;
    section_code: string;
    course_id: string;
    course_code: string | null;
    course_name: string | null;
    course_label: string;
    section_label: string;
    subsection_label: string;
    day_of_week: string;
    day_label: string;
    start_time: string;
    end_time: string;
    schedule_label: string;
    vacancy_label: string;
    section_projected_vacancies: number | null;
    subsection_projected_vacancies: number | null;
    teacher_name: string | null;
    is_child_inherited: boolean;
    inherited_from_schedule_id: string | null;
}

export interface VideoconferenceInheritanceItem {
    id: string;
    parent_schedule_id: string;
    child_schedule_id: string;
    notes: string | null;
    is_active: boolean;
    created_at: string;
    updated_at: string;
    validity: 'ok' | 'inactive' | 'schedule_missing' | 'schedule_mismatch' | 'teacher_mismatch';
    parent: {
        schedule_id: string;
        course_label: string;
        section_label: string;
        subsection_label: string;
        schedule_label: string;
        section_projected_vacancies: number | null;
        teacher_name: string | null;
        day_of_week: string;
        start_time: string;
        end_time: string;
    } | null;
    child: {
        schedule_id: string;
        course_label: string;
        section_label: string;
        subsection_label: string;
        schedule_label: string;
        section_projected_vacancies: number | null;
        teacher_name: string | null;
        day_of_week: string;
        start_time: string;
        end_time: string;
    } | null;
}

export interface VcScheduleHostRule {
    id: string;
    schedule_id: string;
    zoom_group_id: string | null;
    zoom_group_name: string | null;
    zoom_user_id: string | null;
    zoom_user_email: string | null;
    zoom_user_name: string | null;
    notes: string | null;
    is_active: boolean;
    lock_host: boolean;
    skip_zoom: boolean;
    created_at: string;
    updated_at: string;
    section_id: string;
    section_code: string;
    course_id: string | null;
    course_label: string | null;
}

export interface VideoconferenceInheritanceCandidateItem {
    id: string;
    teacher_name: string | null;
    cycle: number | null;
    day_of_week: string;
    day_label: string;
    start_time: string;
    end_time: string;
    faculty_name: string | null;
    parent: {
        schedule_id: string;
        campus_id: string | null;
        campus_name: string | null;
        program_id: string | null;
        program_name: string | null;
        course_id: string;
        course_label: string;
        vc_section_name: string | null;
        section_id: string;
        section_label: string;
        section_projected_vacancies: number | null;
        subsection_label: string;
        schedule_label: string;
    };
    children: Array<{
        schedule_id: string;
        campus_id: string | null;
        campus_name: string | null;
        program_id: string | null;
        program_name: string | null;
        course_id: string;
        course_label: string;
        vc_section_name: string | null;
        section_id: string;
        section_label: string;
        section_projected_vacancies: number | null;
        subsection_label: string;
        schedule_label: string;
    }>;
}

@Injectable({ providedIn: 'root' })
export class VideoconferenceApiService {
    private readonly baseUrl = `${API_BASE_URL}/videoconference`;

    constructor(private http: HttpClient) { }

    getCampuses() {
        return this.http.get<any[]>(`${this.baseUrl}/campuses`);
    }

    getFaculties() {
        return this.http.get<any[]>(`${this.baseUrl}/faculties`);
    }

    getPrograms(facultyIds?: string[]) {
        let params = new HttpParams();
        if (facultyIds?.length) {
            facultyIds.forEach((id) => (params = params.append('facultyIds', id)));
        }
        return this.http.get<any[]>(`${this.baseUrl}/programs`, { params });
    }

    getCourses(programIds?: string[]) {
        let params = new HttpParams();
        if (programIds?.length) {
            programIds.forEach((id) => (params = params.append('programIds', id)));
        }
        return this.http.get<any[]>(`${this.baseUrl}/courses`, { params });
    }

    getFilterOptions(filters: FilterOptionsDto) {
        return this.http.post<VideoconferenceFilterOptionsResponse>(`${this.baseUrl}/filter-options`, filters);
    }

    getInheritanceCatalog(filters: {
        semesterId: string;
        campusId: string;
        facultyId: string;
        programId: string;
    }) {
        let params = new HttpParams()
            .set('semesterId', filters.semesterId)
            .set('campusId', filters.campusId)
            .set('facultyId', filters.facultyId)
            .set('programId', filters.programId);
        return this.http.get<{ schedules: VideoconferenceInheritanceCatalogSchedule[] }>(
            `${this.baseUrl}/inheritances/catalog`,
            { params },
        );
    }

    listInheritances() {
        return this.http.get<VideoconferenceInheritanceItem[]>(`${this.baseUrl}/inheritances`);
    }

    previewInheritanceCandidates(payload: { semesterId: string; facultyId: string }) {
        return this.http.post<{ success: boolean; count: number; items: VideoconferenceInheritanceCandidateItem[] }>(
            `${this.baseUrl}/inheritances/candidates`,
            payload,
        );
    }

    cleanupLegacyInheritances(filters: { semesterId?: string; facultyId?: string }) {
        let params = new HttpParams();
        if (filters.semesterId) {
            params = params.set('semesterId', filters.semesterId);
        }
        if (filters.facultyId) {
            params = params.set('facultyId', filters.facultyId);
        }
        return this.http.delete<{ success: boolean; count: number }>(
            `${this.baseUrl}/inheritances/cleanup-legacy`,
            { params },
        );
    }

    createInheritance(payload: {
        parentScheduleId: string;
        childScheduleId: string;
        notes?: string;
        isActive?: boolean;
    }) {
        return this.http.post<VideoconferenceInheritanceItem>(`${this.baseUrl}/inheritances`, payload);
    }

    updateInheritance(
        id: string,
        payload: {
            parentScheduleId?: string;
            childScheduleId?: string;
            notes?: string;
            isActive?: boolean;
        },
    ) {
        return this.http.patch<VideoconferenceInheritanceItem>(
            `${this.baseUrl}/inheritances/${encodeURIComponent(id)}`,
            payload,
        );
    }

    deleteInheritance(id: string) {
        return this.http.delete<{ success: boolean; id: string }>(
            `${this.baseUrl}/inheritances/${encodeURIComponent(id)}`,
        );
    }

    preview(filters: VideoconferencePreviewDto) {
        return this.http.post<VideoconferencePreviewItem[]>(`${this.baseUrl}/preview`, filters);
    }

    assignmentPreview(payload: {
        zoomGroupId: string;
        selectAllVisible?: boolean;
        semesterId?: string;
        campusIds?: string[];
        facultyIds?: string[];
        programIds?: string[];
        courseIds?: string[];
        modality?: string;
        modalities?: string[];
        days?: string[];
        scheduleIds?: string[];
        occurrenceKeys?: string[];
        startDate?: string;
        endDate?: string;
        temporaryOverrides?: VideoconferenceTemporaryOverridePayload[];
    }) {
        return this.http.post<VideoconferenceAssignmentPreviewResponse>(`${this.baseUrl}/assignment-preview`, payload);
    }

    generate(payload: {
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
        temporaryOverrides?: VideoconferenceTemporaryOverridePayload[];
    }) {
        return this.http.post<VideoconferenceGenerationResponse>(`${this.baseUrl}/generate`, payload);
    }

    checkExisting(payload: {
        occurrenceKeys?: string[];
        scheduleIds?: string[];
        startDate?: string;
        endDate?: string;
    }) {
        return this.http.post<{
            existing: Array<{
                occurrence_key: string;
                schedule_id: string;
                conference_date: string;
                status: string;
                zoom_meeting_id: string | null;
                zoom_user_email: string | null;
                record_id: string;
            }>;
        }>(`${this.baseUrl}/check-existing`, payload);
    }

    getZoomPool() {
        return this.http.get<ZoomPoolResponse>(`${API_BASE_URL}/settings/zoom/pool`);
    }

    getZoomGroupPool(groupId: string) {
        return this.http.get<ZoomPoolResponse>(`${API_BASE_URL}/settings/zoom/groups/${encodeURIComponent(groupId)}/pool`);
    }

    listZoomGroups() {
        return this.http.get<ZoomGroupItem[]>(`${this.baseUrl}/zoom-groups`);
    }

    listReschedules() {
        return this.http.get<VideoconferenceRescheduleListResponse>(`${this.baseUrl}/reschedules`);
    }

    reconcile(id: string) {
        return this.http.post<VideoconferenceReconcileResponse>(`${this.baseUrl}/reconcile/${encodeURIComponent(id)}`, {});
    }

    upsertOverride(payload: VideoconferenceOverridePayload) {
        return this.http.post<any>(`${this.baseUrl}/overrides`, payload);
    }

    deleteOverride(scheduleId: string, conferenceDate: string) {
        const params = new HttpParams()
            .set('scheduleId', scheduleId)
            .set('conferenceDate', conferenceDate);
        return this.http.delete<any>(`${this.baseUrl}/overrides`, { params });
    }

    listHostRules() {
        return this.http.get<VcScheduleHostRule[]>(`${this.baseUrl}/host-rules`);
    }

    createHostRule(payload: { scheduleId: string; zoomGroupId?: string; zoomUserId?: string; notes?: string; lockHost?: boolean; skipZoom?: boolean }) {
        return this.http.post<{ id: string; schedule_id: string }>(`${this.baseUrl}/host-rules`, payload);
    }

    updateHostRule(id: string, payload: { zoomGroupId?: string; zoomUserId?: string; notes?: string; isActive?: boolean; lockHost?: boolean; skipZoom?: boolean }) {
        return this.http.patch<{ id: string; schedule_id: string }>(`${this.baseUrl}/host-rules/${encodeURIComponent(id)}`, payload);
    }

    deleteHostRule(id: string) {
        return this.http.delete<{ success: boolean; id: string }>(`${this.baseUrl}/host-rules/${encodeURIComponent(id)}`);
    }
}
