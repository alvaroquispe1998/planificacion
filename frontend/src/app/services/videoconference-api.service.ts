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
    days?: string[];
}

export interface VideoconferencePreviewDto extends FilterOptionsDto {
    startDate?: string;
    endDate?: string;
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
    vc_period_id: string | null;
    vc_faculty_id: string | null;
    vc_academic_program_id: string | null;
    vc_course_id: string | null;
    vc_section_id: string | null;
    vc_section_name: string | null;
    occurrence_type: 'BASE' | 'RESCHEDULED' | 'SKIPPED';
    base_conference_date: string;
    effective_conference_date: string;
    effective_start_time: string;
    effective_end_time: string;
    override_id: string | null;
    override_reason_code: string | null;
    override_notes: string | null;
    selectable: boolean;
    selected?: boolean;
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
}

export interface VideoconferenceReconcileResponse {
    success: boolean;
    matched: boolean;
    message: string;
    result: VideoconferenceGenerationResultItem;
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

    preview(filters: VideoconferencePreviewDto) {
        return this.http.post<VideoconferencePreviewItem[]>(`${this.baseUrl}/preview`, filters);
    }

    generate(payload: { scheduleIds?: string[]; occurrenceKeys?: string[]; startDate: string; endDate: string }) {
        return this.http.post<VideoconferenceGenerationResponse>(`${this.baseUrl}/generate`, payload);
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
}
