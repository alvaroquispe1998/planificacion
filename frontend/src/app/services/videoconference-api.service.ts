import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { API_BASE_URL } from '../core/api-base';

export interface FilterOptionsDto {
    campusIds?: string[];
    facultyIds?: string[];
    programIds?: string[];
    courseIds?: string[];
    modality?: string;
    days?: string[];
}

export interface FilterCatalogOption {
    id: string;
    label: string;
}

export interface VideoconferenceFilterOptionsResponse {
    campuses: FilterCatalogOption[];
    faculties: FilterCatalogOption[];
    programs: FilterCatalogOption[];
    courses: FilterCatalogOption[];
    modalities: FilterCatalogOption[];
    days: FilterCatalogOption[];
}

export interface VideoconferencePreviewItem {
    id: string;
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
    selected?: boolean;
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

    preview(filters: FilterOptionsDto) {
        return this.http.post<VideoconferencePreviewItem[]>(`${this.baseUrl}/preview`, filters);
    }

    generate(payload: { scheduleIds: string[]; startDate: string; endDate: string }) {
        return this.http.post<any>(`${this.baseUrl}/generate`, payload);
    }
}
