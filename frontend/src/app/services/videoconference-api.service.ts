import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

export interface FilterOptionsDto {
    facultyIds?: string[];
    programIds?: string[];
    courseIds?: string[];
    sectionIds?: string[];
    modality?: string;
    days?: string[];
}

export interface VideoconferencePreviewItem {
    id: string; // meeting id
    faculty: string;
    program: string;
    course: string;
    section: string;
    day: string;
    start_time: string;
    end_time: string;
    zoom_user: string | null;
    _metadata: any;
    selected?: boolean; // UI only
}

@Injectable({ providedIn: 'root' })
export class VideoconferenceApiService {
    private readonly baseUrl = 'http://localhost:3000/videoconference';

    constructor(private http: HttpClient) { }

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

    getSections(courseIds?: string[]) {
        let params = new HttpParams();
        if (courseIds?.length) {
            courseIds.forEach((id) => (params = params.append('courseIds', id)));
        }
        return this.http.get<any[]>(`${this.baseUrl}/sections`, { params });
    }

    preview(filters: FilterOptionsDto) {
        return this.http.post<VideoconferencePreviewItem[]>(`${this.baseUrl}/preview`, filters);
    }

    generate(payload: { meetings: string[]; startDate: string; endDate: string }) {
        return this.http.post<any>(`${this.baseUrl}/generate`, payload);
    }
}
