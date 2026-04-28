import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { API_BASE_URL } from '../core/api-base';

export interface DashboardTodaySummary {
    date: string;
    totals: {
        total: number;
        matched: number;
        createdUnmatched: number;
        creating: number;
        error: number;
        cancelled: number;
        rescheduled: number;
    };
    audit: {
        pending: number;
        synced: number;
        error: number;
    };
    coverage: {
        expectedSchedules: number;
        generatedVideoconferences: number;
        coveragePercent: number;
    };
}

export interface DashboardTodayUpcomingItem {
    videoconferenceId: string;
    scheduledStart: string;
    scheduledEnd: string;
    topic: string | null;
    status: string;
    courseCode: string | null;
    courseName: string | null;
    sectionLabel: string | null;
    teacherName: string | null;
    zoomUserEmail: string | null;
    zoomUserName: string | null;
    joinUrl: string | null;
}

export interface DashboardTodayErrorItem {
    videoconferenceId: string;
    conferenceDate: string;
    scheduledStart: string;
    topic: string | null;
    status: string;
    auditSyncStatus: string;
    deleteStatus: string | null;
    errorMessage: string | null;
    auditError: string | null;
    deleteError: string | null;
    courseCode: string | null;
    courseName: string | null;
    teacherName: string | null;
}

export interface DashboardTodayHostUtilizationBucket {
    hour: number;
    activeMeetings: number;
    distinctZoomUsers: number;
}

export interface DashboardTodayHostUtilizationResponse {
    date: string;
    poolSize: number;
    peakActiveMeetings: number;
    peakHour: number | null;
    buckets: DashboardTodayHostUtilizationBucket[];
}

export type CoverageDimension = 'faculty' | 'campus' | 'program';

export interface DashboardCoverageSummary {
    periodId: string;
    totals: {
        offers: number;
        sections: number;
        subsections: number;
        schedules: number;
    };
    videoconferences: {
        total: number;
        matched: number;
        createdUnmatched: number;
        creating: number;
        error: number;
    };
    coverage: {
        schedulesWithVideoconference: number;
        schedulesWithOverride: number;
        schedulesCovered: number;
        schedulesMissing: number;
        coveragePercent: number;
    };
    audit: {
        pending: number;
        synced: number;
        error: number;
    };
}

export interface DashboardCoverageDimensionRow {
    id: string | null;
    label: string;
    totalSchedules: number;
    schedulesWithVideoconference: number;
    coveragePercent: number;
}

export interface DashboardCoverageMissingItem {
    scheduleId: string;
    planningOfferId: string;
    planningSectionId: string;
    planningSubsectionId: string;
    courseCode: string | null;
    courseName: string | null;
    sectionCode: string | null;
    subsectionCode: string | null;
    dayOfWeek: string;
    startTime: string;
    endTime: string;
    teacherName: string | null;
    facultyName: string | null;
    campusName: string | null;
    reason: string;
}

export interface DashboardCoverageOverrideRow {
    action: string;
    count: number;
}

export interface DashboardCoverageDailyPoint {
    conferenceDate: string;
    total: number;
    matched: number;
    createdUnmatched: number;
    error: number;
}

export interface DashboardCoverageConflictRow {
    conflictType: string;
    severity: string;
    count: number;
}

@Injectable({ providedIn: 'root' })
export class VideoconferenceDashboardApiService {
    private readonly base = `${API_BASE_URL}/videoconference/dashboard`;

    constructor(private readonly http: HttpClient) { }

    getTodaySummary(date?: string): Observable<DashboardTodaySummary> {
        let params = new HttpParams();
        if (date) {
            params = params.set('date', date);
        }
        return this.http.get<DashboardTodaySummary>(`${this.base}/today/summary`, { params });
    }

    getTodayUpcoming(date?: string, withinMinutes?: number): Observable<DashboardTodayUpcomingItem[]> {
        let params = new HttpParams();
        if (date) params = params.set('date', date);
        if (withinMinutes != null) params = params.set('withinMinutes', String(withinMinutes));
        return this.http.get<DashboardTodayUpcomingItem[]>(`${this.base}/today/upcoming`, { params });
    }

    getTodayErrors(date?: string, limit?: number): Observable<DashboardTodayErrorItem[]> {
        let params = new HttpParams();
        if (date) params = params.set('date', date);
        if (limit != null) params = params.set('limit', String(limit));
        return this.http.get<DashboardTodayErrorItem[]>(`${this.base}/today/errors`, { params });
    }

    getTodayHostUtilization(date?: string): Observable<DashboardTodayHostUtilizationResponse> {
        let params = new HttpParams();
        if (date) params = params.set('date', date);
        return this.http.get<DashboardTodayHostUtilizationResponse>(
            `${this.base}/today/host-utilization`,
            { params },
        );
    }

    getCoverageSummary(periodId: string): Observable<DashboardCoverageSummary> {
        const params = new HttpParams().set('periodId', periodId);
        return this.http.get<DashboardCoverageSummary>(`${this.base}/coverage/summary`, { params });
    }

    getCoverageByDimension(
        periodId: string,
        dimension: CoverageDimension,
    ): Observable<DashboardCoverageDimensionRow[]> {
        const params = new HttpParams().set('periodId', periodId).set('dimension', dimension);
        return this.http.get<DashboardCoverageDimensionRow[]>(
            `${this.base}/coverage/by-dimension`,
            { params },
        );
    }

    getCoverageMissing(
        periodId: string,
        limit?: number,
    ): Observable<DashboardCoverageMissingItem[]> {
        let params = new HttpParams().set('periodId', periodId);
        if (limit != null) params = params.set('limit', String(limit));
        return this.http.get<DashboardCoverageMissingItem[]>(
            `${this.base}/coverage/missing`,
            { params },
        );
    }

    getCoverageOverrides(periodId: string): Observable<DashboardCoverageOverrideRow[]> {
        const params = new HttpParams().set('periodId', periodId);
        return this.http.get<DashboardCoverageOverrideRow[]>(
            `${this.base}/coverage/overrides`,
            { params },
        );
    }

    getCoverageDaily(periodId: string): Observable<DashboardCoverageDailyPoint[]> {
        const params = new HttpParams().set('periodId', periodId);
        return this.http.get<DashboardCoverageDailyPoint[]>(`${this.base}/coverage/daily`, {
            params,
        });
    }

    getCoverageConflicts(periodId: string): Observable<DashboardCoverageConflictRow[]> {
        const params = new HttpParams().set('periodId', periodId);
        return this.http.get<DashboardCoverageConflictRow[]>(
            `${this.base}/coverage/conflicts`,
            { params },
        );
    }
}
