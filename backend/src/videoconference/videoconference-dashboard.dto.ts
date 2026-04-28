/**
 * DTOs (interfaces) para los endpoints del dashboard de videoconferencias.
 * Solo tipos: el dashboard expone agregaciones, no escribe datos.
 */

export interface DashboardTodaySummary {
    date: string; // ISO date (YYYY-MM-DD)
    totals: {
        total: number;
        matched: number;
        createdUnmatched: number;
        creating: number;
        error: number;
        cancelled: number; // overrides con action=SKIP en la fecha
        rescheduled: number; // overrides con action=RESCHEDULE en la fecha
    };
    audit: {
        pending: number;
        synced: number;
        error: number;
    };
    coverage: {
        expectedSchedules: number; // schedules activos del día
        generatedVideoconferences: number;
        coveragePercent: number; // 0..100
    };
}

export interface DashboardTodayUpcomingItem {
    videoconferenceId: string;
    scheduledStart: string; // ISO datetime
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
    hour: number; // 0..23
    activeMeetings: number; // suma de meetings que estan corriendo en esa hora
    distinctZoomUsers: number;
}

export interface DashboardTodayHostUtilizationResponse {
    date: string;
    poolSize: number; // usuarios activos del pool
    peakActiveMeetings: number;
    peakHour: number | null;
    buckets: DashboardTodayHostUtilizationBucket[];
}

// =====================================================================
// PERIOD (cobertura del semestre)
// =====================================================================

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
        schedulesCovered: number; // VC u override (skip/reschedule)
        schedulesMissing: number;
        coveragePercent: number; // 0..100
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

// =====================================================================
// HOST CALENDAR
// =====================================================================

export interface DashboardHostOption {
    zoomUserId: string;
    email: string | null;
    name: string | null;
    sessionCount: number;
}

export interface DashboardHostSession {
    videoconferenceId: string;
    scheduledStart: string; // ISO
    scheduledEnd: string;
    conferenceDate: string; // YYYY-MM-DD
    status: string;
    topic: string | null;
    courseCode: string | null;
    courseName: string | null;
    sectionLabel: string | null;
    teacherName: string | null;
    joinUrl: string | null;
}

export interface DashboardHostCalendarResponse {
    zoomUserId: string;
    from: string; // YYYY-MM-DD
    to: string; // YYYY-MM-DD
    sessions: DashboardHostSession[];
}

export interface DashboardCoverageDailyPoint {
    conferenceDate: string; // YYYY-MM-DD
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
