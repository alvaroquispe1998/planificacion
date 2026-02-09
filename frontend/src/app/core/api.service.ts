import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly baseUrl = 'http://localhost:3000';

  constructor(private readonly http: HttpClient) {}

  syncAkademic(payload: Record<string, unknown>) {
    return this.http.post<any>(`${this.baseUrl}/sync/akademic`, payload);
  }

  listSyncResources() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/sync/resources`);
  }

  listSyncSources(probe = false) {
    const params = probe ? new HttpParams().set('probe', 'true') : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/settings/sync/sources`, { params });
  }

  upsertSyncCookie(sourceCode: string, payload: Record<string, unknown>) {
    return this.http.put<any>(`${this.baseUrl}/settings/sync/sources/${sourceCode}/session-cookie`, payload);
  }

  validateSyncSource(sourceCode: string) {
    return this.http.post<any>(`${this.baseUrl}/settings/sync/sources/${sourceCode}/validate-session`, {});
  }

  validateAllSyncSources() {
    return this.http.post<any[]>(`${this.baseUrl}/settings/sync/validate-all-sessions`, {});
  }

  runExternalSync(payload: Record<string, unknown>) {
    return this.http.post<any>(`${this.baseUrl}/settings/sync/run`, payload);
  }

  listSyncJobs(limit = 20) {
    const params = new HttpParams().set('limit', String(limit));
    return this.http.get<any[]>(`${this.baseUrl}/settings/sync/jobs`, { params });
  }

  listClassOfferings() {
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-offerings`);
  }

  getClassOffering(id: string) {
    return this.http.get<any>(`${this.baseUrl}/planning/class-offerings/${id}`);
  }

  createClassOffering(payload: Record<string, unknown>) {
    return this.http.post<any>(`${this.baseUrl}/planning/class-offerings`, payload);
  }

  listClassGroups(classOfferingId?: string) {
    const params = classOfferingId
      ? new HttpParams().set('class_offering_id', classOfferingId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-groups`, { params });
  }

  createClassGroup(payload: Record<string, unknown>) {
    return this.http.post<any>(`${this.baseUrl}/planning/class-groups`, payload);
  }

  listClassMeetings(classOfferingId?: string) {
    const params = classOfferingId
      ? new HttpParams().set('class_offering_id', classOfferingId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-meetings`, { params });
  }

  listClassTeachers(classOfferingId?: string) {
    const params = classOfferingId
      ? new HttpParams().set('class_offering_id', classOfferingId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-teachers`, { params });
  }

  createClassMeeting(payload: Record<string, unknown>) {
    return this.http.post<any>(`${this.baseUrl}/planning/class-meetings`, payload);
  }

  validateHours(classOfferingId: string) {
    return this.http.post<any>(`${this.baseUrl}/planning/hours-validation/${classOfferingId}`, {});
  }

  detectConflicts(semesterId: string) {
    return this.http.post<any>(`${this.baseUrl}/planning/schedule-conflicts/detect/${semesterId}`, {});
  }

  listScheduleConflicts(semesterId?: string) {
    const params = semesterId ? new HttpParams().set('semester_id', semesterId) : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/planning/schedule-conflicts`, { params });
  }

  listVideoConferences(classOfferingId?: string) {
    const params = classOfferingId
      ? new HttpParams().set('class_offering_id', classOfferingId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/audit/video-conferences`, { params });
  }

  listMeetingInstances(videoConferenceId?: string) {
    const params = videoConferenceId
      ? new HttpParams().set('video_conference_id', videoConferenceId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/audit/meeting-instances`, { params });
  }

  listParticipants(meetingInstanceId?: string) {
    const params = meetingInstanceId
      ? new HttpParams().set('meeting_instance_id', meetingInstanceId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/audit/meeting-participants`, { params });
  }

  listTeacherMetrics(meetingInstanceId?: string) {
    const params = meetingInstanceId
      ? new HttpParams().set('meeting_instance_id', meetingInstanceId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/audit/meeting-teacher-metrics`, { params });
  }

  recomputeTeacherMetrics(meetingInstanceId: string) {
    return this.http.post<any>(
      `${this.baseUrl}/audit/meeting-teacher-metrics/recompute/${meetingInstanceId}`,
      {},
    );
  }

  listTranscripts(meetingInstanceId?: string) {
    const params = meetingInstanceId
      ? new HttpParams().set('meeting_instance_id', meetingInstanceId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/audit/meeting-transcripts`, { params });
  }

  listSummaries(meetingInstanceId?: string) {
    const params = meetingInstanceId
      ? new HttpParams().set('meeting_instance_id', meetingInstanceId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/syllabus/summaries`, { params });
  }

  generateSummary(meetingInstanceId: string) {
    return this.http.post<any>(
      `${this.baseUrl}/syllabus/summaries/generate/${meetingInstanceId}`,
      { summary_type: 'EXTRACTIVE' },
    );
  }

  listSyllabusSessions(classOfferingId?: string) {
    const params = classOfferingId
      ? new HttpParams().set('class_offering_id', classOfferingId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/syllabus/sessions`, { params });
  }

  listMatches(meetingInstanceId?: string) {
    const params = meetingInstanceId
      ? new HttpParams().set('meeting_instance_id', meetingInstanceId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/syllabus/matches`, { params });
  }

  runMatch(meetingInstanceId: string, syllabusSessionId: string) {
    return this.http.post<any>(
      `${this.baseUrl}/syllabus/matches/run/${meetingInstanceId}/${syllabusSessionId}`,
      { method: 'HYBRID' },
    );
  }
}
