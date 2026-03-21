import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly baseUrl = environment.apiBaseUrl;

  constructor(private readonly http: HttpClient) { }

  syncAkademic(payload: Record<string, unknown>) {
    return this.http.post<any>(`${this.baseUrl}/sync/akademic`, payload);
  }

  // --- Catalog ---
  listSemesters() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/semesters`);
  }

  listCampuses() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/campuses`);
  }

  listPrograms() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/programs`);
  }

  listCourses() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/courses`);
  }

  listTeachers() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/teachers`);
  }

  listClassrooms() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/classrooms`);
  }

  listBuildings() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/buildings`);
  }

  listStudyPlans() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/study-plans`);
  }

  listCourseSections() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/catalog/course-sections`);
  }

  // --- Settings Sync ---
  listSyncResources() {
    return this.http.get<any[]>(`${this.baseUrl}/settings/sync/resources`);
  }

  listSyncSources(probe = false) {
    const params = probe ? new HttpParams().set('probe', 'true') : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/settings/sync/sources`, { params });
  }

  getSyncCookie(sourceCode: string) {
    return this.http.get<any>(`${this.baseUrl}/settings/sync/sources/${sourceCode}/session-cookie`);
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

  // --- Planning ---
  listClassOfferings(semesterId?: string) {
    const params = semesterId ? new HttpParams().set('semester_id', semesterId) : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-offerings`, { params });
  }

  getPlanningCatalogFilters() {
    return this.http.get<any>(`${this.baseUrl}/planning/catalog/filters`);
  }

  previewPlanningImport(file: File) {
    const formData = new FormData();
    formData.set('file', file);
    return this.http.post<any>(`${this.baseUrl}/planning/imports/excel/preview`, formData);
  }

  getPlanningImportBatch(batchId: string) {
    return this.http.get<any>(`${this.baseUrl}/planning/imports/${batchId}`);
  }

  getPlanningImportReport(batchId: string) {
    return this.http.get<any>(`${this.baseUrl}/planning/imports/${batchId}/report`);
  }

  updatePlanningImportScopeDecisions(batchId: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/imports/${batchId}/scope-decisions`, payload);
  }

  executePlanningImportBatch(batchId: string) {
    return this.http.post<any>(`${this.baseUrl}/planning/imports/${batchId}/execute`, {});
  }

  listPlanningImportAliases(filters: Record<string, string> = {}) {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<any[]>(`${this.baseUrl}/planning/import-aliases`, { params });
  }

  getPlanningImportAliasCatalog() {
    return this.http.get<any>(`${this.baseUrl}/planning/import-aliases/catalog`);
  }

  createPlanningImportAlias(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/import-aliases`, payload);
  }

  updatePlanningImportAlias(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/import-aliases/${id}`, payload);
  }

  listPlanningPlanRules(semesterId?: string, campusId?: string, academicProgramId?: string) {
    let params = new HttpParams();
    if (semesterId) {
      params = params.set('semester_id', semesterId);
    }
    if (campusId) {
      params = params.set('campus_id', campusId);
    }
    if (academicProgramId) {
      params = params.set('academic_program_id', academicProgramId);
    }
    return this.http.get<any[]>(`${this.baseUrl}/planning/plan-rules`, { params });
  }

  listPlanningConfiguredCycles(filters: Record<string, string>) {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<any[]>(`${this.baseUrl}/planning/configured-cycles`, { params });
  }

  createPlanningPlanRule(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/plan-rules`, payload);
  }

  updatePlanningPlanRule(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/plan-rules/${id}`, payload);
  }

  deletePlanningPlanRule(id: string) {
    return this.http.delete<any>(`${this.baseUrl}/planning/plan-rules/${id}`);
  }

  submitPlanningPlanRuleReview(id: string, payload: any = {}) {
    return this.http.post<any>(`${this.baseUrl}/planning/plan-rules/${id}/submit-review`, payload);
  }

  submitPlanningPlanRulesReviewBulk(payload: any = {}) {
    return this.http.post<any>(`${this.baseUrl}/planning/plan-rules/submit-review-bulk`, payload);
  }

  approvePlanningPlanRule(id: string, payload: any = {}) {
    return this.http.post<any>(`${this.baseUrl}/planning/plan-rules/${id}/approve`, payload);
  }

  requestPlanningPlanRuleCorrection(id: string, payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/plan-rules/${id}/request-correction`, payload);
  }

  listPlanningCourseCandidates(filters: Record<string, string>) {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<any>(`${this.baseUrl}/planning/course-candidates`, { params });
  }

  listPlanningOffers(filters: Record<string, string>) {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<any[]>(`${this.baseUrl}/planning/offers`, { params });
  }

  createPlanningOffer(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/offers`, payload);
  }

  getPlanningOffer(id: string) {
    return this.http.get<any>(`${this.baseUrl}/planning/offers/${id}`);
  }

  updatePlanningOffer(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/offers/${id}`, payload);
  }

  listPlanningVcMatchRows(filters: Record<string, string>) {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<any[]>(`${this.baseUrl}/planning/vc-match`, { params });
  }

  recalculatePlanningVcMatches(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/vc-match/recalculate`, payload);
  }

  updatePlanningSubsectionVcMatch(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/subsections/${id}/vc-match`, payload);
  }

  updatePlanningCampusVcLocation(campusId: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/campus-vc-locations/${campusId}`, payload);
  }

  createPlanningSection(offerId: string, payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/offers/${offerId}/sections`, payload);
  }

  getPlanningSection(id: string) {
    return this.http.get<any>(`${this.baseUrl}/planning/sections/${id}`);
  }

  updatePlanningSection(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/sections/${id}`, payload);
  }

  deletePlanningSection(id: string) {
    return this.http.delete<any>(`${this.baseUrl}/planning/sections/${id}`);
  }

  createPlanningSubsection(sectionId: string, payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/sections/${sectionId}/subsections`, payload);
  }

  getPlanningSubsection(id: string) {
    return this.http.get<any>(`${this.baseUrl}/planning/subsections/${id}`);
  }

  updatePlanningSubsection(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/subsections/${id}`, payload);
  }

  createPlanningSubsectionSchedule(subsectionId: string, payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/subsections/${subsectionId}/schedules`, payload);
  }

  updatePlanningSubsectionSchedule(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/planning/subsection-schedules/${id}`, payload);
  }

  deletePlanningSubsectionSchedule(id: string) {
    return this.http.delete<any>(`${this.baseUrl}/planning/subsection-schedules/${id}`);
  }

  listPlanningConflicts(semesterId?: string, offerId?: string) {
    let params = new HttpParams();
    if (semesterId) {
      params = params.set('semester_id', semesterId);
    }
    if (offerId) {
      params = params.set('offer_id', offerId);
    }
    return this.http.get<any[]>(`${this.baseUrl}/planning/conflicts`, { params });
  }

  listPlanningChangeLog(filters: Record<string, string | number | undefined> = {}) {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== undefined && value !== null && `${value}` !== '') {
        params = params.set(key, String(value));
      }
    });
    return this.http.get<any[]>(`${this.baseUrl}/planning/change-log`, { params });
  }

  getPlanningWorkspace(filters: Record<string, string>) {
    let params = new HttpParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        params = params.set(key, value);
      }
    });
    return this.http.get<any>(`${this.baseUrl}/planning/workspace`, { params });
  }

  updatePlanningWorkspaceRow(rowId: string, payload: Record<string, unknown>) {
    return this.http.patch<any>(`${this.baseUrl}/planning/workspace/rows/${encodeURIComponent(rowId)}`, payload);
  }

  bulkAssignPlanningTeacher(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/workspace/bulk-assign-teacher`, payload);
  }

  bulkAssignPlanningClassroom(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/workspace/bulk-assign-classroom`, payload);
  }

  bulkDuplicatePlanning(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/workspace/bulk-duplicate`, payload);
  }

  getClassOffering(id: string) {
    return this.http.get<any>(`${this.baseUrl}/planning/class-offerings/${id}`);
  }

  createClassOffering(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/class-offerings`, payload);
  }

  listClassGroups(offeringId: string) {
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-groups`, { params: { class_offering_id: offeringId } });
  }

  createClassGroup(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/class-groups`, payload);
  }

  listClassMeetings(offeringId: string) {
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-meetings`, { params: { class_offering_id: offeringId } });
  }

  createClassMeeting(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/class-meetings`, payload);
  }

  deleteClassMeeting(id: string) {
    return this.http.delete<any>(`${this.baseUrl}/planning/class-meetings/${id}`);
  }

  validateHours(offeringId: string) {
    return this.http.post<any>(`${this.baseUrl}/planning/hours-validation/${offeringId}`, {});
  }

  // --- Teachers ---
  listClassGroupTeachers(groupId?: string) {
    const params: any = {};
    if (groupId) params.class_group_id = groupId;
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-group-teachers`, { params });
  }

  assignTeacherToGroup(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/planning/class-group-teachers`, payload);
  }

  removeTeacherFromGroup(id: string) {
    return this.http.delete<any>(`${this.baseUrl}/planning/class-group-teachers/${id}`);
  }

  listClassTeachers(classOfferingId?: string) {
    const params = classOfferingId
      ? new HttpParams().set('class_offering_id', classOfferingId)
      : undefined;
    return this.http.get<any[]>(`${this.baseUrl}/planning/class-teachers`, { params });
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

  // --- Auth Admin ---
  listAdminUsers() {
    return this.http.get<any[]>(`${this.baseUrl}/auth/admin/users`);
  }

  createAdminUser(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/auth/admin/users`, payload);
  }

  updateAdminUser(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/auth/admin/users/${id}`, payload);
  }

  replaceAdminUserAssignments(id: string, payload: any) {
    return this.http.put<any>(`${this.baseUrl}/auth/admin/users/${id}/assignments`, payload);
  }

  listAdminRoles() {
    return this.http.get<any[]>(`${this.baseUrl}/auth/admin/roles`);
  }

  createAdminRole(payload: any) {
    return this.http.post<any>(`${this.baseUrl}/auth/admin/roles`, payload);
  }

  updateAdminRole(id: string, payload: any) {
    return this.http.patch<any>(`${this.baseUrl}/auth/admin/roles/${id}`, payload);
  }

  replaceRolePermissions(id: string, payload: any) {
    return this.http.put<any>(`${this.baseUrl}/auth/admin/roles/${id}/permissions`, payload);
  }

  listAdminPermissions() {
    return this.http.get<any[]>(`${this.baseUrl}/auth/admin/permissions`);
  }

  getAdminScopeCatalog() {
    return this.http.get<any>(`${this.baseUrl}/auth/admin/scopes/catalog`);
  }
}
