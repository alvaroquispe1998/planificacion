import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { API_BASE_URL } from '../core/api-base';

const BASE = `${API_BASE_URL}/videoconference-creator`;

export type ManualMeetingType = 'UNIQUE' | 'WEEKLY';
export type ManualMeetingStatus =
    | 'CREATED'
    | 'DRAFT_NO_HOST'
    | 'APPROVED_WITH_BACKUP'
    | 'ERROR'
    | 'CANCELLED';

export interface ManualMeeting {
    id: string;
    created_by_user_id: string;
    zoom_group_id: string;
    assigned_zoom_user_id: string | null;
    backup_zoom_user_id: string | null;
    type: ManualMeetingType;
    topic: string;
    agenda: string | null;
    start_time: string;
    end_time: string;
    duration_minutes: number;
    recurrence_json: Record<string, unknown> | null;
    status: ManualMeetingStatus;
    zoom_meeting_id: string | null;
    join_url: string | null;
    start_url: string | null;
    error_message: string | null;
    created_at: string;
    updated_at: string;
}

export interface ZoomGroupSummary {
    id: string;
    name: string;
    code: string;
}

export interface CreatorProfile {
    can_view: boolean;
    can_create_unique: boolean;
    can_create_weekly: boolean;
    can_approve_backup: boolean;
    assigned_groups: ZoomGroupSummary[];
}

export interface CreateMeetingDto {
    type: ManualMeetingType;
    zoom_group_id: string;
    topic: string;
    agenda?: string;
    start_time: string;
    duration_minutes: number;
    recurrence_end_date?: string;
    recurrence_weekly_days?: string;
}

export interface MeetingDetail {
    meeting: ManualMeeting;
    instances: unknown[];
    participants: unknown[];
}

export interface ApproveDraftDto {
    override_backup_zoom_user_id?: string;
}

@Injectable({ providedIn: 'root' })
export class VideoconferenceCreatorApiService {
    constructor(private readonly http: HttpClient) { }

    getProfile() {
        return this.http.get<CreatorProfile>(`${BASE}/profile`);
    }

    listMeetings() {
        return this.http.get<ManualMeeting[]>(`${BASE}/meetings`);
    }

    createMeeting(dto: CreateMeetingDto) {
        return this.http.post<ManualMeeting>(`${BASE}/meetings`, dto);
    }

    getMeeting(id: string) {
        return this.http.get<MeetingDetail>(`${BASE}/meetings/${id}`);
    }

    syncMeeting(id: string) {
        return this.http.post<{ synced_instances: number }>(`${BASE}/meetings/${id}/sync`, {});
    }

    approveDraft(id: string, dto: ApproveDraftDto = {}) {
        return this.http.post<ManualMeeting>(`${BASE}/meetings/${id}/approve-backup`, dto);
    }

    listDrafts() {
        return this.http.get<ManualMeeting[]>(`${BASE}/drafts`);
    }

    getUserZoomGroups(userId: string) {
        return this.http.get<unknown[]>(`${BASE}/security/users/${userId}/zoom-groups`);
    }

    setUserZoomGroups(userId: string, groupIds: string[]) {
        return this.http.post<unknown[]>(`${BASE}/security/users/${userId}/zoom-groups`, {
            zoom_group_ids: groupIds,
        });
    }
}
