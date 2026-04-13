import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { newId } from '../common';
import { UpdateZoomConfigDto } from './videoconference.dto';
import { ZoomConfigEntity } from './videoconference.entity';

const ZOOM_TOKEN_URL = 'https://zoom.us/oauth/token';
const ZOOM_API_BASE_URL = 'https://api.zoom.us/v2';
const TOKEN_EXPIRY_SAFETY_MS = 30_000;
const ZOOM_REQUEST_TIMEOUT_MS = 15_000;

type ZoomTokenResponse = {
    access_token?: string;
    expires_in?: number;
};

type ZoomUsersResponse = {
    users?: unknown[];
    next_page_token?: string;
};

export type ZoomAccountUserLicenseStatus = 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN';

export type ZoomAccountUserSummary = {
    id: string;
    name: string | null;
    email: string | null;
    type_code: number | null;
    license_status: ZoomAccountUserLicenseStatus;
    is_licensed: boolean;
    status: string | null;
};

type ZoomMeetingsResponse = {
    meetings?: unknown[];
    next_page_token?: string;
};

type ZoomPastMeetingInstancesResponse = {
    meetings?: unknown[];
};

type ZoomPastMeetingParticipantsResponse = {
    participants?: unknown[];
    next_page_token?: string;
};

type ZoomMeetingRecordingsResponse = {
    recording_files?: unknown[];
};

export type ZoomMeetingSummary = {
    id: string;
    topic: string | null;
    start_time: string | null;
    duration_minutes: number | null;
    status: string | null;
    join_url: string | null;
    start_url: string | null;
    host_email: string | null;
    /** Indicates whether this meeting was fetched from the live or upcoming list */
    source_type: 'live' | 'upcoming';
};

export type ZoomPastMeetingInstanceSummary = {
    uuid: string;
    id: string | null;
    topic: string | null;
    start_time: string | null;
    duration_minutes: number | null;
    raw: Record<string, unknown>;
};

export type ZoomPastMeetingDetail = {
    uuid: string;
    id: string | null;
    topic: string | null;
    start_time: string | null;
    end_time: string | null;
    duration_minutes: number | null;
    status: string | null;
    host_email: string | null;
    raw: Record<string, unknown>;
};

export type ZoomPastMeetingParticipant = {
    zoom_participant_id: string | null;
    zoom_user_id: string | null;
    display_name: string;
    email: string | null;
    role: string | null;
    join_time: string | null;
    leave_time: string | null;
    duration_minutes: number | null;
    raw: Record<string, unknown>;
};

export type ZoomMeetingRecordingFile = {
    zoom_recording_id: string | null;
    recording_type: string | null;
    file_extension: string | null;
    file_size_bytes: string | null;
    download_url: string | null;
    play_url: string | null;
    start_time: string | null;
    end_time: string | null;
    status: string | null;
    raw: Record<string, unknown>;
};

@Injectable()
export class ZoomAccountService {
    private cachedAccessToken: { value: string; expiresAt: number } | null = null;

    constructor(
        @InjectRepository(ZoomConfigEntity)
        private readonly zoomConfigRepo: Repository<ZoomConfigEntity>,
    ) { }

    async getConfig() {
        const config = await this.findConfigEntity();
        if (config) {
            return config;
        }

        return {
            id: '',
            accountId: '',
            clientId: '',
            clientSecret: '',
            maxConcurrent: 2,
            pageSize: 20,
            timezone: 'America/Lima',
            created_at: null,
            updated_at: null,
        };
    }

    async updateConfig(dto: UpdateZoomConfigDto) {
        const current = await this.findConfigEntity();
        const now = new Date();

        const entity = current
            ? this.zoomConfigRepo.create({
                ...current,
                accountId: dto.accountId !== undefined ? dto.accountId.trim() : current.accountId,
                clientId: dto.clientId !== undefined ? dto.clientId.trim() : current.clientId,
                clientSecret:
                    dto.clientSecret !== undefined ? dto.clientSecret.trim() : current.clientSecret,
                maxConcurrent: dto.maxConcurrent ?? current.maxConcurrent,
                pageSize: dto.pageSize ?? current.pageSize,
                timezone: dto.timezone !== undefined ? dto.timezone.trim() : current.timezone,
                updated_at: now,
            })
            : this.zoomConfigRepo.create({
                id: newId(),
                accountId: dto.accountId?.trim() ?? '',
                clientId: dto.clientId?.trim() ?? '',
                clientSecret: dto.clientSecret?.trim() ?? '',
                maxConcurrent: dto.maxConcurrent ?? 2,
                pageSize: dto.pageSize ?? 20,
                timezone: dto.timezone?.trim() ?? 'America/Lima',
                created_at: now,
                updated_at: now,
            });

        this.cachedAccessToken = null;
        return this.zoomConfigRepo.save(entity);
    }

    async testConnection() {
        try {
            const config = await this.requireConfiguredConfig();
            const response = await this.fetchZoomJson<ZoomUsersResponse>('/users?page_size=1');
            return {
                ok: true,
                accountId: config.accountId,
                userCount: Array.isArray(response.users) ? response.users.length : 0,
            };
        } catch (error) {
            return {
                ok: false,
                reason: this.toErrorMessage(error),
            };
        }
    }

    async listAccountUsers() {
        const config = await this.requireConfiguredConfig();
        const pageSize = Math.max(1, Math.min(300, config.pageSize || 20));
        const users: ZoomAccountUserSummary[] = [];
        let nextPageToken = '';

        do {
            const query = new URLSearchParams({
                page_size: String(pageSize),
                status: 'active',
            });
            if (nextPageToken) {
                query.set('next_page_token', nextPageToken);
            }

            const response = await this.fetchZoomJson<ZoomUsersResponse>(`/users?${query.toString()}`);
            const rows = Array.isArray(response.users) ? response.users : [];
            for (const row of rows) {
                const user = this.mapAccountUser(row);
                if (user) {
                    users.push(user);
                }
            }
            nextPageToken =
                typeof response.next_page_token === 'string' ? response.next_page_token : '';
        } while (nextPageToken);

        return users;
    }

    async requireConfiguredConfig() {
        const config = await this.findConfigEntity();
        if (!config) {
            throw new BadRequestException('No existe configuracion Zoom registrada.');
        }

        if (!config.accountId.trim() || !config.clientId.trim() || !config.clientSecret.trim()) {
            throw new BadRequestException(
                'La configuracion Zoom esta incompleta. Registra accountId, clientId y clientSecret.',
            );
        }

        return config;
    }

    async listUserMeetings(userEmail: string, type: 'live' | 'upcoming') {
        const config = await this.requireConfiguredConfig();
        const pageSize = Math.max(1, Math.min(300, config.pageSize || 20));
        const meetings: ZoomMeetingSummary[] = [];
        let nextPageToken = '';

        do {
            const query = new URLSearchParams({
                type,
                page_size: String(pageSize),
            });
            if (nextPageToken) {
                query.set('next_page_token', nextPageToken);
            }

            const response = await this.fetchZoomJson<ZoomMeetingsResponse>(
                `/users/${encodeURIComponent(userEmail)}/meetings?${query.toString()}`,
            );
            const rows = Array.isArray(response.meetings) ? response.meetings : [];
            for (const row of rows) {
                const meeting = this.mapMeetingSummary(row);
                if (meeting) {
                    meetings.push({ ...meeting, source_type: type });
                }
            }

            nextPageToken =
                typeof response.next_page_token === 'string' ? response.next_page_token : '';
        } while (nextPageToken);

        return meetings;
    }

    async listUserMeetingsByTypes(userEmail: string, types: Array<'live' | 'upcoming'>) {
        const uniqueTypes = Array.from(new Set(types));
        const groups = await Promise.all(uniqueTypes.map((type) => this.listUserMeetings(userEmail, type)));
        return groups.flat();
    }

    async listPastMeetingInstances(meetingId: string) {
        const response = await this.fetchZoomJsonOrNull<ZoomPastMeetingInstancesResponse>(
            `/past_meetings/${this.encodeZoomIdPathSegment(meetingId)}/instances`,
        );
        const rows = Array.isArray(response?.meetings) ? response.meetings : [];
        return rows
            .map((row) => this.mapPastMeetingInstance(row))
            .filter((item): item is ZoomPastMeetingInstanceSummary => Boolean(item));
    }


    async getMeeting(meetingId: string) {
        const response = await this.fetchZoomJsonOrNull<Record<string, unknown>>(
            `/meetings/${this.encodeZoomIdPathSegment(meetingId)}`,
        );
        if (!response) {
            return null;
        }
        return this.mapMeetingSummary(response);
    }

    async getPastMeetingDetail(meetingUuid: string) {
        const response = await this.fetchZoomJsonOrNull<Record<string, unknown>>(
            `/past_meetings/${this.encodeZoomIdPathSegment(meetingUuid)}`,
        );
        if (!response) {
            return null;
        }
        return this.mapPastMeetingDetail(response);
    }

    async listPastMeetingParticipants(meetingUuid: string) {
        const config = await this.requireConfiguredConfig();
        const pageSize = Math.max(1, Math.min(300, config.pageSize || 20));
        const items: ZoomPastMeetingParticipant[] = [];
        let nextPageToken = '';

        do {
            const query = new URLSearchParams({
                page_size: String(pageSize),
            });
            if (nextPageToken) {
                query.set('next_page_token', nextPageToken);
            }
            const response = await this.fetchZoomJsonOrNull<ZoomPastMeetingParticipantsResponse>(
                `/past_meetings/${this.encodeZoomIdPathSegment(meetingUuid)}/participants?${query.toString()}`,
            );
            if (!response) {
                return [];
            }
            const rows = Array.isArray(response.participants) ? response.participants : [];
            for (const row of rows) {
                const participant = this.mapPastMeetingParticipant(row);
                if (participant) {
                    items.push(participant);
                }
            }
            nextPageToken =
                typeof response.next_page_token === 'string' ? response.next_page_token : '';
        } while (nextPageToken);

        return items;
    }

    async listMeetingRecordings(meetingIdOrUuid: string) {
        const response = await this.fetchZoomJsonOrNull<ZoomMeetingRecordingsResponse>(
            `/meetings/${this.encodeZoomIdPathSegment(meetingIdOrUuid)}/recordings`,
        );
        const rows = Array.isArray(response?.recording_files) ? response.recording_files : [];
        return rows
            .map((row) => this.mapMeetingRecording(row))
            .filter((item): item is ZoomMeetingRecordingFile => Boolean(item));
    }

    private async findConfigEntity() {
        const rows = await this.zoomConfigRepo.find({
            order: { created_at: 'ASC' },
            take: 1,
        });
        return rows[0] ?? null;
    }

    private async getAccessToken() {
        const now = Date.now();
        if (this.cachedAccessToken && this.cachedAccessToken.expiresAt > now + TOKEN_EXPIRY_SAFETY_MS) {
            return this.cachedAccessToken.value;
        }

        const config = await this.requireConfiguredConfig();
        const authorization = Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64');
        const url = `${ZOOM_TOKEN_URL}?grant_type=account_credentials&account_id=${encodeURIComponent(
            config.accountId,
        )}`;

        const response = await this.fetchWithDiagnostics(
            url,
            {
                method: 'POST',
                headers: {
                    authorization: `Basic ${authorization}`,
                    'content-type': 'application/x-www-form-urlencoded',
                },
                body: '',
            },
            'Zoom OAuth',
        );

        const bodyText = await response.text();
        const parsed = this.parseJson<ZoomTokenResponse>(bodyText);
        if (!response.ok) {
            throw new BadRequestException(
                `Zoom OAuth rechazo la solicitud (${response.status}): ${bodyText.slice(0, 300)}`,
            );
        }

        if (!parsed?.access_token) {
            throw new BadRequestException('Zoom OAuth no devolvio access_token.');
        }

        const expiresIn = Number(parsed.expires_in);
        this.cachedAccessToken = {
            value: parsed.access_token,
            expiresAt: now + (Number.isFinite(expiresIn) ? expiresIn * 1000 : 3600 * 1000),
        };

        return parsed.access_token;
    }

    private async fetchZoomJson<T>(path: string, retryOnUnauthorized = true): Promise<T> {
        const token = await this.getAccessToken();
        const response = await this.fetchWithDiagnostics(
            `${ZOOM_API_BASE_URL}${path}`,
            {
                method: 'GET',
                headers: {
                    authorization: `Bearer ${token}`,
                    accept: 'application/json',
                },
            },
            'Zoom API',
        );

        if (response.status === 401 && retryOnUnauthorized) {
            this.cachedAccessToken = null;
            return this.fetchZoomJson<T>(path, false);
        }

        const bodyText = await response.text();
        if (!response.ok) {
            throw new BadRequestException(
                `Zoom API devolvio ${response.status}: ${bodyText.slice(0, 300)}`,
            );
        }

        return this.parseJson<T>(bodyText) ?? ({} as T);
    }

    private async fetchZoomJsonOrNull<T>(path: string) {
        try {
            return await this.fetchZoomJson<T>(path);
        } catch (error) {
            const message = this.toErrorMessage(error);
            if (message.includes('Zoom API devolvio 404') || message.includes('"code":3301')) {
                return null;
            }
            throw error;
        }
    }

    private async fetchWithDiagnostics(url: string, init: RequestInit, label: string) {
        try {
            return await fetch(url, {
                ...init,
                signal: AbortSignal.timeout(ZOOM_REQUEST_TIMEOUT_MS),
            });
        } catch (error) {
            throw new BadRequestException(this.describeFetchError(label, url, error));
        }
    }

    private mapMeetingSummary(value: unknown): ZoomMeetingSummary | null {
        if (!value || typeof value !== 'object') {
            return null;
        }

        const record = value as Record<string, unknown>;
        const id = record.id !== undefined && record.id !== null ? String(record.id) : '';
        if (!id) {
            return null;
        }

        return {
            id,
            topic: typeof record.topic === 'string' ? record.topic : null,
            start_time: typeof record.start_time === 'string' ? record.start_time : null,
            duration_minutes:
                typeof record.duration === 'number'
                    ? record.duration
                    : Number.isFinite(Number(record.duration))
                        ? Number(record.duration)
                        : null,
            status: typeof record.status === 'string' ? record.status : null,
            join_url: typeof record.join_url === 'string' ? record.join_url : null,
            start_url: typeof record.start_url === 'string' ? record.start_url : null,
            host_email: typeof record.host_email === 'string' ? record.host_email : null,
            // source_type is set by the caller (listUserMeetings) who knows the fetch context
            source_type: 'upcoming' as 'live' | 'upcoming',
        };
    }

    private mapAccountUser(value: unknown): ZoomAccountUserSummary | null {
        if (!value || typeof value !== 'object') {
            return null;
        }

        const record = value as Record<string, unknown>;
        const id = record.id !== undefined && record.id !== null ? String(record.id).trim() : '';
        if (!id) {
            return null;
        }

        const firstName = typeof record.first_name === 'string' ? record.first_name.trim() : '';
        const lastName = typeof record.last_name === 'string' ? record.last_name.trim() : '';
        const displayName = typeof record.display_name === 'string' ? record.display_name.trim() : '';
        const name = displayName || [firstName, lastName].filter(Boolean).join(' ') || null;
        const typeCode = Number.isFinite(Number(record.type)) ? Number(record.type) : null;
        const licenseStatus = mapZoomLicenseStatus(typeCode);

        return {
            id,
            name,
            email: typeof record.email === 'string' ? record.email.trim() || null : null,
            type_code: typeCode,
            license_status: licenseStatus,
            is_licensed: licenseStatus === 'LICENSED' || licenseStatus === 'ON_PREM',
            status: typeof record.status === 'string' ? record.status : null,
        };
    }

    private mapPastMeetingInstance(value: unknown): ZoomPastMeetingInstanceSummary | null {
        if (!value || typeof value !== 'object') {
            return null;
        }
        const record = value as Record<string, unknown>;
        const uuid = record.uuid !== undefined && record.uuid !== null ? String(record.uuid) : '';
        if (!uuid) {
            return null;
        }
        return {
            uuid,
            id: record.id !== undefined && record.id !== null ? String(record.id) : null,
            topic: typeof record.topic === 'string' ? record.topic : null,
            start_time: typeof record.start_time === 'string' ? record.start_time : null,
            duration_minutes:
                typeof record.duration === 'number'
                    ? record.duration
                    : Number.isFinite(Number(record.duration))
                        ? Number(record.duration)
                        : null,
            raw: record,
        };
    }

    private mapPastMeetingDetail(value: unknown): ZoomPastMeetingDetail | null {
        if (!value || typeof value !== 'object') {
            return null;
        }
        const record = value as Record<string, unknown>;
        const uuid = record.uuid !== undefined && record.uuid !== null ? String(record.uuid) : '';
        if (!uuid) {
            return null;
        }
        return {
            uuid,
            id: record.id !== undefined && record.id !== null ? String(record.id) : null,
            topic: typeof record.topic === 'string' ? record.topic : null,
            start_time: typeof record.start_time === 'string' ? record.start_time : null,
            end_time: typeof record.end_time === 'string' ? record.end_time : null,
            duration_minutes:
                typeof record.duration === 'number'
                    ? record.duration
                    : Number.isFinite(Number(record.duration))
                        ? Number(record.duration)
                        : null,
            status: typeof record.status === 'string' ? record.status : null,
            host_email: typeof record.host_email === 'string' ? record.host_email : null,
            raw: record,
        };
    }

    private mapPastMeetingParticipant(value: unknown): ZoomPastMeetingParticipant | null {
        if (!value || typeof value !== 'object') {
            return null;
        }
        const record = value as Record<string, unknown>;
        const displayName =
            typeof record.name === 'string'
                ? record.name.trim()
                : typeof record.user_name === 'string'
                    ? record.user_name.trim()
                    : '';
        if (!displayName) {
            return null;
        }
        return {
            zoom_participant_id:
                record.id !== undefined && record.id !== null ? String(record.id) : null,
            zoom_user_id:
                record.user_id !== undefined && record.user_id !== null ? String(record.user_id) : null,
            display_name: displayName,
            email: typeof record.user_email === 'string' ? record.user_email : typeof record.email === 'string' ? record.email : null,
            role: typeof record.role === 'string' ? record.role : null,
            join_time: typeof record.join_time === 'string' ? record.join_time : null,
            leave_time: typeof record.leave_time === 'string' ? record.leave_time : null,
            duration_minutes:
                typeof record.duration === 'number'
                    ? record.duration
                    : Number.isFinite(Number(record.duration))
                        ? Number(record.duration)
                        : null,
            raw: record,
        };
    }

    private mapMeetingRecording(value: unknown): ZoomMeetingRecordingFile | null {
        if (!value || typeof value !== 'object') {
            return null;
        }
        const record = value as Record<string, unknown>;
        return {
            zoom_recording_id:
                record.id !== undefined && record.id !== null ? String(record.id) : null,
            recording_type: typeof record.recording_type === 'string' ? record.recording_type : null,
            file_extension: typeof record.file_extension === 'string' ? record.file_extension : null,
            file_size_bytes:
                record.file_size !== undefined && record.file_size !== null ? String(record.file_size) : null,
            download_url: typeof record.download_url === 'string' ? record.download_url : null,
            play_url: typeof record.play_url === 'string' ? record.play_url : null,
            start_time: typeof record.recording_start === 'string' ? record.recording_start : typeof record.start_time === 'string' ? record.start_time : null,
            end_time: typeof record.recording_end === 'string' ? record.recording_end : typeof record.end_time === 'string' ? record.end_time : null,
            status: typeof record.status === 'string' ? record.status : null,
            raw: record,
        };
    }

    private encodeZoomIdPathSegment(value: string) {
        return encodeURIComponent(encodeURIComponent(value));
    }

    private parseJson<T>(value: string): T | null {
        if (!value.trim()) {
            return null;
        }

        try {
            return JSON.parse(value) as T;
        } catch {
            return null;
        }
    }

    private toErrorMessage(error: unknown) {
        if (error instanceof Error) {
            return error.message;
        }
        if (typeof error === 'string') {
            return error;
        }
        return 'Error no identificado';
    }

    private describeFetchError(label: string, url: string, error: unknown) {
        if (!(error instanceof Error)) {
            return `${label} no pudo conectarse a ${url}: error no identificado.`;
        }

        const cause = this.readErrorCause(error);
        const code = `${cause?.code ?? ''}`.trim().toUpperCase();
        const causeMessage = `${cause?.message ?? ''}`.trim();
        const baseMessage = `${error.message ?? ''}`.trim();
        const detail = causeMessage || baseMessage || 'sin detalle';

        if (error.name === 'TimeoutError' || code.includes('TIMEOUT') || code === 'ETIMEDOUT') {
            return `${label} no respondio antes del timeout de ${Math.round(ZOOM_REQUEST_TIMEOUT_MS / 1000)}s al conectar con ${url}.`;
        }
        if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') {
            return `${label} no pudo resolver DNS para ${url}: ${detail}.`;
        }
        if (code === 'ECONNREFUSED') {
            return `${label} rechazo la conexion hacia ${url}: ${detail}.`;
        }
        if (code.startsWith('CERT_') || baseMessage.toLowerCase().includes('certificate')) {
            return `${label} fallo por TLS/certificado al conectar con ${url}: ${detail}.`;
        }
        return `${label} no pudo conectarse a ${url}: ${detail}.`;
    }

    private readErrorCause(error: Error) {
        const cause = (error as Error & { cause?: unknown }).cause;
        if (!cause || typeof cause !== 'object') {
            return null;
        }
        const record = cause as Record<string, unknown>;
        return {
            code: typeof record.code === 'string' ? record.code : null,
            message: typeof record.message === 'string' ? record.message : null,
        };
    }
}

function mapZoomLicenseStatus(typeCode: number | null): ZoomAccountUserLicenseStatus {
    switch (typeCode) {
        case 2:
            return 'LICENSED';
        case 3:
            return 'ON_PREM';
        case 1:
            return 'BASIC';
        case 40:
            return 'LICENSED'; // Zoom Rooms
        default:
            return 'UNKNOWN';
    }
}
