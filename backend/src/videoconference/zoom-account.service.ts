import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { newId } from '../common';
import { UpdateZoomConfigDto } from './videoconference.dto';
import { ZoomConfigEntity } from './videoconference.entity';

const ZOOM_TOKEN_URL = 'https://zoom.us/oauth/token';
const ZOOM_API_BASE_URL = 'https://api.zoom.us/v2';
const TOKEN_EXPIRY_SAFETY_MS = 30_000;

type ZoomTokenResponse = {
    access_token?: string;
    expires_in?: number;
};

type ZoomUsersResponse = {
    users?: unknown[];
};

type ZoomMeetingsResponse = {
    meetings?: unknown[];
    next_page_token?: string;
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
                    meetings.push(meeting);
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

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                authorization: `Basic ${authorization}`,
                'content-type': 'application/x-www-form-urlencoded',
            },
            body: '',
        });

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
        const response = await fetch(`${ZOOM_API_BASE_URL}${path}`, {
            method: 'GET',
            headers: {
                authorization: `Bearer ${token}`,
                accept: 'application/json',
            },
        });

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
        };
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
}
