import {
    Body,
    Controller,
    Get,
    Param,
    Post,
    UseGuards,
} from '@nestjs/common';
import { AccessTokenGuard } from '../auth/access-token.guard';
import { CurrentAuthUser } from '../auth/current-auth-user.decorator';
import { RequirePermissions } from '../auth/permissions.decorator';
import { PermissionGuard } from '../auth/permission.guard';
import type { AuthenticatedRequestUser } from '../auth/auth.service';
import {
    ApproveDraftBackupDto,
    CreateManualVideoconferenceDto,
    SetUserZoomGroupsDto,
} from './videoconference-creator.dto';
import { VideoconferenceCreatorService } from './videoconference-creator.service';

@UseGuards(AccessTokenGuard, PermissionGuard)
@Controller('videoconference-creator')
export class VideoconferenceCreatorController {
    constructor(private readonly service: VideoconferenceCreatorService) { }

    /**
     * Returns the current user's permissions and assigned Zoom groups.
     */
    @Get('profile')
    @RequirePermissions('action.videoconference_creator.view')
    async getProfile(@CurrentAuthUser() user: AuthenticatedRequestUser) {
        return this.service.getProfile(user.id, user.permissions, user.is_admin);
    }

    /**
     * Lists meetings. Admin/TI sees all; regular users see only their own.
     */
    @Get('meetings')
    @RequirePermissions('action.videoconference_creator.view')
    async listMeetings(@CurrentAuthUser() user: AuthenticatedRequestUser) {
        const isAdminOrTI = user.is_admin || user.roles.some((r) => r.code === 'IT_SUPPORT');
        return this.service.listMeetings(user.id, isAdminOrTI);
    }

    /**
     * Creates a unique or weekly meeting.
     * Requires either create_unique or create_weekly permission depending on dto.type.
     */
    @Post('meetings')
    async createMeeting(
        @CurrentAuthUser() user: AuthenticatedRequestUser,
        @Body() dto: CreateManualVideoconferenceDto,
    ) {
        const requiredPermission =
            dto.type === 'WEEKLY'
                ? 'action.videoconference_creator.create_weekly'
                : 'action.videoconference_creator.create_unique';

        if (!user.is_admin && !user.permissions.includes(requiredPermission)) {
            const { ForbiddenException } = await import('@nestjs/common');
            throw new ForbiddenException(`Se requiere el permiso: ${requiredPermission}`);
        }

        return this.service.createMeeting(user.id, dto);
    }

    /**
     * Detail of a single meeting with instances and participants.
     */
    @Get('meetings/:id')
    @RequirePermissions('action.videoconference_creator.view')
    async getMeeting(
        @Param('id') id: string,
        @CurrentAuthUser() user: AuthenticatedRequestUser,
    ) {
        const isAdminOrTI = user.is_admin || user.roles.some((r) => r.code === 'IT_SUPPORT');
        return this.service.getMeeting(id, user.id, isAdminOrTI);
    }

    /**
     * Syncs past instances from Zoom for a given meeting.
     */
    @Post('meetings/:id/sync')
    @RequirePermissions('action.videoconference_creator.view')
    async syncMeeting(
        @Param('id') id: string,
        @CurrentAuthUser() user: AuthenticatedRequestUser,
    ) {
        const isAdminOrTI = user.is_admin || user.roles.some((r) => r.code === 'IT_SUPPORT');
        return this.service.syncFromZoom(id, user.id, isAdminOrTI);
    }

    /**
     * Approves a DRAFT_NO_HOST meeting using the group backup or an override host.
     * Requires approve_backup permission.
     */
    @Post('meetings/:id/approve-backup')
    @RequirePermissions('action.videoconference_creator.approve_backup')
    async approveDraftBackup(
        @Param('id') id: string,
        @Body() dto: ApproveDraftBackupDto,
    ) {
        return this.service.approveDraftWithBackup(id, dto);
    }

    /**
     * Lists DRAFT_NO_HOST meetings for TI/admin review.
     */
    @Get('drafts')
    @RequirePermissions('action.videoconference_creator.approve_backup')
    async listDrafts() {
        return this.service.listDrafts();
    }

    // ── Security panel: user ↔ Zoom group assignments ─────────────────────────

    @Get('security/users/:userId/zoom-groups')
    @RequirePermissions('action.users.manage')
    async getUserZoomGroups(@Param('userId') userId: string) {
        return this.service.getUserZoomGroups(userId);
    }

    @Post('security/users/:userId/zoom-groups')
    @RequirePermissions('action.users.manage')
    async setUserZoomGroups(
        @Param('userId') userId: string,
        @Body() dto: SetUserZoomGroupsDto,
    ) {
        return this.service.setUserZoomGroups(userId, dto);
    }
}
