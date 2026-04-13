import { Body, Controller, Delete, Get, Param, Patch, Post, Put } from '@nestjs/common';
import { ACTION_PERMISSIONS, WINDOW_PERMISSIONS } from '../auth/auth.constants';
import { RequirePermissions } from '../auth/permissions.decorator';
import {
    CreateZoomGroupDto,
    UpdateZoomConfigDto,
    UpdateZoomGroupDto,
    UpdateZoomPoolDto,
} from './videoconference.dto';
import { VideoconferenceService } from './videoconference.service';
import { ZoomAccountService } from './zoom-account.service';

@Controller('settings/zoom')
@RequirePermissions(WINDOW_PERMISSIONS.SETTINGS, ACTION_PERMISSIONS.SETTINGS_MANAGE)
export class ZoomSettingsController {
    constructor(
        private readonly videoconferenceService: VideoconferenceService,
        private readonly zoomAccountService: ZoomAccountService,
    ) { }

    @Get('config')
    async getConfig() {
        return this.zoomAccountService.getConfig();
    }

    @Put('config')
    async updateConfig(@Body() dto: UpdateZoomConfigDto) {
        return this.zoomAccountService.updateConfig(dto);
    }

    @Post('config/test')
    async testConfig() {
        return this.zoomAccountService.testConnection();
    }

    @Get('pool')
    async getPool() {
        return this.videoconferenceService.getZoomPool();
    }

    @Put('pool')
    async updatePool(@Body() dto: UpdateZoomPoolDto) {
        return this.videoconferenceService.replaceZoomPool(dto);
    }

    @Get('groups')
    async listGroups() {
        return this.videoconferenceService.listZoomGroups();
    }

    @Post('groups')
    async createGroup(@Body() dto: CreateZoomGroupDto) {
        return this.videoconferenceService.createZoomGroup(dto);
    }

    @Patch('groups/:id')
    async updateGroup(@Param('id') id: string, @Body() dto: UpdateZoomGroupDto) {
        return this.videoconferenceService.updateZoomGroup(id, dto);
    }

    @Delete('groups/:id')
    async deleteGroup(@Param('id') id: string) {
        return this.videoconferenceService.deleteZoomGroup(id);
    }

    @Get('groups/:id/pool')
    async getGroupPool(@Param('id') id: string) {
        return this.videoconferenceService.getZoomGroupPool(id);
    }

    @Put('groups/:id/pool')
    async updateGroupPool(@Param('id') id: string, @Body() dto: UpdateZoomPoolDto) {
        return this.videoconferenceService.replaceZoomGroupPool(id, dto);
    }
}
