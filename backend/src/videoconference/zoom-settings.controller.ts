import { Body, Controller, Get, Post, Put } from '@nestjs/common';
import { ACTION_PERMISSIONS, WINDOW_PERMISSIONS } from '../auth/auth.constants';
import { RequirePermissions } from '../auth/permissions.decorator';
import { UpdateZoomConfigDto, UpdateZoomPoolDto } from './videoconference.dto';
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
}
