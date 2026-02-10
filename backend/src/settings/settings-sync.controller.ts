import { Body, Controller, Get, Param, Post, Put, Query } from '@nestjs/common';
import { RunSettingsSyncDto, UpsertSourceSessionDto } from './dto/settings-sync.dto';
import { SettingsSyncService } from './settings-sync.service';

@Controller('settings/sync')
export class SettingsSyncController {
  constructor(private readonly settingsSyncService: SettingsSyncService) {}

  @Get('resources')
  listResources() {
    return this.settingsSyncService.listResources();
  }

  @Get('sources')
  listSources(@Query('probe') probe?: string) {
    return this.settingsSyncService.listSources(probe === 'true' || probe === '1');
  }

  @Get('sources/:code/session-cookie')
  getSourceSessionCookie(@Param('code') code: string) {
    return this.settingsSyncService.getSourceSessionCookie(code);
  }

  @Put('sources/:code/session-cookie')
  upsertSourceSession(@Param('code') code: string, @Body() dto: UpsertSourceSessionDto) {
    return this.settingsSyncService.upsertSourceSession(code, dto);
  }

  @Post('sources/:code/validate-session')
  validateSourceSession(@Param('code') code: string) {
    return this.settingsSyncService.validateSourceSession(code);
  }

  @Post('validate-all-sessions')
  validateAllSourceSessions() {
    return this.settingsSyncService.validateAllSourceSessions();
  }

  @Post('run')
  runSync(@Body() dto: RunSettingsSyncDto) {
    return this.settingsSyncService.runSync(dto);
  }

  @Get('jobs')
  listJobs(@Query('limit') limit?: string) {
    const parsed = Number(limit);
    return this.settingsSyncService.listJobs(Number.isFinite(parsed) ? parsed : 20);
  }
}
