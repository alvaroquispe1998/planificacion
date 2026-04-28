import { Controller, Get, Query } from '@nestjs/common';
import { WINDOW_PERMISSIONS } from '../auth/auth.constants';
import { RequirePermissions } from '../auth/permissions.decorator';
import { VideoconferenceDashboardService } from './videoconference-dashboard.service';
import { CoverageDimension } from './videoconference-dashboard.dto';

@Controller('videoconference/dashboard')
@RequirePermissions(WINDOW_PERMISSIONS.VIDEOCONFERENCES)
export class VideoconferenceDashboardController {
    constructor(private readonly service: VideoconferenceDashboardService) { }

    @Get('today/summary')
    async getTodaySummary(@Query('date') date?: string) {
        return this.service.getTodaySummary(date);
    }

    @Get('today/upcoming')
    async getTodayUpcoming(
        @Query('date') date?: string,
        @Query('withinMinutes') withinMinutes?: string,
    ) {
        const minutes = withinMinutes ? Number(withinMinutes) : undefined;
        return this.service.getTodayUpcoming(date, minutes);
    }

    @Get('today/errors')
    async getTodayErrors(
        @Query('date') date?: string,
        @Query('limit') limit?: string,
    ) {
        const lim = limit ? Number(limit) : undefined;
        return this.service.getTodayErrors(date, lim);
    }

    @Get('today/host-utilization')
    async getTodayHostUtilization(@Query('date') date?: string) {
        return this.service.getTodayHostUtilization(date);
    }

    // ----- Period coverage -----

    @Get('coverage/summary')
    async getCoverageSummary(@Query('periodId') periodId?: string) {
        return this.service.getCoverageSummary(periodId);
    }

    @Get('coverage/by-dimension')
    async getCoverageByDimension(
        @Query('periodId') periodId?: string,
        @Query('dimension') dimension?: string,
    ) {
        const dim = (dimension as CoverageDimension) || 'faculty';
        return this.service.getCoverageByDimension(periodId, dim);
    }

    @Get('coverage/missing')
    async getCoverageMissing(
        @Query('periodId') periodId?: string,
        @Query('limit') limit?: string,
    ) {
        const lim = limit ? Number(limit) : undefined;
        return this.service.getCoverageMissingSchedules(periodId, lim);
    }

    @Get('coverage/overrides')
    async getCoverageOverrides(@Query('periodId') periodId?: string) {
        return this.service.getCoverageOverrides(periodId);
    }

    @Get('coverage/daily')
    async getCoverageDaily(@Query('periodId') periodId?: string) {
        return this.service.getCoverageDailySeries(periodId);
    }

    @Get('coverage/conflicts')
    async getCoverageConflicts(@Query('periodId') periodId?: string) {
        return this.service.getCoverageConflicts(periodId);
    }
}
