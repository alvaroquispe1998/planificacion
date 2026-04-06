import { Controller, Get, Post, Body, Query, Delete, Param } from '@nestjs/common';
import { WINDOW_PERMISSIONS } from '../auth/auth.constants';
import { RequirePermissions } from '../auth/permissions.decorator';
import { VideoconferenceService } from './videoconference.service';
import {
    FilterOptionsDto,
    GenerateVideoconferenceDto,
    PreviewVideoconferenceDto,
    UpsertVideoconferenceOverrideDto,
} from './videoconference.dto';

@Controller('videoconference')
@RequirePermissions(WINDOW_PERMISSIONS.VIDEOCONFERENCES)
export class VideoconferenceController {
    constructor(private readonly service: VideoconferenceService) { }

    @Get('campuses')
    async getCampuses() {
        return this.service.getCampuses();
    }

    @Get('faculties')
    async getFaculties() {
        return this.service.getFaculties();
    }

    @Get('programs')
    async getPrograms(@Query('facultyIds') facultyIds?: string | string[]) {
        // Handle single or array query param
        const ids = Array.isArray(facultyIds) ? facultyIds : (facultyIds ? [facultyIds] : []);
        return this.service.getPrograms(ids);
    }

    @Get('courses')
    async getCourses(@Query('programIds') programIds?: string | string[]) {
        const ids = Array.isArray(programIds) ? programIds : (programIds ? [programIds] : []);
        return this.service.getCourses(ids);
    }

    @Post('filter-options')
    async getFilterOptions(@Body() filters: FilterOptionsDto) {
        return this.service.getFilterOptions(filters);
    }

    @Post('preview')
    async preview(@Body() filters: PreviewVideoconferenceDto) {
        return this.service.preview(filters);
    }

    @Post('generate')
    async generate(@Body() payload: GenerateVideoconferenceDto) {
        return this.service.generate(payload);
    }

    @Post('reconcile/:id')
    async reconcile(@Param('id') id: string) {
        return this.service.reconcile(id);
    }

    @Post('overrides')
    async upsertOverride(@Body() payload: UpsertVideoconferenceOverrideDto) {
        return this.service.upsertOverride(payload);
    }

    @Delete('overrides')
    async deleteOverride(
        @Query('scheduleId') scheduleId: string,
        @Query('conferenceDate') conferenceDate: string,
    ) {
        return this.service.deleteOverride(scheduleId, conferenceDate);
    }
}
