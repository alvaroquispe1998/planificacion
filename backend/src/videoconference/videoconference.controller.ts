import { Controller, Get, Post, Body, Query, Delete, Param, Patch } from '@nestjs/common';
import { WINDOW_PERMISSIONS } from '../auth/auth.constants';
import { RequirePermissions } from '../auth/permissions.decorator';
import { VideoconferenceService } from './videoconference.service';
import {
    AssignmentPreviewVideoconferenceDto,
    CheckExistingVideoconferencesDto,
    CreateVideoconferenceInheritanceDto,
    FilterOptionsDto,
    GenerateVideoconferenceDto,
    PreviewVideoconferenceInheritanceCandidatesDto,
    PreviewVideoconferenceDto,
    UpdateVideoconferenceInheritanceDto,
    UpsertVideoconferenceOverrideDto,
    VideoconferenceInheritanceCatalogDto,
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

    @Get('zoom-groups')
    async listActiveZoomGroups() {
        return this.service.listActiveZoomGroups();
    }

    @Post('filter-options')
    async getFilterOptions(@Body() filters: FilterOptionsDto) {
        return this.service.getFilterOptions(filters);
    }

    @Get('inheritances')
    async listInheritances() {
        return this.service.listVideoconferenceInheritances();
    }

    @Get('inheritances/catalog')
    async getInheritanceCatalog(@Query() query: VideoconferenceInheritanceCatalogDto) {
        return this.service.getInheritanceCatalog(query);
    }

    @Post('inheritances')
    async createInheritance(@Body() payload: CreateVideoconferenceInheritanceDto) {
        return this.service.createVideoconferenceInheritance(payload);
    }

    @Post('inheritances/candidates')
    async previewInheritanceCandidates(@Body() payload: PreviewVideoconferenceInheritanceCandidatesDto) {
        return this.service.previewInheritanceCandidates(payload);
    }

    @Delete('inheritances/cleanup-legacy')
    async cleanupLegacyInheritances(
        @Query('semesterId') semesterId?: string,
        @Query('facultyId') facultyId?: string,
    ) {
        return this.service.cleanupLegacyInheritances({
            semesterId: semesterId?.trim() || undefined,
            facultyId: facultyId?.trim() || undefined,
        });
    }

    @Patch('inheritances/:id')
    async updateInheritance(
        @Param('id') id: string,
        @Body() payload: UpdateVideoconferenceInheritanceDto,
    ) {
        return this.service.updateVideoconferenceInheritance(id, payload);
    }

    @Delete('inheritances/:id')
    async deleteInheritance(@Param('id') id: string) {
        return this.service.deleteVideoconferenceInheritance(id);
    }

    @Post('preview')
    async preview(@Body() filters: PreviewVideoconferenceDto) {
        return this.service.preview(filters);
    }

    @Post('assignment-preview')
    async assignmentPreview(@Body() payload: AssignmentPreviewVideoconferenceDto) {
        return this.service.assignmentPreview(payload);
    }

    @Post('check-existing')
    async checkExisting(@Body() payload: CheckExistingVideoconferencesDto) {
        return this.service.checkExisting(payload);
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
