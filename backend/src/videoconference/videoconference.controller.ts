import { Controller, Get, Post, Body, Query } from '@nestjs/common';
import { VideoconferenceService } from './videoconference.service';
import { FilterOptionsDto, GenerateVideoconferenceDto } from './videoconference.dto';

@Controller('videoconference')
export class VideoconferenceController {
    constructor(private readonly service: VideoconferenceService) { }

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

    @Get('sections')
    async getSections(@Query('courseIds') courseIds?: string | string[]) {
        const ids = Array.isArray(courseIds) ? courseIds : (courseIds ? [courseIds] : []);
        return this.service.getSections(ids);
    }

    @Post('preview')
    async preview(@Body() filters: FilterOptionsDto) {
        return this.service.preview(filters);
    }

    @Post('generate')
    async generate(@Body() payload: GenerateVideoconferenceDto) {
        // Stub for generation logic
        return { success: true, message: 'Videoconferencias generadas (Stub)', count: payload.meetings.length };
    }
}
