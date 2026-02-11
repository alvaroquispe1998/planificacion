import { Controller, Get } from '@nestjs/common';
import { CatalogService } from './catalog.service';

@Controller('settings/catalog')
export class CatalogController {
    constructor(private readonly catalogService: CatalogService) { }

    @Get('semesters')
    listSemesters() {
        return this.catalogService.listSemesters();
    }

    @Get('campuses')
    listCampuses() {
        return this.catalogService.listCampuses();
    }

    @Get('programs')
    listPrograms() {
        return this.catalogService.listPrograms();
    }

    @Get('courses')
    listCourses() {
        return this.catalogService.listCourses();
    }

    @Get('teachers')
    listTeachers() {
        return this.catalogService.listTeachers();
    }

    @Get('classrooms')
    listClassrooms() {
        return this.catalogService.listClassrooms();
    }

    @Get('study-plans')
    listStudyPlans() {
        return this.catalogService.listStudyPlans();
    }

    @Get('course-sections')
    listCourseSections() {
        return this.catalogService.listCourseSections();
    }

    @Get('classroom-section-schedules')
    listClassroomSectionSchedules() {
        return this.catalogService.listClassroomSectionSchedules();
    }
}
