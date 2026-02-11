import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
    AcademicProgramEntity,
    CampusEntity,
    ClassroomSectionScheduleEntity,
    ClassroomEntity,
    CourseEntity,
    CourseSectionEntity,
    SemesterEntity,
    StudyPlanEntity,
    TeacherEntity,
} from '../entities/catalog-sync.entities';

@Injectable()
export class CatalogService {
    constructor(
        @InjectRepository(SemesterEntity)
        private readonly semestersRepo: Repository<SemesterEntity>,
        @InjectRepository(CampusEntity)
        private readonly campusesRepo: Repository<CampusEntity>,
        @InjectRepository(AcademicProgramEntity)
        private readonly programsRepo: Repository<AcademicProgramEntity>,
        @InjectRepository(CourseEntity)
        private readonly coursesRepo: Repository<CourseEntity>,
        @InjectRepository(TeacherEntity)
        private readonly teachersRepo: Repository<TeacherEntity>,
        @InjectRepository(ClassroomEntity)
        private readonly classroomsRepo: Repository<ClassroomEntity>,
        @InjectRepository(StudyPlanEntity)
        private readonly studyPlansRepo: Repository<StudyPlanEntity>,
        @InjectRepository(CourseSectionEntity)
        private readonly courseSectionsRepo: Repository<CourseSectionEntity>,
        @InjectRepository(ClassroomSectionScheduleEntity)
        private readonly classroomSectionSchedulesRepo: Repository<ClassroomSectionScheduleEntity>,
    ) { }

    async listSemesters() {
        // Sort by start_date descending ideally, or name
        return this.semestersRepo.find({ order: { name: 'ASC' } });
    }

    async listCampuses() {
        return this.campusesRepo.find({ order: { name: 'ASC' } });
    }

    async listPrograms() {
        return this.programsRepo.find({ order: { name: 'ASC' } });
    }

    async listCourses(query?: string) {
        // Simple list, maybe limit if too many?
        // For now return all, usually 1000-2000 courses max in small unis
        return this.coursesRepo.find({
            order: { name: 'ASC' },
            take: 2000,
        });
    }

    async listTeachers() {
        return this.teachersRepo.find({
            order: { full_name: 'ASC' },
            take: 2000,
        });
    }

    async listClassrooms() {
        return this.classroomsRepo.find({ order: { name: 'ASC' } });
    }

    async listStudyPlans() {
        return this.studyPlansRepo.find({ order: { name: 'ASC' } });
    }

    async listCourseSections() {
        return this.courseSectionsRepo.find({ take: 3000 });
    }

    async listClassroomSectionSchedules() {
        return this.classroomSectionSchedulesRepo.find({
            order: { day_of_week: 'ASC', start_time: 'ASC' },
            take: 10000,
        });
    }
}
