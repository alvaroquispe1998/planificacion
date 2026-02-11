import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { ClassroomSectionScheduleEntity } from '../entities/catalog-sync.entities';
import {
    VcAcademicProgramEntity,
    VcCourseEntity,
    VcFacultyEntity,
    VcSectionEntity,
} from './videoconference.entity';
import { FilterOptionsDto } from './videoconference.dto';

@Injectable()
export class VideoconferenceService {
    constructor(
        @InjectRepository(ClassroomSectionScheduleEntity)
        private readonly classroomSectionSchedulesRepo: Repository<ClassroomSectionScheduleEntity>,
        @InjectRepository(VcFacultyEntity)
        private readonly vcFacultiesRepo: Repository<VcFacultyEntity>,
        @InjectRepository(VcAcademicProgramEntity)
        private readonly vcProgramsRepo: Repository<VcAcademicProgramEntity>,
        @InjectRepository(VcCourseEntity)
        private readonly vcCoursesRepo: Repository<VcCourseEntity>,
        @InjectRepository(VcSectionEntity)
        private readonly vcSectionsRepo: Repository<VcSectionEntity>,
    ) { }

    async getFaculties() {
        return this.vcFacultiesRepo.find({ order: { name: 'ASC' } });
    }

    async getPrograms(facultyIds: string[]) {
        if (!facultyIds?.length) return [];
        return this.vcProgramsRepo.find({
            where: { faculty_id: In(facultyIds) },
            order: { name: 'ASC' },
        });
    }

    async getCourses(programIds: string[]) {
        if (!programIds?.length) return [];
        return this.vcCoursesRepo.find({
            where: { program_id: In(programIds) },
            order: { name: 'ASC' },
        });
    }

    async getSections(courseIds: string[]) {
        if (!courseIds?.length) return [];
        // Only return sections for selected courses
        return this.vcSectionsRepo.find({
            where: { course_id: In(courseIds) },
            order: { name: 'ASC' },
        });
    }

    async preview(filters: FilterOptionsDto) {
        const qb = this.classroomSectionSchedulesRepo
            .createQueryBuilder('schedule')
            .leftJoin(VcSectionEntity, 'section', 'section.id = schedule.course_section_id')
            .leftJoin(VcCourseEntity, 'course', 'course.id = section.course_id')
            .leftJoin(VcAcademicProgramEntity, 'program', 'program.id = course.program_id')
            .leftJoin(VcFacultyEntity, 'faculty', 'faculty.id = program.faculty_id')
            .select([
                'schedule.id',
                'schedule.classroom_id',
                'schedule.course_section_id',
                'schedule.day_of_week',
                'schedule.start_time',
                'schedule.end_time',
                'schedule.title',
                'schedule.description',
                'schedule.all_day',
                'section.id',
                'section.name',
                'course.id',
                'course.name',
                'course.code',
                'program.id',
                'program.name',
                'faculty.id',
                'faculty.name',
            ]);

        if (filters.facultyIds?.length) {
            qb.andWhere('faculty.id IN (:...facultyIds)', { facultyIds: filters.facultyIds });
        }
        if (filters.programIds?.length) {
            qb.andWhere('program.id IN (:...programIds)', { programIds: filters.programIds });
        }
        if (filters.courseIds?.length) {
            qb.andWhere('course.id IN (:...courseIds)', { courseIds: filters.courseIds });
        }
        if (filters.sectionIds?.length) {
            qb.andWhere('schedule.course_section_id IN (:...sectionIds)', {
                sectionIds: filters.sectionIds,
            });
        }
        if (filters.days?.length) {
            qb.andWhere('schedule.day_of_week IN (:...days)', { days: filters.days });
        }

        const raw = await qb
            .orderBy('faculty.name', 'ASC')
            .addOrderBy('program.name', 'ASC')
            .addOrderBy('course.name', 'ASC')
            .addOrderBy('section.name', 'ASC')
            .addOrderBy('schedule.day_of_week', 'ASC')
            .addOrderBy('schedule.start_time', 'ASC')
            .getRawMany();

        return raw.map((row) => ({
            id: row['schedule_id'],
            faculty: row['faculty_name'] ?? '(Sin Facultad)',
            program: row['program_name'] ?? '(Sin Programa)',
            course: row['course_name'] ?? '(Sin Curso)',
            section: row['section_name'] ?? row['schedule_course_section_id'],
            day: row['schedule_day_of_week'],
            start_time: row['schedule_start_time'],
            end_time: row['schedule_end_time'],
            zoom_user: null,
            _metadata: {
                classroom_id: row['schedule_classroom_id'],
                course_section_id: row['schedule_course_section_id'],
                section_id: row['section_id'] ?? null,
                course_id: row['course_id'] ?? null,
                program_id: row['program_id'] ?? null,
                faculty_id: row['faculty_id'] ?? null,
                title: row['schedule_title'] ?? null,
                description: row['schedule_description'] ?? null,
                all_day: row['schedule_all_day'] ?? false,
            },
        }));
    }
}
