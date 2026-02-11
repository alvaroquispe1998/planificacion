import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { VideoconferenceController } from './videoconference.controller';
import { VideoconferenceService } from './videoconference.service';
import {
    VideoconferenceEntity,
    VcPeriodEntity,
    VcFacultyEntity,
    VcAcademicProgramEntity,
    VcCourseEntity,
    VcSectionEntity,
} from './videoconference.entity';
import {
    ClassMeetingEntity,
    ClassGroupEntity,
    ClassOfferingEntity,
} from '../entities/planning.entities';
import {
    ClassroomSectionScheduleEntity,
} from '../entities/catalog-sync.entities';

@Module({
    imports: [
        TypeOrmModule.forFeature([
            VideoconferenceEntity,
            VcPeriodEntity,
            VcFacultyEntity,
            VcAcademicProgramEntity,
            VcCourseEntity,
            VcSectionEntity,
            ClassroomSectionScheduleEntity,
            ClassMeetingEntity,
            ClassGroupEntity,
            ClassOfferingEntity,
        ]),
    ],
    controllers: [VideoconferenceController],
    providers: [VideoconferenceService],
})
export class VideoconferenceModule { }
