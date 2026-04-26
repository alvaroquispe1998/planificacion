import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ZoomUserEntity } from '../entities/audit.entities';
import {
    AcademicProgramEntity,
    CampusEntity,
    ExternalSourceEntity,
    FacultyEntity,
    TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
    CourseModalityEntity,
    PlanningOfferEntity,
    PlanningSectionEntity,
    PlanningSubsectionEntity,
    PlanningSubsectionScheduleEntity,
} from '../entities/planning.entities';
import { SettingsModule } from '../settings/settings.module';
import { VideoconferenceController } from './videoconference.controller';
import { VideoconferenceService } from './videoconference.service';
import {
    VcAcademicProgramEntity,
    VcCourseEntity,
    VcFacultyEntity,
    VcSectionEntity,
    PlanningSubsectionScheduleVcInheritanceEntity,
    PlanningInheritanceCopyLogEntity,
    VideoconferenceZoomPoolUserEntity,
    PlanningSubsectionVideoconferenceEntity,
    PlanningSubsectionVideoconferenceOverrideEntity,
    ZoomGroupEntity,
    ZoomGroupUserEntity,
    VideoconferenceGenerationBatchEntity,
    VideoconferenceGenerationBatchResultEntity,
    ZoomConfigEntity,
    VcScheduleHostRuleEntity,
} from './videoconference.entity';
import { ZoomSettingsController } from './zoom-settings.controller';
import { ZoomAccountService } from './zoom-account.service';

@Module({
    imports: [
        SettingsModule,
        TypeOrmModule.forFeature([
            VcFacultyEntity,
            VcAcademicProgramEntity,
            VcCourseEntity,
            VcSectionEntity,
            ZoomConfigEntity,
            VideoconferenceZoomPoolUserEntity,
            ZoomGroupEntity,
            ZoomGroupUserEntity,
            PlanningSubsectionScheduleVcInheritanceEntity,
            PlanningInheritanceCopyLogEntity,
            PlanningSubsectionVideoconferenceEntity,
            PlanningSubsectionVideoconferenceOverrideEntity,
            VideoconferenceGenerationBatchEntity,
            VideoconferenceGenerationBatchResultEntity,
            ZoomUserEntity,
            ExternalSourceEntity,
            CampusEntity,
            FacultyEntity,
            AcademicProgramEntity,
            TeacherEntity,
            CourseModalityEntity,
            PlanningOfferEntity,
            PlanningSectionEntity,
            PlanningSubsectionEntity,
            PlanningSubsectionScheduleEntity,
            VcScheduleHostRuleEntity,
        ]),
    ],
    controllers: [VideoconferenceController, ZoomSettingsController],
    providers: [VideoconferenceService, ZoomAccountService],
    exports: [VideoconferenceService, ZoomAccountService],
})
export class VideoconferenceModule { }
