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
    PlanningScheduleConflictV2Entity,
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
import { VideoconferenceDashboardController } from './videoconference-dashboard.controller';
import { VideoconferenceDashboardService } from './videoconference-dashboard.service';

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
            PlanningScheduleConflictV2Entity,
            VcScheduleHostRuleEntity,
        ]),
    ],
    controllers: [VideoconferenceController, ZoomSettingsController, VideoconferenceDashboardController],
    providers: [VideoconferenceService, ZoomAccountService, VideoconferenceDashboardService],
    exports: [VideoconferenceService, ZoomAccountService],
})
export class VideoconferenceModule { }
