import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ZoomUserEntity } from '../entities/audit.entities';
import {
  VcPeriodEntity,
  VcFacultyEntity,
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcSectionEntity,
} from '../videoconference/videoconference.entity';
import {
  AcademicProgramCampusEntity,
  AcademicProgramEntity,
  BuildingEntity,
  CampusEntity,
  ClassroomSectionScheduleEntity,
  ClassroomEntity,
  ClassroomTypeEntity,
  CourseEntity,
  CourseSectionEntity,
  ExternalSessionEntity,
  ExternalSourceEntity,
  FacultyEntity,
  SectionEntity,
  SemesterEntity,
  StudyPlanEntity,
  SyncJobEntity,
  SyncLogEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
import { CatalogController } from './catalog.controller';
import { CatalogService } from './catalog.service';
import { SettingsSyncController } from './settings-sync.controller';
import { SettingsSyncService } from './settings-sync.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      SemesterEntity,
      CampusEntity,
      FacultyEntity,
      AcademicProgramEntity,
      SectionEntity,
      CourseEntity,
      ClassroomTypeEntity,
      ZoomUserEntity,
      StudyPlanEntity,
      TeacherEntity,
      BuildingEntity,
      ClassroomEntity,
      ClassroomSectionScheduleEntity,
      AcademicProgramCampusEntity,
      CourseSectionEntity,
      ExternalSourceEntity,
      ExternalSessionEntity,
      SyncJobEntity,
      SyncLogEntity,
      VcPeriodEntity,
      VcFacultyEntity,
      VcAcademicProgramEntity,
      VcCourseEntity,
      VcSectionEntity,
    ]),
  ],
  controllers: [SettingsSyncController, CatalogController],
  providers: [SettingsSyncService, CatalogService],
})
export class SettingsModule { }
