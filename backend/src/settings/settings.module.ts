import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ZoomUserEntity } from '../entities/audit.entities';
import {
  VcPeriodEntity,
  VcFacultyEntity,
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcSectionEntity,
  VcSectionStudentEntity,
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
  StudyPlanCourseDetailEntity,
  StudyPlanCourseEntity,
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
      StudyPlanCourseEntity,
      StudyPlanCourseDetailEntity,
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
      VcSectionStudentEntity,
    ]),
  ],
  controllers: [SettingsSyncController, CatalogController],
  providers: [SettingsSyncService, CatalogService],
  exports: [SettingsSyncService, CatalogService],
})
export class SettingsModule { }
