import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ZoomUserEntity } from '../entities/audit.entities';
import {
  AcademicProgramCampusEntity,
  AcademicProgramEntity,
  BuildingEntity,
  CampusEntity,
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
      AcademicProgramCampusEntity,
      CourseSectionEntity,
      ExternalSourceEntity,
      ExternalSessionEntity,
      SyncJobEntity,
      SyncLogEntity,
    ]),
  ],
  controllers: [SettingsSyncController],
  providers: [SettingsSyncService],
})
export class SettingsModule {}
