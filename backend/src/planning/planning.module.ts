import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthModule } from '../auth/auth.module';
import {
  AcademicProgramEntity,
  BuildingEntity,
  CampusEntity,
  ClassroomEntity,
  CourseEntity,
  CourseSectionEntity,
  FacultyEntity,
  SemesterEntity,
  StudyPlanCourseDetailEntity,
  StudyPlanCourseEntity,
  StudyPlanEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
  ClassGroupEntity,
  ClassGroupTeacherEntity,
  ClassMeetingEntity,
  ClassOfferingEntity,
  ClassTeacherEntity,
  CourseSectionHourRequirementEntity,
  CourseModalityEntity,
  PlanningChangeLogEntity,
  PlanningCampusVcLocationMappingEntity,
  PlanningCyclePlanRuleEntity,
  PlanningOfferEntity,
  PlanningScheduleConflictV2Entity,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionScheduleEntity,
  ScheduleConflictEntity,
  StudyTypeEntity,
} from '../entities/planning.entities';
import {
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcFacultyEntity,
  VcPeriodEntity,
  VcSectionEntity,
} from '../videoconference/videoconference.entity';
import { PlanningController } from './planning.controller';
import { PlanningManualService } from './planning-manual.service';
import { PlanningService } from './planning.service';

@Module({
  imports: [
    AuthModule,
    TypeOrmModule.forFeature([
      ClassOfferingEntity,
      ClassGroupEntity,
      ClassMeetingEntity,
      ClassTeacherEntity,
      ClassGroupTeacherEntity,
      CourseSectionHourRequirementEntity,
      ScheduleConflictEntity,
      StudyTypeEntity,
      CourseModalityEntity,
      PlanningCampusVcLocationMappingEntity,
      PlanningCyclePlanRuleEntity,
      PlanningOfferEntity,
      PlanningSectionEntity,
      PlanningSubsectionEntity,
      PlanningSubsectionScheduleEntity,
      PlanningScheduleConflictV2Entity,
      PlanningChangeLogEntity,
      SemesterEntity,
      CampusEntity,
      FacultyEntity,
      AcademicProgramEntity,
      StudyPlanEntity,
      StudyPlanCourseEntity,
      StudyPlanCourseDetailEntity,
      CourseEntity,
      TeacherEntity,
      BuildingEntity,
      ClassroomEntity,
      CourseSectionEntity,
      VcPeriodEntity,
      VcFacultyEntity,
      VcAcademicProgramEntity,
      VcCourseEntity,
      VcSectionEntity,
    ]),
  ],
  controllers: [PlanningController],
  providers: [PlanningService, PlanningManualService],
})
export class PlanningModule {}
