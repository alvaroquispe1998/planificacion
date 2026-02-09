import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import {
  ClassGroupEntity,
  ClassGroupTeacherEntity,
  ClassMeetingEntity,
  ClassOfferingEntity,
  ClassTeacherEntity,
  CourseSectionHourRequirementEntity,
  ScheduleConflictEntity,
} from '../entities/planning.entities';
import { PlanningController } from './planning.controller';
import { PlanningService } from './planning.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      ClassOfferingEntity,
      ClassGroupEntity,
      ClassMeetingEntity,
      ClassTeacherEntity,
      ClassGroupTeacherEntity,
      CourseSectionHourRequirementEntity,
      ScheduleConflictEntity,
    ]),
  ],
  controllers: [PlanningController],
  providers: [PlanningService],
})
export class PlanningModule {}
