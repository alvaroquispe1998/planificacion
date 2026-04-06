import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import {
  AcademicProgramEntity,
  CampusEntity,
  FacultyEntity,
  SemesterEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
  ClassZoomMeetingEntity,
  MeetingAttendanceSegmentEntity,
  MeetingInstanceEntity,
  MeetingParticipantEntity,
  MeetingRecordingEntity,
  MeetingTeacherMetricEntity,
  MeetingTranscriptEntity,
  VideoConferenceEntity,
} from '../entities/audit.entities';
import {
  PlanningOfferEntity,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionScheduleEntity,
} from '../entities/planning.entities';
import { MeetingSummaryEntity } from '../entities/syllabus.entities';
import { VideoconferenceModule } from '../videoconference/videoconference.module';
import { PlanningSubsectionVideoconferenceEntity } from '../videoconference/videoconference.entity';
import { AuditController } from './audit.controller';
import { AuditService } from './audit.service';

@Module({
  imports: [
    VideoconferenceModule,
    TypeOrmModule.forFeature([
      VideoConferenceEntity,
      ClassZoomMeetingEntity,
      MeetingInstanceEntity,
      MeetingParticipantEntity,
      MeetingAttendanceSegmentEntity,
      MeetingTeacherMetricEntity,
      MeetingRecordingEntity,
      MeetingTranscriptEntity,
      MeetingSummaryEntity,
      PlanningSubsectionVideoconferenceEntity,
      PlanningOfferEntity,
      PlanningSectionEntity,
      PlanningSubsectionEntity,
      PlanningSubsectionScheduleEntity,
      SemesterEntity,
      CampusEntity,
      FacultyEntity,
      AcademicProgramEntity,
      TeacherEntity,
    ]),
  ],
  controllers: [AuditController],
  providers: [AuditService],
  exports: [AuditService],
})
export class AuditModule {}
