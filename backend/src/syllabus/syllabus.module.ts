import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MeetingTranscriptEntity } from '../entities/audit.entities';
import {
  ClassSyllabusKeywordEntity,
  ClassSyllabusSessionEntity,
  MeetingSummaryEntity,
  MeetingSyllabusMatchEntity,
} from '../entities/syllabus.entities';
import { SyllabusController } from './syllabus.controller';
import { SyllabusService } from './syllabus.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      ClassSyllabusSessionEntity,
      ClassSyllabusKeywordEntity,
      MeetingSummaryEntity,
      MeetingSyllabusMatchEntity,
      MeetingTranscriptEntity,
    ]),
  ],
  controllers: [SyllabusController],
  providers: [SyllabusService],
})
export class SyllabusModule {}
