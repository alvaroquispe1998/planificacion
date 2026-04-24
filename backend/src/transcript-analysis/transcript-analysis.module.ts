import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import {
  MeetingInstanceEntity,
  MeetingRecordingEntity,
} from '../entities/audit.entities';
import { VideoconferenceModule } from '../videoconference/videoconference.module';
import { TranscriptAnalysisController } from './transcript-analysis.controller';
import { TranscriptAnalysisService } from './transcript-analysis.service';

@Module({
  imports: [
    ConfigModule,
    TypeOrmModule.forFeature([MeetingInstanceEntity, MeetingRecordingEntity]),
    VideoconferenceModule,
  ],
  controllers: [TranscriptAnalysisController],
  providers: [TranscriptAnalysisService],
})
export class TranscriptAnalysisModule {}
