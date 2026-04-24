import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TranscriptAnalysisController } from './transcript-analysis.controller';
import { TranscriptAnalysisService } from './transcript-analysis.service';

@Module({
  imports: [ConfigModule],
  controllers: [TranscriptAnalysisController],
  providers: [TranscriptAnalysisService],
})
export class TranscriptAnalysisModule {}
