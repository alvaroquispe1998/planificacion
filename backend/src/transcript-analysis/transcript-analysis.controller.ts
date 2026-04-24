import { Body, Controller, Post } from '@nestjs/common';
import { WINDOW_PERMISSIONS } from '../auth/auth.constants';
import { RequirePermissions } from '../auth/permissions.decorator';
import { RunTranscriptAnalysisDto } from './dto/run-analysis.dto';
import { TranscriptAnalysisService } from './transcript-analysis.service';

@Controller('transcript-analysis')
@RequirePermissions(WINDOW_PERMISSIONS.AUDIT)
export class TranscriptAnalysisController {
  constructor(private readonly service: TranscriptAnalysisService) {}

  @Post('run')
  run(@Body() dto: RunTranscriptAnalysisDto) {
    return this.service.analyze(dto);
  }
}
