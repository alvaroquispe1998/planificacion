import { Body, Controller, Delete, Get, Param, Patch, Post, Query } from '@nestjs/common';
import {
  CreateSyllabusKeywordDto,
  CreateSyllabusSessionDto,
  GenerateSummaryDto,
  MatchSyllabusDto,
  UpdateSyllabusSessionDto,
} from './dto/syllabus.dto';
import { SyllabusService } from './syllabus.service';

@Controller('syllabus')
export class SyllabusController {
  constructor(private readonly syllabusService: SyllabusService) {}

  @Get('sessions')
  listSessions(@Query('class_offering_id') classOfferingId?: string) {
    return this.syllabusService.listSessions(classOfferingId);
  }

  @Post('sessions')
  createSession(@Body() dto: CreateSyllabusSessionDto) {
    return this.syllabusService.createSession(dto);
  }

  @Patch('sessions/:id')
  updateSession(@Param('id') id: string, @Body() dto: UpdateSyllabusSessionDto) {
    return this.syllabusService.updateSession(id, dto);
  }

  @Delete('sessions/:id')
  deleteSession(@Param('id') id: string) {
    return this.syllabusService.deleteSession(id);
  }

  @Get('keywords')
  listKeywords(@Query('syllabus_session_id') syllabusSessionId?: string) {
    return this.syllabusService.listKeywords(syllabusSessionId);
  }

  @Post('keywords')
  createKeyword(@Body() dto: CreateSyllabusKeywordDto) {
    return this.syllabusService.createKeyword(dto);
  }

  @Delete('keywords/:id')
  deleteKeyword(@Param('id') id: string) {
    return this.syllabusService.deleteKeyword(id);
  }

  @Get('summaries')
  listSummaries(@Query('meeting_instance_id') meetingInstanceId?: string) {
    return this.syllabusService.listSummaries(meetingInstanceId);
  }

  @Post('summaries/generate/:meetingInstanceId')
  generateSummary(
    @Param('meetingInstanceId') meetingInstanceId: string,
    @Body() dto: GenerateSummaryDto,
  ) {
    return this.syllabusService.generateSummary(meetingInstanceId, dto);
  }

  @Get('matches')
  listMatches(@Query('meeting_instance_id') meetingInstanceId?: string) {
    return this.syllabusService.listMatches(meetingInstanceId);
  }

  @Post('matches/run/:meetingInstanceId/:syllabusSessionId')
  runMatch(
    @Param('meetingInstanceId') meetingInstanceId: string,
    @Param('syllabusSessionId') syllabusSessionId: string,
    @Body() dto: MatchSyllabusDto,
  ) {
    return this.syllabusService.runMatch(meetingInstanceId, syllabusSessionId, dto);
  }
}
