import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
} from '@nestjs/common';
import {
  CreateClassZoomMeetingDto,
  CreateMeetingAttendanceSegmentDto,
  CreateMeetingInstanceDto,
  CreateMeetingParticipantDto,
  CreateMeetingRecordingDto,
  CreateMeetingTranscriptDto,
  CreateVideoConferenceDto,
  UpdateMeetingInstanceDto,
} from './dto/audit.dto';
import { AuditService } from './audit.service';

@Controller('audit')
export class AuditController {
  constructor(private readonly auditService: AuditService) {}

  @Get('video-conferences')
  listVideoConferences(@Query('class_offering_id') classOfferingId?: string) {
    return this.auditService.listVideoConferences(classOfferingId);
  }

  @Post('video-conferences')
  createVideoConference(@Body() dto: CreateVideoConferenceDto) {
    return this.auditService.createVideoConference(dto);
  }

  @Get('class-zoom-meetings')
  listClassZoomMeetings(@Query('class_offering_id') classOfferingId?: string) {
    return this.auditService.listClassZoomMeetings(classOfferingId);
  }

  @Post('class-zoom-meetings')
  createClassZoomMeeting(@Body() dto: CreateClassZoomMeetingDto) {
    return this.auditService.createClassZoomMeeting(dto);
  }

  @Get('meeting-instances')
  listMeetingInstances(@Query('video_conference_id') videoConferenceId?: string) {
    return this.auditService.listMeetingInstances(videoConferenceId);
  }

  @Get('meeting-instances/:id')
  getMeetingInstance(@Param('id') id: string) {
    return this.auditService.getMeetingInstance(id);
  }

  @Post('meeting-instances')
  createMeetingInstance(@Body() dto: CreateMeetingInstanceDto) {
    return this.auditService.createMeetingInstance(dto);
  }

  @Patch('meeting-instances/:id')
  updateMeetingInstance(@Param('id') id: string, @Body() dto: UpdateMeetingInstanceDto) {
    return this.auditService.updateMeetingInstance(id, dto);
  }

  @Delete('meeting-instances/:id')
  deleteMeetingInstance(@Param('id') id: string) {
    return this.auditService.deleteMeetingInstance(id);
  }

  @Get('meeting-participants')
  listParticipants(@Query('meeting_instance_id') meetingInstanceId?: string) {
    return this.auditService.listParticipants(meetingInstanceId);
  }

  @Post('meeting-participants')
  createParticipant(@Body() dto: CreateMeetingParticipantDto) {
    return this.auditService.createParticipant(dto);
  }

  @Get('meeting-attendance-segments')
  listAttendanceSegments(@Query('meeting_instance_id') meetingInstanceId?: string) {
    return this.auditService.listAttendanceSegments(meetingInstanceId);
  }

  @Post('meeting-attendance-segments')
  createAttendanceSegment(@Body() dto: CreateMeetingAttendanceSegmentDto) {
    return this.auditService.createAttendanceSegment(dto);
  }

  @Get('meeting-teacher-metrics')
  listTeacherMetrics(@Query('meeting_instance_id') meetingInstanceId?: string) {
    return this.auditService.listTeacherMetrics(meetingInstanceId);
  }

  @Post('meeting-teacher-metrics/recompute/:meetingInstanceId')
  recomputeTeacherMetrics(@Param('meetingInstanceId') meetingInstanceId: string) {
    return this.auditService.recomputeTeacherMetrics(meetingInstanceId);
  }

  @Get('meeting-recordings')
  listRecordings(@Query('meeting_instance_id') meetingInstanceId?: string) {
    return this.auditService.listRecordings(meetingInstanceId);
  }

  @Post('meeting-recordings')
  createRecording(@Body() dto: CreateMeetingRecordingDto) {
    return this.auditService.createRecording(dto);
  }

  @Get('meeting-transcripts')
  listTranscripts(@Query('meeting_instance_id') meetingInstanceId?: string) {
    return this.auditService.listTranscripts(meetingInstanceId);
  }

  @Post('meeting-transcripts')
  createTranscript(@Body() dto: CreateMeetingTranscriptDto) {
    return this.auditService.createTranscript(dto);
  }
}
