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
import { WINDOW_PERMISSIONS } from '../auth/auth.constants';
import { RequirePermissions } from '../auth/permissions.decorator';
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
@RequirePermissions(WINDOW_PERMISSIONS.AUDIT)
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

  @Get('planning-videoconferences')
  listPlanningVideoconferenceAudits(
    @Query('page') page?: string,
    @Query('page_size') pageSize?: string,
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('faculty_id') facultyId?: string,
    @Query('academic_program_id') academicProgramId?: string,
    @Query('status') status?: string,
    @Query('audit_sync_status') auditSyncStatus?: string,
    @Query('search') search?: string,
    @Query('hide_inherited') hideInherited?: string,
  ) {
    return this.auditService.listPlanningVideoconferenceAudits({
      page: page ? Number(page) : undefined,
      page_size: pageSize ? Number(pageSize) : undefined,
      semester_id: semesterId,
      campus_id: campusId,
      faculty_id: facultyId,
      academic_program_id: academicProgramId,
      status,
      audit_sync_status: auditSyncStatus,
      search,
      hide_inherited: hideInherited !== 'false',
    });
  }

  @Get('planning-videoconferences/:id')
  getPlanningVideoconferenceAuditDetail(@Param('id') id: string) {
    return this.auditService.getPlanningVideoconferenceAuditDetail(id);
  }

  @Post('planning-videoconferences/:id/sync')
  syncPlanningVideoconference(@Param('id') id: string) {
    return this.auditService.syncPlanningVideoconference(id);
  }
}
