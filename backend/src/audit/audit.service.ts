import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository } from 'typeorm';
import { newId } from '../common';
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
  CreateClassZoomMeetingDto,
  CreateMeetingAttendanceSegmentDto,
  CreateMeetingInstanceDto,
  CreateMeetingParticipantDto,
  CreateMeetingRecordingDto,
  CreateMeetingTranscriptDto,
  CreateVideoConferenceDto,
  UpdateMeetingInstanceDto,
} from './dto/audit.dto';

@Injectable()
export class AuditService {
  constructor(
    @InjectRepository(VideoConferenceEntity)
    private readonly videoConferencesRepo: Repository<VideoConferenceEntity>,
    @InjectRepository(ClassZoomMeetingEntity)
    private readonly classZoomMeetingsRepo: Repository<ClassZoomMeetingEntity>,
    @InjectRepository(MeetingInstanceEntity)
    private readonly meetingInstancesRepo: Repository<MeetingInstanceEntity>,
    @InjectRepository(MeetingParticipantEntity)
    private readonly participantsRepo: Repository<MeetingParticipantEntity>,
    @InjectRepository(MeetingAttendanceSegmentEntity)
    private readonly segmentsRepo: Repository<MeetingAttendanceSegmentEntity>,
    @InjectRepository(MeetingTeacherMetricEntity)
    private readonly teacherMetricsRepo: Repository<MeetingTeacherMetricEntity>,
    @InjectRepository(MeetingRecordingEntity)
    private readonly recordingsRepo: Repository<MeetingRecordingEntity>,
    @InjectRepository(MeetingTranscriptEntity)
    private readonly transcriptsRepo: Repository<MeetingTranscriptEntity>,
  ) {}

  listVideoConferences(classOfferingId?: string) {
    return this.videoConferencesRepo.find({
      where: classOfferingId ? { class_offering_id: classOfferingId } : {},
      order: { start_time: 'DESC' },
    });
  }

  createVideoConference(dto: CreateVideoConferenceDto) {
    return this.videoConferencesRepo.save(
      this.videoConferencesRepo.create({
        ...dto,
        start_time: new Date(dto.start_time),
        end_time: new Date(dto.end_time),
        created_at: new Date(),
      }),
    );
  }

  listClassZoomMeetings(classOfferingId?: string) {
    return this.classZoomMeetingsRepo.find({
      where: classOfferingId ? { class_offering_id: classOfferingId } : {},
      order: { updated_at: 'DESC' },
    });
  }

  createClassZoomMeeting(dto: CreateClassZoomMeetingDto) {
    const now = new Date();
    return this.classZoomMeetingsRepo.save(
      this.classZoomMeetingsRepo.create({
        ...dto,
        created_at: now,
        updated_at: now,
      }),
    );
  }

  listMeetingInstances(videoConferenceId?: string) {
    return this.meetingInstancesRepo.find({
      where: videoConferenceId ? { video_conference_id: videoConferenceId } : {},
      order: { created_at: 'DESC' },
    });
  }

  async getMeetingInstance(id: string) {
    const found = await this.meetingInstancesRepo.findOne({ where: { id } });
    if (!found) {
      throw new NotFoundException(`meeting_instances ${id} not found`);
    }
    return found;
  }

  createMeetingInstance(dto: CreateMeetingInstanceDto) {
    const now = new Date();
    return this.meetingInstancesRepo.save(
      this.meetingInstancesRepo.create({
        ...dto,
        scheduled_start: dto.scheduled_start ? new Date(dto.scheduled_start) : null,
        scheduled_end: dto.scheduled_end ? new Date(dto.scheduled_end) : null,
        actual_start: dto.actual_start ? new Date(dto.actual_start) : null,
        actual_end: dto.actual_end ? new Date(dto.actual_end) : null,
        raw_json: dto.raw_json ?? null,
        created_at: now,
        updated_at: now,
      }),
    );
  }

  async updateMeetingInstance(id: string, dto: UpdateMeetingInstanceDto) {
    const { raw_json, ...rest } = dto;
    const updatePayload: Record<string, unknown> = {
      ...rest,
      scheduled_start: dto.scheduled_start ? new Date(dto.scheduled_start) : undefined,
      scheduled_end: dto.scheduled_end ? new Date(dto.scheduled_end) : undefined,
      actual_start: dto.actual_start ? new Date(dto.actual_start) : undefined,
      actual_end: dto.actual_end ? new Date(dto.actual_end) : undefined,
      updated_at: new Date(),
    };
    if (raw_json !== undefined) {
      updatePayload.raw_json = raw_json;
    }
    await this.meetingInstancesRepo.update(
      { id },
      updatePayload as never,
    );
    return this.getMeetingInstance(id);
  }

  async deleteMeetingInstance(id: string) {
    await this.meetingInstancesRepo.delete({ id });
    return { deleted: true, id };
  }

  listParticipants(meetingInstanceId?: string) {
    return this.participantsRepo.find({
      where: meetingInstanceId ? { meeting_instance_id: meetingInstanceId } : {},
      order: { created_at: 'ASC' },
    });
  }

  createParticipant(dto: CreateMeetingParticipantDto) {
    return this.participantsRepo.save(
      this.participantsRepo.create({
        ...dto,
        created_at: new Date(),
      }),
    );
  }

  listAttendanceSegments(meetingInstanceId?: string) {
    return this.segmentsRepo.find({
      where: meetingInstanceId ? { meeting_instance_id: meetingInstanceId } : {},
      order: { join_time: 'ASC' },
    });
  }

  createAttendanceSegment(dto: CreateMeetingAttendanceSegmentDto) {
    return this.segmentsRepo.save(
      this.segmentsRepo.create({
        ...dto,
        join_time: new Date(dto.join_time),
        leave_time: dto.leave_time ? new Date(dto.leave_time) : null,
        created_at: new Date(),
      }),
    );
  }

  listTeacherMetrics(meetingInstanceId?: string) {
    return this.teacherMetricsRepo.find({
      where: meetingInstanceId ? { meeting_instance_id: meetingInstanceId } : {},
      order: { created_at: 'DESC' },
    });
  }

  createRecording(dto: CreateMeetingRecordingDto) {
    return this.recordingsRepo.save(
      this.recordingsRepo.create({
        ...dto,
        start_time: dto.start_time ? new Date(dto.start_time) : null,
        end_time: dto.end_time ? new Date(dto.end_time) : null,
        raw_json: dto.raw_json ?? null,
        created_at: new Date(),
      }),
    );
  }

  listRecordings(meetingInstanceId?: string) {
    return this.recordingsRepo.find({
      where: meetingInstanceId ? { meeting_instance_id: meetingInstanceId } : {},
      order: { created_at: 'DESC' },
    });
  }

  createTranscript(dto: CreateMeetingTranscriptDto) {
    return this.transcriptsRepo.save(
      this.transcriptsRepo.create({
        ...dto,
        created_at: new Date(),
      }),
    );
  }

  listTranscripts(meetingInstanceId?: string) {
    return this.transcriptsRepo.find({
      where: meetingInstanceId ? { meeting_instance_id: meetingInstanceId } : {},
      order: { created_at: 'DESC' },
    });
  }

  async recomputeTeacherMetrics(meetingInstanceId: string) {
    const meeting = await this.meetingInstancesRepo.findOne({ where: { id: meetingInstanceId } });
    if (!meeting) {
      throw new NotFoundException(`meeting_instances ${meetingInstanceId} not found`);
    }

    const participants = await this.participantsRepo.find({ where: { meeting_instance_id: meetingInstanceId } });
    const teacherParticipants = participants.filter((participant) => Boolean(participant.teacher_id));
    const participantIds = teacherParticipants.map((participant) => participant.id);

    const segments = participantIds.length
      ? await this.segmentsRepo.find({ where: { participant_id: In(participantIds) } })
      : [];

    const byTeacher = new Map<
      string,
      {
        join_count: number;
        total_minutes: number;
        first_join: Date;
        last_leave: Date | null;
        is_host: boolean;
      }
    >();

    for (const participant of teacherParticipants) {
      const teacherId = participant.teacher_id as string;
      if (!byTeacher.has(teacherId)) {
        byTeacher.set(teacherId, {
          join_count: 0,
          total_minutes: 0,
          first_join: new Date('9999-12-31T23:59:59.000Z'),
          last_leave: null,
          is_host: participant.role === 'HOST' || participant.role === 'CO_HOST',
        });
      }
      if (participant.role === 'HOST' || participant.role === 'CO_HOST') {
        byTeacher.get(teacherId)!.is_host = true;
      }
    }

    for (const segment of segments) {
      const participant = teacherParticipants.find((item) => item.id === segment.participant_id);
      if (!participant?.teacher_id) {
        continue;
      }
      const aggregate = byTeacher.get(participant.teacher_id);
      if (!aggregate) {
        continue;
      }

      aggregate.join_count += 1;
      const segmentMinutes =
        segment.minutes ??
        (segment.leave_time
          ? Math.max(0, Math.floor((segment.leave_time.getTime() - segment.join_time.getTime()) / 60000))
          : 0);
      aggregate.total_minutes += segmentMinutes;
      if (segment.join_time.getTime() < aggregate.first_join.getTime()) {
        aggregate.first_join = segment.join_time;
      }
      if (segment.leave_time && (!aggregate.last_leave || segment.leave_time > aggregate.last_leave)) {
        aggregate.last_leave = segment.leave_time;
      }
    }

    await this.teacherMetricsRepo.delete({ meeting_instance_id: meetingInstanceId });

    const now = new Date();
    const rows = [...byTeacher.entries()].map(([teacherId, aggregate]) =>
      this.teacherMetricsRepo.create({
        id: newId(),
        meeting_instance_id: meetingInstanceId,
        teacher_id: teacherId,
        join_count: aggregate.join_count,
        total_minutes: aggregate.total_minutes,
        first_join: aggregate.first_join.getFullYear() === 9999 ? now : aggregate.first_join,
        last_leave: aggregate.last_leave,
        is_host: aggregate.is_host,
        created_at: now,
        updated_at: now,
      }),
    );

    if (rows.length > 0) {
      await this.teacherMetricsRepo.save(rows);
    }

    return {
      meeting_instance_id: meetingInstanceId,
      metrics_created: rows.length,
      computed_at: now.toISOString(),
    };
  }
}
