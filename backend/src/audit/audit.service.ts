import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository, SelectQueryBuilder } from 'typeorm';
import { newId } from '../common';
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
  RecordingStatusValues,
  RecordingTypeValues,
  VideoConferenceEntity,
} from '../entities/audit.entities';
import {
  PlanningOfferEntity,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionScheduleEntity,
} from '../entities/planning.entities';
import { MeetingSummaryEntity } from '../entities/syllabus.entities';
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
import { VideoconferenceService } from '../videoconference/videoconference.service';
import { ZoomAccountService } from '../videoconference/zoom-account.service';
import {
  PlanningSubsectionVideoconferenceAuditStatusValues,
  PlanningSubsectionVideoconferenceEntity,
} from '../videoconference/videoconference.entity';

function toDateOnly(value: string | Date | null | undefined): string {
  if (!value) return '';
  const str = typeof value === 'string' ? value : value.toISOString();
  return str.slice(0, 10);
}

type PlanningAuditListRow = {
  id: string;
  planning_offer_id: string;
  planning_section_id: string;
  planning_subsection_id: string;
  planning_subsection_schedule_id: string;
  conference_date: string;
  day_of_week: string;
  start_time: string;
  end_time: string;
  scheduled_start: Date;
  scheduled_end: Date;
  zoom_user_id: string | null;
  zoom_user_email: string | null;
  zoom_user_name: string | null;
  zoom_meeting_id: string | null;
  created_at: Date;
  topic: string | null;
  aula_virtual_name: string | null;
  join_url: string | null;
  start_url: string | null;
  link_mode: 'OWNED' | 'INHERITED';
  owner_videoconference_id: string | null;
  inheritance_mapping_id: string | null;
  sync_owner_videoconference_id: string;
  sync_owner_topic: string | null;
  sync_owner_zoom_meeting_id: string | null;
  sync_owner_join_url: string | null;
  sync_owner_start_url: string | null;
  owner_course_code: string | null;
  owner_course_name: string | null;
  owner_section_code: string | null;
  owner_section_external_code: string | null;
  owner_subsection_code: string | null;
  owner_schedule_day_of_week: string | null;
  owner_schedule_start_time: string | null;
  owner_schedule_end_time: string | null;
  status: string;
  audit_sync_status: string | null;
  audit_synced_at: Date | null;
  audit_sync_error: string | null;
  error_message: string | null;
  semester_id: string | null;
  semester_name: string | null;
  campus_id: string | null;
  campus_name: string | null;
  faculty_id: string | null;
  faculty_name: string | null;
  academic_program_id: string | null;
  academic_program_name: string | null;
  cycle: number | null;
  course_code: string | null;
  course_name: string | null;
  section_code: string;
  section_external_code: string | null;
  subsection_code: string;
  subsection_kind: string | null;
  subsection_denomination: string | null;
  schedule_session_type: string | null;
  effective_teacher_id: string | null;
  schedule_day_of_week: string | null;
  schedule_start_time: string | null;
  schedule_end_time: string | null;
};

type PlanningAuditInstanceBundle = {
  instance: MeetingInstanceEntity;
  participants: MeetingParticipantEntity[];
  recordings: MeetingRecordingEntity[];
  transcripts: MeetingTranscriptEntity[];
  summaries: MeetingSummaryEntity[];
};

type PlanningAuditFilters = {
  ids?: string[];
  semester_id?: string;
  campus_id?: string;
  faculty_id?: string;
  academic_program_id?: string;
  status?: string;
  audit_sync_status?: string;
  search?: string;
  hide_inherited?: boolean;
};

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
    @InjectRepository(MeetingSummaryEntity)
    private readonly summariesRepo: Repository<MeetingSummaryEntity>,
    @InjectRepository(PlanningSubsectionVideoconferenceEntity)
    private readonly planningVideoconferencesRepo: Repository<PlanningSubsectionVideoconferenceEntity>,
    @InjectRepository(PlanningOfferEntity)
    private readonly planningOffersRepo: Repository<PlanningOfferEntity>,
    @InjectRepository(PlanningSectionEntity)
    private readonly planningSectionsRepo: Repository<PlanningSectionEntity>,
    @InjectRepository(PlanningSubsectionEntity)
    private readonly planningSubsectionsRepo: Repository<PlanningSubsectionEntity>,
    @InjectRepository(PlanningSubsectionScheduleEntity)
    private readonly planningSchedulesRepo: Repository<PlanningSubsectionScheduleEntity>,
    @InjectRepository(SemesterEntity)
    private readonly semestersRepo: Repository<SemesterEntity>,
    @InjectRepository(CampusEntity)
    private readonly campusesRepo: Repository<CampusEntity>,
    @InjectRepository(FacultyEntity)
    private readonly facultiesRepo: Repository<FacultyEntity>,
    @InjectRepository(AcademicProgramEntity)
    private readonly programsRepo: Repository<AcademicProgramEntity>,
    @InjectRepository(TeacherEntity)
    private readonly teachersRepo: Repository<TeacherEntity>,
    private readonly zoomAccountService: ZoomAccountService,
    private readonly videoconferenceService: VideoconferenceService,
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
    await this.meetingInstancesRepo.update({ id }, updatePayload as never);
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

  async listPlanningVideoconferenceAudits(filters: PlanningAuditFilters & { page?: number; page_size?: number } = {}) {
    const pageSize = Math.max(1, Math.min(100, Number(filters.page_size ?? 20) || 20));
    const page = Math.max(1, Number(filters.page ?? 1) || 1);
    const baseQuery = this.buildPlanningAuditBaseQuery(filters);
    const rowsQuery = this.buildPlanningAuditQuery(filters);
    const [rows, total, summaryRows] = await Promise.all([
      rowsQuery.clone().offset((page - 1) * pageSize).limit(pageSize).getRawMany<PlanningAuditListRow>(),
      baseQuery.clone().getCount(),
      baseQuery
        .clone()
        .select('vc.status', 'status')
        .addSelect('COALESCE(vc.audit_sync_status, :pending)', 'audit_sync_status')
        .addSelect('COUNT(*)', 'count')
        .setParameter('pending', 'PENDING')
        .groupBy('vc.status')
        .addGroupBy('COALESCE(vc.audit_sync_status, :pending)')
        .getRawMany<Record<string, unknown>>(),
    ]);

    if (rows.length === 0) {
      return {
        items: [] as Array<Record<string, unknown>>,
        totals: { total, matched: 0, errors: 0, pending_audit: 0 },
        page,
        page_size: pageSize,
      };
    }

    const recordIds = rows.map((row) => row.id);
    const ownerIds = Array.from(
      new Set(rows.map((row) => row.sync_owner_videoconference_id || row.id).filter(Boolean)),
    );
    const teacherIds = [...new Set(rows.map((row) => row.effective_teacher_id).filter(Boolean))] as string[];
    const [teachers, instanceCounts, participantCounts, recordingCounts] = await Promise.all([
      teacherIds.length ? this.teachersRepo.find({ where: { id: In(teacherIds) } }) : [],
      this.countInstancesByPlanningVideoconference(ownerIds),
      this.countParticipantsByPlanningVideoconference(ownerIds),
      this.countRecordingsByPlanningVideoconference(ownerIds),
    ]);
    const ownerIdMap = new Map(rows.map((row) => [row.id, row.sync_owner_videoconference_id || row.id] as const));
    const teacherMap = new Map(teachers.map((item) => [item.id, item] as const));
    const instanceCountMap = new Map(instanceCounts.map((item) => [item.id, item.count] as const));
    const participantCountMap = new Map(participantCounts.map((item) => [item.id, item.count] as const));
    const recordingCountMap = new Map(recordingCounts.map((item) => [item.id, item.count] as const));

    const items = rows.map((row) => {
      const teacher = row.effective_teacher_id ? teacherMap.get(row.effective_teacher_id) ?? null : null;
      return {
        id: row.id,
        planning_offer_id: row.planning_offer_id,
        planning_section_id: row.planning_section_id,
        planning_subsection_id: row.planning_subsection_id,
        planning_subsection_schedule_id: row.planning_subsection_schedule_id,
        conference_date: toDateOnly(row.conference_date),
        day_of_week: row.day_of_week,
        start_time: row.start_time,
        end_time: row.end_time,
        scheduled_start: row.scheduled_start,
        scheduled_end: row.scheduled_end,
        zoom_user_id: row.zoom_user_id,
        zoom_user_email: row.zoom_user_email,
        zoom_user_name: row.zoom_user_name,
        zoom_meeting_id: row.zoom_meeting_id,
        created_at: row.created_at,
        topic: row.topic,
        aula_virtual_name: row.aula_virtual_name,
        join_url: row.join_url,
        start_url: row.start_url,
        link_mode: row.link_mode,
        owner_videoconference_id: row.owner_videoconference_id,
        inheritance_mapping_id: row.inheritance_mapping_id,
        owner_label:
          row.link_mode === 'INHERITED'
            ? this.buildOwnerLabel({
                course_code: row.owner_course_code,
                course_name: row.owner_course_name,
                section_code: row.owner_section_external_code || row.owner_section_code,
                subsection_code: row.owner_subsection_code,
                day_of_week: row.owner_schedule_day_of_week,
                start_time: row.owner_schedule_start_time,
                end_time: row.owner_schedule_end_time,
              })
            : null,
        status: row.status,
        audit_sync_status: this.normalizeAuditSyncStatus(row.audit_sync_status),
        audit_synced_at: row.audit_synced_at,
        audit_sync_error: row.audit_sync_error,
        error_message: row.error_message,
        semester_id: row.semester_id,
        semester_name: row.semester_name,
        campus_id: row.campus_id,
        campus_name: row.campus_name,
        faculty_id: row.faculty_id,
        faculty_name: row.faculty_name,
        academic_program_id: row.academic_program_id,
        academic_program_name: row.academic_program_name,
        cycle: row.cycle,
        course_code: row.course_code,
        course_name: row.course_name,
        section_code: row.section_code,
        section_external_code: row.section_external_code,
        subsection_code: row.subsection_code,
        subsection_kind: row.subsection_kind,
        subsection_denomination: row.subsection_denomination,
        session_type: row.schedule_session_type,
        planning_sync_state: this.resolvePlanningSyncState(row),
        teacher_id: teacher?.id ?? row.effective_teacher_id,
        teacher_name: teacher?.full_name ?? teacher?.name ?? null,
        teacher_dni: teacher?.dni ?? null,
        instance_count: instanceCountMap.get(ownerIdMap.get(row.id) ?? row.id) ?? 0,
        participant_count: participantCountMap.get(ownerIdMap.get(row.id) ?? row.id) ?? 0,
        recording_count: recordingCountMap.get(ownerIdMap.get(row.id) ?? row.id) ?? 0,
        can_sync: Boolean(
          ((row.sync_owner_zoom_meeting_id ?? row.zoom_meeting_id) && (row.sync_owner_videoconference_id ?? row.id))
          || row.status === 'CREATED_UNMATCHED'
        ),
        is_sync_owner: row.link_mode !== 'INHERITED',
        sync_owner_videoconference_id: row.sync_owner_videoconference_id,
      };
    });

    return {
      items,
      totals: {
        total,
        matched: summaryRows
          .filter((item) => recordString(item, 'status') === 'MATCHED')
          .reduce((sum, item) => sum + recordNumber(item, 'count'), 0),
        errors: summaryRows
          .filter((item) => recordString(item, 'status') === 'ERROR')
          .reduce((sum, item) => sum + recordNumber(item, 'count'), 0),
        pending_audit: summaryRows
          .filter((item) => recordString(item, 'audit_sync_status') === 'PENDING')
          .reduce((sum, item) => sum + recordNumber(item, 'count'), 0),
      },
      page,
      page_size: pageSize,
    };
  }

  async getPlanningVideoconferenceAuditDetail(id: string) {
    const rows = await this.listPlanningAuditRows([id]);
    const row = rows[0] ?? null;
    if (!row) {
      throw new NotFoundException(`No existe la videoconferencia ${id}.`);
    }

    const syncOwnerId = row.sync_owner_videoconference_id || row.id;
    const teacher = row.effective_teacher_id
      ? await this.teachersRepo.findOne({ where: { id: row.effective_teacher_id } })
      : null;
    const instances = await this.meetingInstancesRepo.find({
      where: { planning_subsection_videoconference_id: syncOwnerId },
      order: { actual_start: 'DESC', created_at: 'DESC' },
    });
    const bundles = await this.loadInstanceBundles(instances);

    return {
      record: {
        id: row.id,
        planning_offer_id: row.planning_offer_id,
        planning_section_id: row.planning_section_id,
        planning_subsection_id: row.planning_subsection_id,
        planning_subsection_schedule_id: row.planning_subsection_schedule_id,
        conference_date: toDateOnly(row.conference_date),
        day_of_week: row.day_of_week,
        start_time: row.start_time,
        end_time: row.end_time,
        scheduled_start: row.scheduled_start,
        scheduled_end: row.scheduled_end,
        zoom_user_id: row.zoom_user_id,
        zoom_user_email: row.zoom_user_email,
        zoom_user_name: row.zoom_user_name,
        zoom_meeting_id: row.zoom_meeting_id,
        created_at: row.created_at,
        topic: row.topic,
        aula_virtual_name: row.aula_virtual_name,
        join_url: row.join_url,
        start_url: row.start_url,
        link_mode: row.link_mode,
        owner_videoconference_id: row.owner_videoconference_id,
        inheritance_mapping_id: row.inheritance_mapping_id,
        owner_label:
          row.link_mode === 'INHERITED'
            ? this.buildOwnerLabel({
                course_code: row.owner_course_code,
                course_name: row.owner_course_name,
                section_code: row.owner_section_external_code || row.owner_section_code,
                subsection_code: row.owner_subsection_code,
                day_of_week: row.owner_schedule_day_of_week,
                start_time: row.owner_schedule_start_time,
                end_time: row.owner_schedule_end_time,
              })
            : null,
        status: row.status,
        audit_sync_status: this.normalizeAuditSyncStatus(row.audit_sync_status),
        audit_synced_at: row.audit_synced_at,
        audit_sync_error: row.audit_sync_error,
        error_message: row.error_message,
        semester_id: row.semester_id,
        semester_name: row.semester_name,
        campus_id: row.campus_id,
        campus_name: row.campus_name,
        faculty_id: row.faculty_id,
        faculty_name: row.faculty_name,
        academic_program_id: row.academic_program_id,
        academic_program_name: row.academic_program_name,
        cycle: row.cycle,
        course_code: row.course_code,
        course_name: row.course_name,
        section_code: row.section_code,
        section_external_code: row.section_external_code,
        subsection_code: row.subsection_code,
        subsection_kind: row.subsection_kind,
        subsection_denomination: row.subsection_denomination,
        session_type: row.schedule_session_type,
        planning_sync_state: this.resolvePlanningSyncState(row),
        teacher_id: teacher?.id ?? row.effective_teacher_id,
        teacher_name: teacher?.full_name ?? teacher?.name ?? null,
        teacher_dni: teacher?.dni ?? null,
        can_sync: Boolean(
          ((row.sync_owner_zoom_meeting_id ?? row.zoom_meeting_id) && syncOwnerId)
          || row.status === 'CREATED_UNMATCHED'
        ),
        is_sync_owner: row.link_mode !== 'INHERITED',
        sync_owner_videoconference_id: syncOwnerId,
        needs_reconcile: !row.zoom_meeting_id && row.status === 'CREATED_UNMATCHED',
      },
      selected_instance_id: bundles[0]?.instance.id ?? null,
      instances: bundles.map((bundle) => ({
        ...bundle.instance,
        participants: bundle.participants,
        recordings: bundle.recordings,
        transcripts: bundle.transcripts,
        summaries: bundle.summaries,
      })),
    };
  }

  async syncPlanningVideoconference(id: string) {
    const requestedRecord = await this.planningVideoconferencesRepo.findOne({ where: { id } });
    if (!requestedRecord) {
      throw new NotFoundException(`No existe la videoconferencia ${id}.`);
    }
    const syncOwnerId =
      requestedRecord.link_mode === 'INHERITED' && requestedRecord.owner_videoconference_id
        ? requestedRecord.owner_videoconference_id
        : requestedRecord.id;
    const record =
      syncOwnerId === requestedRecord.id
        ? requestedRecord
        : await this.planningVideoconferencesRepo.findOne({ where: { id: syncOwnerId } });
    if (!record) {
      throw new NotFoundException(`No existe la videoconferencia owner ${syncOwnerId}.`);
    }
    if (!record.zoom_meeting_id) {
      // No Zoom meeting ID yet — try to reconcile (match) it like generation does.
      const reconciled = await this.videoconferenceService.reconcile(record.id);
      return this.getPlanningVideoconferenceAuditDetail(requestedRecord.id);
    }

    if (!record.start_url) {
      const activeMeeting = await this.zoomAccountService.getMeeting(record.zoom_meeting_id);
      if (activeMeeting?.start_url) {
        record.start_url = activeMeeting.start_url;
        await this.planningVideoconferencesRepo.save(record);
        if (record.id !== requestedRecord.id) {
          requestedRecord.start_url = activeMeeting.start_url;
          await this.planningVideoconferencesRepo.save(requestedRecord);
        }
      }
    }

    try {
      const instances = await this.zoomAccountService.listPastMeetingInstances(record.zoom_meeting_id);
      const seenInstanceIds = new Set<string>();
      for (const item of instances) {
        const detail = (await this.zoomAccountService.getPastMeetingDetail(item.uuid)) ?? {
          uuid: item.uuid,
          id: item.id,
          topic: item.topic,
          start_time: item.start_time,
          end_time: null,
          duration_minutes: item.duration_minutes,
          status: null,
          host_email: null,
          raw: item.raw,
        };
        const storedInstance = await this.upsertPlanningMeetingInstance(record, detail);
        seenInstanceIds.add(storedInstance.id);

        const [participants, recordings] = await Promise.all([
          this.zoomAccountService.listPastMeetingParticipants(item.uuid),
          this.zoomAccountService.listMeetingRecordings(item.uuid),
        ]);
        await this.replaceParticipantsForInstance(storedInstance, participants);
        await this.replaceRecordingsForInstance(storedInstance, recordings);
      }

      const staleInstances = await this.meetingInstancesRepo.find({
        where: { planning_subsection_videoconference_id: record.id },
        select: { id: true },
      });
      const staleIds = staleInstances.map((item) => item.id).filter((item) => !seenInstanceIds.has(item));
      if (staleIds.length > 0) {
        await this.purgeMeetingInstanceTrees(staleIds);
      }

      record.audit_sync_status = 'SYNCED';
      record.audit_synced_at = new Date();
      record.audit_sync_error = null;
      record.updated_at = new Date();
      await this.planningVideoconferencesRepo.save(record);
      await this.syncInheritedAuditState(record.id);
      return this.getPlanningVideoconferenceAuditDetail(requestedRecord.id);
    } catch (error) {
      record.audit_sync_status = 'ERROR';
      record.audit_synced_at = new Date();
      record.audit_sync_error = this.toErrorMessage(error);
      record.updated_at = new Date();
      await this.planningVideoconferencesRepo.save(record);
      await this.syncInheritedAuditState(record.id);
      throw error;
    }
  }

  private buildPlanningAuditQuery(filters: PlanningAuditFilters = {}) {
    const query = this.buildPlanningAuditBaseQuery(filters)
      .select('vc.id', 'id')
      .addSelect('vc.planning_offer_id', 'planning_offer_id')
      .addSelect('vc.planning_section_id', 'planning_section_id')
      .addSelect('vc.planning_subsection_id', 'planning_subsection_id')
      .addSelect('vc.planning_subsection_schedule_id', 'planning_subsection_schedule_id')
      .addSelect('vc.conference_date', 'conference_date')
      .addSelect('vc.day_of_week', 'day_of_week')
      .addSelect('vc.start_time', 'start_time')
      .addSelect('vc.end_time', 'end_time')
      .addSelect('vc.scheduled_start', 'scheduled_start')
      .addSelect('vc.scheduled_end', 'scheduled_end')
      .addSelect('vc.zoom_user_id', 'zoom_user_id')
      .addSelect('vc.zoom_user_email', 'zoom_user_email')
      .addSelect('vc.zoom_user_name', 'zoom_user_name')
      .addSelect('vc.zoom_meeting_id', 'zoom_meeting_id')
      .addSelect('vc.created_at', 'created_at')
      .addSelect('vc.topic', 'topic')
      .addSelect('vc.aula_virtual_name', 'aula_virtual_name')
      .addSelect('vc.join_url', 'join_url')
      .addSelect('vc.start_url', 'start_url')
      .addSelect('vc.link_mode', 'link_mode')
      .addSelect('vc.owner_videoconference_id', 'owner_videoconference_id')
      .addSelect('vc.inheritance_mapping_id', 'inheritance_mapping_id')
      .addSelect('owner_vc.id', 'sync_owner_videoconference_id')
      .addSelect('owner_vc.topic', 'sync_owner_topic')
      .addSelect('owner_vc.zoom_meeting_id', 'sync_owner_zoom_meeting_id')
      .addSelect('owner_vc.join_url', 'sync_owner_join_url')
      .addSelect('owner_vc.start_url', 'sync_owner_start_url')
      .addSelect('owner_offer.course_code', 'owner_course_code')
      .addSelect('owner_offer.course_name', 'owner_course_name')
      .addSelect('owner_section.code', 'owner_section_code')
      .addSelect('owner_section.external_code', 'owner_section_external_code')
      .addSelect('owner_subsection.code', 'owner_subsection_code')
      .addSelect('owner_schedule.day_of_week', 'owner_schedule_day_of_week')
      .addSelect('owner_schedule.start_time', 'owner_schedule_start_time')
      .addSelect('owner_schedule.end_time', 'owner_schedule_end_time')
      .addSelect('vc.status', 'status')
      .addSelect('vc.audit_sync_status', 'audit_sync_status')
      .addSelect('vc.audit_synced_at', 'audit_synced_at')
      .addSelect('vc.audit_sync_error', 'audit_sync_error')
      .addSelect('vc.error_message', 'error_message')
      .addSelect('offer.semester_id', 'semester_id')
      .addSelect('semester.name', 'semester_name')
      .addSelect('offer.campus_id', 'campus_id')
      .addSelect('campus.name', 'campus_name')
      .addSelect('offer.faculty_id', 'faculty_id')
      .addSelect('faculty.name', 'faculty_name')
      .addSelect('offer.academic_program_id', 'academic_program_id')
      .addSelect('program.name', 'academic_program_name')
      .addSelect('offer.cycle', 'cycle')
      .addSelect('offer.course_code', 'course_code')
      .addSelect('offer.course_name', 'course_name')
      .addSelect('section.code', 'section_code')
      .addSelect('section.external_code', 'section_external_code')
      .addSelect('subsection.code', 'subsection_code')
      .addSelect('subsection.kind', 'subsection_kind')
      .addSelect('subsection.denomination', 'subsection_denomination')
      .addSelect('schedule.day_of_week', 'schedule_day_of_week')
      .addSelect('schedule.start_time', 'schedule_start_time')
      .addSelect('schedule.end_time', 'schedule_end_time')
      .addSelect('schedule.session_type', 'schedule_session_type')
      .addSelect(
        'COALESCE(schedule.teacher_id, subsection.responsible_teacher_id, section.teacher_id)',
        'effective_teacher_id',
      )
      .orderBy('vc.scheduled_start', 'DESC')
      .addOrderBy('vc.created_at', 'DESC');

    return query;
  }

  private buildPlanningAuditBaseQuery(filters: PlanningAuditFilters = {}) {
    const query = this.planningVideoconferencesRepo
      .createQueryBuilder('vc')
      .leftJoin(PlanningOfferEntity, 'offer', 'offer.id = vc.planning_offer_id')
      .leftJoin(PlanningSectionEntity, 'section', 'section.id = vc.planning_section_id')
      .leftJoin(PlanningSubsectionEntity, 'subsection', 'subsection.id = vc.planning_subsection_id')
      .leftJoin(
        PlanningSubsectionScheduleEntity,
        'schedule',
        'schedule.id = vc.planning_subsection_schedule_id',
      )
      .leftJoin(
        PlanningSubsectionVideoconferenceEntity,
        'owner_vc',
        'owner_vc.id = COALESCE(vc.owner_videoconference_id, vc.id)',
      )
      .leftJoin(PlanningOfferEntity, 'owner_offer', 'owner_offer.id = owner_vc.planning_offer_id')
      .leftJoin(PlanningSectionEntity, 'owner_section', 'owner_section.id = owner_vc.planning_section_id')
      .leftJoin(
        PlanningSubsectionEntity,
        'owner_subsection',
        'owner_subsection.id = owner_vc.planning_subsection_id',
      )
      .leftJoin(
        PlanningSubsectionScheduleEntity,
        'owner_schedule',
        'owner_schedule.id = owner_vc.planning_subsection_schedule_id',
      )
      .leftJoin(SemesterEntity, 'semester', 'semester.id = offer.semester_id')
      .leftJoin(CampusEntity, 'campus', 'campus.id = offer.campus_id')
      .leftJoin(FacultyEntity, 'faculty', 'faculty.id = offer.faculty_id')
      .leftJoin(AcademicProgramEntity, 'program', 'program.id = offer.academic_program_id');

    this.applyPlanningAuditFilters(query, filters);
    return query;
  }

  private async listPlanningAuditRows(ids?: string[]) {
    return this.buildPlanningAuditQuery({ ids }).getRawMany<PlanningAuditListRow>();
  }

  private applyPlanningAuditFilters(
    query: SelectQueryBuilder<PlanningSubsectionVideoconferenceEntity>,
    filters: PlanningAuditFilters,
  ) {
    if (filters.ids?.length) {
      query.andWhere('vc.id IN (:...ids)', { ids: filters.ids });
    }
    if (filters.semester_id) {
      query.andWhere('offer.semester_id = :semesterId', { semesterId: filters.semester_id });
    }
    if (filters.campus_id) {
      query.andWhere('offer.campus_id = :campusId', { campusId: filters.campus_id });
    }
    if (filters.faculty_id) {
      query.andWhere('offer.faculty_id = :facultyId', { facultyId: filters.faculty_id });
    }
    if (filters.academic_program_id) {
      query.andWhere('offer.academic_program_id = :programId', {
        programId: filters.academic_program_id,
      });
    }
    if (filters.status) {
      query.andWhere('vc.status = :status', { status: filters.status });
    }
    if (filters.audit_sync_status) {
      query.andWhere('COALESCE(vc.audit_sync_status, :pendingAudit) = :auditStatus', {
        pendingAudit: 'PENDING',
        auditStatus: filters.audit_sync_status,
      });
    }
    if (filters.hide_inherited) {
      query.andWhere("vc.link_mode != 'INHERITED'");
    }
    const search = `${filters.search ?? ''}`.trim();
    if (search) {
      const searchLike = `%${search.toLowerCase()}%`;
      query.andWhere(
        `(
          LOWER(COALESCE(vc.zoom_meeting_id, '')) LIKE :searchLike OR
          LOWER(COALESCE(offer.course_code, '')) LIKE :searchLike OR
          LOWER(COALESCE(offer.course_name, '')) LIKE :searchLike OR
          LOWER(COALESCE(section.external_code, section.code, '')) LIKE :searchLike OR
          LOWER(COALESCE(subsection.code, '')) LIKE :searchLike OR
          LOWER(COALESCE(campus.name, '')) LIKE :searchLike OR
          LOWER(COALESCE(faculty.name, '')) LIKE :searchLike OR
          LOWER(COALESCE(program.name, '')) LIKE :searchLike
        )`,
        { searchLike },
      );
    }
  }

  private async countInstancesByPlanningVideoconference(ids: string[]) {
    if (!ids.length) {
      return [] as Array<{ id: string; count: number }>;
    }
    const rows = await this.meetingInstancesRepo
      .createQueryBuilder('instance')
      .select('instance.planning_subsection_videoconference_id', 'id')
      .addSelect('COUNT(*)', 'count')
      .where('instance.planning_subsection_videoconference_id IN (:...ids)', { ids })
      .groupBy('instance.planning_subsection_videoconference_id')
      .getRawMany<Record<string, unknown>>();

    return rows.map((row) => ({
      id: recordString(row, 'id') ?? '',
      count: recordNumber(row, 'count'),
    }));
  }

  private async countParticipantsByPlanningVideoconference(ids: string[]) {
    if (!ids.length) {
      return [] as Array<{ id: string; count: number }>;
    }
    const rows = await this.meetingInstancesRepo
      .createQueryBuilder('instance')
      .innerJoin(
        MeetingParticipantEntity,
        'participant',
        'participant.meeting_instance_id = instance.id',
      )
      .select('instance.planning_subsection_videoconference_id', 'id')
      .addSelect('COUNT(participant.id)', 'count')
      .where('instance.planning_subsection_videoconference_id IN (:...ids)', { ids })
      .groupBy('instance.planning_subsection_videoconference_id')
      .getRawMany<Record<string, unknown>>();

    return rows.map((row) => ({
      id: recordString(row, 'id') ?? '',
      count: recordNumber(row, 'count'),
    }));
  }

  private async countRecordingsByPlanningVideoconference(ids: string[]) {
    if (!ids.length) {
      return [] as Array<{ id: string; count: number }>;
    }
    const rows = await this.meetingInstancesRepo
      .createQueryBuilder('instance')
      .innerJoin(
        MeetingRecordingEntity,
        'recording',
        'recording.meeting_instance_id = instance.id',
      )
      .select('instance.planning_subsection_videoconference_id', 'id')
      .addSelect('COUNT(recording.id)', 'count')
      .where('instance.planning_subsection_videoconference_id IN (:...ids)', { ids })
      .groupBy('instance.planning_subsection_videoconference_id')
      .getRawMany<Record<string, unknown>>();

    return rows.map((row) => ({
      id: recordString(row, 'id') ?? '',
      count: recordNumber(row, 'count'),
    }));
  }

  private async loadInstanceBundles(instances: MeetingInstanceEntity[]): Promise<PlanningAuditInstanceBundle[]> {
    if (!instances.length) {
      return [];
    }
    const instanceIds = instances.map((item) => item.id);
    const [participants, recordings, transcripts, summaries] = await Promise.all([
      this.participantsRepo.find({
        where: { meeting_instance_id: In(instanceIds) },
        order: { display_name: 'ASC' },
      }),
      this.recordingsRepo.find({
        where: { meeting_instance_id: In(instanceIds) },
        order: { created_at: 'DESC' },
      }),
      this.transcriptsRepo.find({
        where: { meeting_instance_id: In(instanceIds) },
        order: { created_at: 'DESC' },
      }),
      this.summariesRepo.find({
        where: { meeting_instance_id: In(instanceIds) },
        order: { created_at: 'DESC' },
      }),
    ]);

    const participantsByInstance = groupBy(participants, (item) => item.meeting_instance_id);
    const recordingsByInstance = groupBy(recordings, (item) => item.meeting_instance_id);
    const transcriptsByInstance = groupBy(transcripts, (item) => item.meeting_instance_id);
    const summariesByInstance = groupBy(summaries, (item) => item.meeting_instance_id);

    return instances.map((instance) => ({
      instance,
      participants: participantsByInstance.get(instance.id) ?? [],
      recordings: recordingsByInstance.get(instance.id) ?? [],
      transcripts: transcriptsByInstance.get(instance.id) ?? [],
      summaries: summariesByInstance.get(instance.id) ?? [],
    }));
  }

  private async upsertPlanningMeetingInstance(
    record: PlanningSubsectionVideoconferenceEntity,
    detail: {
      uuid: string;
      id: string | null;
      topic: string | null;
      start_time: string | null;
      end_time: string | null;
      duration_minutes: number | null;
      status: string | null;
      raw: Record<string, unknown>;
    },
  ) {
    const now = new Date();
    const existing = await this.meetingInstancesRepo.findOne({
      where: {
        planning_subsection_videoconference_id: record.id,
        zoom_meeting_uuid: detail.uuid,
      },
    });
    const next = this.meetingInstancesRepo.create({
      ...(existing ?? {
        id: newId(),
        created_at: now,
        video_conference_id: null,
        planning_subsection_videoconference_id: record.id,
      }),
      planning_subsection_videoconference_id: record.id,
      zoom_meeting_id: detail.id ?? record.zoom_meeting_id ?? '',
      zoom_meeting_uuid: detail.uuid,
      scheduled_start: record.scheduled_start ?? null,
      scheduled_end: record.scheduled_end ?? null,
      actual_start: detail.start_time ? new Date(detail.start_time) : null,
      actual_end: detail.end_time ? new Date(detail.end_time) : null,
      duration_minutes: detail.duration_minutes ?? null,
      status: this.normalizeMeetingInstanceStatus(detail.status, detail.end_time),
      raw_json: detail.raw ?? null,
      updated_at: now,
    });
    return this.meetingInstancesRepo.save(next);
  }

  private async replaceParticipantsForInstance(
    instance: MeetingInstanceEntity,
    participants: Array<{
      zoom_participant_id: string | null;
      zoom_user_id: string | null;
      display_name: string;
      email: string | null;
      role: string | null;
    }>,
  ) {
    const existing = await this.participantsRepo.find({
      where: { meeting_instance_id: instance.id },
      select: { id: true },
    });
    if (existing.length > 0) {
      await this.segmentsRepo.delete({ participant_id: In(existing.map((item) => item.id)) });
      await this.participantsRepo.delete({ meeting_instance_id: instance.id });
    }
    await this.teacherMetricsRepo.delete({ meeting_instance_id: instance.id });

    if (!participants.length) {
      return;
    }

    const uniqueParticipants = dedupeParticipants(participants);
    const now = new Date();
    await this.participantsRepo.save(
      uniqueParticipants.map((participant) =>
        this.participantsRepo.create({
          id: newId(),
          meeting_instance_id: instance.id,
          zoom_participant_id: participant.zoom_participant_id,
          zoom_user_id: participant.zoom_user_id,
          display_name: participant.display_name,
          email: participant.email,
          role: this.normalizeParticipantRole(participant.role),
          teacher_id: null,
          created_at: now,
        }),
      ),
    );
  }

  private async replaceRecordingsForInstance(
    instance: MeetingInstanceEntity,
    recordings: Array<{
      zoom_recording_id: string | null;
      recording_type: string | null;
      file_extension: string | null;
      file_size_bytes: string | null;
      download_url: string | null;
      play_url: string | null;
      start_time: string | null;
      end_time: string | null;
      status: string | null;
      raw: Record<string, unknown>;
    }>,
  ) {
    await this.recordingsRepo.delete({ meeting_instance_id: instance.id });
    if (!recordings.length) {
      return;
    }

    const now = new Date();
    await this.recordingsRepo.save(
      recordings.map((recording) =>
        this.recordingsRepo.create({
          id: newId(),
          meeting_instance_id: instance.id,
          zoom_recording_id: recording.zoom_recording_id,
          recording_type: this.normalizeRecordingType(
            recording.recording_type,
            recording.file_extension,
          ),
          file_extension: recording.file_extension,
          file_size_bytes: recording.file_size_bytes,
          download_url: recording.download_url,
          play_url: recording.play_url,
          start_time: recording.start_time ? new Date(recording.start_time) : null,
          end_time: recording.end_time ? new Date(recording.end_time) : null,
          status: this.normalizeRecordingStatus(recording.status),
          raw_json: recording.raw ?? null,
          created_at: now,
        }),
      ),
    );
  }

  private async purgeMeetingInstanceTrees(instanceIds: string[]) {
    if (!instanceIds.length) {
      return;
    }
    const participants = await this.participantsRepo.find({
      where: { meeting_instance_id: In(instanceIds) },
      select: { id: true },
    });
    const participantIds = participants.map((item) => item.id);
    if (participantIds.length > 0) {
      await this.segmentsRepo.delete({ participant_id: In(participantIds) });
    }
    await this.teacherMetricsRepo.delete({ meeting_instance_id: In(instanceIds) });
    await this.transcriptsRepo.delete({ meeting_instance_id: In(instanceIds) });
    await this.summariesRepo.delete({ meeting_instance_id: In(instanceIds) });
    await this.recordingsRepo.delete({ meeting_instance_id: In(instanceIds) });
    await this.participantsRepo.delete({ meeting_instance_id: In(instanceIds) });
    await this.meetingInstancesRepo.delete({ id: In(instanceIds) });
  }

  private async syncInheritedAuditState(ownerId: string) {
    const children = await this.planningVideoconferencesRepo.find({
      where: { owner_videoconference_id: ownerId, link_mode: 'INHERITED' },
    });
    if (!children.length) {
      return;
    }
    const owner = await this.planningVideoconferencesRepo.findOne({ where: { id: ownerId } });
    if (!owner) {
      return;
    }
    const now = new Date();
    await this.planningVideoconferencesRepo.save(
      children.map((child) =>
        this.planningVideoconferencesRepo.merge(child, {
          zoom_user_id: owner.zoom_user_id,
          zoom_user_email: owner.zoom_user_email,
          zoom_user_name: owner.zoom_user_name,
          zoom_meeting_id: owner.zoom_meeting_id,
          topic: owner.topic,
          join_url: owner.join_url,
          start_url: owner.start_url,
          status: owner.status,
          audit_sync_status: owner.audit_sync_status,
          audit_synced_at: owner.audit_synced_at,
          audit_sync_error: owner.audit_sync_error,
          updated_at: now,
        }),
      ),
    );
  }

  private buildOwnerLabel(input: {
    course_code: string | null;
    course_name: string | null;
    section_code: string | null;
    subsection_code: string | null;
    day_of_week: string | null;
    start_time: string | null;
    end_time: string | null;
  }) {
    const course = [input.course_code, input.course_name].filter(Boolean).join(' - ') || 'Sin curso';
    const section = input.section_code ? `Seccion ${input.section_code}` : 'Seccion sin codigo';
    const group = input.subsection_code ? `Grupo ${input.subsection_code}` : 'Grupo sin codigo';
    const schedule =
      input.day_of_week && input.start_time && input.end_time
        ? `${input.day_of_week} ${shortTime(input.start_time)}-${shortTime(input.end_time)}`
        : 'Horario sin definir';
    return `${course} | ${section} | ${group} | ${schedule}`;
  }

  private normalizeAuditSyncStatus(value: string | null | undefined) {
    return PlanningSubsectionVideoconferenceAuditStatusValues.includes(
      value as (typeof PlanningSubsectionVideoconferenceAuditStatusValues)[number],
    )
      ? value
      : 'PENDING';
  }

  private resolvePlanningSyncState(row: PlanningAuditListRow) {
    if (!row.section_code) {
      return 'SECTION_REMOVED';
    }
    if (!row.subsection_code) {
      return 'GROUP_REMOVED';
    }
    if (!row.schedule_day_of_week || !row.schedule_start_time || !row.schedule_end_time) {
      return 'SCHEDULE_REMOVED';
    }

    const sameDay = `${row.schedule_day_of_week}` === `${row.day_of_week}`;
    const sameStart = shortTime(`${row.schedule_start_time}`) === shortTime(`${row.start_time}`);
    const sameEnd = shortTime(`${row.schedule_end_time}`) === shortTime(`${row.end_time}`);

    return sameDay && sameStart && sameEnd ? 'ALIGNED' : 'OUTDATED';
  }

  private normalizeMeetingInstanceStatus(value: string | null | undefined, endTime?: string | null) {
    const normalized = `${value ?? ''}`.trim().toUpperCase();
    if (['ERROR', 'FAILED'].includes(normalized)) {
      return 'ERROR' as const;
    }
    if (endTime || ['ENDED', 'STOPPED', 'FINISHED', 'COMPLETED'].includes(normalized)) {
      return 'ENDED' as const;
    }
    if (['IN_PROGRESS', 'STARTED', 'RUNNING', 'LIVE'].includes(normalized)) {
      return 'IN_PROGRESS' as const;
    }
    return 'CREATED' as const;
  }

  private normalizeParticipantRole(value: string | null | undefined) {
    const normalized = `${value ?? ''}`.trim().toUpperCase();
    if (normalized === 'HOST') {
      return 'HOST' as const;
    }
    if (['CO_HOST', 'CO-HOST', 'COHOST'].includes(normalized)) {
      return 'CO_HOST' as const;
    }
    if (normalized === 'PANELIST') {
      return 'PANELIST' as const;
    }
    if (['ATTENDEE', 'PARTICIPANT'].includes(normalized)) {
      return 'ATTENDEE' as const;
    }
    return 'UNKNOWN' as const;
  }

  private normalizeRecordingType(value: string | null | undefined, extension?: string | null) {
    const normalized = `${value ?? extension ?? ''}`.trim().toUpperCase();
    if (normalized === 'MP4') {
      return 'MP4' as const;
    }
    if (normalized === 'M4A') {
      return 'M4A' as const;
    }
    if (normalized === 'CHAT') {
      return 'CHAT' as const;
    }
    if (['TRANSCRIPT', 'TRANSCRIPT_FILE'].includes(normalized)) {
      return 'TRANSCRIPT' as const;
    }
    if (normalized === 'VTT') {
      return 'VTT' as const;
    }
    return 'OTHER' as const;
  }

  private normalizeRecordingStatus(value: string | null | undefined) {
    const normalized = `${value ?? ''}`.trim().toUpperCase();
    if (normalized === 'DELETED') {
      return 'DELETED' as const;
    }
    if (normalized === 'EXPIRED') {
      return 'EXPIRED' as const;
    }
    if (normalized === 'ERROR') {
      return 'ERROR' as const;
    }
    return 'AVAILABLE' as const;
  }

  private toErrorMessage(error: unknown) {
    if (error instanceof Error && error.message) {
      return error.message;
    }
    if (typeof error === 'string') {
      return error;
    }
    return 'No se pudo sincronizar la reunion con Zoom.';
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

    const byTeacher = new Map<string, {
      join_count: number;
      total_minutes: number;
      first_join: Date;
      last_leave: Date | null;
      is_host: boolean;
    }>();

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

function groupBy<T>(items: T[], keySelector: (item: T) => string) {
  const grouped = new Map<string, T[]>();
  for (const item of items) {
    const key = keySelector(item);
    const bucket = grouped.get(key) ?? [];
    bucket.push(item);
    grouped.set(key, bucket);
  }
  return grouped;
}

function dedupeParticipants(
  participants: Array<{
    zoom_participant_id: string | null;
    zoom_user_id: string | null;
    display_name: string;
    email: string | null;
    role: string | null;
  }>,
) {
  const seen = new Set<string>();
  const unique: typeof participants = [];

  for (const participant of participants) {
    const displayName = `${participant.display_name ?? ''}`.trim().toUpperCase();
    const email = `${participant.email ?? ''}`.trim().toLowerCase();
    const zoomParticipantId = `${participant.zoom_participant_id ?? ''}`.trim();
    const zoomUserId = `${participant.zoom_user_id ?? ''}`.trim();
    const role = `${participant.role ?? ''}`.trim().toUpperCase();
    const key =
      [displayName, email, role].filter((value) => value !== '').join('|') ||
      zoomParticipantId ||
      zoomUserId;

    if (!key || seen.has(key)) {
      continue;
    }

    seen.add(key);
    unique.push(participant);
  }

  return unique;
}

function recordString(row: Record<string, unknown>, key: string) {
  const value = row[key];
  if (value === undefined || value === null) {
    return null;
  }
  const stringValue = `${value}`.trim();
  return stringValue || null;
}

function recordNumber(row: Record<string, unknown>, key: string) {
  const value = row[key];
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function shortTime(value: string | null | undefined) {
  const normalized = `${value ?? ''}`.trim();
  return normalized ? normalized.slice(0, 5) : '';
}
