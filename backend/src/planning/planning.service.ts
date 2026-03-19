import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository } from 'typeorm';
import { newId } from '../common';
import {
  AcademicProgramEntity,
  CampusEntity,
  ClassroomEntity,
  CourseEntity,
  CourseSectionEntity,
  SemesterEntity,
  StudyPlanEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
  ClassGroupEntity,
  ClassGroupTeacherEntity,
  ClassMeetingEntity,
  ClassOfferingEntity,
  ClassTeacherEntity,
  ConflictSeverityValues,
  ConflictTypeValues,
  CourseSectionHourRequirementEntity,
  ScheduleConflictEntity,
} from '../entities/planning.entities';
import {
  BulkAssignClassroomDto,
  BulkAssignTeacherDto,
  BulkDuplicateDto,
  CreateClassGroupDto,
  CreateClassGroupTeacherDto,
  CreateClassMeetingDto,
  CreateClassOfferingDto,
  CreateClassTeacherDto,
  CreateCourseSectionHourRequirementDto,
  UpdateClassGroupDto,
  UpdateClassGroupTeacherDto,
  UpdateClassMeetingDto,
  UpdateClassOfferingDto,
  UpdateClassTeacherDto,
  UpdateCourseSectionHourRequirementDto,
  UpdatePlanningWorkspaceRowDto,
} from './dto/planning.dto';

type ConflictType = (typeof ConflictTypeValues)[number];
type ConflictSeverity = (typeof ConflictSeverityValues)[number];
type WorkspaceState = 'DRAFT' | 'IN_PROGRESS' | 'COMPLETE' | 'BLOCKED';
type WorkspaceFilters = {
  semesterId?: string;
  campusId?: string;
  academicProgramId?: string;
  studyPlanId?: string;
  deliveryModalityId?: string;
  shiftId?: string;
  search?: string;
};

type WorkspaceAlert = {
  code: string;
  severity: ConflictSeverity;
  message: string;
  blocking: boolean;
};

type WorkspaceTeacher = {
  assignment_id: string | null;
  teacher_id: string;
  full_name: string | null;
  role: string;
  is_primary: boolean;
};

type WorkspaceRow = {
  row_id: string;
  row_kind: 'GROUP' | 'MEETING';
  offering_id: string;
  group_id: string;
  meeting_id: string | null;
  semester_id: string;
  semester_name: string | null;
  campus_id: string;
  campus_name: string | null;
  academic_program_id: string;
  academic_program_name: string | null;
  study_plan_id: string;
  study_plan_name: string | null;
  course_id: string;
  course_code: string | null;
  course_name: string | null;
  course_section_id: string;
  course_section_name: string | null;
  delivery_modality_id: string;
  shift_id: string;
  projected_vacancies: number | null;
  offering_status: boolean;
  group_type: string;
  group_code: string;
  group_capacity: number | null;
  group_note: string | null;
  teachers: WorkspaceTeacher[];
  primary_teacher_name: string | null;
  day_of_week: string | null;
  start_time: string | null;
  end_time: string | null;
  minutes: number;
  academic_hours: number | null;
  classroom_id: string | null;
  classroom_name: string | null;
  classroom_code: string | null;
  alerts: WorkspaceAlert[];
  alert_count: number;
  blocking_alert_count: number;
};

type WorkspaceSummary = {
  offering_id: string;
  semester_id: string;
  course_id: string;
  course_code: string | null;
  course_name: string | null;
  academic_program_name: string | null;
  study_plan_name: string | null;
  campus_name: string | null;
  group_count: number;
  row_count: number;
  scheduled_row_count: number;
  teacher_count: number;
  total_alerts: number;
  blocking_alerts: number;
  hours_required: {
    theory: number;
    practice: number;
    lab: number;
  };
  hours_planned: {
    theory: number;
    practice: number;
    lab: number;
  };
  state: WorkspaceState;
};

type HoursValidationSnapshot = {
  planned: {
    theory: number;
    practice: number;
    lab: number;
  };
  expected: {
    theory: number;
    practice: number;
    lab: number;
  };
  diff: {
    theory: number;
    practice: number;
    lab: number;
  };
};

type WorkspaceTarget = {
  offering: ClassOfferingEntity;
  group: ClassGroupEntity;
  meeting: ClassMeetingEntity | null;
};

@Injectable()
export class PlanningService {
  constructor(
    @InjectRepository(ClassOfferingEntity)
    private readonly offeringsRepo: Repository<ClassOfferingEntity>,
    @InjectRepository(ClassGroupEntity)
    private readonly groupsRepo: Repository<ClassGroupEntity>,
    @InjectRepository(ClassMeetingEntity)
    private readonly meetingsRepo: Repository<ClassMeetingEntity>,
    @InjectRepository(ClassTeacherEntity)
    private readonly classTeachersRepo: Repository<ClassTeacherEntity>,
    @InjectRepository(ClassGroupTeacherEntity)
    private readonly groupTeachersRepo: Repository<ClassGroupTeacherEntity>,
    @InjectRepository(CourseSectionHourRequirementEntity)
    private readonly hourRequirementsRepo: Repository<CourseSectionHourRequirementEntity>,
    @InjectRepository(ScheduleConflictEntity)
    private readonly conflictsRepo: Repository<ScheduleConflictEntity>,
    @InjectRepository(SemesterEntity)
    private readonly semestersRepo: Repository<SemesterEntity>,
    @InjectRepository(CampusEntity)
    private readonly campusesRepo: Repository<CampusEntity>,
    @InjectRepository(AcademicProgramEntity)
    private readonly programsRepo: Repository<AcademicProgramEntity>,
    @InjectRepository(StudyPlanEntity)
    private readonly studyPlansRepo: Repository<StudyPlanEntity>,
    @InjectRepository(CourseEntity)
    private readonly coursesRepo: Repository<CourseEntity>,
    @InjectRepository(TeacherEntity)
    private readonly teachersRepo: Repository<TeacherEntity>,
    @InjectRepository(ClassroomEntity)
    private readonly classroomsRepo: Repository<ClassroomEntity>,
    @InjectRepository(CourseSectionEntity)
    private readonly courseSectionsRepo: Repository<CourseSectionEntity>,
  ) {}

  listClassOfferings(semesterId?: string) {
    return this.offeringsRepo.find({
      where: semesterId ? { semester_id: semesterId } : {},
      order: { id: 'ASC' },
    });
  }

  async listWorkspace(filters: WorkspaceFilters) {
    const offerings = (await this.offeringsRepo.find({ order: { id: 'ASC' } })).filter((offering) =>
      this.matchOfferingFilters(offering, filters),
    );

    if (offerings.length === 0) {
      return {
        filters,
        summaries: [] as WorkspaceSummary[],
        rows: [] as WorkspaceRow[],
        totals: {
          offerings: 0,
          rows: 0,
          alerts: 0,
          blocking_alerts: 0,
        },
      };
    }

    const offeringIds = offerings.map((offering) => offering.id);
    const [
      groups,
      meetings,
      classTeachers,
      hourRequirements,
      semesters,
      campuses,
      programs,
      studyPlans,
      courses,
      courseSections,
    ] = await Promise.all([
      this.groupsRepo.find({ where: { class_offering_id: In(offeringIds) }, order: { code: 'ASC' } }),
      this.meetingsRepo.find({
        where: { class_offering_id: In(offeringIds) },
        order: { day_of_week: 'ASC', start_time: 'ASC' },
      }),
      this.classTeachersRepo.find({ where: { class_offering_id: In(offeringIds) } }),
      this.hourRequirementsRepo.find(),
      this.findManyByIds(this.semestersRepo, uniqueIds(offerings.map((item) => item.semester_id))),
      this.findManyByIds(this.campusesRepo, uniqueIds(offerings.map((item) => item.campus_id))),
      this.findManyByIds(this.programsRepo, uniqueIds(offerings.map((item) => item.academic_program_id))),
      this.findManyByIds(this.studyPlansRepo, uniqueIds(offerings.map((item) => item.study_plan_id))),
      this.findManyByIds(this.coursesRepo, uniqueIds(offerings.map((item) => item.course_id))),
      this.findManyByIds(this.courseSectionsRepo, uniqueIds(offerings.map((item) => item.course_section_id))),
    ]);
    const classrooms = await this.findManyByIds(
      this.classroomsRepo,
      uniqueIds(meetings.map((item) => item.classroom_id)),
    );

    const groupIds = groups.map((group) => group.id);
    const groupTeachers = groupIds.length
      ? await this.groupTeachersRepo.find({ where: { class_group_id: In(groupIds) }, order: { created_at: 'ASC' } })
      : [];
    const allTeacherIds = uniqueIds([
      ...classTeachers.map((item) => item.teacher_id),
      ...groupTeachers.map((item) => item.teacher_id),
    ]);
    const teachers = await this.findManyByIds(this.teachersRepo, allTeacherIds);
    const conflicts = this.calculateConflictsForDataset(
      offerings,
      meetings,
      groups,
      groupTeachers,
      classTeachers,
    );

    const context = {
      semesterMap: toMap(semesters),
      campusMap: toMap(campuses),
      programMap: toMap(programs),
      studyPlanMap: toMap(studyPlans),
      courseMap: toMap(courses),
      teacherMap: toMap(teachers),
      classroomMap: toMap(classrooms),
      courseSectionMap: toMap(courseSections),
      groupsByOffering: groupBy(groups, (item) => item.class_offering_id),
      meetingsByGroup: groupBy(meetings, (item) => item.class_group_id),
      groupTeachersByGroup: groupBy(groupTeachers, (item) => item.class_group_id),
      classTeachersByOffering: groupBy(classTeachers, (item) => item.class_offering_id),
      conflictAlertsByMeeting: this.buildConflictAlertMap(conflicts),
      validationByOffering: this.buildHoursValidationMap(offerings, groups, meetings, hourRequirements),
    };

    const rows = offerings.flatMap((offering) => this.buildWorkspaceRows(offering, context));
    const searchedRows = rows.filter((row) => this.matchRowSearch(row, filters.search));
    const includedOfferingIds = new Set(searchedRows.map((row) => row.offering_id));
    const searchedOfferings = offerings.filter((offering) => {
      if (includedOfferingIds.has(offering.id)) {
        return true;
      }
      if (!filters.search) {
        return true;
      }
      return this.matchOfferingSearch(offering, context, filters.search);
    });

    const summaries = searchedOfferings.map((offering) =>
      this.buildWorkspaceSummary(
        offering,
        searchedRows.filter((row) => row.offering_id === offering.id),
        context,
      ),
    );
    const totals = {
      offerings: summaries.length,
      rows: searchedRows.length,
      alerts: searchedRows.reduce((sum, row) => sum + row.alert_count, 0),
      blocking_alerts: searchedRows.reduce((sum, row) => sum + row.blocking_alert_count, 0),
    };

    return {
      filters,
      summaries,
      rows: searchedRows,
      totals,
    };
  }

  async getClassOffering(id: string) {
    const found = await this.offeringsRepo.findOne({ where: { id } });
    if (!found) {
      throw new NotFoundException(`class_offerings ${id} not found`);
    }
    return found;
  }

  createClassOffering(dto: CreateClassOfferingDto) {
    return this.offeringsRepo.save(this.offeringsRepo.create(dto));
  }

  async updateClassOffering(id: string, dto: UpdateClassOfferingDto) {
    await this.offeringsRepo.update({ id }, dto);
    return this.getClassOffering(id);
  }

  async deleteClassOffering(id: string) {
    await this.offeringsRepo.delete({ id });
    return { deleted: true, id };
  }

  listClassGroups(classOfferingId?: string) {
    return this.groupsRepo.find({
      where: classOfferingId ? { class_offering_id: classOfferingId } : {},
      order: { id: 'ASC' },
    });
  }

  createClassGroup(dto: CreateClassGroupDto) {
    return this.groupsRepo.save(this.groupsRepo.create(dto));
  }

  async updateClassGroup(id: string, dto: UpdateClassGroupDto) {
    await this.groupsRepo.update({ id }, dto);
    return this.groupsRepo.findOne({ where: { id } });
  }

  async deleteClassGroup(id: string) {
    await this.groupsRepo.delete({ id });
    return { deleted: true, id };
  }

  listClassMeetings(classOfferingId?: string) {
    return this.meetingsRepo.find({
      where: classOfferingId ? { class_offering_id: classOfferingId } : {},
      order: { id: 'ASC' },
    });
  }

  createClassMeeting(dto: CreateClassMeetingDto) {
    return this.meetingsRepo.save(
      this.meetingsRepo.create({
        ...dto,
        minutes: dto.minutes ?? computeMinutesFromTimes(dto.start_time, dto.end_time),
      }),
    );
  }

  async updateClassMeeting(id: string, dto: UpdateClassMeetingDto) {
    const current = await this.meetingsRepo.findOne({ where: { id } });
    if (!current) {
      throw new NotFoundException(`class_meetings ${id} not found`);
    }
    const startTime = dto.start_time ?? current.start_time;
    const endTime = dto.end_time ?? current.end_time;
    await this.meetingsRepo.update(
      { id },
      {
        ...dto,
        minutes: dto.minutes ?? computeMinutesFromTimes(startTime, endTime),
      },
    );
    return this.meetingsRepo.findOne({ where: { id } });
  }

  async deleteClassMeeting(id: string) {
    await this.meetingsRepo.delete({ id });
    return { deleted: true, id };
  }

  listClassTeachers(classOfferingId?: string) {
    return this.classTeachersRepo.find({
      where: classOfferingId ? { class_offering_id: classOfferingId } : {},
      order: { id: 'ASC' },
    });
  }

  createClassTeacher(dto: CreateClassTeacherDto) {
    return this.classTeachersRepo.save(this.classTeachersRepo.create(dto));
  }

  async updateClassTeacher(id: string, dto: UpdateClassTeacherDto) {
    await this.classTeachersRepo.update({ id }, dto);
    return this.classTeachersRepo.findOne({ where: { id } });
  }

  async deleteClassTeacher(id: string) {
    await this.classTeachersRepo.delete({ id });
    return { deleted: true, id };
  }

  listClassGroupTeachers(classGroupId?: string) {
    return this.groupTeachersRepo.find({
      where: classGroupId ? { class_group_id: classGroupId } : {},
      order: { id: 'ASC' },
    });
  }

  createClassGroupTeacher(dto: CreateClassGroupTeacherDto) {
    const now = new Date();
    return this.groupTeachersRepo.save(
      this.groupTeachersRepo.create({
        ...dto,
        assigned_from: dto.assigned_from ? new Date(dto.assigned_from) : null,
        assigned_to: dto.assigned_to ? new Date(dto.assigned_to) : null,
        created_at: now,
        updated_at: now,
      }),
    );
  }

  async updateClassGroupTeacher(id: string, dto: UpdateClassGroupTeacherDto) {
    await this.groupTeachersRepo.update(
      { id },
      {
        ...dto,
        assigned_from: dto.assigned_from ? new Date(dto.assigned_from) : undefined,
        assigned_to: dto.assigned_to ? new Date(dto.assigned_to) : undefined,
        updated_at: new Date(),
      },
    );
    return this.groupTeachersRepo.findOne({ where: { id } });
  }

  async deleteClassGroupTeacher(id: string) {
    await this.groupTeachersRepo.delete({ id });
    return { deleted: true, id };
  }

  listHourRequirements(courseSectionId?: string) {
    return this.hourRequirementsRepo.find({
      where: courseSectionId ? { course_section_id: courseSectionId } : {},
      order: { id: 'ASC' },
    });
  }

  createHourRequirement(dto: CreateCourseSectionHourRequirementDto) {
    const now = new Date();
    return this.hourRequirementsRepo.save(
      this.hourRequirementsRepo.create({
        ...dto,
        created_at: now,
        updated_at: now,
      }),
    );
  }

  async updateHourRequirement(id: string, dto: UpdateCourseSectionHourRequirementDto) {
    await this.hourRequirementsRepo.update({ id }, { ...dto, updated_at: new Date() });
    return this.hourRequirementsRepo.findOne({ where: { id } });
  }

  async deleteHourRequirement(id: string) {
    await this.hourRequirementsRepo.delete({ id });
    return { deleted: true, id };
  }

  async validateHours(classOfferingId: string) {
    const offering = await this.offeringsRepo.findOne({ where: { id: classOfferingId } });
    if (!offering) {
      throw new NotFoundException(`class_offerings ${classOfferingId} not found`);
    }

    const requirement = await this.hourRequirementsRepo.findOne({
      where: { course_section_id: offering.course_section_id },
    });
    if (!requirement) {
      throw new NotFoundException(
        `course_section_hour_requirements not found for course_section_id ${offering.course_section_id}`,
      );
    }

    const groups = await this.groupsRepo.find({ where: { class_offering_id: classOfferingId } });
    const meetings = await this.meetingsRepo.find({ where: { class_offering_id: classOfferingId } });
    const snapshot = this.computeHoursValidation(offering, groups, meetings, requirement);

    return {
      class_offering_id: classOfferingId,
      course_section_id: offering.course_section_id,
      course_format: requirement.course_format,
      expected: snapshot.expected,
      planned: snapshot.planned,
      diff: snapshot.diff,
      compliant:
        snapshot.diff.theory >= 0 &&
        snapshot.diff.practice >= 0 &&
        snapshot.diff.lab >= 0,
      computed_at: new Date().toISOString(),
    };
  }

  async listConflicts(semesterId?: string) {
    const conflicts = await this.conflictsRepo.find({
      where: semesterId ? { semester_id: semesterId } : {},
      order: { detected_at: 'DESC' },
    });
    return this.enrichConflicts(conflicts);
  }

  async detectConflicts(semesterId: string) {
    const offerings = await this.offeringsRepo.find({ where: { semester_id: semesterId } });
    const conflicts = await this.buildPersistedConflicts(offerings);
    await this.conflictsRepo.delete({ semester_id: semesterId });
    if (conflicts.length > 0) {
      await this.conflictsRepo.save(conflicts);
    }
    return { semester_id: semesterId, created: conflicts.length };
  }

  async updateWorkspaceRow(rowId: string, dto: UpdatePlanningWorkspaceRowDto) {
    const target = await this.resolveWorkspaceTarget(rowId);
    const { offering, group } = target;

    if (dto.projected_vacancies !== undefined || dto.offering_status !== undefined) {
      await this.offeringsRepo.update(
        { id: offering.id },
        {
          projected_vacancies: dto.projected_vacancies ?? offering.projected_vacancies,
          status: dto.offering_status ?? offering.status,
        },
      );
    }

    if (dto.group_code !== undefined || dto.capacity !== undefined || dto.group_note !== undefined) {
      await this.groupsRepo.update(
        { id: group.id },
        {
          code: dto.group_code ?? group.code,
          capacity: dto.capacity ?? group.capacity,
          note: dto.group_note ?? group.note,
        },
      );
    }

    if (dto.clear_teacher) {
      await this.groupTeachersRepo.delete({ class_group_id: group.id });
    } else if (dto.teacher_id) {
      await this.assignTeacherToGroup(group.id, dto.teacher_id, dto.teacher_role ?? 'PRIMARY', true);
    }

    const shouldTouchMeeting =
      dto.day_of_week !== undefined ||
      dto.start_time !== undefined ||
      dto.end_time !== undefined ||
      dto.minutes !== undefined ||
      dto.academic_hours !== undefined ||
      dto.classroom_id !== undefined ||
      dto.clear_classroom;

    if (shouldTouchMeeting) {
      const meeting = await this.upsertMeetingForWorkspace(target, dto);
      return this.getWorkspaceRowById(meeting.id, offering.semester_id);
    }

    if (rowId.startsWith('group:')) {
      return this.getWorkspaceRowById(rowId, offering.semester_id);
    }
    return this.getWorkspaceRowById(target.meeting?.id ?? `group:${group.id}`, offering.semester_id);
  }

  async bulkAssignTeacher(dto: BulkAssignTeacherDto) {
    const targets = await this.resolveSelection(dto.row_ids);
    const groupIds = [...new Set(targets.map((item) => item.group.id))];
    for (const groupId of groupIds) {
      await this.assignTeacherToGroup(groupId, dto.teacher_id, dto.role ?? 'PRIMARY', dto.is_primary ?? true);
    }
    return {
      updated_groups: groupIds.length,
      teacher_id: dto.teacher_id,
    };
  }

  async bulkAssignClassroom(dto: BulkAssignClassroomDto) {
    const targets = await this.resolveSelection(dto.row_ids);
    const meetingIds = [...new Set(targets.map((item) => item.meeting?.id).filter(Boolean) as string[])];
    if (meetingIds.length === 0) {
      return {
        updated_meetings: 0,
        skipped: dto.row_ids.length,
      };
    }

    for (const meetingId of meetingIds) {
      await this.meetingsRepo.update(
        { id: meetingId },
        {
          classroom_id: dto.clear_classroom ? null : dto.classroom_id ?? null,
        },
      );
    }

    return {
      updated_meetings: meetingIds.length,
    };
  }

  async bulkDuplicate(dto: BulkDuplicateDto) {
    if (dto.duplicate_group && dto.source_group_id) {
      return this.duplicateGroup(dto.source_group_id);
    }
    if (dto.source_row_id && dto.target_group_ids?.length) {
      return this.duplicateMeetingToGroups(dto.source_row_id, dto.target_group_ids);
    }
    throw new BadRequestException('bulk duplicate requires source_group_id or source_row_id + target_group_ids');
  }

  private async duplicateGroup(sourceGroupId: string) {
    const sourceGroup = await this.groupsRepo.findOne({ where: { id: sourceGroupId } });
    if (!sourceGroup) {
      throw new NotFoundException(`class_groups ${sourceGroupId} not found`);
    }

    const sourceTeachers = await this.groupTeachersRepo.find({ where: { class_group_id: sourceGroupId } });
    const sourceMeetings = await this.meetingsRepo.find({ where: { class_group_id: sourceGroupId } });
    const duplicatedGroup = this.groupsRepo.create({
      ...sourceGroup,
      id: newId(),
      code: await this.nextGroupCode(sourceGroup.class_offering_id, sourceGroup.code),
    });
    await this.groupsRepo.save(duplicatedGroup);

    const now = new Date();
    if (sourceTeachers.length > 0) {
      await this.groupTeachersRepo.save(
        sourceTeachers.map((teacher) =>
          this.groupTeachersRepo.create({
            ...teacher,
            id: newId(),
            class_group_id: duplicatedGroup.id,
            created_at: now,
            updated_at: now,
          }),
        ),
      );
    }

    if (sourceMeetings.length > 0) {
      await this.meetingsRepo.save(
        sourceMeetings.map((meeting) =>
          this.meetingsRepo.create({
            ...meeting,
            id: newId(),
            class_group_id: duplicatedGroup.id,
          }),
        ),
      );
    }

    return {
      duplicated_group_id: duplicatedGroup.id,
      created_meetings: sourceMeetings.length,
      created_teachers: sourceTeachers.length,
    };
  }

  private async duplicateMeetingToGroups(sourceRowId: string, targetGroupIds: string[]) {
    const sourceTarget = await this.resolveWorkspaceTarget(sourceRowId);
    if (!sourceTarget.meeting) {
      throw new BadRequestException('source_row_id must point to a scheduled row');
    }

    const targetGroups = await this.groupsRepo.find({ where: { id: In(targetGroupIds) } });
    if (targetGroups.length === 0) {
      throw new NotFoundException('target groups not found');
    }

    const created = await this.meetingsRepo.save(
      targetGroups.map((group) =>
        this.meetingsRepo.create({
          ...sourceTarget.meeting,
          id: newId(),
          class_group_id: group.id,
          class_offering_id: group.class_offering_id,
        }),
      ),
    );

    return {
      created_meetings: created.length,
      meeting_ids: created.map((item) => item.id),
    };
  }

  private async assignTeacherToGroup(
    groupId: string,
    teacherId: string,
    role: ClassGroupTeacherEntity['role'],
    isPrimary: boolean,
  ) {
    const assignments = await this.groupTeachersRepo.find({ where: { class_group_id: groupId } });
    const sameTeacher = assignments.find((item) => item.teacher_id === teacherId);
    const primary = assignments.find((item) => item.is_primary);
    const now = new Date();

    if (sameTeacher) {
      await this.groupTeachersRepo.update(
        { id: sameTeacher.id },
        {
          role,
          is_primary: isPrimary,
          updated_at: now,
        },
      );
    } else if (primary) {
      await this.groupTeachersRepo.update(
        { id: primary.id },
        {
          teacher_id: teacherId,
          role,
          is_primary: isPrimary,
          updated_at: now,
        },
      );
    } else {
      await this.groupTeachersRepo.save(
        this.groupTeachersRepo.create({
          id: newId(),
          class_group_id: groupId,
          teacher_id: teacherId,
          role,
          is_primary: isPrimary,
          assigned_from: null,
          assigned_to: null,
          notes: null,
          created_at: now,
          updated_at: now,
        }),
      );
    }

    if (isPrimary) {
      const refreshed = await this.groupTeachersRepo.find({ where: { class_group_id: groupId } });
      const primaryAssignment = refreshed.find((item) => item.teacher_id === teacherId);
      if (primaryAssignment) {
        const secondaryIds = refreshed
          .filter((item) => item.id !== primaryAssignment.id && item.is_primary)
          .map((item) => item.id);
        if (secondaryIds.length > 0) {
          await this.groupTeachersRepo.update({ id: In(secondaryIds) }, { is_primary: false, updated_at: now });
        }
      }
    }
  }

  private async upsertMeetingForWorkspace(target: WorkspaceTarget, dto: UpdatePlanningWorkspaceRowDto) {
    const currentMeeting = target.meeting;
    if (!currentMeeting) {
      if (!dto.day_of_week || !dto.start_time || !dto.end_time) {
        throw new BadRequestException('day_of_week, start_time and end_time are required to create a meeting');
      }
      return this.meetingsRepo.save(
        this.meetingsRepo.create({
          id: newId(),
          class_offering_id: target.offering.id,
          class_group_id: target.group.id,
          day_of_week: dto.day_of_week,
          start_time: dto.start_time,
          end_time: dto.end_time,
          minutes: dto.minutes ?? computeMinutesFromTimes(dto.start_time, dto.end_time),
          academic_hours: dto.academic_hours ?? null,
          classroom_id: dto.clear_classroom ? null : dto.classroom_id ?? null,
        }),
      );
    }

    const startTime = dto.start_time ?? currentMeeting.start_time;
    const endTime = dto.end_time ?? currentMeeting.end_time;
    await this.meetingsRepo.update(
      { id: currentMeeting.id },
      {
        day_of_week: dto.day_of_week ?? currentMeeting.day_of_week,
        start_time: startTime,
        end_time: endTime,
        minutes: dto.minutes ?? computeMinutesFromTimes(startTime, endTime),
        academic_hours: dto.academic_hours ?? currentMeeting.academic_hours,
        classroom_id: dto.clear_classroom ? null : dto.classroom_id ?? currentMeeting.classroom_id,
      },
    );

    const updated = await this.meetingsRepo.findOne({ where: { id: currentMeeting.id } });
    if (!updated) {
      throw new NotFoundException(`class_meetings ${currentMeeting.id} not found`);
    }
    return updated;
  }

  private async resolveSelection(rowIds: string[]) {
    const targets: WorkspaceTarget[] = [];
    for (const rowId of rowIds) {
      targets.push(await this.resolveWorkspaceTarget(rowId));
    }
    return targets;
  }

  private async resolveWorkspaceTarget(rowId: string): Promise<WorkspaceTarget> {
    if (rowId.startsWith('group:')) {
      const groupId = rowId.slice('group:'.length);
      const group = await this.groupsRepo.findOne({ where: { id: groupId } });
      if (!group) {
        throw new NotFoundException(`class_groups ${groupId} not found`);
      }
      const offering = await this.getClassOffering(group.class_offering_id);
      return {
        offering,
        group,
        meeting: null,
      };
    }

    const meeting = await this.meetingsRepo.findOne({ where: { id: rowId } });
    if (meeting) {
      const group = await this.groupsRepo.findOne({ where: { id: meeting.class_group_id } });
      if (!group) {
        throw new NotFoundException(`class_groups ${meeting.class_group_id} not found`);
      }
      const offering = await this.getClassOffering(meeting.class_offering_id);
      return {
        offering,
        group,
        meeting,
      };
    }

    const group = await this.groupsRepo.findOne({ where: { id: rowId } });
    if (group) {
      const offering = await this.getClassOffering(group.class_offering_id);
      return {
        offering,
        group,
        meeting: null,
      };
    }

    throw new NotFoundException(`workspace row ${rowId} not found`);
  }

  private async getWorkspaceRowById(rowId: string, semesterId?: string) {
    const workspace = await this.listWorkspace({ semesterId });
    const row = workspace.rows.find((item) => item.row_id === rowId);
    if (!row) {
      throw new NotFoundException(`workspace row ${rowId} not found`);
    }
    return row;
  }

  private buildWorkspaceRows(
    offering: ClassOfferingEntity,
    context: {
      semesterMap: Map<string, SemesterEntity>;
      campusMap: Map<string, CampusEntity>;
      programMap: Map<string, AcademicProgramEntity>;
      studyPlanMap: Map<string, StudyPlanEntity>;
      courseMap: Map<string, CourseEntity>;
      teacherMap: Map<string, TeacherEntity>;
      classroomMap: Map<string, ClassroomEntity>;
      courseSectionMap: Map<string, CourseSectionEntity>;
      groupsByOffering: Map<string, ClassGroupEntity[]>;
      meetingsByGroup: Map<string, ClassMeetingEntity[]>;
      groupTeachersByGroup: Map<string, ClassGroupTeacherEntity[]>;
      classTeachersByOffering: Map<string, ClassTeacherEntity[]>;
      conflictAlertsByMeeting: Map<string, WorkspaceAlert[]>;
      validationByOffering: Map<string, HoursValidationSnapshot | null>;
    },
  ) {
    const groups = context.groupsByOffering.get(offering.id) ?? [];
    if (groups.length === 0) {
      return [] as WorkspaceRow[];
    }

    return groups.flatMap((group) => {
      const teachers = this.resolveWorkspaceTeachers(
        group.id,
        offering.id,
        context.groupTeachersByGroup,
        context.classTeachersByOffering,
        context.teacherMap,
      );
      const hoursValidation = context.validationByOffering.get(offering.id) ?? null;
      const sharedAlerts = this.buildSharedRowAlerts(group, teachers, hoursValidation);
      const meetings = context.meetingsByGroup.get(group.id) ?? [];
      if (meetings.length === 0) {
        return [
          this.makeWorkspaceRow(offering, group, null, teachers, sharedAlerts, context),
        ];
      }
      return meetings.map((meeting) =>
        this.makeWorkspaceRow(
          offering,
          group,
          meeting,
          teachers,
          [...sharedAlerts, ...(context.conflictAlertsByMeeting.get(meeting.id) ?? [])],
          context,
        ),
      );
    });
  }

  private makeWorkspaceRow(
    offering: ClassOfferingEntity,
    group: ClassGroupEntity,
    meeting: ClassMeetingEntity | null,
    teachers: WorkspaceTeacher[],
    alerts: WorkspaceAlert[],
    context: {
      semesterMap: Map<string, SemesterEntity>;
      campusMap: Map<string, CampusEntity>;
      programMap: Map<string, AcademicProgramEntity>;
      studyPlanMap: Map<string, StudyPlanEntity>;
      courseMap: Map<string, CourseEntity>;
      classroomMap: Map<string, ClassroomEntity>;
      courseSectionMap: Map<string, CourseSectionEntity>;
    },
  ): WorkspaceRow {
    const classroom = meeting?.classroom_id ? context.classroomMap.get(meeting.classroom_id) : undefined;
    const normalizedAlerts = dedupeAlerts(alerts);
    const primaryTeacher = teachers.find((item) => item.is_primary) ?? teachers[0] ?? null;
    if (meeting && !meeting.classroom_id) {
      normalizedAlerts.push({
        code: 'NO_CLASSROOM',
        severity: 'WARNING',
        message: 'Falta aula asignada',
        blocking: true,
      });
    }

    return {
      row_id: meeting?.id ?? `group:${group.id}`,
      row_kind: meeting ? 'MEETING' : 'GROUP',
      offering_id: offering.id,
      group_id: group.id,
      meeting_id: meeting?.id ?? null,
      semester_id: offering.semester_id,
      semester_name: context.semesterMap.get(offering.semester_id)?.name ?? null,
      campus_id: offering.campus_id,
      campus_name: context.campusMap.get(offering.campus_id)?.name ?? null,
      academic_program_id: offering.academic_program_id,
      academic_program_name: context.programMap.get(offering.academic_program_id)?.name ?? null,
      study_plan_id: offering.study_plan_id,
      study_plan_name: context.studyPlanMap.get(offering.study_plan_id)?.name ?? null,
      course_id: offering.course_id,
      course_code: context.courseMap.get(offering.course_id)?.code ?? null,
      course_name: context.courseMap.get(offering.course_id)?.name ?? null,
      course_section_id: offering.course_section_id,
      course_section_name: context.courseSectionMap.get(offering.course_section_id)?.text ?? null,
      delivery_modality_id: offering.delivery_modality_id,
      shift_id: offering.shift_id,
      projected_vacancies: offering.projected_vacancies,
      offering_status: offering.status,
      group_type: group.group_type,
      group_code: group.code,
      group_capacity: group.capacity,
      group_note: group.note,
      teachers,
      primary_teacher_name: primaryTeacher?.full_name ?? null,
      day_of_week: meeting?.day_of_week ?? null,
      start_time: meeting?.start_time ?? null,
      end_time: meeting?.end_time ?? null,
      minutes: meeting?.minutes ?? 0,
      academic_hours: meeting?.academic_hours ?? null,
      classroom_id: meeting?.classroom_id ?? null,
      classroom_name: classroom?.name ?? null,
      classroom_code: classroom?.code ?? null,
      alerts: normalizedAlerts,
      alert_count: normalizedAlerts.length,
      blocking_alert_count: normalizedAlerts.filter((item) => item.blocking).length,
    };
  }

  private buildWorkspaceSummary(
    offering: ClassOfferingEntity,
    rows: WorkspaceRow[],
    context: {
      courseMap: Map<string, CourseEntity>;
      programMap: Map<string, AcademicProgramEntity>;
      studyPlanMap: Map<string, StudyPlanEntity>;
      campusMap: Map<string, CampusEntity>;
      validationByOffering: Map<string, HoursValidationSnapshot | null>;
      groupsByOffering: Map<string, ClassGroupEntity[]>;
    },
  ): WorkspaceSummary {
    const groupedRows = rows.filter((row) => row.offering_id === offering.id);
    const uniqueTeachers = new Set(
      groupedRows.flatMap((row) => row.teachers.map((teacher) => teacher.teacher_id)),
    );
    const validation = context.validationByOffering.get(offering.id) ?? null;
    const groups = context.groupsByOffering.get(offering.id) ?? [];
    const totalAlerts = groupedRows.reduce((sum, row) => sum + row.alert_count, 0);
    const blockingAlerts = groupedRows.reduce((sum, row) => sum + row.blocking_alert_count, 0);
    const state = this.resolveWorkspaceState(groups.length, groupedRows.length, blockingAlerts, totalAlerts);

    return {
      offering_id: offering.id,
      semester_id: offering.semester_id,
      course_id: offering.course_id,
      course_code: context.courseMap.get(offering.course_id)?.code ?? null,
      course_name: context.courseMap.get(offering.course_id)?.name ?? null,
      academic_program_name: context.programMap.get(offering.academic_program_id)?.name ?? null,
      study_plan_name: context.studyPlanMap.get(offering.study_plan_id)?.name ?? null,
      campus_name: context.campusMap.get(offering.campus_id)?.name ?? null,
      group_count: groups.length,
      row_count: groupedRows.length,
      scheduled_row_count: groupedRows.filter((row) => row.meeting_id).length,
      teacher_count: uniqueTeachers.size,
      total_alerts: totalAlerts,
      blocking_alerts: blockingAlerts,
      hours_required: validation?.expected ?? { theory: 0, practice: 0, lab: 0 },
      hours_planned: validation?.planned ?? { theory: 0, practice: 0, lab: 0 },
      state,
    };
  }

  private resolveWorkspaceState(
    groupCount: number,
    rowCount: number,
    blockingAlerts: number,
    totalAlerts: number,
  ): WorkspaceState {
    if (groupCount === 0 || rowCount === 0) {
      return 'DRAFT';
    }
    if (blockingAlerts > 0) {
      return 'BLOCKED';
    }
    if (totalAlerts > 0) {
      return 'IN_PROGRESS';
    }
    return 'COMPLETE';
  }

  private buildSharedRowAlerts(
    group: ClassGroupEntity,
    teachers: WorkspaceTeacher[],
    hoursValidation: HoursValidationSnapshot | null,
  ) {
    const alerts: WorkspaceAlert[] = [];
    if (teachers.length === 0) {
      alerts.push({
        code: 'NO_TEACHER',
        severity: 'WARNING',
        message: 'Falta docente asignado',
        blocking: true,
      });
    }

    if (hoursValidation) {
      const key = toHoursKey(group.group_type);
      const remaining = hoursValidation.diff[key];
      if (remaining < 0) {
        alerts.push({
          code: 'INCOMPLETE_HOURS',
          severity: 'WARNING',
          message: `Faltan ${Math.abs(remaining)} minutos para ${group.group_type}`,
          blocking: true,
        });
      }
    }

    return alerts;
  }

  private resolveWorkspaceTeachers(
    groupId: string,
    offeringId: string,
    groupTeachersByGroup: Map<string, ClassGroupTeacherEntity[]>,
    classTeachersByOffering: Map<string, ClassTeacherEntity[]>,
    teacherMap: Map<string, TeacherEntity>,
  ) {
    const groupTeachers = groupTeachersByGroup.get(groupId) ?? [];
    if (groupTeachers.length > 0) {
      return groupTeachers.map((assignment) => ({
        assignment_id: assignment.id,
        teacher_id: assignment.teacher_id,
        full_name: teacherMap.get(assignment.teacher_id)?.full_name ?? null,
        role: assignment.role,
        is_primary: assignment.is_primary,
      }));
    }

    return (classTeachersByOffering.get(offeringId) ?? []).map((assignment) => ({
      assignment_id: assignment.id,
      teacher_id: assignment.teacher_id,
      full_name: teacherMap.get(assignment.teacher_id)?.full_name ?? null,
      role: assignment.role,
      is_primary: assignment.is_primary,
    }));
  }

  private buildHoursValidationMap(
    offerings: ClassOfferingEntity[],
    groups: ClassGroupEntity[],
    meetings: ClassMeetingEntity[],
    requirements: CourseSectionHourRequirementEntity[],
  ) {
    const requirementsByCourseSection = new Map(requirements.map((item) => [item.course_section_id, item]));
    const groupsByOffering = groupBy(groups, (item) => item.class_offering_id);
    const meetingsByOffering = groupBy(meetings, (item) => item.class_offering_id);
    const result = new Map<string, HoursValidationSnapshot | null>();
    for (const offering of offerings) {
      const requirement = requirementsByCourseSection.get(offering.course_section_id) ?? null;
      if (!requirement) {
        result.set(offering.id, null);
        continue;
      }
      result.set(
        offering.id,
        this.computeHoursValidation(
          offering,
          groupsByOffering.get(offering.id) ?? [],
          meetingsByOffering.get(offering.id) ?? [],
          requirement,
        ),
      );
    }
    return result;
  }

  private computeHoursValidation(
    offering: ClassOfferingEntity,
    groups: ClassGroupEntity[],
    meetings: ClassMeetingEntity[],
    requirement: CourseSectionHourRequirementEntity,
  ): HoursValidationSnapshot {
    const groupMap = new Map(groups.map((group) => [group.id, group]));
    const planned = {
      theory: 0,
      practice: 0,
      lab: 0,
    };

    for (const meeting of meetings) {
      const group = groupMap.get(meeting.class_group_id);
      if (!group || meeting.class_offering_id !== offering.id) {
        continue;
      }
      const minutes = meeting.minutes ?? computeMinutesFromTimes(meeting.start_time, meeting.end_time);
      const key = toHoursKey(group.group_type);
      planned[key] += minutes;
    }

    const expected = {
      theory: requirement.theory_hours_academic * requirement.academic_minutes_per_hour,
      practice: requirement.practice_hours_academic * requirement.academic_minutes_per_hour,
      lab: requirement.lab_hours_academic * requirement.academic_minutes_per_hour,
    };

    return {
      planned,
      expected,
      diff: {
        theory: planned.theory - expected.theory,
        practice: planned.practice - expected.practice,
        lab: planned.lab - expected.lab,
      },
    };
  }

  private buildConflictAlertMap(conflicts: ScheduleConflictEntity[]) {
    const map = new Map<string, WorkspaceAlert[]>();
    for (const conflict of conflicts) {
      const alert: WorkspaceAlert = {
        code: conflict.conflict_type,
        severity: conflict.severity,
        message: describeConflict(conflict),
        blocking: conflict.severity !== 'INFO',
      };
      for (const meetingId of [conflict.meeting_a_id, conflict.meeting_b_id]) {
        if (!map.has(meetingId)) {
          map.set(meetingId, []);
        }
        map.get(meetingId)?.push(alert);
      }
    }
    return map;
  }

  private async buildPersistedConflicts(offerings: ClassOfferingEntity[]) {
    if (offerings.length === 0) {
      return [];
    }

    const offeringIds = offerings.map((offering) => offering.id);
    const meetings = await this.meetingsRepo.find({
      where: { class_offering_id: In(offeringIds) },
      order: { day_of_week: 'ASC', start_time: 'ASC' },
    });
    const groups = await this.groupsRepo.find({ where: { class_offering_id: In(offeringIds) } });
    const groupIds = groups.map((group) => group.id);
    const groupTeachers = groupIds.length
      ? await this.groupTeachersRepo.find({ where: { class_group_id: In(groupIds) } })
      : [];
    const classTeachers = await this.classTeachersRepo.find({ where: { class_offering_id: In(offeringIds) } });

    return this.calculateConflictsForDataset(offerings, meetings, groups, groupTeachers, classTeachers);
  }

  private calculateConflictsForDataset(
    offerings: ClassOfferingEntity[],
    meetings: ClassMeetingEntity[],
    groups: ClassGroupEntity[],
    groupTeachers: ClassGroupTeacherEntity[],
    classTeachers: ClassTeacherEntity[],
  ) {
    if (offerings.length === 0) {
      return [] as ScheduleConflictEntity[];
    }

    const offeringById = new Map(offerings.map((offering) => [offering.id, offering]));
    const meetingTeacherIds = this.buildMeetingTeacherMap(meetings, groupTeachers, classTeachers);
    const conflicts: ScheduleConflictEntity[] = [];
    const now = new Date();

    for (let i = 0; i < meetings.length; i += 1) {
      for (let j = i + 1; j < meetings.length; j += 1) {
        const a = meetings[i];
        const b = meetings[j];
        if (a.day_of_week !== b.day_of_week) {
          continue;
        }

        const overlapMinutes = overlap(a.start_time, a.end_time, b.start_time, b.end_time);
        if (overlapMinutes <= 0) {
          continue;
        }

        const semesterId = offeringById.get(a.class_offering_id)?.semester_id;
        if (!semesterId || semesterId !== offeringById.get(b.class_offering_id)?.semester_id) {
          continue;
        }

        if (a.classroom_id && b.classroom_id && a.classroom_id === b.classroom_id) {
          conflicts.push(
            this.newConflict(
              'CLASSROOM_OVERLAP',
              semesterId,
              overlapMinutes,
              a,
              b,
              now,
              undefined,
              a.classroom_id,
            ),
          );
        }

        if (a.class_group_id === b.class_group_id) {
          conflicts.push(
            this.newConflict('GROUP_OVERLAP', semesterId, overlapMinutes, a, b, now, undefined, undefined, a.class_group_id),
          );
        }

        if (a.class_offering_id === b.class_offering_id) {
          conflicts.push(
            this.newConflict(
              'SECTION_OVERLAP',
              semesterId,
              overlapMinutes,
              a,
              b,
              now,
              undefined,
              undefined,
              undefined,
              a.class_offering_id,
            ),
          );
        }

        const teachersA = meetingTeacherIds.get(a.id) ?? new Set<string>();
        const teachersB = meetingTeacherIds.get(b.id) ?? new Set<string>();
        const sharedTeachers = [...teachersA].filter((teacherId) => teachersB.has(teacherId));
        for (const teacherId of sharedTeachers) {
          conflicts.push(
            this.newConflict('TEACHER_OVERLAP', semesterId, overlapMinutes, a, b, now, teacherId),
          );
        }
      }
    }

    return conflicts;
  }

  private newConflict(
    type: ConflictType,
    semesterId: string,
    overlapMinutes: number,
    meetingA: ClassMeetingEntity,
    meetingB: ClassMeetingEntity,
    detectedAt: Date,
    teacherId?: string,
    classroomId?: string,
    classGroupId?: string,
    classOfferingId?: string,
  ): ScheduleConflictEntity {
    return this.conflictsRepo.create({
      id: newId(),
      semester_id: semesterId,
      conflict_type: type,
      severity: toSeverity(overlapMinutes),
      teacher_id: teacherId ?? null,
      classroom_id: classroomId ?? null,
      class_group_id: classGroupId ?? null,
      class_offering_id: classOfferingId ?? null,
      meeting_a_id: meetingA.id,
      meeting_b_id: meetingB.id,
      overlap_minutes: overlapMinutes,
      detail_json: {
        day_of_week: meetingA.day_of_week,
        meeting_a_start: meetingA.start_time,
        meeting_a_end: meetingA.end_time,
        meeting_b_start: meetingB.start_time,
        meeting_b_end: meetingB.end_time,
      },
      detected_at: detectedAt,
      created_at: detectedAt,
    });
  }

  private buildMeetingTeacherMap(
    meetings: ClassMeetingEntity[],
    groupTeachers: ClassGroupTeacherEntity[],
    classTeachers: ClassTeacherEntity[],
  ) {
    const groupTeacherMap = new Map<string, Set<string>>();
    for (const item of groupTeachers) {
      if (!groupTeacherMap.has(item.class_group_id)) {
        groupTeacherMap.set(item.class_group_id, new Set<string>());
      }
      groupTeacherMap.get(item.class_group_id)?.add(item.teacher_id);
    }

    const offeringTeacherMap = new Map<string, Set<string>>();
    for (const item of classTeachers) {
      if (!offeringTeacherMap.has(item.class_offering_id)) {
        offeringTeacherMap.set(item.class_offering_id, new Set<string>());
      }
      offeringTeacherMap.get(item.class_offering_id)?.add(item.teacher_id);
    }

    const meetingTeacherMap = new Map<string, Set<string>>();
    for (const meeting of meetings) {
      const fromGroup = groupTeacherMap.get(meeting.class_group_id);
      const fromOffering = offeringTeacherMap.get(meeting.class_offering_id);
      const teacherIds = new Set<string>();
      for (const teacherId of fromGroup ?? []) {
        teacherIds.add(teacherId);
      }
      if (teacherIds.size === 0) {
        for (const teacherId of fromOffering ?? []) {
          teacherIds.add(teacherId);
        }
      }
      meetingTeacherMap.set(meeting.id, teacherIds);
    }

    return meetingTeacherMap;
  }

  private matchOfferingFilters(offering: ClassOfferingEntity, filters: WorkspaceFilters) {
    return (
      (!filters.semesterId || offering.semester_id === filters.semesterId) &&
      (!filters.campusId || offering.campus_id === filters.campusId) &&
      (!filters.academicProgramId || offering.academic_program_id === filters.academicProgramId) &&
      (!filters.studyPlanId || offering.study_plan_id === filters.studyPlanId) &&
      (!filters.deliveryModalityId || offering.delivery_modality_id === filters.deliveryModalityId) &&
      (!filters.shiftId || offering.shift_id === filters.shiftId)
    );
  }

  private matchRowSearch(row: WorkspaceRow, search?: string) {
    if (!search) {
      return true;
    }
    const needle = search.toLowerCase();
    const haystack = [
      row.course_code,
      row.course_name,
      row.course_section_name,
      row.primary_teacher_name,
      row.classroom_name,
      row.classroom_code,
      row.group_code,
      row.group_type,
      row.delivery_modality_id,
      row.shift_id,
      ...row.teachers.map((teacher) => teacher.full_name),
      ...row.alerts.map((alert) => alert.message),
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();
    return haystack.includes(needle);
  }

  private matchOfferingSearch(
    offering: ClassOfferingEntity,
    context: {
      courseMap: Map<string, CourseEntity>;
      programMap: Map<string, AcademicProgramEntity>;
      studyPlanMap: Map<string, StudyPlanEntity>;
      campusMap: Map<string, CampusEntity>;
    },
    search: string,
  ) {
    const needle = search.toLowerCase();
    const haystack = [
      context.courseMap.get(offering.course_id)?.code,
      context.courseMap.get(offering.course_id)?.name,
      context.programMap.get(offering.academic_program_id)?.name,
      context.studyPlanMap.get(offering.study_plan_id)?.name,
      context.campusMap.get(offering.campus_id)?.name,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();
    return haystack.includes(needle);
  }

  private async nextGroupCode(classOfferingId: string, currentCode: string) {
    const groups = await this.groupsRepo.find({ where: { class_offering_id: classOfferingId } });
    const existing = new Set(groups.map((group) => group.code));
    const base = (currentCode || 'GRUPO').slice(0, 14);
    let counter = 2;
    let candidate = `${base}-${counter}`;
    while (existing.has(candidate)) {
      counter += 1;
      candidate = `${base}-${counter}`;
    }
    return candidate.slice(0, 20);
  }

  private async enrichConflicts(conflicts: ScheduleConflictEntity[]) {
    if (conflicts.length === 0) {
      return [];
    }

    const meetingIds = uniqueIds(
      conflicts.flatMap((item) => [item.meeting_a_id, item.meeting_b_id]),
    );
    const meetings = await this.findManyByIds(this.meetingsRepo, meetingIds);
    const meetingMap = toMap(meetings);

    const offeringIds = uniqueIds([
      ...conflicts.map((item) => item.class_offering_id),
      ...meetings.map((item) => item.class_offering_id),
    ]);
    const groupIds = uniqueIds([
      ...conflicts.map((item) => item.class_group_id),
      ...meetings.map((item) => item.class_group_id),
    ]);
    const teacherIds = uniqueIds(conflicts.map((item) => item.teacher_id));
    const classroomIds = uniqueIds([
      ...conflicts.map((item) => item.classroom_id),
      ...meetings.map((item) => item.classroom_id),
    ]);

    const [offerings, groups, teachers, classrooms, semesters] = await Promise.all([
      this.findManyByIds(this.offeringsRepo, offeringIds),
      this.findManyByIds(this.groupsRepo, groupIds),
      this.findManyByIds(this.teachersRepo, teacherIds),
      this.findManyByIds(this.classroomsRepo, classroomIds),
      this.findManyByIds(this.semestersRepo, uniqueIds(conflicts.map((item) => item.semester_id))),
    ]);

    const [programs, campuses, courses, courseSections] = await Promise.all([
      this.findManyByIds(this.programsRepo, uniqueIds(offerings.map((item) => item.academic_program_id))),
      this.findManyByIds(this.campusesRepo, uniqueIds(offerings.map((item) => item.campus_id))),
      this.findManyByIds(this.coursesRepo, uniqueIds(offerings.map((item) => item.course_id))),
      this.findManyByIds(this.courseSectionsRepo, uniqueIds(offerings.map((item) => item.course_section_id))),
    ]);

    const offeringMap = toMap(offerings);
    const groupMap = toMap(groups);
    const teacherMap = toMap(teachers);
    const classroomMap = toMap(classrooms);
    const semesterMap = toMap(semesters);
    const programMap = toMap(programs);
    const campusMap = toMap(campuses);
    const courseMap = toMap(courses);
    const courseSectionMap = toMap(courseSections);

    return conflicts.map((conflict) => {
      const meetingA = meetingMap.get(conflict.meeting_a_id);
      const meetingB = meetingMap.get(conflict.meeting_b_id);
      const offering =
        offeringMap.get(
          conflict.class_offering_id ?? meetingA?.class_offering_id ?? meetingB?.class_offering_id ?? '',
        ) ?? null;
      const group =
        groupMap.get(conflict.class_group_id ?? meetingA?.class_group_id ?? meetingB?.class_group_id ?? '') ?? null;
      const teacher = teacherMap.get(conflict.teacher_id ?? '') ?? null;
      const classroom = classroomMap.get(conflict.classroom_id ?? '') ?? null;
      const semester = semesterMap.get(conflict.semester_id) ?? null;
      const program = offering ? programMap.get(offering.academic_program_id) ?? null : null;
      const campus = offering ? campusMap.get(offering.campus_id) ?? null : null;
      const course = offering ? courseMap.get(offering.course_id) ?? null : null;
      const courseSection = offering ? courseSectionMap.get(offering.course_section_id) ?? null : null;
      const detail = (conflict.detail_json ?? {}) as Record<string, unknown>;

      return {
        ...conflict,
        semester_name: semester?.name ?? null,
        academic_program_id: offering?.academic_program_id ?? null,
        academic_program_name: program?.name ?? null,
        campus_name: campus?.name ?? null,
        teacher_name: formatTeacherDisplay(teacher),
        classroom_name: classroom?.name ?? null,
        course_code: course?.code ?? null,
        course_name: course?.name ?? null,
        section_name: courseSection?.text ?? courseSection?.section_id ?? null,
        affected_label: buildConflictAffectedLabel(conflict, teacher, course, courseSection, group),
        overlap_day: (detail['day_of_week'] as string | undefined) ?? meetingA?.day_of_week ?? meetingB?.day_of_week ?? null,
        overlap_start: (detail['overlap_start'] as string | undefined) ?? null,
        overlap_end: (detail['overlap_end'] as string | undefined) ?? null,
        meeting_a: buildConflictMeetingDetail(
          meetingA,
          offeringMap,
          groupMap,
          classroomMap,
          campusMap,
          programMap,
          courseMap,
          courseSectionMap,
        ),
        meeting_b: buildConflictMeetingDetail(
          meetingB,
          offeringMap,
          groupMap,
          classroomMap,
          campusMap,
          programMap,
          courseMap,
          courseSectionMap,
        ),
      };
    });
  }

  private async findManyByIds<T extends { id: string }>(repo: Repository<T>, ids: string[]) {
    if (ids.length === 0) {
      return [] as T[];
    }
    return repo.find({ where: { id: In(ids) as never } as never });
  }
}

function computeMinutesFromTimes(start: string, end: string): number {
  return Math.max(0, toMinutes(end) - toMinutes(start));
}

function toMinutes(time: string): number {
  const [hh = '0', mm = '0'] = time.split(':');
  return Number(hh) * 60 + Number(mm);
}

function overlap(startA: string, endA: string, startB: string, endB: string): number {
  const start = Math.max(toMinutes(startA), toMinutes(startB));
  const end = Math.min(toMinutes(endA), toMinutes(endB));
  return Math.max(0, end - start);
}

function toSeverity(overlapMinutes: number): ConflictSeverity {
  if (overlapMinutes >= 30) {
    return 'CRITICAL';
  }
  if (overlapMinutes >= 10) {
    return 'WARNING';
  }
  return 'INFO';
}

function toMap<T extends { id: string }>(items: T[]) {
  return new Map(items.map((item) => [item.id, item]));
}

function uniqueIds(ids: Array<string | null | undefined>) {
  return [...new Set(ids.filter((item): item is string => Boolean(item)))];
}

function groupBy<T>(items: T[], getKey: (item: T) => string) {
  const map = new Map<string, T[]>();
  for (const item of items) {
    const key = getKey(item);
    if (!map.has(key)) {
      map.set(key, []);
    }
    map.get(key)?.push(item);
  }
  return map;
}

function dedupeAlerts(alerts: WorkspaceAlert[]) {
  const map = new Map<string, WorkspaceAlert>();
  for (const alert of alerts) {
    map.set(`${alert.code}:${alert.message}`, alert);
  }
  return [...map.values()];
}

function describeConflict(conflict: ScheduleConflictEntity) {
  switch (conflict.conflict_type) {
    case 'TEACHER_OVERLAP':
      return 'Cruce de docente';
    case 'CLASSROOM_OVERLAP':
      return 'Cruce de aula';
    case 'GROUP_OVERLAP':
      return 'Cruce de grupo';
    case 'SECTION_OVERLAP':
      return 'Cruce de seccion';
    default:
      return 'Cruce detectado';
  }
}

function formatTeacherDisplay(teacher: TeacherEntity | null) {
  if (!teacher) {
    return null;
  }
  const name = teacher.full_name || teacher.name || null;
  if (!name) {
    return teacher.dni || null;
  }
  return teacher.dni ? `${teacher.dni} - ${name}` : name;
}

function buildConflictAffectedLabel(
  conflict: ScheduleConflictEntity,
  teacher: TeacherEntity | null,
  course: CourseEntity | null,
  courseSection: CourseSectionEntity | null,
  group: ClassGroupEntity | null,
) {
  if (conflict.conflict_type === 'TEACHER_OVERLAP') {
    return formatTeacherDisplay(teacher) ?? 'Docente sin asignar';
  }
  if (conflict.conflict_type === 'SECTION_OVERLAP') {
    const courseLabel = buildCourseLabel(course);
    const sectionLabel = buildSectionLabel(courseSection);
    return [courseLabel, sectionLabel].filter(Boolean).join(' · ') || group?.code || 'Seccion sin referencia';
  }
  return describeConflict(conflict);
}

function buildConflictMeetingDetail(
  meeting: ClassMeetingEntity | undefined,
  offeringMap: Map<string, ClassOfferingEntity>,
  groupMap: Map<string, ClassGroupEntity>,
  classroomMap: Map<string, ClassroomEntity>,
  campusMap: Map<string, CampusEntity>,
  programMap: Map<string, AcademicProgramEntity>,
  courseMap: Map<string, CourseEntity>,
  courseSectionMap: Map<string, CourseSectionEntity>,
) {
  if (!meeting) {
    return null;
  }
  const offering = offeringMap.get(meeting.class_offering_id) ?? null;
  const group = groupMap.get(meeting.class_group_id) ?? null;
  const classroom = classroomMap.get(meeting.classroom_id ?? '') ?? null;
  const campus = offering ? campusMap.get(offering.campus_id) ?? null : null;
  const program = offering ? programMap.get(offering.academic_program_id) ?? null : null;
  const course = offering ? courseMap.get(offering.course_id) ?? null : null;
  const courseSection = offering ? courseSectionMap.get(offering.course_section_id) ?? null : null;
  const courseLabel = buildCourseLabel(course);
  const sectionLabel = buildSectionLabel(courseSection);
  const summary = [
    courseLabel,
    sectionLabel,
    group?.code ? `Grupo ${group.code}` : null,
    `${displayDay(meeting.day_of_week)} ${compactTime(meeting.start_time)}-${compactTime(meeting.end_time)}`,
  ]
    .filter(Boolean)
    .join(' · ');

  return {
    id: meeting.id,
    offering_id: offering?.id ?? null,
    group_id: group?.id ?? null,
    academic_program_id: offering?.academic_program_id ?? null,
    academic_program_name: program?.name ?? null,
    campus_name: campus?.name ?? null,
    course_code: course?.code ?? null,
    course_name: course?.name ?? null,
    course_label: courseLabel,
    section_name: sectionLabel,
    group_code: group?.code ?? null,
    day_of_week: meeting.day_of_week,
    start_time: meeting.start_time,
    end_time: meeting.end_time,
    classroom_id: meeting.classroom_id ?? null,
    classroom_name: classroom?.name ?? null,
    summary,
  };
}

function buildCourseLabel(course: CourseEntity | null) {
  if (!course) {
    return null;
  }
  if (course.code && course.name) {
    return `${course.code} - ${course.name}`;
  }
  return course.code || course.name || null;
}

function buildSectionLabel(courseSection: CourseSectionEntity | null) {
  return courseSection?.text || courseSection?.section_id || null;
}

function displayDay(value: string | null | undefined) {
  switch (value) {
    case 'LUNES':
      return 'Lunes';
    case 'MARTES':
      return 'Martes';
    case 'MIERCOLES':
      return 'Miercoles';
    case 'JUEVES':
      return 'Jueves';
    case 'VIERNES':
      return 'Viernes';
    case 'SABADO':
      return 'Sabado';
    case 'DOMINGO':
      return 'Domingo';
    default:
      return value || 'Sin dia';
  }
}

function compactTime(value: string | null | undefined) {
  if (!value) {
    return '--:--';
  }
  return String(value).slice(0, 5);
}

function toHoursKey(groupType: string): 'theory' | 'practice' | 'lab' {
  if (groupType === 'PRACTICE') {
    return 'practice';
  }
  if (groupType === 'LAB') {
    return 'lab';
  }
  return 'theory';
}
