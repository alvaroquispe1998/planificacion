import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository } from 'typeorm';
import { newId } from '../common';
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
} from './dto/planning.dto';

type ConflictType = (typeof ConflictTypeValues)[number];
type ConflictSeverity = (typeof ConflictSeverityValues)[number];

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
  ) {}

  listClassOfferings() {
    return this.offeringsRepo.find({ order: { id: 'ASC' } });
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
    return this.meetingsRepo.save(this.meetingsRepo.create(dto));
  }

  async updateClassMeeting(id: string, dto: UpdateClassMeetingDto) {
    await this.meetingsRepo.update({ id }, dto);
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
    const groupMap = new Map(groups.map((group) => [group.id, group]));

    const meetings = await this.meetingsRepo.find({ where: { class_offering_id: classOfferingId } });
    const planned = {
      theory: 0,
      practice: 0,
      lab: 0,
    };

    for (const meeting of meetings) {
      const group = groupMap.get(meeting.class_group_id);
      if (!group) {
        continue;
      }
      const minutes = meeting.minutes ?? computeMinutesFromTimes(meeting.start_time, meeting.end_time);
      if (group.group_type === 'THEORY') {
        planned.theory += minutes;
      } else if (group.group_type === 'PRACTICE') {
        planned.practice += minutes;
      } else if (group.group_type === 'LAB') {
        planned.lab += minutes;
      }
    }

    const expected = {
      theory: requirement.theory_hours_academic * requirement.academic_minutes_per_hour,
      practice: requirement.practice_hours_academic * requirement.academic_minutes_per_hour,
      lab: requirement.lab_hours_academic * requirement.academic_minutes_per_hour,
    };

    const diff = {
      theory: planned.theory - expected.theory,
      practice: planned.practice - expected.practice,
      lab: planned.lab - expected.lab,
    };

    return {
      class_offering_id: classOfferingId,
      course_section_id: offering.course_section_id,
      course_format: requirement.course_format,
      expected,
      planned,
      diff,
      compliant: diff.theory >= 0 && diff.practice >= 0 && diff.lab >= 0,
      computed_at: new Date().toISOString(),
    };
  }

  listConflicts(semesterId?: string) {
    return this.conflictsRepo.find({
      where: semesterId ? { semester_id: semesterId } : {},
      order: { detected_at: 'DESC' },
    });
  }

  async detectConflicts(semesterId: string) {
    const offerings = await this.offeringsRepo.find({ where: { semester_id: semesterId } });
    const offeringIds = offerings.map((offering) => offering.id);
    if (offeringIds.length === 0) {
      await this.conflictsRepo.delete({ semester_id: semesterId });
      return { semester_id: semesterId, created: 0 };
    }

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

    await this.conflictsRepo.delete({ semester_id: semesterId });
    if (conflicts.length > 0) {
      await this.conflictsRepo.save(conflicts);
    }

    return { semester_id: semesterId, created: conflicts.length };
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
