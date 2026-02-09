import { Injectable } from '@nestjs/common';
import { DataSource, DeleteResult, In, Repository } from 'typeorm';
import {
  ClassGroupEntity,
  ClassGroupTeacherEntity,
  ClassMeetingEntity,
  ClassOfferingEntity,
  ClassTeacherEntity,
  CourseSectionHourRequirementEntity,
  ScheduleConflictEntity,
} from '../entities/planning.entities';
import {
  CreateClassGroupDto,
  CreateClassMeetingDto,
  CreateClassOfferingDto,
  CreateClassTeacherDto,
} from '../planning/dto/planning.dto';
import { SyncAkademicDto } from './dto/sync-akademic.dto';

type IdRow = { id: string };

type SyncBlockSummary = {
  received: number;
  processed: number;
  created: number;
  updated: number;
  deduplicated: number;
};

@Injectable()
export class SyncService {
  constructor(private readonly dataSource: DataSource) {}

  async syncAkademic(dto: SyncAkademicDto) {
    return this.dataSource.transaction(async (manager) => {
      const offeringsRepo = manager.getRepository(ClassOfferingEntity);
      const groupsRepo = manager.getRepository(ClassGroupEntity);
      const meetingsRepo = manager.getRepository(ClassMeetingEntity);
      const classTeachersRepo = manager.getRepository(ClassTeacherEntity);
      const groupTeachersRepo = manager.getRepository(ClassGroupTeacherEntity);
      const hourRequirementsRepo = manager.getRepository(CourseSectionHourRequirementEntity);
      const conflictsRepo = manager.getRepository(ScheduleConflictEntity);

      const inferredSemesterId = this.inferSemesterId(dto.class_offerings ?? []);
      const semesterId = (dto.semester_id ?? inferredSemesterId)?.trim() || null;

      const cleanup = await this.cleanupSemesterData(
        dto.replace_semester === true,
        semesterId,
        offeringsRepo,
        groupsRepo,
        meetingsRepo,
        classTeachersRepo,
        groupTeachersRepo,
        conflictsRepo,
      );

      const offerings = await this.upsertOfferings(offeringsRepo, dto.class_offerings ?? []);
      const groups = await this.upsertGroups(groupsRepo, dto.class_groups ?? []);
      const meetings = await this.upsertMeetings(meetingsRepo, dto.class_meetings ?? []);
      const classTeachers = await this.upsertClassTeachers(
        classTeachersRepo,
        dto.class_teachers ?? [],
      );
      const groupTeachers = await this.upsertClassGroupTeachers(
        groupTeachersRepo,
        dto.class_group_teachers ?? [],
      );
      const hourRequirements = await this.upsertHourRequirements(
        hourRequirementsRepo,
        dto.course_section_hour_requirements ?? [],
      );

      return {
        source_system: dto.source_system ?? 'AKADEMIC',
        semester_id: semesterId,
        replace_semester_applied: cleanup.applied,
        cleanup,
        imported: {
          class_offerings: offerings,
          class_groups: groups,
          class_meetings: meetings,
          class_teachers: classTeachers,
          class_group_teachers: groupTeachers,
          course_section_hour_requirements: hourRequirements,
        },
        totals: {
          received:
            offerings.received +
            groups.received +
            meetings.received +
            classTeachers.received +
            groupTeachers.received +
            hourRequirements.received,
          processed:
            offerings.processed +
            groups.processed +
            meetings.processed +
            classTeachers.processed +
            groupTeachers.processed +
            hourRequirements.processed,
          created:
            offerings.created +
            groups.created +
            meetings.created +
            classTeachers.created +
            groupTeachers.created +
            hourRequirements.created,
          updated:
            offerings.updated +
            groups.updated +
            meetings.updated +
            classTeachers.updated +
            groupTeachers.updated +
            hourRequirements.updated,
          deduplicated:
            offerings.deduplicated +
            groups.deduplicated +
            meetings.deduplicated +
            classTeachers.deduplicated +
            groupTeachers.deduplicated +
            hourRequirements.deduplicated,
        },
        imported_at: new Date().toISOString(),
      };
    });
  }

  private inferSemesterId(offerings: Array<{ semester_id: string }>) {
    return offerings.find((item) => item.semester_id?.trim())?.semester_id;
  }

  private async cleanupSemesterData(
    replaceSemester: boolean,
    semesterId: string | null,
    offeringsRepo: Repository<ClassOfferingEntity>,
    groupsRepo: Repository<ClassGroupEntity>,
    meetingsRepo: Repository<ClassMeetingEntity>,
    classTeachersRepo: Repository<ClassTeacherEntity>,
    groupTeachersRepo: Repository<ClassGroupTeacherEntity>,
    conflictsRepo: Repository<ScheduleConflictEntity>,
  ) {
    if (!replaceSemester || !semesterId) {
      return {
        applied: false,
        semester_id: semesterId,
        deleted: {
          schedule_conflicts: 0,
          class_group_teachers: 0,
          class_meetings: 0,
          class_teachers: 0,
          class_groups: 0,
          class_offerings: 0,
        },
      };
    }

    const offeringRows = await offeringsRepo.find({
      where: { semester_id: semesterId },
      select: { id: true },
    });
    const offeringIds = offeringRows.map((row) => row.id);

    const groupRows =
      offeringIds.length > 0
        ? await groupsRepo.find({
            where: { class_offering_id: In(offeringIds) },
            select: { id: true },
          })
        : [];
    const groupIds = groupRows.map((row) => row.id);

    const deletedConflicts = await conflictsRepo.delete({ semester_id: semesterId });
    const deletedGroupTeachers =
      groupIds.length > 0
        ? await groupTeachersRepo.delete({ class_group_id: In(groupIds) })
        : ({ affected: 0 } as DeleteResult);
    const deletedMeetings =
      offeringIds.length > 0
        ? await meetingsRepo.delete({ class_offering_id: In(offeringIds) })
        : ({ affected: 0 } as DeleteResult);
    const deletedClassTeachers =
      offeringIds.length > 0
        ? await classTeachersRepo.delete({ class_offering_id: In(offeringIds) })
        : ({ affected: 0 } as DeleteResult);
    const deletedGroups =
      offeringIds.length > 0
        ? await groupsRepo.delete({ class_offering_id: In(offeringIds) })
        : ({ affected: 0 } as DeleteResult);
    const deletedOfferings = await offeringsRepo.delete({ semester_id: semesterId });

    return {
      applied: true,
      semester_id: semesterId,
      deleted: {
        schedule_conflicts: deletedConflicts.affected ?? 0,
        class_group_teachers: deletedGroupTeachers.affected ?? 0,
        class_meetings: deletedMeetings.affected ?? 0,
        class_teachers: deletedClassTeachers.affected ?? 0,
        class_groups: deletedGroups.affected ?? 0,
        class_offerings: deletedOfferings.affected ?? 0,
      },
    };
  }

  private async upsertOfferings(
    repo: Repository<ClassOfferingEntity>,
    rows: CreateClassOfferingDto[],
  ): Promise<SyncBlockSummary> {
    return this.upsertWithTransformer(repo, rows, (row) => ({
      ...row,
      projected_vacancies: row.projected_vacancies ?? null,
    }));
  }

  private async upsertGroups(
    repo: Repository<ClassGroupEntity>,
    rows: CreateClassGroupDto[],
  ): Promise<SyncBlockSummary> {
    return this.upsertWithTransformer(repo, rows, (row) => ({
      ...row,
      capacity: row.capacity ?? null,
      note: row.note ?? null,
    }));
  }

  private async upsertMeetings(
    repo: Repository<ClassMeetingEntity>,
    rows: CreateClassMeetingDto[],
  ): Promise<SyncBlockSummary> {
    return this.upsertWithTransformer(repo, rows, (row) => ({
      ...row,
      minutes: row.minutes ?? null,
      academic_hours: row.academic_hours ?? null,
      classroom_id: row.classroom_id ?? null,
    }));
  }

  private async upsertClassTeachers(
    repo: Repository<ClassTeacherEntity>,
    rows: CreateClassTeacherDto[],
  ): Promise<SyncBlockSummary> {
    return this.upsertWithTransformer(repo, rows, (row) => row);
  }

  private async upsertClassGroupTeachers(
    repo: Repository<ClassGroupTeacherEntity>,
    rows: SyncAkademicDto['class_group_teachers'],
  ): Promise<SyncBlockSummary> {
    return this.upsertWithTransformer(repo, rows ?? [], (row, existingMap) => {
      const now = new Date();
      const current = existingMap.get(row.id);

      return {
        ...row,
        assigned_from: row.assigned_from ? new Date(row.assigned_from) : null,
        assigned_to: row.assigned_to ? new Date(row.assigned_to) : null,
        notes: row.notes ?? null,
        created_at: current?.created_at ?? now,
        updated_at: now,
      };
    });
  }

  private async upsertHourRequirements(
    repo: Repository<CourseSectionHourRequirementEntity>,
    rows: SyncAkademicDto['course_section_hour_requirements'],
  ): Promise<SyncBlockSummary> {
    return this.upsertWithTransformer(repo, rows ?? [], (row, existingMap) => {
      const now = new Date();
      const current = existingMap.get(row.id);

      return {
        ...row,
        notes: row.notes ?? null,
        created_at: current?.created_at ?? now,
        updated_at: now,
      };
    });
  }

  private async upsertWithTransformer<TInput extends IdRow, TEntity extends IdRow>(
    repo: Repository<TEntity>,
    rows: TInput[],
    transform: (row: TInput, existingMap: Map<string, TEntity>) => TEntity,
  ): Promise<SyncBlockSummary> {
    const received = rows.length;
    if (received === 0) {
      return { received, processed: 0, created: 0, updated: 0, deduplicated: 0 };
    }

    const deduplicatedRows = this.deduplicateById(rows);
    const ids = deduplicatedRows.rows.map((row) => row.id);
    const existingRows = await repo.find({ where: { id: In(ids) } as never });
    const existingMap = new Map(existingRows.map((row) => [row.id, row]));

    const toSave = deduplicatedRows.rows.map((row) => transform(row, existingMap));
    await repo.save(toSave);

    const existingIds = new Set(existingRows.map((row) => row.id));
    const updated = deduplicatedRows.rows.filter((row) => existingIds.has(row.id)).length;
    const created = deduplicatedRows.rows.length - updated;

    return {
      received,
      processed: deduplicatedRows.rows.length,
      created,
      updated,
      deduplicated: deduplicatedRows.deduplicated,
    };
  }

  private deduplicateById<T extends IdRow>(rows: T[]) {
    const byId = new Map<string, T>();
    for (const row of rows) {
      byId.set(row.id, row);
    }
    return {
      rows: [...byId.values()],
      deduplicated: rows.length - byId.size,
    };
  }
}
