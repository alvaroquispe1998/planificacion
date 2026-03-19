import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { createHash } from 'crypto';
import { InjectRepository } from '@nestjs/typeorm';
import { In, ObjectLiteral, Repository } from 'typeorm';
import { newId } from '../common';
import {
  AcademicProgramEntity,
  BuildingEntity,
  CampusEntity,
  ClassroomEntity,
  FacultyEntity,
  SemesterEntity,
  StudyPlanCourseDetailEntity,
  StudyPlanCourseEntity,
  StudyPlanEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';
import {
  ConflictSeverityValues,
  CourseModalityEntity,
  DayOfWeekValues,
  PlanningChangeLogEntity,
  PlanningCyclePlanRuleEntity,
  PlanningOfferEntity,
  PlanningOfferStatusValues,
  PlanningScheduleConflictV2Entity,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionKindValues,
  PlanningSubsectionScheduleEntity,
  PlanningV2ConflictTypeValues,
  StudyTypeEntity,
} from '../entities/planning.entities';
import {
  CreatePlanningCyclePlanRuleDto,
  CreatePlanningOfferDto,
  CreatePlanningSectionDto,
  CreatePlanningSubsectionDto,
  CreatePlanningSubsectionScheduleDto,
  UpdatePlanningCyclePlanRuleDto,
  UpdatePlanningOfferDto,
  UpdatePlanningSectionDto,
  UpdatePlanningSubsectionDto,
  UpdatePlanningSubsectionScheduleDto,
} from './dto/planning.dto';

type PlanningOfferStatus = (typeof PlanningOfferStatusValues)[number];
type PlanningSubsectionKind = (typeof PlanningSubsectionKindValues)[number];
type PlanningConflictType = (typeof PlanningV2ConflictTypeValues)[number];
type ConflictSeverity = (typeof ConflictSeverityValues)[number];
type DayOfWeek = (typeof DayOfWeekValues)[number];

type CourseCandidateFilters = {
  semester_id?: string;
  campus_id?: string;
  faculty_id?: string;
  academic_program_id?: string;
  cycle?: number;
  study_plan_id?: string;
};

type ConfiguredCycleFilters = {
  semester_id?: string;
  campus_id?: string;
  faculty_id?: string;
  academic_program_id?: string;
};

type ChangeLogActor = {
  user_id?: string | null;
  username?: string | null;
  display_name?: string | null;
};

type ChangeLogFilters = {
  entity_type?: string;
  entity_id?: string;
  action?: string;
  offer_id?: string;
  changed_by?: string;
  from?: string;
  to?: string;
  limit?: number;
};

type ChangeLogReferenceMaps = {
  offers: Map<string, PlanningOfferEntity>;
  sections: Map<string, PlanningSectionEntity>;
  subsections: Map<string, PlanningSubsectionEntity>;
  semesters: Map<string, SemesterEntity>;
  campuses: Map<string, CampusEntity>;
  faculties: Map<string, FacultyEntity>;
  programs: Map<string, AcademicProgramEntity>;
  studyPlans: Map<string, StudyPlanEntity>;
};

type PlanningContext = {
  offers: PlanningOfferEntity[];
  sections: PlanningSectionEntity[];
  subsections: PlanningSubsectionEntity[];
  schedules: PlanningSubsectionScheduleEntity[];
  teachers: TeacherEntity[];
  buildings: BuildingEntity[];
  classrooms: ClassroomEntity[];
  conflicts: PlanningScheduleConflictV2Entity[];
  changeLogs: PlanningChangeLogEntity[];
};

type PlanningOfferBlueprint = {
  study_plan_course_id: string;
  study_plan_id: string;
  cycle: number;
  course_code: string | null;
  course_name: string | null;
  credits: number;
  theoretical_hours: number;
  practical_hours: number;
  total_hours: number;
  course_type: string;
};

@Injectable()
export class PlanningManualService {
  constructor(
    @InjectRepository(SemesterEntity)
    private readonly semestersRepo: Repository<SemesterEntity>,
    @InjectRepository(CampusEntity)
    private readonly campusesRepo: Repository<CampusEntity>,
    @InjectRepository(FacultyEntity)
    private readonly facultiesRepo: Repository<FacultyEntity>,
    @InjectRepository(AcademicProgramEntity)
    private readonly programsRepo: Repository<AcademicProgramEntity>,
    @InjectRepository(StudyPlanEntity)
    private readonly studyPlansRepo: Repository<StudyPlanEntity>,
    @InjectRepository(StudyPlanCourseEntity)
    private readonly studyPlanCoursesRepo: Repository<StudyPlanCourseEntity>,
    @InjectRepository(StudyPlanCourseDetailEntity)
    private readonly studyPlanCourseDetailsRepo: Repository<StudyPlanCourseDetailEntity>,
    @InjectRepository(TeacherEntity)
    private readonly teachersRepo: Repository<TeacherEntity>,
    @InjectRepository(BuildingEntity)
    private readonly buildingsRepo: Repository<BuildingEntity>,
    @InjectRepository(ClassroomEntity)
    private readonly classroomsRepo: Repository<ClassroomEntity>,
    @InjectRepository(StudyTypeEntity)
    private readonly studyTypesRepo: Repository<StudyTypeEntity>,
    @InjectRepository(CourseModalityEntity)
    private readonly courseModalitiesRepo: Repository<CourseModalityEntity>,
    @InjectRepository(PlanningCyclePlanRuleEntity)
    private readonly planRulesRepo: Repository<PlanningCyclePlanRuleEntity>,
    @InjectRepository(PlanningOfferEntity)
    private readonly offersRepo: Repository<PlanningOfferEntity>,
    @InjectRepository(PlanningSectionEntity)
    private readonly sectionsRepo: Repository<PlanningSectionEntity>,
    @InjectRepository(PlanningSubsectionEntity)
    private readonly subsectionsRepo: Repository<PlanningSubsectionEntity>,
    @InjectRepository(PlanningSubsectionScheduleEntity)
    private readonly schedulesRepo: Repository<PlanningSubsectionScheduleEntity>,
    @InjectRepository(PlanningScheduleConflictV2Entity)
    private readonly conflictsRepo: Repository<PlanningScheduleConflictV2Entity>,
    @InjectRepository(PlanningChangeLogEntity)
    private readonly changeLogsRepo: Repository<PlanningChangeLogEntity>,
  ) {}

  async listCatalogFilters() {
    await this.ensureDefaultCatalogs();
    const [
      semesters,
      campuses,
      faculties,
      academicPrograms,
      studyPlans,
      studyPlanCourses,
      studyPlanCourseDetails,
      rules,
      studyTypes,
      modalities,
    ] =
      await Promise.all([
        this.semestersRepo.find({ order: { name: 'DESC' } }),
        this.campusesRepo.find({ order: { name: 'ASC' } }),
        this.facultiesRepo.find({ order: { name: 'ASC' } }),
        this.programsRepo.find({ order: { name: 'ASC' } }),
        this.studyPlansRepo.find({ order: { career: 'ASC', year: 'ASC' } }),
        this.studyPlanCoursesRepo.find({
          select: { id: true, study_plan_id: true, year_label: true },
        }),
        this.studyPlanCourseDetailsRepo.find({
          select: { study_plan_course_id: true, academic_year: true },
        }),
        this.planRulesRepo.find({
          where: { is_active: true },
          order: { academic_program_id: 'ASC', cycle: 'ASC' },
        }),
        this.studyTypesRepo.find({ where: { is_active: true }, order: { name: 'ASC' } }),
        this.courseModalitiesRepo.find({ where: { is_active: true }, order: { name: 'ASC' } }),
      ]);
    const derivedCatalog = this.buildStudyPlanCatalog(studyPlans, faculties, academicPrograms);
    const cycles = buildStudyPlanCycles(studyPlanCourses, studyPlanCourseDetails);

    return {
      semesters,
      campuses,
      faculties: derivedCatalog.faculties,
      academic_programs: derivedCatalog.academicPrograms,
      cycles,
      study_plan_cycles: cycles,
      study_plans: derivedCatalog.studyPlans,
      plan_rules: rules,
      study_types: studyTypes,
      course_modalities: modalities,
    };
  }

  async listConfiguredCycles(filters: ConfiguredCycleFilters) {
    await this.ensureDefaultCatalogs();
    const rules = await this.planRulesRepo.find({
      where: {
        is_active: true,
        ...(filters.semester_id ? { semester_id: filters.semester_id } : {}),
        ...(filters.campus_id ? { campus_id: filters.campus_id } : {}),
        ...(filters.faculty_id ? { faculty_id: filters.faculty_id } : {}),
        ...(filters.academic_program_id
          ? { academic_program_id: filters.academic_program_id }
          : {}),
      },
      order: {
        semester_id: 'ASC',
        academic_program_id: 'ASC',
        cycle: 'ASC',
        created_at: 'DESC',
      },
    });
    if (rules.length === 0) {
      return [];
    }

    const offers = await this.offersRepo.find({
      where: {
        ...(filters.semester_id ? { semester_id: filters.semester_id } : {}),
        ...(filters.campus_id ? { campus_id: filters.campus_id } : {}),
        ...(filters.faculty_id ? { faculty_id: filters.faculty_id } : {}),
        ...(filters.academic_program_id
          ? { academic_program_id: filters.academic_program_id }
          : {}),
      },
      order: { updated_at: 'DESC' },
    });
    const semesterIds = uniqueIds(rules.map((item) => item.semester_id));
    const programIds = uniqueIds(rules.map((item) => item.academic_program_id));
    const facultyIds = uniqueIds(rules.map((item) => item.faculty_id));
    const studyPlanIds = uniqueIds(rules.map((item) => item.study_plan_id));
    const campusIds = uniqueIds([
      ...offers.map((item) => item.campus_id),
      ...rules.map((item) => item.campus_id),
    ]);
    const [semesters, programs, faculties, studyPlans, campuses] = await Promise.all([
      this.findManyByIds(this.semestersRepo, semesterIds),
      this.findManyByIds(this.programsRepo, programIds),
      this.findManyByIds(this.facultiesRepo, facultyIds),
      this.findManyByIds(this.studyPlansRepo, studyPlanIds),
      this.findManyByIds(this.campusesRepo, campusIds),
    ]);
    const offerConfiguration = await this.buildOfferConfigurationContext(offers.map((item) => item.id));
    const offerReviewStateById = new Map(
      offers.map((offer) => [
        offer.id,
        this.buildOfferReviewState(
          offer.id,
          offerConfiguration.sectionsByOfferId,
          offerConfiguration.subsectionsBySectionId,
          offerConfiguration.schedulesBySubsectionId,
        ),
      ]),
    );
    const planCourses = studyPlanIds.length
      ? await this.studyPlanCoursesRepo.find({
          where: { study_plan_id: In(studyPlanIds) },
          order: { order: 'ASC', course_code: 'ASC' },
        })
      : [];
    const planCourseDetails = planCourses.length
      ? await this.findManyByField(
          this.studyPlanCourseDetailsRepo,
          'study_plan_course_id',
          planCourses.map((item) => item.id),
        )
      : [];
    const planCourseDetailById = new Map(
      planCourseDetails.map((item) => [item.study_plan_course_id, item] as const),
    );
    const expectedCourseCountByRuleKey = new Map<string, number>();
    for (const course of planCourses) {
      const detail = planCourseDetailById.get(course.id) ?? null;
      const resolvedCycle =
        detail?.academic_year ?? extractCycleFromLabel(course.year_label) ?? null;
      if (!resolvedCycle) {
        continue;
      }
      const key = configuredCycleKey({
        semester_id: '',
        campus_id: '',
        academic_program_id: '',
        study_plan_id: course.study_plan_id,
        cycle: resolvedCycle,
      });
      expectedCourseCountByRuleKey.set(key, (expectedCourseCountByRuleKey.get(key) ?? 0) + 1);
    }

    const semesterMap = mapById(semesters);
    const programMap = mapById(programs);
    const facultyMap = mapById(faculties);
    const planMap = mapById(studyPlans);
    const campusMap = mapById(campuses);
    const offersByRuleKey = new Map<string, PlanningOfferEntity[]>();
    for (const offer of offers) {
      const key = configuredCycleKey({
        semester_id: offer.semester_id,
        campus_id: offer.campus_id,
        academic_program_id: offer.academic_program_id ?? '',
        study_plan_id: offer.study_plan_id,
        cycle: offer.cycle,
      });
      const bucket = offersByRuleKey.get(key) ?? [];
      bucket.push(offer);
      offersByRuleKey.set(key, bucket);
    }

    return rules
      .map((rule) => {
        const cycle = rule.cycle;
        const rowOffers = rule.campus_id
          ? offersByRuleKey.get(
              configuredCycleKey({
                semester_id: rule.semester_id,
                campus_id: rule.campus_id,
                academic_program_id: rule.academic_program_id,
                study_plan_id: rule.study_plan_id,
                cycle,
              }),
            ) ?? []
          : offers.filter(
              (offer) =>
                offer.semester_id === rule.semester_id &&
                offer.academic_program_id === rule.academic_program_id &&
                offer.study_plan_id === rule.study_plan_id &&
                offer.cycle === cycle,
            );
        const rowCampusIds = uniqueIds([...rowOffers.map((item) => item.campus_id), rule.campus_id]);
        const rowCampuses = rowCampusIds
          .map((id) => campusMap.get(id))
          .filter((item): item is CampusEntity => Boolean(item));
        const readyCourseCount = rowOffers.filter(
          (item) => offerReviewStateById.get(item.id)?.is_ready,
        ).length;
        const expectedCourseCount =
          expectedCourseCountByRuleKey.get(
            configuredCycleKey({
              semester_id: '',
              campus_id: '',
              academic_program_id: '',
              study_plan_id: rule.study_plan_id,
              cycle,
            }),
          ) ?? 0;
        return {
          id: rule.id,
          semester_id: rule.semester_id,
          campus_id: rule.campus_id,
          faculty_id: rule.faculty_id,
          academic_program_id: rule.academic_program_id,
          career_name: rule.career_name ?? null,
          study_plan_id: rule.study_plan_id,
          cycle,
          workflow_status: rule.workflow_status,
          submitted_at: rule.submitted_at,
          submitted_by_user_id: rule.submitted_by_user_id,
          submitted_by: rule.submitted_by,
          reviewed_at: rule.reviewed_at,
          reviewed_by_user_id: rule.reviewed_by_user_id,
          reviewed_by: rule.reviewed_by,
          review_comment: rule.review_comment,
          semester: semesterMap.get(rule.semester_id) ?? null,
          faculty: facultyMap.get(rule.faculty_id ?? '') ?? null,
          academic_program: programMap.get(rule.academic_program_id) ?? null,
          study_plan: planMap.get(rule.study_plan_id) ?? null,
          campuses: rowCampuses,
          campus_ids: rowCampusIds,
          campus_display: rowCampuses.length
            ? rowCampuses.map((item) => item.name).filter(Boolean).join(', ')
            : 'Sin local',
          primary_campus_id: rowCampusIds[0] ?? null,
          offer_count: rowOffers.length,
          expected_course_count: expectedCourseCount,
          ready_course_count: readyCourseCount,
          review_ready:
            expectedCourseCount > 0 &&
            rowOffers.length === expectedCourseCount &&
            readyCourseCount === expectedCourseCount,
        };
      })
      .filter((row) =>
        filters.campus_id ? row.campus_ids.includes(filters.campus_id) : true,
      )
      .sort((left, right) => {
        const semesterDelta = compareCatalogLabels(left.semester?.name, right.semester?.name);
        if (semesterDelta !== 0) {
          return semesterDelta;
        }

        const campusDelta = compareCatalogLabels(left.campus_display, right.campus_display);
        if (campusDelta !== 0) {
          return campusDelta;
        }

        const facultyDelta = compareCatalogLabels(
          left.faculty?.name ?? left.study_plan?.faculty,
          right.faculty?.name ?? right.study_plan?.faculty,
        );
        if (facultyDelta !== 0) {
          return facultyDelta;
        }

        const programDelta = compareCatalogLabels(
          left.academic_program?.name ?? left.study_plan?.career ?? left.career_name,
          right.academic_program?.name ?? right.study_plan?.career ?? right.career_name,
        );
        if (programDelta !== 0) {
          return programDelta;
        }

        return Number(left.cycle ?? 0) - Number(right.cycle ?? 0);
      });
  }

  async listPlanRules(semesterId?: string, campusId?: string, academicProgramId?: string) {
    await this.ensureDefaultCatalogs();
    const rules = await this.planRulesRepo.find({
      where: {
        ...(semesterId ? { semester_id: semesterId } : {}),
        ...(campusId ? { campus_id: campusId } : {}),
        ...(academicProgramId ? { academic_program_id: academicProgramId } : {}),
      },
      order: { semester_id: 'ASC', academic_program_id: 'ASC', cycle: 'ASC' },
    });
    return this.enrichPlanRules(rules);
  }

  async createPlanRule(actor: ChangeLogActor | null | undefined, dto: CreatePlanningCyclePlanRuleDto) {
    await this.ensureDefaultCatalogs();
    await this.ensureStudyPlanMatchesContext(
      dto.study_plan_id,
      dto.academic_program_id,
      dto.faculty_id ?? null,
      dto.cycle,
    );
    await this.ensureNoOverlappingRule(dto);
    const now = new Date();
    const entity = this.planRulesRepo.create({
      id: dto.id ?? newId(),
      semester_id: dto.semester_id,
      campus_id: dto.campus_id,
      academic_program_id: dto.academic_program_id,
      faculty_id: dto.faculty_id ?? null,
      career_name: dto.career_name ?? null,
      cycle: dto.cycle,
      study_plan_id: dto.study_plan_id,
      is_active: dto.is_active ?? true,
      workflow_status: 'DRAFT',
      submitted_at: null,
      submitted_by_user_id: null,
      submitted_by: null,
      reviewed_at: null,
      reviewed_by_user_id: null,
      reviewed_by: null,
      review_comment: null,
      created_at: now,
      updated_at: now,
    });
    const saved = await this.planRulesRepo.save(entity);
    await this.logChange(
      'planning_cycle_plan_rule',
      saved.id,
      'CREATE',
      null,
      saved,
      this.buildPlanRuleLogContext(saved),
      actor,
    );
    const offerSync = await this.ensureCycleOffersForRule(saved, actor);
    return {
      ...(await this.enrichPlanRule(saved)),
      created_offer_count: offerSync.created,
      existing_offer_count: offerSync.existing,
      total_offer_count: offerSync.total,
    };
  }

  async updatePlanRule(actor: ChangeLogActor | null | undefined, id: string, dto: UpdatePlanningCyclePlanRuleDto) {
    const current = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    this.ensurePlanRuleEditable(current);
    const next = {
      ...current,
      ...dto,
      updated_at: new Date(),
    };
    await this.ensureStudyPlanMatchesContext(
      next.study_plan_id,
      next.academic_program_id,
      next.faculty_id ?? null,
      next.cycle,
    );
    await this.ensureNoOverlappingRule(next, id);
    await this.planRulesRepo.save(next);
    const saved = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    await this.logChange(
      'planning_cycle_plan_rule',
      id,
      'UPDATE',
      current,
      saved,
      this.buildPlanRuleLogContext(saved),
      actor,
    );
    return this.enrichPlanRule(saved);
  }

  async deletePlanRule(actor: ChangeLogActor | null | undefined, id: string) {
    const current = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    this.ensurePlanRuleEditable(current);
    const deletedHierarchy = await this.deletePlanRuleHierarchy(current, actor);
    await this.logChange(
      'planning_cycle_plan_rule',
      id,
      'DELETE',
      current,
      null,
      this.buildPlanRuleLogContext(current),
      actor,
    );
    await this.rebuildConflictsForSemester(current.semester_id, actor);
    return {
      deleted: true,
      id,
      ...deletedHierarchy,
    };
  }

  async getPlanRule(id: string) {
    const rule = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    return this.enrichPlanRule(rule);
  }

  async submitPlanRuleReview(
    actor: ChangeLogActor | null | undefined,
    id: string,
    reviewComment?: string | null,
  ) {
    const current = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    if (!['DRAFT', 'IN_CORRECTION'].includes(current.workflow_status)) {
      throw new BadRequestException('Solo puedes enviar a revision planes en borrador o en correccion.');
    }
    await this.assertPlanRuleReadyForReview(current);
    const next = this.planRulesRepo.create({
      ...current,
      workflow_status: 'IN_REVIEW',
      submitted_at: new Date(),
      submitted_by_user_id: actor?.user_id ?? null,
      submitted_by: actor?.display_name || actor?.username || 'SYSTEM',
      review_comment: normalizeOptionalComment(reviewComment),
      updated_at: new Date(),
    });
    await this.planRulesRepo.save(next);
    const saved = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    await this.logChange(
      'planning_cycle_plan_rule',
      id,
      'UPDATE',
      current,
      saved,
      { ...this.buildPlanRuleLogContext(saved), workflow_action: 'submit_review' },
      actor,
    );
    return this.enrichPlanRule(saved);
  }

  async submitPlanRulesReviewBulk(
    actor: ChangeLogActor | null | undefined,
    ids: string[],
    reviewComment?: string | null,
  ) {
    const uniqueRuleIds = uniqueIds(ids);
    if (uniqueRuleIds.length === 0) {
      throw new BadRequestException('Debes indicar al menos un plan para enviar a revision.');
    }

    const currentRules = await this.planRulesRepo.find({
      where: { id: In(uniqueRuleIds) },
      order: { cycle: 'ASC', created_at: 'ASC' },
    });
    if (currentRules.length !== uniqueRuleIds.length) {
      throw new NotFoundException('Uno o mas planes ya no existen.');
    }

    const eligibleRules = currentRules.filter((item) =>
      ['DRAFT', 'IN_CORRECTION'].includes(item.workflow_status),
    );
    if (eligibleRules.length === 0) {
      throw new BadRequestException(
        'No hay planes elegibles para enviar a revision en la seleccion actual.',
      );
    }

    const updated = [];
    for (const rule of eligibleRules) {
      updated.push(await this.submitPlanRuleReview(actor, rule.id, reviewComment));
    }

    return {
      submitted_count: updated.length,
      skipped_count: currentRules.length - updated.length,
      submitted_ids: updated.map((item) => item.id),
      items: updated,
    };
  }

  async approvePlanRule(
    actor: ChangeLogActor | null | undefined,
    id: string,
    reviewComment?: string | null,
  ) {
    const current = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    if (current.workflow_status !== 'IN_REVIEW') {
      throw new BadRequestException('Solo puedes aprobar planes que estan en revision.');
    }
    const next = this.planRulesRepo.create({
      ...current,
      workflow_status: 'APPROVED',
      reviewed_at: new Date(),
      reviewed_by_user_id: actor?.user_id ?? null,
      reviewed_by: actor?.display_name || actor?.username || 'SYSTEM',
      review_comment: normalizeOptionalComment(reviewComment),
      updated_at: new Date(),
    });
    await this.planRulesRepo.save(next);
    const saved = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    await this.logChange(
      'planning_cycle_plan_rule',
      id,
      'UPDATE',
      current,
      saved,
      { ...this.buildPlanRuleLogContext(saved), workflow_action: 'approve' },
      actor,
    );
    return this.enrichPlanRule(saved);
  }

  async requestPlanRuleCorrection(
    actor: ChangeLogActor | null | undefined,
    id: string,
    reviewComment: string,
  ) {
    const current = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    if (current.workflow_status !== 'IN_REVIEW') {
      throw new BadRequestException('Solo puedes mandar a correccion planes que estan en revision.');
    }
    const next = this.planRulesRepo.create({
      ...current,
      workflow_status: 'IN_CORRECTION',
      reviewed_at: new Date(),
      reviewed_by_user_id: actor?.user_id ?? null,
      reviewed_by: actor?.display_name || actor?.username || 'SYSTEM',
      review_comment: normalizeRequiredComment(reviewComment),
      updated_at: new Date(),
    });
    await this.planRulesRepo.save(next);
    const saved = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    await this.logChange(
      'planning_cycle_plan_rule',
      id,
      'UPDATE',
      current,
      saved,
      { ...this.buildPlanRuleLogContext(saved), workflow_action: 'request_correction' },
      actor,
    );
    return this.enrichPlanRule(saved);
  }

  async listCourseCandidates(filters: CourseCandidateFilters) {
    await this.ensureDefaultCatalogs();
    const cycle = filters.cycle ?? null;
    if (
      !filters.semester_id ||
      !filters.campus_id ||
      !filters.faculty_id ||
      !filters.academic_program_id ||
      !cycle
    ) {
      return {
        filters,
        plan_rule: null,
        study_plan: null,
        candidates: [],
        message:
          'Seleccione semestre, local, facultad, programa y ciclo para listar cursos.',
      };
    }

    const selectedStudyPlanId = filters.study_plan_id?.trim() || null;
    const matchedRules = await this.findMatchingRules(
      filters.semester_id,
      filters.campus_id,
      filters.academic_program_id,
      cycle,
    );
    if (!selectedStudyPlanId && matchedRules.length === 0) {
      return {
        filters,
        plan_rule: null,
        study_plan: null,
        candidates: [],
        message: 'No hay plan asignado para esta carrera/ciclo en este semestre.',
      };
    }
    if (!selectedStudyPlanId && matchedRules.length > 1) {
      return {
        filters,
        plan_rule: null,
        study_plan: null,
        candidates: [],
        message: 'Hay reglas solapadas para la carrera/ciclo seleccionados.',
      };
    }

    const planRule =
      matchedRules.length === 1 &&
      (!selectedStudyPlanId || matchedRules[0].study_plan_id === selectedStudyPlanId)
        ? matchedRules[0]
        : null;
    const activeStudyPlanId = selectedStudyPlanId ?? planRule?.study_plan_id ?? null;
    if (!activeStudyPlanId) {
      return {
        filters,
        plan_rule: null,
        study_plan: null,
        candidates: [],
        message: 'Seleccione un plan de estudio para listar cursos.',
      };
    }

    const studyPlan = await this.studyPlansRepo.findOne({ where: { id: activeStudyPlanId } });
    if (!studyPlan) {
      throw new NotFoundException(`study_plan ${activeStudyPlanId} no existe.`);
    }
    await this.ensureStudyPlanMatchesContext(
      activeStudyPlanId,
      filters.academic_program_id,
      filters.faculty_id,
      cycle,
    );

    const courses = await this.studyPlanCoursesRepo.find({
      where: { study_plan_id: activeStudyPlanId },
      order: { order: 'ASC', course_code: 'ASC' },
    });
    const details = await this.findManyByField(
      this.studyPlanCourseDetailsRepo,
      'study_plan_course_id',
      courses.map((item) => item.id),
    );
    const detailById = new Map(details.map((item) => [item.study_plan_course_id, item]));
    const offers = planRule
      ? await this.offersRepo.find({
          where: {
            semester_id: planRule.semester_id,
            campus_id: planRule.campus_id ?? undefined,
            academic_program_id: planRule.academic_program_id,
            study_plan_id: planRule.study_plan_id,
            cycle: planRule.cycle,
          },
        })
      : [];
    const offerContext = await this.buildOfferConfigurationContext(offers.map((item) => item.id));
    const offerReviewStateById = new Map(
      offers.map((offer) => [
        offer.id,
        this.buildOfferReviewState(offer.id, offerContext.sectionsByOfferId, offerContext.subsectionsBySectionId, offerContext.schedulesBySubsectionId),
      ]),
    );
    const offerByCourseId = new Map(offers.map((item) => [item.study_plan_course_id, item]));

    const candidates = courses
      .map((course) => {
        const detail = detailById.get(course.id) ?? null;
        const resolvedCycle =
          detail?.academic_year ?? extractCycleFromLabel(course.year_label) ?? null;
        return {
          course,
          detail,
          resolvedCycle,
        };
      })
      .filter((item) => item.resolvedCycle === cycle)
      .map(({ course, detail, resolvedCycle }) => {
        const theoreticalHours = numberValue(detail?.theoretical_hours);
        const practicalHours = numberValue(detail?.practical_hours);
        const totalHours = roundToTwo(theoreticalHours + practicalHours);
        const offer = offerByCourseId.get(course.id) ?? null;
        return {
          study_plan_course_id: course.id,
          study_plan_id: activeStudyPlanId,
          study_plan_name: studyPlan?.name ?? null,
          cycle: resolvedCycle,
          course_code: detail?.short_code ?? course.course_code ?? null,
          course_name: detail?.name ?? course.course_name ?? null,
          credits: numberValue(detail?.credits, numberValue(course.credits)),
          theoretical_hours: theoreticalHours,
          practical_hours: practicalHours,
          total_hours: totalHours,
          course_type: deriveCourseType(theoreticalHours, practicalHours),
          has_offer: Boolean(offer),
          offer_id: offer?.id ?? null,
          offer_status: offer?.status ?? null,
          review_ready: offer ? (offerReviewStateById.get(offer.id)?.is_ready ?? false) : false,
        };
      });

    return {
      filters,
      plan_rule: planRule ? await this.enrichPlanRule(planRule) : null,
      study_plan: studyPlan,
      candidates,
      message: candidates.length ? null : 'No hay cursos del plan para el ciclo seleccionado.',
    };
  }

  async createOffer(actor: ChangeLogActor | null | undefined, dto: CreatePlanningOfferDto) {
    await this.ensureDefaultCatalogs();
    await this.ensureStudyPlanMatchesContext(
      dto.study_plan_id,
      dto.academic_program_id ?? null,
      dto.faculty_id ?? null,
      dto.cycle,
    );
    const planRule = await this.findPlanRuleForOfferContext({
      semester_id: dto.semester_id,
      campus_id: dto.campus_id,
      academic_program_id: dto.academic_program_id ?? null,
      study_plan_id: dto.study_plan_id,
      cycle: dto.cycle,
    });
    this.ensurePlanRuleEditable(planRule);
    const existing = await this.offersRepo.findOne({
      where: {
        semester_id: dto.semester_id,
        campus_id: dto.campus_id || undefined,
        academic_program_id: dto.academic_program_id ?? undefined,
        study_plan_id: dto.study_plan_id,
        cycle: dto.cycle,
        study_plan_course_id: dto.study_plan_course_id,
      },
    });
    if (existing) {
      return this.getOffer(existing.id);
    }

    const course = await this.requireEntity(
      this.studyPlanCoursesRepo,
      dto.study_plan_course_id,
      'study_plan_course',
    );
    if (course.study_plan_id !== dto.study_plan_id) {
      throw new BadRequestException(
        'El curso seleccionado no pertenece al plan de estudio indicado.',
      );
    }
    const blueprint = await this.findCycleOfferBlueprint(
      dto.study_plan_id,
      dto.cycle,
      dto.study_plan_course_id,
    );
    if (!blueprint) {
      throw new BadRequestException(
        `El curso seleccionado no pertenece al ciclo ${dto.cycle}.`,
      );
    }
    const now = new Date();
    const entity = this.offersRepo.create({
      id: dto.id ?? newId(),
      semester_id: dto.semester_id,
      campus_id: dto.campus_id,
      faculty_id: dto.faculty_id ?? null,
      academic_program_id: dto.academic_program_id ?? null,
      study_plan_id: dto.study_plan_id,
      cycle: dto.cycle,
      study_plan_course_id: dto.study_plan_course_id,
      course_code: blueprint.course_code,
      course_name: blueprint.course_name,
      study_type_id: dto.study_type_id ?? (await this.defaultStudyTypeId()),
      course_type: blueprint.course_type,
      theoretical_hours: blueprint.theoretical_hours,
      practical_hours: blueprint.practical_hours,
      total_hours: blueprint.total_hours,
      status: dto.status ?? 'DRAFT',
      created_at: now,
      updated_at: now,
    });
    const saved = await this.offersRepo.save(entity);
    await this.logChange(
      'planning_offer',
      saved.id,
      'CREATE',
      null,
      saved,
      this.buildOfferLogContext(saved),
      actor,
    );
    return this.getOffer(saved.id);
  }

  async listOffers(
    semesterId?: string,
    campusId?: string,
    facultyId?: string,
    academicProgramId?: string,
    cycle?: number,
    studyPlanId?: string,
  ) {
    const offers = await this.offersRepo.find({
      where: {
        ...(semesterId ? { semester_id: semesterId } : {}),
        ...(campusId ? { campus_id: campusId } : {}),
        ...(facultyId ? { faculty_id: facultyId } : {}),
        ...(academicProgramId ? { academic_program_id: academicProgramId } : {}),
        ...(cycle ? { cycle } : {}),
        ...(studyPlanId ? { study_plan_id: studyPlanId } : {}),
      },
      order: { updated_at: 'DESC', course_name: 'ASC' },
    });
    return this.enrichOffers(offers);
  }

  async getOffer(id: string) {
    const offer = await this.requireEntity(this.offersRepo, id, 'planning_offer');
    const context = await this.buildContext([offer.id]);
    const planRule = await this.findPlanRuleForOfferContext({
      semester_id: offer.semester_id,
      campus_id: offer.campus_id,
      academic_program_id: offer.academic_program_id ?? null,
      study_plan_id: offer.study_plan_id,
      cycle: offer.cycle,
    });
    return {
      ...this.buildOfferDetail(offer, context),
      plan_rule: planRule ? await this.enrichPlanRule(planRule) : null,
    };
  }

  async updateOffer(actor: ChangeLogActor | null | undefined, id: string, dto: UpdatePlanningOfferDto) {
    const current = await this.requireEntity(this.offersRepo, id, 'planning_offer');
    await this.assertOfferPlanEditable(current);
    const next = this.offersRepo.create({
      ...current,
      ...dto,
      updated_at: new Date(),
    });
    await this.offersRepo.save(next);
    const saved = await this.requireEntity(this.offersRepo, id, 'planning_offer');
    await this.logChange(
      'planning_offer',
      id,
      'UPDATE',
      current,
      saved,
      this.buildOfferLogContext(saved),
      actor,
    );
    return this.getOffer(id);
  }

  async createSection(actor: ChangeLogActor | null | undefined, offerId: string, dto: CreatePlanningSectionDto) {
    await this.ensureDefaultCatalogs();
    const offer = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    const now = new Date();
    const subsectionCount = Math.max(1, Math.trunc(dto.subsection_count));
    const modalityId = dto.course_modality_id ?? (await this.defaultCourseModalityId());
    const teacherId = emptyToNull(dto.teacher_id);
    const projectedVacancies =
      dto.projected_vacancies !== undefined ? Math.max(0, Math.trunc(dto.projected_vacancies)) : null;

    const saved = await this.sectionsRepo.manager.transaction(async (manager) => {
      const existingSections = await manager.find(PlanningSectionEntity, {
        where: { planning_offer_id: offerId },
        order: { code: 'ASC' },
      });
      const sectionCode = nextSectionCodeValue(existingSections.map((item) => item.code));
      const section = manager.create(PlanningSectionEntity, {
        id: dto.id ?? newId(),
        planning_offer_id: offerId,
        code: sectionCode,
        teacher_id: teacherId,
        course_modality_id: modalityId,
        projected_vacancies: projectedVacancies,
        has_subsections: subsectionCount > 1,
        default_theoretical_hours: offer.theoretical_hours,
        default_practical_hours: offer.practical_hours,
        default_virtual_hours: 0,
        default_seminar_hours: 0,
        default_total_hours: offer.total_hours,
        status: 'DRAFT',
        created_at: now,
        updated_at: now,
      });
      const savedSection = await manager.save(PlanningSectionEntity, section);

      const subsections = Array.from({ length: subsectionCount }, (_, index) => {
        const subsectionCode = `${sectionCode}${index}`;
        const kind = resolveGeneratedSubsectionKind(offer.course_type, subsectionCount, index);
        const assigned = resolveAssignedHours(kind, offer);
        return manager.create(PlanningSubsectionEntity, {
          id: newId(),
          planning_section_id: savedSection.id,
          code: subsectionCode,
          kind,
          responsible_teacher_id: teacherId,
          course_modality_id: modalityId,
          building_id: null,
          classroom_id: null,
          capacity_snapshot: null,
          shift: null,
          projected_vacancies: resolveSubsectionProjectedVacancies(projectedVacancies, subsectionCount, index),
          course_type: offer.course_type,
          assigned_theoretical_hours: assigned.theoretical_hours,
          assigned_practical_hours: assigned.practical_hours,
          assigned_virtual_hours: assigned.virtual_hours,
          assigned_seminar_hours: assigned.seminar_hours,
          assigned_total_hours: assigned.total_hours,
          denomination: buildDenomination(
            offer.course_code,
            offer.course_name,
            savedSection.code,
            subsectionCode,
            offer.campus_id,
          ),
          status: 'DRAFT',
          created_at: now,
          updated_at: now,
        });
      });

      await manager.save(PlanningSubsectionEntity, subsections);
      return savedSection;
    });

    await this.logChange(
      'planning_section',
      saved.id,
      'CREATE',
      null,
      saved,
      this.buildSectionLogContext(saved, offer),
      actor,
    );
    const createdSubsections = await this.subsectionsRepo.find({
      where: { planning_section_id: saved.id },
      order: { code: 'ASC' },
    });
    for (const subsection of createdSubsections) {
      await this.logChange(
        'planning_subsection',
        subsection.id,
        'CREATE',
        null,
        subsection,
        this.buildSubsectionLogContext(subsection, saved, offer),
        actor,
      );
    }
    await this.refreshOfferStatus(offerId, undefined, actor);
    return this.getSection(saved.id);
  }

  async getSection(id: string) {
    const section = await this.requireEntity(this.sectionsRepo, id, 'planning_section');
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    const subsections = await this.subsectionsRepo.find({
      where: { planning_section_id: id },
      order: { code: 'ASC' },
    });
    const schedules = subsections.length
      ? await this.schedulesRepo.find({
          where: { planning_subsection_id: In(subsections.map((item) => item.id)) },
          order: { day_of_week: 'ASC', start_time: 'ASC' },
        })
      : [];
    const conflicts = await this.conflictsRepo.find({
      where: { planning_section_id: id },
      order: { detected_at: 'DESC' },
    });
    const [teachers, modalities, buildings, classrooms] = await Promise.all([
      this.findManyByIds(
        this.teachersRepo,
        uniqueIds([section.teacher_id, ...subsections.map((item) => item.responsible_teacher_id)]),
      ),
      this.findManyByIds(
        this.courseModalitiesRepo,
        uniqueIds([section.course_modality_id, ...subsections.map((item) => item.course_modality_id)]),
      ),
      this.findManyByIds(this.buildingsRepo, uniqueIds(subsections.map((item) => item.building_id))),
      this.findManyByIds(this.classroomsRepo, uniqueIds(subsections.map((item) => item.classroom_id))),
    ]);

    return {
      ...section,
      offer,
      teacher: mapById(teachers).get(section.teacher_id ?? '') ?? null,
      modality: mapById(modalities).get(section.course_modality_id ?? '') ?? null,
      subsections: subsections.map((subsection) =>
        this.buildSubsectionDetail(subsection, {
          schedules,
          teachers,
          modalities,
          buildings,
          classrooms,
          conflicts,
        }),
      ),
    };
  }

  async updateSection(actor: ChangeLogActor | null | undefined, id: string, dto: UpdatePlanningSectionDto) {
    const current = await this.requireEntity(this.sectionsRepo, id, 'planning_section');
    const currentOffer = await this.requireEntity(this.offersRepo, current.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(currentOffer);
    const nextCode = dto.code ? dto.code.trim().toUpperCase() : current.code;
    if (nextCode !== current.code) {
      const existing = await this.sectionsRepo.findOne({
        where: { planning_offer_id: current.planning_offer_id, code: nextCode },
      });
      if (existing && existing.id !== id) {
        throw new BadRequestException(`La seccion ${nextCode} ya existe para esta oferta.`);
      }
    }
    const next = this.sectionsRepo.create({
      ...current,
      ...dto,
      code: nextCode,
      teacher_id:
        dto.teacher_id !== undefined ? emptyToNull(dto.teacher_id) : current.teacher_id,
      projected_vacancies:
        dto.projected_vacancies !== undefined
          ? Math.max(0, Math.trunc(dto.projected_vacancies))
          : current.projected_vacancies,
      updated_at: new Date(),
    });
    await this.sectionsRepo.save(next);
    if (dto.projected_vacancies !== undefined) {
      const sectionSubsections = await this.subsectionsRepo.find({
        where: { planning_section_id: id },
        order: { code: 'ASC' },
      });
      const subsectionBeforeMap = new Map(sectionSubsections.map((subsection) => [subsection.id, { ...subsection }]));
      const updatedSubsections = sectionSubsections.map((subsection, index) =>
        this.subsectionsRepo.create({
          ...subsection,
          projected_vacancies: resolveSubsectionProjectedVacancies(
            next.projected_vacancies,
            sectionSubsections.length,
            index,
          ),
          updated_at: new Date(),
        }),
      );
      await this.subsectionsRepo.save(updatedSubsections);
      for (const updated of updatedSubsections) {
        const before = subsectionBeforeMap.get(updated.id);
        if (
          before &&
          JSON.stringify(toLogJson(before)) !== JSON.stringify(toLogJson(updated))
        ) {
          await this.logChange(
            'planning_subsection',
            updated.id,
            'UPDATE',
            before,
            updated,
            this.buildSubsectionLogContext(updated, next, currentOffer),
            actor,
          );
        }
      }
    }
    const saved = await this.requireEntity(this.sectionsRepo, id, 'planning_section');
    await this.logChange(
      'planning_section',
      id,
      'UPDATE',
      current,
      saved,
      this.buildSectionLogContext(saved, currentOffer),
      actor,
    );
    await this.refreshOfferStatus(saved.planning_offer_id, undefined, actor);
    return this.getSection(id);
  }

  async deleteSection(actor: ChangeLogActor | null | undefined, id: string) {
    const section = await this.requireEntity(this.sectionsRepo, id, 'planning_section');
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    const offerSections = await this.sectionsRepo.find({
      where: { planning_offer_id: offer.id },
      select: ['id', 'code'],
      order: { code: 'ASC' },
    });
    const highestSectionIndex = offerSections.reduce((currentMax, item) => {
      const index = parseSectionCodeIndex(item.code);
      return index === null ? currentMax : Math.max(currentMax, index);
    }, -1);
    const currentSectionIndex = parseSectionCodeIndex(section.code);
    if (
      currentSectionIndex === null ||
      (highestSectionIndex >= 0 && currentSectionIndex !== highestSectionIndex)
    ) {
      throw new BadRequestException(
        'Solo puedes eliminar la ultima seccion creada para mantener el correlativo.',
      );
    }
    const subsections = await this.subsectionsRepo.find({
      where: { planning_section_id: id },
      order: { code: 'ASC' },
    });
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = subsectionIds.length
      ? await this.schedulesRepo.find({
          where: { planning_subsection_id: In(subsectionIds) },
          order: { day_of_week: 'ASC', start_time: 'ASC' },
        })
      : [];

    for (const schedule of schedules) {
      const subsection = subsections.find((item) => item.id === schedule.planning_subsection_id);
      if (!subsection) {
        continue;
      }
      await this.logChange(
        'planning_subsection_schedule',
        schedule.id,
        'DELETE',
        schedule,
        null,
        this.buildScheduleLogContext(schedule, subsection, section, offer),
        actor,
      );
    }
    for (const subsection of subsections) {
      await this.logChange(
        'planning_subsection',
        subsection.id,
        'DELETE',
        subsection,
        null,
        this.buildSubsectionLogContext(subsection, section, offer),
        actor,
      );
    }
    await this.logChange(
      'planning_section',
      section.id,
      'DELETE',
      section,
      null,
      this.buildSectionLogContext(section, offer),
      actor,
    );

    if (schedules.length > 0) {
      await this.schedulesRepo.remove(schedules);
    }
    if (subsections.length > 0) {
      await this.subsectionsRepo.remove(subsections);
    }
    await this.sectionsRepo.remove(section);
    await this.rebuildConflictsForSemesterByOffer(offer.id, actor);

    return {
      deleted: true,
      id,
      deleted_subsection_count: subsections.length,
      deleted_schedule_count: schedules.length,
    };
  }

  async createSubsection(actor: ChangeLogActor | null | undefined, sectionId: string, dto: CreatePlanningSubsectionDto) {
    const section = await this.requireEntity(this.sectionsRepo, sectionId, 'planning_section');
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    const code = dto.code?.trim().toUpperCase() || (await this.nextSubsectionCode(sectionId, section.code));
    const existing = await this.subsectionsRepo.findOne({
      where: { planning_section_id: sectionId, code },
    });
    if (existing) {
      throw new BadRequestException(`La subseccion ${code} ya existe.`);
    }
    const classroomId = emptyToNull(dto.classroom_id);
    const buildingId = emptyToNull(dto.building_id);
    const classroom = classroomId
      ? await this.classroomsRepo.findOne({ where: { id: classroomId } })
      : null;
    const projectedVacancies =
      dto.projected_vacancies !== undefined
        ? Math.max(0, Math.trunc(dto.projected_vacancies))
        : section.projected_vacancies;
    this.ensureSubsectionVacanciesWithinSection(projectedVacancies, section.projected_vacancies);
    await this.validateSubsectionLocationForOffer(
      offer,
      classroom?.building_id ?? buildingId ?? null,
      classroomId,
    );
    const sectionSubsections = await this.subsectionsRepo.find({
      where: { planning_section_id: sectionId },
      order: { code: 'ASC' },
    });
    this.ensureSubsectionKindAllowed(
      offer,
      dto.kind,
      sectionSubsections.map((item) => item.kind),
    );
    const assigned = resolveAssignedHours(dto.kind, offer);
    const now = new Date();
    const subsection = this.subsectionsRepo.create({
      id: dto.id ?? newId(),
      planning_section_id: sectionId,
      code,
      kind: dto.kind,
      responsible_teacher_id: emptyToNull(dto.responsible_teacher_id),
      course_modality_id: emptyToNull(dto.course_modality_id) ?? section.course_modality_id ?? null,
      building_id: buildingId ?? classroom?.building_id ?? null,
      classroom_id: classroomId,
      capacity_snapshot: dto.capacity_snapshot ?? classroom?.capacity ?? null,
      shift: emptyToNull(dto.shift),
      projected_vacancies: projectedVacancies,
      course_type: offer.course_type,
      assigned_theoretical_hours: assigned.theoretical_hours,
      assigned_practical_hours: assigned.practical_hours,
      assigned_virtual_hours: assigned.virtual_hours,
      assigned_seminar_hours: assigned.seminar_hours,
      assigned_total_hours: assigned.total_hours,
      denomination:
        dto.denomination ??
        buildDenomination(offer.course_code, offer.course_name, section.code, code, offer.campus_id),
      status: 'DRAFT',
      created_at: now,
      updated_at: now,
    });
    const saved = await this.subsectionsRepo.save(subsection);
    await this.logChange(
      'planning_subsection',
      saved.id,
      'CREATE',
      null,
      saved,
      this.buildSubsectionLogContext(saved, section, offer),
      actor,
    );
    await this.refreshOfferStatus(section.planning_offer_id, undefined, actor);
    return this.getSubsection(saved.id);
  }

  async getSubsection(id: string) {
    const subsection = await this.requireEntity(this.subsectionsRepo, id, 'planning_subsection');
    const section = await this.requireEntity(
      this.sectionsRepo,
      subsection.planning_section_id,
      'planning_section',
    );
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    const [schedules, conflicts, teachers, modalities, buildings, classrooms] = await Promise.all([
      this.schedulesRepo.find({
        where: { planning_subsection_id: id },
        order: { day_of_week: 'ASC', start_time: 'ASC' },
      }),
      this.conflictsRepo.find({
        where: { planning_subsection_id: id },
        order: { detected_at: 'DESC' },
      }),
      this.findManyByIds(this.teachersRepo, uniqueIds([subsection.responsible_teacher_id, section.teacher_id])),
      this.findManyByIds(this.courseModalitiesRepo, uniqueIds([subsection.course_modality_id])),
      this.findManyByIds(this.buildingsRepo, uniqueIds([subsection.building_id])),
      this.findManyByIds(this.classroomsRepo, uniqueIds([subsection.classroom_id])),
    ]);

    return this.buildSubsectionDetail(subsection, {
      section,
      offer,
      schedules,
      conflicts,
      teachers,
      modalities,
      buildings,
      classrooms,
    });
  }

  async updateSubsection(actor: ChangeLogActor | null | undefined, id: string, dto: UpdatePlanningSubsectionDto) {
    const current = await this.requireEntity(this.subsectionsRepo, id, 'planning_subsection');
    const section = await this.requireEntity(
      this.sectionsRepo,
      current.planning_section_id,
      'planning_section',
    );
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    const nextCode = dto.code ? dto.code.trim().toUpperCase() : current.code;
    if (nextCode !== current.code) {
      const duplicate = await this.subsectionsRepo.findOne({
        where: { planning_section_id: current.planning_section_id, code: nextCode },
      });
      if (duplicate && duplicate.id !== id) {
        throw new BadRequestException(`La subseccion ${nextCode} ya existe.`);
      }
    }
    const classroomId =
      dto.classroom_id !== undefined ? emptyToNull(dto.classroom_id) : current.classroom_id;
    const classroom = classroomId
      ? await this.classroomsRepo.findOne({ where: { id: classroomId } })
      : null;
    const buildingId =
      dto.building_id !== undefined
        ? emptyToNull(dto.building_id)
        : classroom?.building_id ?? current.building_id;
    const nextProjectedVacancies =
      dto.projected_vacancies !== undefined
        ? Math.max(0, Math.trunc(dto.projected_vacancies))
        : current.projected_vacancies;
    if (dto.projected_vacancies !== undefined) {
      this.ensureSubsectionVacanciesWithinSection(nextProjectedVacancies, section.projected_vacancies);
    }
    const nextBuildingId = classroom?.building_id ?? buildingId ?? null;
    if (dto.building_id !== undefined || dto.classroom_id !== undefined) {
      await this.validateSubsectionLocationForOffer(offer, nextBuildingId, classroomId);
    }
    const nextKind = dto.kind ?? current.kind;
    if (dto.kind !== undefined) {
      const sectionSubsections = await this.subsectionsRepo.find({
        where: { planning_section_id: current.planning_section_id },
        order: { code: 'ASC' },
      });
      const siblingKinds = sectionSubsections
        .filter((item) => item.id !== id)
        .map((item) => item.kind);
      this.ensureSubsectionKindAllowed(offer, nextKind, siblingKinds);
    }
    const assigned = resolveAssignedHours(nextKind, offer);
    const next = this.subsectionsRepo.create({
      ...current,
      ...dto,
      code: nextCode,
      kind: nextKind,
      responsible_teacher_id:
        dto.responsible_teacher_id !== undefined
          ? emptyToNull(dto.responsible_teacher_id)
          : current.responsible_teacher_id,
      course_modality_id:
        dto.course_modality_id !== undefined
          ? emptyToNull(dto.course_modality_id)
          : current.course_modality_id,
      building_id: nextBuildingId,
      classroom_id: classroomId,
      capacity_snapshot:
        dto.capacity_snapshot !== undefined
          ? dto.capacity_snapshot
          : classroom
            ? classroom.capacity
            : classroomId
              ? current.capacity_snapshot
              : null,
      shift: dto.shift !== undefined ? emptyToNull(dto.shift) : current.shift,
      projected_vacancies: nextProjectedVacancies,
      course_type: offer.course_type,
      assigned_theoretical_hours: assigned.theoretical_hours,
      assigned_practical_hours: assigned.practical_hours,
      assigned_virtual_hours: assigned.virtual_hours,
      assigned_seminar_hours: assigned.seminar_hours,
      assigned_total_hours: assigned.total_hours,
      updated_at: new Date(),
    });
    await this.subsectionsRepo.save(next);
    const saved = await this.requireEntity(this.subsectionsRepo, id, 'planning_subsection');
    await this.logChange(
      'planning_subsection',
      id,
      'UPDATE',
      current,
      saved,
      this.buildSubsectionLogContext(saved, section, offer),
      actor,
    );
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id, actor);
    return this.getSubsection(id);
  }

  async createSubsectionSchedule(
    actor: ChangeLogActor | null | undefined,
    subsectionId: string,
    dto: CreatePlanningSubsectionScheduleDto,
  ) {
    const subsection = await this.requireEntity(
      this.subsectionsRepo,
      subsectionId,
      'planning_subsection',
    );
    const section = await this.requireEntity(
      this.sectionsRepo,
      subsection.planning_section_id,
      'planning_section',
    );
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    const existingSchedules = await this.schedulesRepo.count({
      where: { planning_subsection_id: subsectionId },
    });
    if (existingSchedules > 0) {
      throw new BadRequestException('Solo se permite un horario por subseccion.');
    }
    const now = new Date();
    const minutes = computeMinutesFromTimes(dto.start_time, dto.end_time);
    const schedule = this.schedulesRepo.create({
      id: dto.id ?? newId(),
      planning_subsection_id: subsectionId,
      day_of_week: dto.day_of_week,
      start_time: dto.start_time,
      end_time: dto.end_time,
      duration_minutes: minutes,
      academic_hours: roundToTwo(minutes / 50),
      created_at: now,
      updated_at: now,
    });
    const saved = await this.schedulesRepo.save(schedule);
    await this.logChange(
      'planning_subsection_schedule',
      saved.id,
      'CREATE',
      null,
      saved,
      this.buildScheduleLogContext(saved, subsection, section, offer),
      actor,
    );
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id, actor);
    return this.getSubsection(subsectionId);
  }

  async updateSubsectionSchedule(
    actor: ChangeLogActor | null | undefined,
    id: string,
    dto: UpdatePlanningSubsectionScheduleDto,
  ) {
    const current = await this.requireEntity(
      this.schedulesRepo,
      id,
      'planning_subsection_schedule',
    );
    const start = dto.start_time ?? current.start_time;
    const end = dto.end_time ?? current.end_time;
    const minutes = computeMinutesFromTimes(start, end);
    const next = this.schedulesRepo.create({
      ...current,
      ...dto,
      duration_minutes: minutes,
      academic_hours: roundToTwo(minutes / 50),
      updated_at: new Date(),
    });
    await this.schedulesRepo.save(next);
    const saved = await this.requireEntity(this.schedulesRepo, id, 'planning_subsection_schedule');
    const subsection = await this.requireEntity(
      this.subsectionsRepo,
      current.planning_subsection_id,
      'planning_subsection',
    );
    const section = await this.requireEntity(
      this.sectionsRepo,
      subsection.planning_section_id,
      'planning_section',
    );
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    await this.logChange(
      'planning_subsection_schedule',
      id,
      'UPDATE',
      current,
      saved,
      this.buildScheduleLogContext(saved, subsection, section, offer),
      actor,
    );
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id, actor);
    return this.getSubsection(subsection.id);
  }

  async getSubsectionBySchedule(id: string) {
    const schedule = await this.requireEntity(
      this.schedulesRepo,
      id,
      'planning_subsection_schedule',
    );
    return this.getSubsection(schedule.planning_subsection_id);
  }

  async deleteSubsectionSchedule(actor: ChangeLogActor | null | undefined, id: string) {
    const current = await this.requireEntity(
      this.schedulesRepo,
      id,
      'planning_subsection_schedule',
    );
    const subsection = await this.requireEntity(
      this.subsectionsRepo,
      current.planning_subsection_id,
      'planning_subsection',
    );
    const section = await this.requireEntity(
      this.sectionsRepo,
      subsection.planning_section_id,
      'planning_section',
    );
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    await this.schedulesRepo.delete({ id });
    await this.logChange(
      'planning_subsection_schedule',
      id,
      'DELETE',
      current,
      null,
      this.buildScheduleLogContext(current, subsection, section, offer),
      actor,
    );
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id, actor);
    return this.getSubsection(subsection.id);
  }

  async listConflicts(semesterId?: string, offerId?: string) {
    const conflicts = await this.conflictsRepo.find({
      where: {
        ...(semesterId ? { semester_id: semesterId } : {}),
        ...(offerId ? { planning_offer_id: offerId } : {}),
      },
      order: { detected_at: 'DESC' },
    });
    if (conflicts.length === 0) {
      return [];
    }

    const [offers, sections, subsections, schedules, teachers, semesters] = await Promise.all([
      this.findManyByIds(this.offersRepo, uniqueIds(conflicts.map((item) => item.planning_offer_id))),
      this.findManyByIds(this.sectionsRepo, uniqueIds(conflicts.map((item) => item.planning_section_id))),
      this.findManyByIds(this.subsectionsRepo, uniqueIds(conflicts.map((item) => item.planning_subsection_id))),
      this.findManyByIds(
        this.schedulesRepo,
        uniqueIds(conflicts.flatMap((item) => [item.schedule_a_id, item.schedule_b_id])),
      ),
      this.findManyByIds(this.teachersRepo, uniqueIds(conflicts.map((item) => item.teacher_id))),
      this.findManyByIds(this.semestersRepo, uniqueIds(conflicts.map((item) => item.semester_id))),
    ]);
    const [programs, classrooms] = await Promise.all([
      this.findManyByIds(
        this.programsRepo,
        uniqueIds(offers.map((item) => item.academic_program_id)),
      ),
      this.findManyByIds(
        this.classroomsRepo,
        uniqueIds([
          ...conflicts.map((item) => item.classroom_id),
          ...subsections.map((item) => item.classroom_id),
        ]),
      ),
    ]);

    const offerMap = mapById(offers);
    const sectionMap = mapById(sections);
    const subsectionMap = mapById(subsections);
    const scheduleMap = mapById(schedules);
    const teacherMap = mapById(teachers);
    const semesterMap = mapById(semesters);
    const programMap = mapById(programs);
    const classroomMap = mapById(classrooms);

    return conflicts.map((conflict) => {
      const offer = offerMap.get(conflict.planning_offer_id ?? '') ?? null;
      const section = sectionMap.get(conflict.planning_section_id ?? '') ?? null;
      const subsection = subsectionMap.get(conflict.planning_subsection_id ?? '') ?? null;
      const scheduleA = scheduleMap.get(conflict.schedule_a_id) ?? null;
      const scheduleB = scheduleMap.get(conflict.schedule_b_id) ?? null;
      const teacher = teacherMap.get(conflict.teacher_id ?? '') ?? null;
      const semester = semesterMap.get(conflict.semester_id) ?? null;
      const program = offer ? programMap.get(offer.academic_program_id ?? '') ?? null : null;
      const classroom = classroomMap.get(conflict.classroom_id ?? '') ?? null;

      return {
        ...conflict,
        offer,
        section,
        subsection,
        semester_name: semester?.name ?? null,
        academic_program_name: program?.name ?? null,
        teacher_name: formatPlanningTeacherDisplay(teacher),
        classroom_name: classroom?.name ?? null,
        affected_label: buildPlanningConflictAffectedLabel(conflict, offer, section, subsection, teacher),
        overlap_day: scheduleA?.day_of_week ?? scheduleB?.day_of_week ?? null,
        overlap_start:
          scheduleA && scheduleB
            ? minutesToTime(Math.max(toMinutes(scheduleA.start_time), toMinutes(scheduleB.start_time)))
            : null,
        overlap_end:
          scheduleA && scheduleB
            ? minutesToTime(Math.min(toMinutes(scheduleA.end_time), toMinutes(scheduleB.end_time)))
            : null,
        meeting_a: buildPlanningConflictScheduleCard(
          scheduleA,
          subsectionMap,
          sectionMap,
          offerMap,
          classroomMap,
        ),
        meeting_b: buildPlanningConflictScheduleCard(
          scheduleB,
          subsectionMap,
          sectionMap,
          offerMap,
          classroomMap,
        ),
      };
    });
  }

  async listChangeLog(filters: ChangeLogFilters = {}) {
    const limit = normalizeLogLimit(filters.limit);
    const fromDate = parseLogDate(filters.from, false);
    const toDate = parseLogDate(filters.to, true);
    const logs = await this.changeLogsRepo.find({
      where: {
        ...(filters.entity_type ? { entity_type: filters.entity_type } : {}),
        ...(filters.entity_id ? { entity_id: filters.entity_id } : {}),
        ...(filters.action ? { action: filters.action as 'CREATE' | 'UPDATE' | 'DELETE' } : {}),
      },
      order: { changed_at: 'DESC' },
      take: Math.min(Math.max(limit * 5, 300), 1000),
    });
    const changedByQuery = normalizeSearchValue(filters.changed_by);
    const filteredLogs = logs
      .filter((log) => {
        const context = asRecord(log.context_json);
        const changedAt = new Date(log.changed_at);
        if (filters.offer_id && context?.offer_id !== filters.offer_id) {
          return false;
        }
        if (changedByQuery && !normalizeSearchValue(log.changed_by).includes(changedByQuery)) {
          return false;
        }
        if (fromDate && changedAt < fromDate) {
          return false;
        }
        if (toDate && changedAt > toDate) {
          return false;
        }
        return true;
      })
      .slice(0, limit);
    const referenceMaps = await this.buildChangeLogReferenceMaps(filteredLogs);
    return filteredLogs.map((log) => ({
        ...log,
        reference_label: this.buildChangeLogReferenceLabel(log, referenceMaps),
        changes: buildChangeRows(log.before_json, log.after_json, log.action),
      }));
  }

  private async ensureDefaultCatalogs() {
    const now = new Date();
    const defaults = [
      {
        repo: this.studyTypesRepo,
        code: 'PREGRADO',
        name: 'Pregrado',
      },
    ] as const;
    for (const item of defaults) {
      const existing = await item.repo.findOne({ where: { code: item.code } });
      if (!existing) {
        await item.repo.save(
          item.repo.create({
            id: newId(),
            code: item.code,
            name: item.name,
            is_active: true,
            created_at: now,
            updated_at: now,
          }),
        );
      }
    }

    const modalityDefaults = [
      { code: 'PRESENCIAL', name: 'Presencial' },
      { code: 'VIRTUAL', name: 'Virtual' },
      { code: 'HIBRIDO_VIRTUAL', name: 'Hibrido virtual' },
      { code: 'HIBRIDO_PRESENCIAL', name: 'Hibrido presencial' },
    ] as const;
    for (const item of modalityDefaults) {
      const existing = await this.courseModalitiesRepo.findOne({ where: { code: item.code } });
      if (!existing) {
        await this.courseModalitiesRepo.save(
          this.courseModalitiesRepo.create({
            id: newId(),
            code: item.code,
            name: item.name,
            is_active: true,
            created_at: now,
            updated_at: now,
          }),
        );
      }
    }
  }

  private buildStudyPlanCatalog(
    studyPlans: StudyPlanEntity[],
    faculties: FacultyEntity[],
    academicPrograms: AcademicProgramEntity[],
  ) {
    if (studyPlans.length === 0) {
      return {
        faculties,
        academicPrograms,
        studyPlans,
      };
    }

    const facultyIdByName = new Map(
      faculties
        .filter((item) => item.name)
        .map((item) => [normalizeCatalogName(item.name), item.id] as const),
    );
    const programsByName = new Map<string, AcademicProgramEntity[]>();
    for (const program of academicPrograms) {
      const key = normalizeCatalogName(program.name);
      if (!key) {
        continue;
      }
      const bucket = programsByName.get(key) ?? [];
      bucket.push(program);
      programsByName.set(key, bucket);
    }

    const normalizedStudyPlans = studyPlans.map((plan) => {
      const facultyName = coalesceCatalogLabel(plan.faculty);
      const careerName = coalesceCatalogLabel(plan.career, plan.academic_program);
      const resolvedFacultyId =
        plan.faculty_id ??
        (facultyName ? facultyIdByName.get(normalizeCatalogName(facultyName)) ?? null : null) ??
        (facultyName ? buildStableCatalogId('faculty', facultyName) : null);
      const resolvedProgramId =
        plan.academic_program_id ??
        this.resolveCatalogProgramId(
          programsByName,
          careerName,
          facultyName,
          resolvedFacultyId,
        ) ??
        (careerName ? buildStableCatalogId('program', facultyName, careerName) : null);

      return {
        ...plan,
        faculty: facultyName,
        career: careerName,
        academic_program: careerName,
        faculty_id: resolvedFacultyId,
        academic_program_id: resolvedProgramId,
      };
    });

    const facultyMap = new Map<string, { id: string; name: string }>();
    const academicProgramMap = new Map<
      string,
      { id: string; name: string; faculty_id: string | null; faculty: string | null }
    >();

    for (const plan of normalizedStudyPlans) {
      if (plan.faculty && plan.faculty_id && !facultyMap.has(plan.faculty_id)) {
        facultyMap.set(plan.faculty_id, {
          id: plan.faculty_id,
          name: plan.faculty,
        });
      }
      if (plan.career && plan.academic_program_id && !academicProgramMap.has(plan.academic_program_id)) {
        academicProgramMap.set(plan.academic_program_id, {
          id: plan.academic_program_id,
          name: plan.career,
          faculty_id: plan.faculty_id ?? null,
          faculty: plan.faculty ?? null,
        });
      }
    }

    return {
      faculties: [...facultyMap.values()].sort((a, b) => compareCatalogLabels(a.name, b.name)),
      academicPrograms: [...academicProgramMap.values()].sort((a, b) =>
        compareCatalogLabels(`${a.faculty ?? ''} ${a.name}`, `${b.faculty ?? ''} ${b.name}`),
      ),
      studyPlans: normalizedStudyPlans,
    };
  }

  private resolveCatalogProgramId(
    programsByName: Map<string, AcademicProgramEntity[]>,
    careerName: string | null,
    facultyName: string | null,
    facultyId: string | null,
  ) {
    const normalizedCareer = normalizeCatalogName(careerName);
    if (!normalizedCareer) {
      return null;
    }

    const matches = programsByName.get(normalizedCareer) ?? [];
    if (matches.length === 0) {
      return null;
    }
    if (matches.length === 1) {
      return matches[0].id;
    }

    if (facultyId) {
      const byFacultyId = matches.find((item) => item.faculty_id === facultyId);
      if (byFacultyId) {
        return byFacultyId.id;
      }
    }

    const normalizedFaculty = normalizeCatalogName(facultyName);
    if (normalizedFaculty) {
      const byFacultyName = matches.find(
        (item) => normalizeCatalogName(item.faculty) === normalizedFaculty,
      );
      if (byFacultyName) {
        return byFacultyName.id;
      }
    }

    return matches[0].id;
  }

  // validateCycleRange removed — single cycle field, no range needed

  private async ensureStudyPlanMatchesContext(
    studyPlanId: string,
    academicProgramId: string | null | undefined,
    facultyId: string | null | undefined,
    cycle: number,
  ) {
    const plan = await this.resolveCatalogStudyPlan(studyPlanId);
    if (!plan) {
      throw new NotFoundException(`study_plan ${studyPlanId} no existe.`);
    }
    if (academicProgramId && plan.academic_program_id && plan.academic_program_id !== academicProgramId) {
      throw new BadRequestException(
        'El plan de estudio seleccionado no pertenece al programa indicado.',
      );
    }
    if (facultyId && plan.faculty_id && plan.faculty_id !== facultyId) {
      throw new BadRequestException(
        'El plan de estudio seleccionado no pertenece a la facultad indicada.',
      );
    }

    const cycles = await this.listCyclesForStudyPlan(studyPlanId);
    if (!cycles.some((item) => item.value === cycle)) {
      throw new BadRequestException(
        `El plan de estudio seleccionado no contiene cursos para el ciclo ${cycle}.`,
      );
    }
  }

  private async ensureNoOverlappingRule(
    candidate: {
      semester_id: string;
      campus_id: string | null;
      academic_program_id: string;
      cycle: number;
      is_active?: boolean;
    },
    currentId?: string,
  ) {
    if (candidate.is_active === false) {
      return;
    }
    const rules = await this.planRulesRepo.find({
      where: {
        semester_id: candidate.semester_id,
        ...(candidate.campus_id ? { campus_id: candidate.campus_id } : {}),
        academic_program_id: candidate.academic_program_id,
        cycle: candidate.cycle,
        is_active: true,
      },
    });
    for (const rule of rules) {
      if (rule.id === currentId) {
        continue;
      }
      throw new BadRequestException(
        `Ya existe una regla para el ciclo ${candidate.cycle}.`,
      );
    }
  }

  private async enrichPlanRules(rules: PlanningCyclePlanRuleEntity[]) {
    const [semesters, campuses, programs, plans, faculties] = await Promise.all([
      this.findManyByIds(this.semestersRepo, uniqueIds(rules.map((item) => item.semester_id))),
      this.findManyByIds(this.campusesRepo, uniqueIds(rules.map((item) => item.campus_id))),
      this.findManyByIds(this.programsRepo, uniqueIds(rules.map((item) => item.academic_program_id))),
      this.findManyByIds(this.studyPlansRepo, uniqueIds(rules.map((item) => item.study_plan_id))),
      this.findManyByIds(this.facultiesRepo, uniqueIds(rules.map((item) => item.faculty_id))),
    ]);
    const semesterMap = mapById(semesters);
    const campusMap = mapById(campuses);
    const programMap = mapById(programs);
    const planMap = mapById(plans);
    const facultyMap = mapById(faculties);
    return rules.map((rule) => {
      const plan = planMap.get(rule.study_plan_id) ?? null;
      const fallbackFacultyName = coalesceCatalogLabel(plan?.faculty);
      const fallbackCareerName = coalesceCatalogLabel(
        rule.career_name,
        plan?.career,
        plan?.academic_program,
      );
      const fallbackFacultyId =
        rule.faculty_id ??
        (fallbackFacultyName ? buildStableCatalogId('faculty', fallbackFacultyName) : null);
      const fallbackProgramId =
        rule.academic_program_id ??
        (fallbackCareerName
            ? buildStableCatalogId('program', fallbackFacultyName, fallbackCareerName)
            : null);
        return {
          ...rule,
          semester: semesterMap.get(rule.semester_id) ?? null,
          campus: campusMap.get(rule.campus_id ?? '') ?? null,
          academic_program:
          programMap.get(rule.academic_program_id) ??
          (fallbackCareerName && fallbackProgramId
            ? {
                id: fallbackProgramId,
                name: fallbackCareerName,
                faculty_id: fallbackFacultyId,
                faculty: fallbackFacultyName,
              }
            : null),
        study_plan: plan,
        faculty:
          facultyMap.get(rule.faculty_id ?? '') ??
          (fallbackFacultyName && fallbackFacultyId
            ? { id: fallbackFacultyId, name: fallbackFacultyName }
            : null),
      };
    });
  }

  private async enrichPlanRule(rule: PlanningCyclePlanRuleEntity) {
    const [item] = await this.enrichPlanRules([rule]);
    return item;
  }

  private async findMatchingRules(
    semesterId: string,
    campusId: string,
    academicProgramId: string,
    cycle: number,
  ) {
    const rules = await this.planRulesRepo.find({
      where: {
        semester_id: semesterId,
        campus_id: campusId,
        academic_program_id: academicProgramId,
        is_active: true,
      },
      order: { cycle: 'ASC' },
    });
    return rules.filter((item) => cycle === item.cycle);
  }

  private async listCycleOfferBlueprints(
    studyPlanId: string,
    cycle: number,
  ): Promise<PlanningOfferBlueprint[]> {
    const courses = await this.studyPlanCoursesRepo.find({
      where: { study_plan_id: studyPlanId },
      order: { order: 'ASC', course_code: 'ASC' },
    });
    const details = await this.findManyByField(
      this.studyPlanCourseDetailsRepo,
      'study_plan_course_id',
      courses.map((item) => item.id),
    );
    const detailById = new Map(details.map((item) => [item.study_plan_course_id, item]));

    return courses
      .map((course) => {
        const detail = detailById.get(course.id) ?? null;
        const resolvedCycle =
          detail?.academic_year ?? extractCycleFromLabel(course.year_label) ?? null;
        if (resolvedCycle !== cycle) {
          return null;
        }

        const theoreticalHours = numberValue(detail?.theoretical_hours);
        const practicalHours = numberValue(detail?.practical_hours);

        return {
          study_plan_course_id: course.id,
          study_plan_id: studyPlanId,
          cycle: resolvedCycle,
          course_code: detail?.short_code ?? course.course_code ?? null,
          course_name: detail?.name ?? course.course_name ?? null,
          credits: numberValue(detail?.credits, numberValue(course.credits)),
          theoretical_hours: theoreticalHours,
          practical_hours: practicalHours,
          total_hours: roundToTwo(theoreticalHours + practicalHours),
          course_type: deriveCourseType(theoreticalHours, practicalHours),
        } satisfies PlanningOfferBlueprint;
      })
      .filter((item): item is PlanningOfferBlueprint => Boolean(item));
  }

  private async findCycleOfferBlueprint(
    studyPlanId: string,
    cycle: number,
    studyPlanCourseId: string,
  ) {
    const blueprints = await this.listCycleOfferBlueprints(studyPlanId, cycle);
    return blueprints.find((item) => item.study_plan_course_id === studyPlanCourseId) ?? null;
  }

  private async ensureCycleOffersForRule(rule: PlanningCyclePlanRuleEntity, actor?: ChangeLogActor | null) {
    const blueprints = await this.listCycleOfferBlueprints(rule.study_plan_id, rule.cycle);
    const existingOffers = await this.offersRepo.find({
      where: {
        semester_id: rule.semester_id,
        campus_id: rule.campus_id ?? undefined,
        study_plan_id: rule.study_plan_id,
        cycle: rule.cycle,
      },
    });
    const existingByCourseId = new Map(
      existingOffers.map((offer) => [offer.study_plan_course_id, offer]),
    );
    const studyTypeId = await this.defaultStudyTypeId();
    const now = new Date();
    const newOffers = blueprints
      .filter((item) => !existingByCourseId.has(item.study_plan_course_id))
      .map((item) =>
        this.offersRepo.create({
          id: newId(),
          semester_id: rule.semester_id,
          campus_id: rule.campus_id ?? '',
          faculty_id: rule.faculty_id ?? null,
          academic_program_id: rule.academic_program_id,
          study_plan_id: rule.study_plan_id,
          cycle: rule.cycle,
          study_plan_course_id: item.study_plan_course_id,
          course_code: item.course_code,
          course_name: item.course_name,
          study_type_id: studyTypeId,
          course_type: item.course_type,
          theoretical_hours: item.theoretical_hours,
          practical_hours: item.practical_hours,
          total_hours: item.total_hours,
          status: 'DRAFT',
          created_at: now,
          updated_at: now,
        }),
      );

    const savedOffers = newOffers.length > 0 ? await this.offersRepo.save(newOffers) : [];
    for (const offer of savedOffers) {
      await this.logChange(
        'planning_offer',
        offer.id,
        'CREATE',
        null,
        offer,
        {
          ...this.buildOfferLogContext(offer),
          source: 'create_plan_rule',
          plan_rule_id: rule.id,
        },
        actor,
      );
    }

    return {
      created: savedOffers.length,
      existing: blueprints.length - savedOffers.length,
      total: blueprints.length,
    };
  }

  private async resolveCatalogStudyPlan(studyPlanId: string) {
    const [studyPlans, faculties, academicPrograms] = await Promise.all([
      this.studyPlansRepo.find(),
      this.facultiesRepo.find(),
      this.programsRepo.find(),
    ]);
    const catalog = this.buildStudyPlanCatalog(studyPlans, faculties, academicPrograms);
    return catalog.studyPlans.find((item) => item.id === studyPlanId) ?? null;
  }

  private async listCyclesForStudyPlan(studyPlanId: string) {
    const courses = await this.studyPlanCoursesRepo.find({
      where: { study_plan_id: studyPlanId },
      select: { id: true, study_plan_id: true, year_label: true },
    });
    const details = await this.findManyByField(
      this.studyPlanCourseDetailsRepo,
      'study_plan_course_id',
      courses.map((item) => item.id),
    );
    return buildStudyPlanCycles(courses, details);
  }

  private async enrichOffers(offers: PlanningOfferEntity[]) {
    const context = await this.buildContext(offers.map((item) => item.id));
    return offers.map((offer) => this.buildOfferSummary(offer, context));
  }

  private async buildContext(offerIds: string[]): Promise<PlanningContext> {
    if (offerIds.length === 0) {
      return {
        offers: [],
        sections: [],
        subsections: [],
        schedules: [],
        teachers: [],
        buildings: [],
        classrooms: [],
        conflicts: [],
        changeLogs: [],
      };
    }
    const offers = await this.offersRepo.find({ where: { id: In(offerIds) } });
    const sections = await this.sectionsRepo.find({
      where: { planning_offer_id: In(offerIds) },
      order: { code: 'ASC' },
    });
    const sectionIds = sections.map((item) => item.id);
    const subsections = sectionIds.length
      ? await this.subsectionsRepo.find({
          where: { planning_section_id: In(sectionIds) },
          order: { code: 'ASC' },
        })
      : [];
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = subsectionIds.length
      ? await this.schedulesRepo.find({
          where: { planning_subsection_id: In(subsectionIds) },
          order: { day_of_week: 'ASC', start_time: 'ASC' },
        })
      : [];
    const conflicts = await this.conflictsRepo.find({
      where: { planning_offer_id: In(offerIds) },
      order: { detected_at: 'DESC' },
    });
    const entityIds = uniqueIds([
      ...offerIds,
      ...sectionIds,
      ...subsectionIds,
      ...schedules.map((item) => item.id),
    ]);
    const changeLogs = await this.changeLogsRepo.find({
      where: { entity_id: In(entityIds) },
      order: { changed_at: 'DESC' },
    });
    const teachers = await this.findManyByIds(
      this.teachersRepo,
      uniqueIds([
        ...sections.map((item) => item.teacher_id),
        ...subsections.map((item) => item.responsible_teacher_id),
      ]),
    );
    const buildings = await this.findManyByIds(
      this.buildingsRepo,
      uniqueIds(subsections.map((item) => item.building_id)),
    );
    const classrooms = await this.findManyByIds(
      this.classroomsRepo,
      uniqueIds(subsections.map((item) => item.classroom_id)),
    );
    return {
      offers,
      sections,
      subsections,
      schedules,
      teachers,
      buildings,
      classrooms,
      conflicts,
      changeLogs,
    };
  }

  private buildOfferSummary(offer: PlanningOfferEntity, context: PlanningContext) {
    const sections = context.sections.filter((item) => item.planning_offer_id === offer.id);
    const sectionIds = sections.map((item) => item.id);
    const subsections = context.subsections.filter((item) => sectionIds.includes(item.planning_section_id));
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = context.schedules.filter((item) => subsectionIds.includes(item.planning_subsection_id));
    const conflicts = context.conflicts.filter((item) => item.planning_offer_id === offer.id);
    return {
      ...offer,
      section_count: sections.length,
      subsection_count: subsections.length,
      schedule_count: schedules.length,
      conflict_count: conflicts.length,
    };
  }

  private buildOfferDetail(offer: PlanningOfferEntity, context: PlanningContext) {
    const sectionMap = groupBy(context.sections, (item) => item.planning_offer_id);
    const subsectionMap = groupBy(context.subsections, (item) => item.planning_section_id);
    const scheduleMap = groupBy(context.schedules, (item) => item.planning_subsection_id);
    const teacherMap = mapById(context.teachers);
    const buildingMap = mapById(context.buildings);
    const classroomMap = mapById(context.classrooms);
    const sectionConflicts = groupBy(context.conflicts, (item) => item.planning_section_id ?? '__null__');
    const subsectionConflicts = groupBy(
      context.conflicts,
      (item) => item.planning_subsection_id ?? '__null__',
    );

    return {
      ...this.buildOfferSummary(offer, context),
      sections: (sectionMap.get(offer.id) ?? []).map((section) => ({
        ...section,
        teacher: teacherMap.get(section.teacher_id ?? '') ?? null,
        subsections: (subsectionMap.get(section.id) ?? []).map((subsection) =>
          this.buildSubsectionDetail(subsection, {
            schedules: scheduleMap.get(subsection.id) ?? [],
            conflicts: subsectionConflicts.get(subsection.id) ?? [],
            teachers: context.teachers,
            buildings: context.buildings,
            classrooms: context.classrooms,
          }),
        ),
        conflicts: sectionConflicts.get(section.id) ?? [],
      })),
      conflicts: context.conflicts.filter((item) => item.planning_offer_id === offer.id),
      change_log: context.changeLogs,
    };
  }

  private async buildOfferConfigurationContext(offerIds: string[]) {
    if (offerIds.length === 0) {
      return {
        sectionsByOfferId: new Map<string, PlanningSectionEntity[]>(),
        subsectionsBySectionId: new Map<string, PlanningSubsectionEntity[]>(),
        schedulesBySubsectionId: new Map<string, PlanningSubsectionScheduleEntity[]>(),
      };
    }

    const sections = await this.sectionsRepo.find({
      where: { planning_offer_id: In(offerIds) },
      order: { code: 'ASC' },
    });
    const sectionIds = sections.map((item) => item.id);
    const subsections = sectionIds.length
      ? await this.subsectionsRepo.find({
          where: { planning_section_id: In(sectionIds) },
          order: { code: 'ASC' },
        })
      : [];
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = subsectionIds.length
      ? await this.schedulesRepo.find({
          where: { planning_subsection_id: In(subsectionIds) },
          order: { day_of_week: 'ASC', start_time: 'ASC' },
        })
      : [];

    return {
      sectionsByOfferId: groupBy(sections, (item) => item.planning_offer_id),
      subsectionsBySectionId: groupBy(subsections, (item) => item.planning_section_id),
      schedulesBySubsectionId: groupBy(schedules, (item) => item.planning_subsection_id),
    };
  }

  private buildOfferReviewState(
    offerId: string,
    sectionsByOfferId: Map<string, PlanningSectionEntity[]>,
    subsectionsBySectionId: Map<string, PlanningSubsectionEntity[]>,
    schedulesBySubsectionId: Map<string, PlanningSubsectionScheduleEntity[]>,
  ) {
    const sections = sectionsByOfferId.get(offerId) ?? [];
    if (sections.length === 0) {
      return {
        is_ready: false,
        section_count: 0,
        subsection_count: 0,
        ready_subsection_count: 0,
      };
    }

    let subsectionCount = 0;
    let readySubsectionCount = 0;
    for (const section of sections) {
      const sectionSubsections = subsectionsBySectionId.get(section.id) ?? [];
      if (sectionSubsections.length === 0) {
        return {
          is_ready: false,
          section_count: sections.length,
          subsection_count: subsectionCount,
          ready_subsection_count: readySubsectionCount,
        };
      }
      subsectionCount += sectionSubsections.length;
      for (const subsection of sectionSubsections) {
        if (
          this.subsectionHasRequiredConfiguration(
            subsection,
            (schedulesBySubsectionId.get(subsection.id) ?? []).length,
          )
        ) {
          readySubsectionCount += 1;
        }
      }
    }

    return {
      is_ready: subsectionCount > 0 && readySubsectionCount === subsectionCount,
      section_count: sections.length,
      subsection_count: subsectionCount,
      ready_subsection_count: readySubsectionCount,
    };
  }

  private subsectionHasRequiredConfiguration(
    subsection: PlanningSubsectionEntity,
    scheduleCount: number,
  ) {
    return Boolean(
      subsection.responsible_teacher_id &&
        subsection.shift &&
        subsection.course_modality_id &&
        scheduleCount > 0,
    );
  }

  private async assertPlanRuleReadyForReview(rule: PlanningCyclePlanRuleEntity) {
    const candidatesPayload = await this.listCourseCandidates({
      semester_id: rule.semester_id,
      campus_id: rule.campus_id ?? undefined,
      faculty_id: rule.faculty_id ?? undefined,
      academic_program_id: rule.academic_program_id,
      cycle: rule.cycle,
      study_plan_id: rule.study_plan_id,
    });
    const candidates = Array.isArray(candidatesPayload?.candidates)
      ? candidatesPayload.candidates
      : [];
    if (candidates.length === 0) {
      throw new BadRequestException(
        'El plan no puede enviarse a revision porque no tiene cursos configurados en el ciclo.',
      );
    }

    const pending = candidates.filter((item: any) => !item?.has_offer || !item?.review_ready);
    if (pending.length === 0) {
      return;
    }

    const sample = pending
      .slice(0, 3)
      .map((item: any) => [item?.course_code, item?.course_name].filter(Boolean).join(' - '))
      .filter(Boolean)
      .join(', ');
    throw new BadRequestException(
      sample
        ? `El plan no puede enviarse a revision hasta que todos sus cursos esten listos. Revisa: ${sample}.`
        : 'El plan no puede enviarse a revision hasta que todos sus cursos tengan al menos una seccion y todas sus subsecciones configuradas.',
    );
  }

  private buildSubsectionDetail(
    subsection: PlanningSubsectionEntity,
    input: {
      section?: PlanningSectionEntity;
      offer?: PlanningOfferEntity;
      schedules: PlanningSubsectionScheduleEntity[];
      conflicts: PlanningScheduleConflictV2Entity[];
      teachers: TeacherEntity[];
      modalities?: CourseModalityEntity[];
      buildings: BuildingEntity[];
      classrooms: ClassroomEntity[];
    },
  ) {
    const teacherMap = mapById(input.teachers);
    const modalityMap = mapById(input.modalities ?? []);
    const buildingMap = mapById(input.buildings);
    const classroomMap = mapById(input.classrooms);
    const ownSchedules = input.schedules.filter((item) => item.planning_subsection_id === subsection.id);
    return {
      ...subsection,
      section: input.section ?? null,
      offer: input.offer ?? null,
      responsible_teacher: teacherMap.get(subsection.responsible_teacher_id ?? '') ?? null,
      modality: modalityMap.get(subsection.course_modality_id ?? '') ?? null,
      building: buildingMap.get(subsection.building_id ?? '') ?? null,
      classroom: classroomMap.get(subsection.classroom_id ?? '') ?? null,
      schedules: ownSchedules,
      conflicts: input.conflicts.filter((item) => item.planning_subsection_id === subsection.id),
    };
  }

  private async findPlanRuleForOfferContext(input: {
    semester_id: string;
    campus_id: string | null;
    academic_program_id: string | null;
    study_plan_id: string;
    cycle: number;
  }) {
    if (!input.academic_program_id) {
      return null;
    }
    return this.planRulesRepo.findOne({
      where: {
        semester_id: input.semester_id,
        campus_id: input.campus_id ?? undefined,
        academic_program_id: input.academic_program_id,
        study_plan_id: input.study_plan_id,
        cycle: input.cycle,
        is_active: true,
      },
      order: { created_at: 'DESC' },
    });
  }

  private ensurePlanRuleEditable(rule: PlanningCyclePlanRuleEntity | null) {
    if (!rule) {
      return;
    }
    if (rule.workflow_status === 'IN_REVIEW' || rule.workflow_status === 'APPROVED') {
      throw new BadRequestException(
        'Este plan esta bloqueado por workflow. Solo puede editarse si vuelve a correccion.',
      );
    }
  }

  private async assertOfferPlanEditable(offer: PlanningOfferEntity) {
    const rule = await this.findPlanRuleForOfferContext({
      semester_id: offer.semester_id,
      campus_id: offer.campus_id,
      academic_program_id: offer.academic_program_id ?? null,
      study_plan_id: offer.study_plan_id,
      cycle: offer.cycle,
    });
    this.ensurePlanRuleEditable(rule);
    return rule;
  }

  private ensureSubsectionKindAllowed(
    offer: PlanningOfferEntity,
    nextKind: PlanningSubsectionKind,
    siblingKinds: PlanningSubsectionKind[],
  ) {
    const allowedKinds = resolveAllowedSubsectionKinds(offer.course_type, siblingKinds.length + 1);
    if (!allowedKinds.includes(nextKind)) {
      throw new BadRequestException(buildSubsectionKindErrorMessage(offer.course_type, siblingKinds.length + 1));
    }
    if (!validateSubsectionKindDistribution(offer.course_type, [...siblingKinds, nextKind])) {
      throw new BadRequestException(
        'En cursos teorico practico debe existir al menos una subseccion teorica y una practica.',
      );
    }
  }

  private async defaultStudyTypeId() {
    await this.ensureDefaultCatalogs();
    const item = await this.studyTypesRepo.findOne({ where: { code: 'PREGRADO' } });
    return item?.id ?? null;
  }

  private async defaultCourseModalityId() {
    await this.ensureDefaultCatalogs();
    const item = await this.courseModalitiesRepo.findOne({ where: { code: 'PRESENCIAL' } });
    return item?.id ?? null;
  }

  private async nextSubsectionCode(sectionId: string, sectionCode: string) {
    const subsections = await this.subsectionsRepo.find({
      where: { planning_section_id: sectionId },
      order: { code: 'ASC' },
    });
    const existing = new Set(subsections.map((item) => item.code));
    let counter = 0;
    let candidate = `${sectionCode}${counter}`;
    while (existing.has(candidate)) {
      counter += 1;
      candidate = `${sectionCode}${counter}`;
    }
    return candidate;
  }

  private async rebuildConflictsForSemesterByOffer(offerId: string, actor?: ChangeLogActor | null) {
    const offer = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    await this.rebuildConflictsForSemester(offer.semester_id, actor);
  }

  private async rebuildConflictsForSemester(semesterId: string, actor?: ChangeLogActor | null) {
    const offers = await this.offersRepo.find({ where: { semester_id: semesterId } });
    const offerIds = offers.map((item) => item.id);
    const sections = offerIds.length
      ? await this.sectionsRepo.find({ where: { planning_offer_id: In(offerIds) } })
      : [];
    const sectionIds = sections.map((item) => item.id);
    const subsections = sectionIds.length
      ? await this.subsectionsRepo.find({ where: { planning_section_id: In(sectionIds) } })
      : [];
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = subsectionIds.length
      ? await this.schedulesRepo.find({ where: { planning_subsection_id: In(subsectionIds) } })
      : [];
    await this.conflictsRepo.delete({ semester_id: semesterId });
    const conflicts = this.calculateConflictsForDataset(semesterId, offers, sections, subsections, schedules);
    if (conflicts.length) {
      await this.conflictsRepo.save(conflicts);
    }
    for (const item of offers) {
      await this.refreshOfferStatus(item.id, conflicts, actor);
    }
  }

  private async refreshOfferStatus(
    offerId: string,
    cachedConflicts?: PlanningScheduleConflictV2Entity[],
    actor?: ChangeLogActor | null,
  ) {
    const offer = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    const configuration = await this.buildOfferConfigurationContext([offerId]);
    const reviewState = this.buildOfferReviewState(
      offerId,
      configuration.sectionsByOfferId,
      configuration.subsectionsBySectionId,
      configuration.schedulesBySubsectionId,
    );
    const conflicts =
      cachedConflicts?.filter((item) => item.planning_offer_id === offerId) ??
      (await this.conflictsRepo.find({ where: { planning_offer_id: offerId } }));
    let status: PlanningOfferStatus = 'DRAFT';
    if (conflicts.length > 0) {
      status = 'OBSERVED';
    } else if (reviewState.is_ready) {
      status = 'ACTIVE';
    }
    if (status !== offer.status) {
      const before = { ...offer };
      offer.status = status;
      offer.updated_at = new Date();
      await this.offersRepo.save(offer);
      await this.logChange(
        'planning_offer',
        offer.id,
        'UPDATE',
        before,
        offer,
        {
          ...this.buildOfferLogContext(offer),
          reason: 'status_refresh',
        },
        actor,
      );
    }
  }

  private calculateConflictsForDataset(
    semesterId: string,
    offers: PlanningOfferEntity[],
    sections: PlanningSectionEntity[],
    subsections: PlanningSubsectionEntity[],
    schedules: PlanningSubsectionScheduleEntity[],
  ): PlanningScheduleConflictV2Entity[] {
    const offerMap = mapById(offers);
    const sectionMap = mapById(sections);
    const subsectionMap = mapById(subsections);
    const conflicts: PlanningScheduleConflictV2Entity[] = [];
    const now = new Date();

    for (let i = 0; i < schedules.length; i += 1) {
      for (let j = i + 1; j < schedules.length; j += 1) {
        const a = schedules[i];
        const b = schedules[j];
        if (a.day_of_week !== b.day_of_week) {
          continue;
        }
        const overlapMinutes = overlap(a.start_time, a.end_time, b.start_time, b.end_time);
        if (overlapMinutes <= 0) {
          continue;
        }
        const subsectionA = subsectionMap.get(a.planning_subsection_id);
        const subsectionB = subsectionMap.get(b.planning_subsection_id);
        if (!subsectionA || !subsectionB) {
          continue;
        }
        const sectionA = sectionMap.get(subsectionA.planning_section_id);
        const sectionB = sectionMap.get(subsectionB.planning_section_id);
        if (!sectionA || !sectionB) {
          continue;
        }
        const offerA = offerMap.get(sectionA.planning_offer_id);
        const offerB = offerMap.get(sectionB.planning_offer_id);
        if (!offerA || !offerB || offerA.semester_id !== semesterId || offerB.semester_id !== semesterId) {
          continue;
        }

        if (subsectionA.id === subsectionB.id) {
          conflicts.push(
            this.newConflict(
              'SUBSECTION_OVERLAP',
              semesterId,
              offerA.id,
              sectionA.id,
              subsectionA.id,
              a.id,
              b.id,
              overlapMinutes,
              now,
            ),
          );
        }

        if (
          subsectionA.responsible_teacher_id &&
          subsectionA.responsible_teacher_id === subsectionB.responsible_teacher_id
        ) {
          conflicts.push(
            this.newConflict(
              'TEACHER_OVERLAP',
              semesterId,
              offerA.id,
              sectionA.id,
              subsectionA.id,
              a.id,
              b.id,
              overlapMinutes,
              now,
              subsectionA.responsible_teacher_id,
              undefined,
              {
                other_offer_id: offerB.id,
                other_section_id: sectionB.id,
                other_subsection_id: subsectionB.id,
              },
            ),
            this.newConflict(
              'TEACHER_OVERLAP',
              semesterId,
              offerB.id,
              sectionB.id,
              subsectionB.id,
              a.id,
              b.id,
              overlapMinutes,
              now,
              subsectionB.responsible_teacher_id,
              undefined,
              {
                other_offer_id: offerA.id,
                other_section_id: sectionA.id,
                other_subsection_id: subsectionA.id,
              },
            ),
          );
        }

        if (subsectionA.classroom_id && subsectionA.classroom_id === subsectionB.classroom_id) {
          conflicts.push(
            this.newConflict(
              'CLASSROOM_OVERLAP',
              semesterId,
              offerA.id,
              sectionA.id,
              subsectionA.id,
              a.id,
              b.id,
              overlapMinutes,
              now,
              undefined,
              subsectionA.classroom_id,
              {
                other_offer_id: offerB.id,
                other_section_id: sectionB.id,
                other_subsection_id: subsectionB.id,
              },
            ),
            this.newConflict(
              'CLASSROOM_OVERLAP',
              semesterId,
              offerB.id,
              sectionB.id,
              subsectionB.id,
              a.id,
              b.id,
              overlapMinutes,
              now,
              undefined,
              subsectionB.classroom_id,
              {
                other_offer_id: offerA.id,
                other_section_id: sectionA.id,
                other_subsection_id: subsectionA.id,
              },
            ),
          );
        }

        if (
          sectionA.code === sectionB.code &&
          offerA.id !== offerB.id &&
          offerA.campus_id === offerB.campus_id &&
          offerA.academic_program_id === offerB.academic_program_id &&
          offerA.cycle === offerB.cycle
        ) {
          conflicts.push(
            this.newConflict(
              'SECTION_OVERLAP',
              semesterId,
              offerA.id,
              sectionA.id,
              subsectionA.id,
              a.id,
              b.id,
              overlapMinutes,
              now,
              undefined,
              undefined,
              {
                other_offer_id: offerB.id,
                other_section_id: sectionB.id,
                other_subsection_id: subsectionB.id,
              },
            ),
            this.newConflict(
              'SECTION_OVERLAP',
              semesterId,
              offerB.id,
              sectionB.id,
              subsectionB.id,
              a.id,
              b.id,
              overlapMinutes,
              now,
              undefined,
              undefined,
              {
                other_offer_id: offerA.id,
                other_section_id: sectionA.id,
                other_subsection_id: subsectionA.id,
              },
            ),
          );
        }
      }
    }

    return dedupePlanningConflicts(conflicts);
  }

  private async deletePlanRuleHierarchy(
    rule: PlanningCyclePlanRuleEntity,
    actor?: ChangeLogActor | null,
  ) {
    const offers = await this.offersRepo.find({
      where: {
        semester_id: rule.semester_id,
        academic_program_id: rule.academic_program_id,
        study_plan_id: rule.study_plan_id,
        cycle: rule.cycle,
        ...(rule.campus_id ? { campus_id: rule.campus_id } : {}),
      },
      order: { course_code: 'ASC', created_at: 'ASC' },
    });

    const offerIds = offers.map((item) => item.id);
    const sections = offerIds.length
      ? await this.sectionsRepo.find({
          where: { planning_offer_id: In(offerIds) },
          order: { code: 'ASC', created_at: 'ASC' },
        })
      : [];
    const sectionIds = sections.map((item) => item.id);
    const subsections = sectionIds.length
      ? await this.subsectionsRepo.find({
          where: { planning_section_id: In(sectionIds) },
          order: { code: 'ASC', created_at: 'ASC' },
        })
      : [];
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = subsectionIds.length
      ? await this.schedulesRepo.find({
          where: { planning_subsection_id: In(subsectionIds) },
          order: { planning_subsection_id: 'ASC', day_of_week: 'ASC', start_time: 'ASC' },
        })
      : [];

    await this.planRulesRepo.manager.transaction(async (manager) => {
      if (subsectionIds.length > 0) {
        await manager.delete(PlanningSubsectionScheduleEntity, {
          planning_subsection_id: In(subsectionIds),
        });
      }
      if (sectionIds.length > 0) {
        await manager.delete(PlanningSubsectionEntity, {
          planning_section_id: In(sectionIds),
        });
      }
      if (offerIds.length > 0) {
        await manager.delete(PlanningSectionEntity, {
          planning_offer_id: In(offerIds),
        });
        await manager.delete(PlanningOfferEntity, {
          id: In(offerIds),
        });
      }
      await manager.delete(PlanningCyclePlanRuleEntity, { id: rule.id });
    });

    const offerMap = mapById(offers);
    const sectionMap = mapById(sections);
    const subsectionMap = mapById(subsections);

    for (const schedule of schedules) {
      const subsection = subsectionMap.get(schedule.planning_subsection_id);
      if (!subsection) {
        continue;
      }
      const section = sectionMap.get(subsection.planning_section_id);
      if (!section) {
        continue;
      }
      const offer = offerMap.get(section.planning_offer_id);
      if (!offer) {
        continue;
      }
      await this.logChange(
        'planning_subsection_schedule',
        schedule.id,
        'DELETE',
        schedule,
        null,
        {
          ...this.buildScheduleLogContext(schedule, subsection, section, offer),
          source: 'delete_plan_rule',
          plan_rule_id: rule.id,
        },
        actor,
      );
    }

    for (const subsection of subsections) {
      const section = sectionMap.get(subsection.planning_section_id);
      if (!section) {
        continue;
      }
      const offer = offerMap.get(section.planning_offer_id);
      if (!offer) {
        continue;
      }
      await this.logChange(
        'planning_subsection',
        subsection.id,
        'DELETE',
        subsection,
        null,
        {
          ...this.buildSubsectionLogContext(subsection, section, offer),
          source: 'delete_plan_rule',
          plan_rule_id: rule.id,
        },
        actor,
      );
    }

    for (const section of sections) {
      const offer = offerMap.get(section.planning_offer_id);
      if (!offer) {
        continue;
      }
      await this.logChange(
        'planning_section',
        section.id,
        'DELETE',
        section,
        null,
        {
          ...this.buildSectionLogContext(section, offer),
          source: 'delete_plan_rule',
          plan_rule_id: rule.id,
        },
        actor,
      );
    }

    for (const offer of offers) {
      await this.logChange(
        'planning_offer',
        offer.id,
        'DELETE',
        offer,
        null,
        {
          ...this.buildOfferLogContext(offer),
          source: 'delete_plan_rule',
          plan_rule_id: rule.id,
        },
        actor,
      );
    }

    return {
      deleted_offer_count: offers.length,
      deleted_section_count: sections.length,
      deleted_subsection_count: subsections.length,
      deleted_schedule_count: schedules.length,
    };
  }

  private newConflict(
    type: PlanningConflictType,
    semesterId: string,
    offerId: string,
    sectionId: string,
    subsectionId: string,
    scheduleAId: string,
    scheduleBId: string,
    overlapMinutes: number,
    detectedAt: Date,
    teacherId?: string,
    classroomId?: string,
    detail?: Record<string, unknown>,
  ) {
    return this.conflictsRepo.create({
      id: newId(),
      semester_id: semesterId,
      conflict_type: type,
      severity: conflictSeverity(overlapMinutes),
      planning_offer_id: offerId,
      planning_section_id: sectionId,
      planning_subsection_id: subsectionId,
      teacher_id: teacherId ?? null,
      classroom_id: classroomId ?? null,
      schedule_a_id: scheduleAId,
      schedule_b_id: scheduleBId,
      overlap_minutes: overlapMinutes,
      detail_json: detail ?? null,
      detected_at: detectedAt,
      created_at: detectedAt,
    });
  }

  private buildPlanRuleLogContext(rule: PlanningCyclePlanRuleEntity) {
    return {
      semester_id: rule.semester_id,
      campus_id: rule.campus_id ?? null,
      faculty_id: rule.faculty_id ?? null,
      academic_program_id: rule.academic_program_id,
      study_plan_id: rule.study_plan_id,
      career_name: rule.career_name ?? null,
      cycle: rule.cycle,
    };
  }

  private buildOfferLogContext(offer: PlanningOfferEntity) {
    return {
      offer_id: offer.id,
      semester_id: offer.semester_id,
      campus_id: offer.campus_id,
      faculty_id: offer.faculty_id ?? null,
      academic_program_id: offer.academic_program_id ?? null,
      study_plan_id: offer.study_plan_id,
      course_code: offer.course_code ?? null,
      course_name: offer.course_name ?? null,
      cycle: offer.cycle,
    };
  }

  private buildSectionLogContext(section: PlanningSectionEntity, offer: PlanningOfferEntity) {
    return {
      ...this.buildOfferLogContext(offer),
      section_id: section.id,
      section_code: section.code,
    };
  }

  private buildSubsectionLogContext(
    subsection: PlanningSubsectionEntity,
    section: PlanningSectionEntity,
    offer: PlanningOfferEntity,
  ) {
    return {
      ...this.buildSectionLogContext(section, offer),
      subsection_id: subsection.id,
      subsection_code: subsection.code,
    };
  }

  private buildScheduleLogContext(
    schedule: PlanningSubsectionScheduleEntity,
    subsection: PlanningSubsectionEntity,
    section: PlanningSectionEntity,
    offer: PlanningOfferEntity,
  ) {
    return {
      ...this.buildSubsectionLogContext(subsection, section, offer),
      schedule_id: schedule.id,
      day_of_week: schedule.day_of_week,
      start_time: schedule.start_time,
      end_time: schedule.end_time,
    };
  }

  private async buildChangeLogReferenceMaps(logs: PlanningChangeLogEntity[]) {
    const offerIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [
          recordString(context, 'offer_id'),
          log.entity_type === 'planning_offer' ? log.entity_id : null,
          recordString(snapshot, 'planning_offer_id'),
        ];
      }),
    );
    const sectionIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [
          recordString(context, 'section_id'),
          log.entity_type === 'planning_section' ? log.entity_id : null,
          recordString(snapshot, 'planning_section_id'),
        ];
      }),
    );
    const subsectionIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [
          recordString(context, 'subsection_id'),
          log.entity_type === 'planning_subsection' ? log.entity_id : null,
          recordString(snapshot, 'planning_subsection_id'),
        ];
      }),
    );
    const semesterIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [recordString(context, 'semester_id'), recordString(snapshot, 'semester_id')];
      }),
    );
    const campusIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [recordString(context, 'campus_id'), recordString(snapshot, 'campus_id')];
      }),
    );
    const facultyIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [recordString(context, 'faculty_id'), recordString(snapshot, 'faculty_id')];
      }),
    );
    const programIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [
          recordString(context, 'academic_program_id'),
          recordString(snapshot, 'academic_program_id'),
        ];
      }),
    );
    const studyPlanIds = uniqueIds(
      logs.flatMap((log) => {
        const context = asRecord(log.context_json);
        const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
        return [recordString(context, 'study_plan_id'), recordString(snapshot, 'study_plan_id')];
      }),
    );

    const [offers, sections, subsections, semesters, campuses, faculties, programs, studyPlans] =
      await Promise.all([
        this.findManyByIds(this.offersRepo, offerIds),
        this.findManyByIds(this.sectionsRepo, sectionIds),
        this.findManyByIds(this.subsectionsRepo, subsectionIds),
        this.findManyByIds(this.semestersRepo, semesterIds),
        this.findManyByIds(this.campusesRepo, campusIds),
        this.findManyByIds(this.facultiesRepo, facultyIds),
        this.findManyByIds(this.programsRepo, programIds),
        this.findManyByIds(this.studyPlansRepo, studyPlanIds),
      ]);

    return {
      offers: mapById(offers),
      sections: mapById(sections),
      subsections: mapById(subsections),
      semesters: mapById(semesters),
      campuses: mapById(campuses),
      faculties: mapById(faculties),
      programs: mapById(programs),
      studyPlans: mapById(studyPlans),
    } as ChangeLogReferenceMaps;
  }

  private buildChangeLogReferenceLabel(
    log: PlanningChangeLogEntity,
    maps: ChangeLogReferenceMaps,
  ) {
    const context = asRecord(log.context_json);
    const snapshot = asRecord(log.after_json) ?? asRecord(log.before_json);
    const offerId =
      recordString(context, 'offer_id') ??
      (log.entity_type === 'planning_offer' ? log.entity_id : null) ??
      recordString(snapshot, 'planning_offer_id');
    const sectionId =
      recordString(context, 'section_id') ??
      (log.entity_type === 'planning_section' ? log.entity_id : null) ??
      recordString(snapshot, 'planning_section_id');
    const subsectionId =
      recordString(context, 'subsection_id') ??
      (log.entity_type === 'planning_subsection' ? log.entity_id : null) ??
      recordString(snapshot, 'planning_subsection_id');
    const offer = offerId ? maps.offers.get(offerId) ?? null : null;
    const section = sectionId ? maps.sections.get(sectionId) ?? null : null;
    const subsection = subsectionId ? maps.subsections.get(subsectionId) ?? null : null;
    const semester =
      maps.semesters.get(
        recordString(context, 'semester_id') ?? recordString(snapshot, 'semester_id') ?? '',
      ) ?? null;
    const campus =
      maps.campuses.get(
        recordString(context, 'campus_id') ?? recordString(snapshot, 'campus_id') ?? '',
      ) ?? null;
    const faculty =
      maps.faculties.get(
        recordString(context, 'faculty_id') ?? recordString(snapshot, 'faculty_id') ?? '',
      ) ?? null;
    const program =
      maps.programs.get(
        recordString(context, 'academic_program_id') ??
          recordString(snapshot, 'academic_program_id') ??
          '',
      ) ?? null;
    const studyPlan =
      maps.studyPlans.get(
        recordString(context, 'study_plan_id') ?? recordString(snapshot, 'study_plan_id') ?? '',
      ) ?? null;

    const offerLabel = buildChangeLogOfferLabel(offer, studyPlan, snapshot, context);
    const sectionCode =
      section?.code ??
      recordString(context, 'section_code') ??
      (log.entity_type === 'planning_section' ? recordString(snapshot, 'code') : null);
    const subsectionCode =
      subsection?.code ??
      recordString(context, 'subsection_code') ??
      (log.entity_type === 'planning_subsection' ? recordString(snapshot, 'code') : null);
    const semesterLabel = coalesceCatalogLabel(semester?.name, recordString(context, 'semester_name'));
    const campusLabel = coalesceCatalogLabel(campus?.name, recordString(context, 'campus_name'));
    const facultyLabel = coalesceCatalogLabel(
      faculty?.name,
      recordString(context, 'faculty_name'),
      recordString(snapshot, 'faculty'),
    );
    const programLabel = coalesceCatalogLabel(
      program?.name,
      recordString(context, 'academic_program_name'),
      recordString(snapshot, 'career_name'),
      recordString(snapshot, 'academic_program'),
    );
    const studyPlanLabel = buildChangeLogStudyPlanLabel(studyPlan, snapshot, context);
    const cycleValue = recordNumber(context, 'cycle') ?? recordNumber(snapshot, 'cycle');

    if (log.entity_type === 'planning_subsection_schedule') {
      const scheduleLabel = buildChangeLogScheduleLabel(snapshot, context);
      return joinReferenceParts([
        offerLabel,
        sectionCode ? `Seccion ${sectionCode}` : null,
        subsectionCode ? `Subseccion ${subsectionCode}` : null,
        scheduleLabel ? `Horario ${scheduleLabel}` : null,
      ]);
    }
    if (log.entity_type === 'planning_subsection') {
      return joinReferenceParts([
        offerLabel,
        sectionCode ? `Seccion ${sectionCode}` : null,
        subsectionCode ? `Subseccion ${subsectionCode}` : null,
      ]);
    }
    if (log.entity_type === 'planning_section') {
      return joinReferenceParts([offerLabel, sectionCode ? `Seccion ${sectionCode}` : null]);
    }
    if (log.entity_type === 'planning_offer') {
      return offerLabel || 'Oferta configurada';
    }
    if (log.entity_type === 'planning_cycle_plan_rule') {
      return (
        joinReferenceParts([
          programLabel,
          studyPlanLabel,
          cycleValue ? `Ciclo ${cycleValue}` : null,
          semesterLabel,
          campusLabel,
          facultyLabel,
        ]) || 'Plan individual'
      );
    }
    return (
      joinReferenceParts([
        offerLabel,
        sectionCode ? `Seccion ${sectionCode}` : null,
        subsectionCode ? `Subseccion ${subsectionCode}` : null,
      ]) || 'Sin contexto adicional'
    );
  }

  private ensureSubsectionVacanciesWithinSection(
    subsectionProjectedVacancies: number | null | undefined,
    sectionProjectedVacancies: number | null | undefined,
  ) {
    if (subsectionProjectedVacancies === null || subsectionProjectedVacancies === undefined) {
      return;
    }
    if (sectionProjectedVacancies === null || sectionProjectedVacancies === undefined) {
      return;
    }
    if (subsectionProjectedVacancies > sectionProjectedVacancies) {
      throw new BadRequestException(
        'La vacante de la subseccion no puede ser mayor a la vacante de la seccion.',
      );
    }
  }

  private async validateSubsectionLocationForOffer(
    offer: PlanningOfferEntity,
    buildingId: string | null | undefined,
    classroomId: string | null | undefined,
  ) {
    const normalizedBuildingId = emptyToNull(buildingId);
    const normalizedClassroomId = emptyToNull(classroomId);
    const [building, classroom] = await Promise.all([
      normalizedBuildingId
        ? this.requireEntity(this.buildingsRepo, normalizedBuildingId, 'building')
        : Promise.resolve(null),
      normalizedClassroomId
        ? this.requireEntity(this.classroomsRepo, normalizedClassroomId, 'classroom')
        : Promise.resolve(null),
    ]);

    if (classroom && normalizedBuildingId && classroom.building_id && classroom.building_id !== normalizedBuildingId) {
      throw new BadRequestException('El aula seleccionada no pertenece al pabellon indicado.');
    }
    if (building && offer.campus_id && building.campus_id !== offer.campus_id) {
      throw new BadRequestException('El pabellon seleccionado no pertenece a la sede del plan.');
    }
    if (classroom && offer.campus_id && classroom.campus_id !== offer.campus_id) {
      throw new BadRequestException('El aula seleccionada no pertenece a la sede del plan.');
    }
  }

  private async logChange(
    entityType: string,
    entityId: string,
    action: 'CREATE' | 'UPDATE' | 'DELETE',
    beforeValue: unknown,
    afterValue: unknown,
    context?: Record<string, unknown>,
    actor?: ChangeLogActor | null,
  ) {
    await this.changeLogsRepo.save(
      this.changeLogsRepo.create({
        id: newId(),
        entity_type: entityType,
        entity_id: entityId,
        action,
        before_json: toLogJson(beforeValue),
        after_json: toLogJson(afterValue),
        changed_by_user_id: actor?.user_id ?? null,
        changed_by: actor?.display_name || actor?.username || 'SYSTEM',
        context_json: context ?? null,
        changed_at: new Date(),
      }),
    );
  }

  private async requireEntity<T extends ObjectLiteral & { id: string }>(
    repo: Repository<T>,
    id: string,
    label: string,
  ) {
    const found = await repo.findOne({ where: { id } as never });
    if (!found) {
      throw new NotFoundException(`${label} ${id} no existe.`);
    }
    return found;
  }

  private async findManyByIds<T extends ObjectLiteral & { id: string }>(
    repo: Repository<T>,
    ids: Array<string | null | undefined>,
  ) {
    const unique = uniqueIds(ids);
    if (unique.length === 0) {
      return [] as T[];
    }
    return repo.find({
      where: {
        id: In(unique),
      } as never,
    });
  }

  private async findManyByField<T extends ObjectLiteral>(
    repo: Repository<T>,
    field: keyof T & string,
    ids: Array<string | null | undefined>,
  ) {
    const unique = uniqueIds(ids);
    if (unique.length === 0) {
      return [] as T[];
    }
    return repo.find({
      where: {
        [field]: In(unique),
      } as never,
    });
  }
}

function numberValue(value: unknown, fallback = 0) {
  const fallbackNumber = Number(fallback);
  if (value === null || value === undefined || value === '') {
    return Number.isFinite(fallbackNumber) ? fallbackNumber : 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.isFinite(fallbackNumber) ? fallbackNumber : 0;
}

function roundToTwo(value: number) {
  return Math.round(value * 100) / 100;
}

function deriveCourseType(theoreticalHours: number, practicalHours: number) {
  if (theoreticalHours > 0 && practicalHours > 0) {
    return 'TEORICO_PRACTICO';
  }
  if (practicalHours > 0) {
    return 'PRACTICO';
  }
  if (theoreticalHours > 0) {
    return 'TEORICO';
  }
  return 'TEORICO_PRACTICO';
}

function chooseDefaultSubsectionKind(
  theoreticalHours: number,
  practicalHours: number,
): PlanningSubsectionKind {
  if (theoreticalHours > 0 && practicalHours > 0) {
    return 'MIXED';
  }
  if (practicalHours > 0) {
    return 'PRACTICE';
  }
  return 'THEORY';
}

function resolveAssignedHours(kind: PlanningSubsectionKind, offer: PlanningOfferEntity) {
  if (kind === 'THEORY') {
    return {
      theoretical_hours: numberValue(offer.theoretical_hours),
      practical_hours: 0,
      virtual_hours: 0,
      seminar_hours: 0,
      total_hours: numberValue(offer.theoretical_hours),
    };
  }
  if (kind === 'PRACTICE') {
    return {
      theoretical_hours: 0,
      practical_hours: numberValue(offer.practical_hours),
      virtual_hours: 0,
      seminar_hours: 0,
      total_hours: numberValue(offer.practical_hours),
    };
  }
  return {
    theoretical_hours: numberValue(offer.theoretical_hours),
    practical_hours: numberValue(offer.practical_hours),
    virtual_hours: 0,
    seminar_hours: 0,
    total_hours: numberValue(offer.total_hours),
  };
}

function resolveSubsectionProjectedVacancies(
  sectionProjectedVacancies: number | null | undefined,
  subsectionCount: number,
  index: number,
) {
  if (sectionProjectedVacancies === null || sectionProjectedVacancies === undefined) {
    return null;
  }
  const total = Math.max(0, Math.trunc(sectionProjectedVacancies));
  const count = Math.max(1, Math.trunc(subsectionCount));
  if (count === 1) {
    return total;
  }
  if (count === 2) {
    return splitIntegerEvenly(total, count, index);
  }
  if (index === 0) {
    return total;
  }
  return splitIntegerEvenly(total, count - 1, index - 1);
}

function splitIntegerEvenly(total: number, parts: number, index: number) {
  const safeParts = Math.max(1, Math.trunc(parts));
  const safeIndex = Math.max(0, Math.trunc(index));
  const base = Math.floor(total / safeParts);
  const remainder = total % safeParts;
  return base + (safeIndex < remainder ? 1 : 0);
}

function buildDenomination(
  courseCode: string | null,
  courseName: string | null,
  sectionCode: string,
  subsectionCode: string,
  campusId: string,
) {
  return [courseCode, courseName, sectionCode, subsectionCode, campusId].filter(Boolean).join(' | ');
}

function emptyToNull(value: string | null | undefined) {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed === '' ? null : trimmed;
}

function normalizeOptionalComment(value: string | null | undefined) {
  return emptyToNull(value);
}

function normalizeRequiredComment(value: string | null | undefined) {
  const normalized = emptyToNull(value);
  if (!normalized) {
    throw new BadRequestException('Debes ingresar un comentario para mandar el plan a correccion.');
  }
  return normalized;
}

function resolveGeneratedSubsectionKind(
  courseType: string | null | undefined,
  subsectionCount: number,
  index: number,
): PlanningSubsectionKind {
  if (courseType === 'TEORICO') {
    return 'THEORY';
  }
  if (courseType === 'PRACTICO') {
    return 'PRACTICE';
  }
  if (subsectionCount <= 1) {
    return 'MIXED';
  }
  return index === 0 ? 'THEORY' : 'PRACTICE';
}

function resolveAllowedSubsectionKinds(
  courseType: string | null | undefined,
  subsectionCount: number,
): PlanningSubsectionKind[] {
  if (courseType === 'TEORICO') {
    return ['THEORY'];
  }
  if (courseType === 'PRACTICO') {
    return ['PRACTICE'];
  }
  if (Math.max(1, subsectionCount) <= 1) {
    return ['MIXED'];
  }
  return ['THEORY', 'PRACTICE'];
}

function validateSubsectionKindDistribution(
  courseType: string | null | undefined,
  kinds: PlanningSubsectionKind[],
) {
  const normalizedKinds = kinds.filter(Boolean);
  if (courseType === 'TEORICO') {
    return normalizedKinds.every((item) => item === 'THEORY');
  }
  if (courseType === 'PRACTICO') {
    return normalizedKinds.every((item) => item === 'PRACTICE');
  }
  if (normalizedKinds.length <= 1) {
    return normalizedKinds[0] === 'MIXED';
  }
  return normalizedKinds.includes('THEORY') && normalizedKinds.includes('PRACTICE');
}

function buildSubsectionKindErrorMessage(
  courseType: string | null | undefined,
  subsectionCount: number,
) {
  if (courseType === 'TEORICO') {
    return 'Este curso solo admite subsecciones teoricas.';
  }
  if (courseType === 'PRACTICO') {
    return 'Este curso solo admite subsecciones practicas.';
  }
  if (Math.max(1, subsectionCount) <= 1) {
    return 'Si solo existe una subseccion en un curso teorico practico, debe ser mixta.';
  }
  return 'En cursos teorico practico solo puedes usar subsecciones teoricas o practicas.';
}

function nextSectionCodeValue(existingCodes: Array<string | null | undefined>) {
  const maxIndex = existingCodes.reduce((currentMax, code) => {
    const index = parseSectionCodeIndex(code);
    return index === null ? currentMax : Math.max(currentMax, index);
  }, -1);
  return sectionCodeFromIndex(maxIndex + 1);
}

function parseSectionCodeIndex(code: string | null | undefined) {
  const normalized = `${code ?? ''}`.trim().toUpperCase();
  if (!/^[A-Z]+$/.test(normalized)) {
    return null;
  }
  let index = 0;
  for (const char of normalized) {
    index = index * 26 + (char.charCodeAt(0) - 64);
  }
  return index - 1;
}

function sectionCodeFromIndex(index: number) {
  let value = Math.max(0, Math.trunc(index));
  let code = '';
  do {
    code = String.fromCharCode(65 + (value % 26)) + code;
    value = Math.floor(value / 26) - 1;
  } while (value >= 0);
  return code;
}

function extractCycleFromLabel(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const match = value.match(/(\d+)/);
  return match ? Number(match[1]) : null;
}

function computeMinutesFromTimes(start: string, end: string) {
  validateAcademicBlockTime(start);
  validateAcademicBlockTime(end);
  const minutes = Math.max(0, toMinutes(end) - toMinutes(start));
  if (minutes <= 0) {
    throw new BadRequestException('La hora fin debe ser mayor que la hora inicio.');
  }
  if (minutes % 50 !== 0) {
    throw new BadRequestException('El horario debe avanzar en bloques de 50 minutos.');
  }
  return minutes;
}

function validateAcademicBlockTime(value: string) {
  const minutes = toMinutes(value);
  const academicGridStart = 7 * 60 + 40;
  const academicGridEnd = 23 * 60 + 30;
  if (
    minutes < academicGridStart ||
    minutes > academicGridEnd ||
    (minutes - academicGridStart) % 50 !== 0
  ) {
    throw new BadRequestException('Las horas deben alinearse a la malla academica de 50 minutos.');
  }
}

function toMinutes(value: string) {
  const [hours = '0', minutes = '0'] = value.split(':');
  return Number(hours) * 60 + Number(minutes);
}

function minutesToTime(totalMinutes: number) {
  const hours = Math.floor(totalMinutes / 60)
    .toString()
    .padStart(2, '0');
  const minutes = Math.floor(totalMinutes % 60)
    .toString()
    .padStart(2, '0');
  return `${hours}:${minutes}:00`;
}

function overlap(startA: string, endA: string, startB: string, endB: string) {
  const start = Math.max(toMinutes(startA), toMinutes(startB));
  const end = Math.min(toMinutes(endA), toMinutes(endB));
  return Math.max(0, end - start);
}

function conflictSeverity(overlapMinutes: number): ConflictSeverity {
  if (overlapMinutes >= 30) {
    return 'CRITICAL';
  }
  if (overlapMinutes >= 10) {
    return 'WARNING';
  }
  return 'INFO';
}

function rangesOverlap(startA: number, endA: number, startB: number, endB: number) {
  return startA <= endB && startB <= endA;
}

function uniqueIds(values: Array<string | null | undefined>) {
  return [...new Set(values.filter((item): item is string => Boolean(item)))];
}

function uniqueNumbers(values: Array<number | null | undefined>) {
  return [...new Set(values.filter((item): item is number => Number.isInteger(item)))];
}

function coalesceCatalogLabel(...values: Array<string | null | undefined>) {
  for (const value of values) {
    const normalized = value?.trim().replace(/\s+/g, ' ') ?? '';
    if (normalized) {
      return normalized;
    }
  }
  return null;
}

function normalizeCatalogName(value: string | null | undefined) {
  const normalized = coalesceCatalogLabel(value);
  if (!normalized) {
    return '';
  }
  return normalized.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
}

function compareCatalogLabels(left: string | null | undefined, right: string | null | undefined) {
  return (left ?? '').localeCompare(right ?? '', 'es', { sensitivity: 'base' });
}

function buildStableCatalogId(namespace: string, ...parts: Array<string | null | undefined>) {
  const seed = [namespace, ...parts.map((item) => normalizeCatalogName(item)).filter(Boolean)].join('|');
  const digest = createHash('sha1').update(seed).digest('hex').slice(0, 32);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-${digest.slice(12, 16)}-${digest.slice(16, 20)}-${digest.slice(20, 32)}`;
}

function buildStudyPlanCycles(
  courses: Pick<StudyPlanCourseEntity, 'id' | 'study_plan_id' | 'year_label'>[],
  details: Pick<StudyPlanCourseDetailEntity, 'study_plan_course_id' | 'academic_year'>[] = [],
) {
  const cycles = new Map<string, { study_plan_id: string; value: number; label: string }>();
  const detailByCourseId = new Map(
    details.map((item) => [item.study_plan_course_id, item.academic_year] as const),
  );
  for (const course of courses) {
    const detailCycle = detailByCourseId.get(course.id) ?? null;
    const fallbackLabel = coalesceCatalogLabel(course.year_label);
    const value = detailCycle ?? extractCycleFromLabel(fallbackLabel) ?? null;
    if (!course.study_plan_id || !value) {
      continue;
    }
    const label = fallbackLabel ?? `Ciclo ${value}`;
    const key = `${course.study_plan_id}|${value}`;
    if (cycles.has(key)) {
      continue;
    }
    cycles.set(key, {
      study_plan_id: course.study_plan_id,
      value,
      label,
    });
  }
  return [...cycles.values()].sort((a, b) => {
    if (a.value !== b.value) {
      return a.value - b.value;
    }
    return a.study_plan_id.localeCompare(b.study_plan_id);
  });
}

function configuredCycleKey(input: {
  semester_id: string;
  campus_id: string;
  academic_program_id: string;
  study_plan_id: string;
  cycle: number;
}) {
  return [
    input.semester_id,
    input.campus_id,
    input.academic_program_id,
    input.study_plan_id,
    String(input.cycle),
  ].join('|');
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

function mapById<T extends { id: string }>(items: T[]) {
  return new Map(items.map((item) => [item.id, item]));
}

function formatPlanningTeacherDisplay(teacher: TeacherEntity | null) {
  if (!teacher) {
    return null;
  }
  const name = teacher.full_name || teacher.name || null;
  if (!name) {
    return teacher.dni || null;
  }
  return teacher.dni ? `${teacher.dni} - ${name}` : name;
}

function buildPlanningConflictAffectedLabel(
  conflict: PlanningScheduleConflictV2Entity,
  offer: PlanningOfferEntity | null,
  section: PlanningSectionEntity | null,
  subsection: PlanningSubsectionEntity | null,
  teacher: TeacherEntity | null,
) {
  if (conflict.conflict_type === 'TEACHER_OVERLAP') {
    return formatPlanningTeacherDisplay(teacher) ?? 'Docente sin asignar';
  }
  if (conflict.conflict_type === 'SECTION_OVERLAP') {
    return [
      offer?.course_code ? `${offer.course_code} - ${offer.course_name}` : offer?.course_name ?? null,
      section?.code ? `Seccion ${section.code}` : null,
      subsection?.code ?? null,
    ]
      .filter(Boolean)
      .join(' · ');
  }
  return subsection?.code ?? section?.code ?? offer?.course_name ?? 'Cruce detectado';
}

function buildPlanningConflictScheduleCard(
  schedule: PlanningSubsectionScheduleEntity | null,
  subsectionMap: Map<string, PlanningSubsectionEntity>,
  sectionMap: Map<string, PlanningSectionEntity>,
  offerMap: Map<string, PlanningOfferEntity>,
  classroomMap: Map<string, ClassroomEntity>,
) {
  if (!schedule) {
    return null;
  }
  const subsection = subsectionMap.get(schedule.planning_subsection_id) ?? null;
  const section = subsection ? sectionMap.get(subsection.planning_section_id) ?? null : null;
  const offer = section ? offerMap.get(section.planning_offer_id) ?? null : null;
  const classroom = classroomMap.get(subsection?.classroom_id ?? '') ?? null;
  const courseLabel = offer?.course_code
    ? `${offer.course_code} - ${offer.course_name}`
    : offer?.course_name ?? 'Sin curso';

  return {
    id: schedule.id,
    offering_id: offer?.id ?? null,
    course_label: courseLabel,
    section_name: section?.code ? `Seccion ${section.code}` : null,
    group_code: subsection?.code ?? null,
    day_of_week: schedule.day_of_week,
    start_time: schedule.start_time,
    end_time: schedule.end_time,
    classroom_name: classroom?.name ?? null,
    summary: [
      courseLabel,
      section?.code ? `Seccion ${section.code}` : null,
      subsection?.code ?? null,
    ]
      .filter(Boolean)
      .join(' · '),
  };
}

function dedupePlanningConflicts(conflicts: PlanningScheduleConflictV2Entity[]) {
  const map = new Map<string, PlanningScheduleConflictV2Entity>();
  for (const conflict of conflicts) {
    const pair = [conflict.schedule_a_id, conflict.schedule_b_id].sort().join(':');
    const key = [
      conflict.conflict_type,
      conflict.planning_offer_id,
      conflict.planning_section_id,
      conflict.planning_subsection_id,
      conflict.teacher_id,
      conflict.classroom_id,
      pair,
    ].join('|');
    if (!map.has(key)) {
      map.set(key, conflict);
    }
  }
  return [...map.values()];
}

function toLogJson(value: unknown): Record<string, unknown> | null {
  if (value === null || value === undefined) {
    return null;
  }
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

function asRecord(value: unknown) {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function buildChangeRows(
  beforeValue: Record<string, unknown> | null,
  afterValue: Record<string, unknown> | null,
  action: 'CREATE' | 'UPDATE' | 'DELETE',
) {
  const before = asRecord(beforeValue) ?? {};
  const after = asRecord(afterValue) ?? {};
  const ignoredFields = new Set(['created_at', 'updated_at']);
  const keys =
    action === 'CREATE'
      ? Object.keys(after)
      : action === 'DELETE'
        ? Object.keys(before)
        : [...new Set([...Object.keys(before), ...Object.keys(after)])];

  return keys
    .filter((field) => !ignoredFields.has(field))
    .filter((field) => action !== 'UPDATE' || !logValuesEqual(before[field], after[field]))
    .sort((left, right) => left.localeCompare(right))
    .map((field) => ({
      field,
      before: field in before ? before[field] : null,
      after: field in after ? after[field] : null,
    }));
}

function logValuesEqual(left: unknown, right: unknown) {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}

function normalizeLogLimit(value: number | undefined) {
  const parsed = Math.trunc(Number(value ?? 100));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 100;
  }
  return Math.min(parsed, 200);
}

function parseLogDate(value: string | undefined, isEnd: boolean) {
  if (!value) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return new Date(`${value}T${isEnd ? '23:59:59.999' : '00:00:00.000'}Z`);
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalizeSearchValue(value: unknown) {
  return `${value ?? ''}`
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

function recordString(record: Record<string, unknown> | null, key: string) {
  const value = record?.[key];
  if (typeof value !== 'string') {
    return null;
  }
  const normalized = value.trim();
  return normalized || null;
}

function recordNumber(record: Record<string, unknown> | null, key: string) {
  const value = record?.[key];
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function joinReferenceParts(parts: Array<string | null | undefined>) {
  return parts.map((item) => item?.trim()).filter((item): item is string => Boolean(item)).join(' · ');
}

function buildChangeLogOfferLabel(
  offer: PlanningOfferEntity | null,
  studyPlan: StudyPlanEntity | null,
  snapshot: Record<string, unknown> | null,
  context: Record<string, unknown> | null,
) {
  const courseCode = coalesceCatalogLabel(offer?.course_code, recordString(snapshot, 'course_code'));
  const courseName = coalesceCatalogLabel(offer?.course_name, recordString(snapshot, 'course_name'));
  const studyPlanLabel = buildChangeLogStudyPlanLabel(studyPlan, snapshot, context);
  const cycle = recordNumber(context, 'cycle') ?? recordNumber(snapshot, 'cycle');
  const courseLabel = [courseCode, courseName].filter(Boolean).join(' - ');
  return (
    joinReferenceParts([
      courseLabel || null,
      studyPlanLabel,
      cycle ? `Ciclo ${cycle}` : null,
    ]) || null
  );
}

function buildChangeLogStudyPlanLabel(
  studyPlan: StudyPlanEntity | null,
  snapshot: Record<string, unknown> | null,
  context: Record<string, unknown> | null,
) {
  const career = coalesceCatalogLabel(
    studyPlan?.career,
    studyPlan?.academic_program,
    recordString(snapshot, 'career_name'),
    recordString(context, 'career_name'),
    recordString(snapshot, 'academic_program'),
  );
  const year = coalesceCatalogLabel(studyPlan?.year, recordString(snapshot, 'study_plan_year'));
  const planName = coalesceCatalogLabel(studyPlan?.name, recordString(snapshot, 'study_plan_name'));
  return coalesceCatalogLabel(
    [career, year].filter(Boolean).join(' - ') || null,
    planName,
  );
}

function buildChangeLogScheduleLabel(
  snapshot: Record<string, unknown> | null,
  context: Record<string, unknown> | null,
) {
  const day = formatScheduleDayLabel(
    recordString(snapshot, 'day_of_week') ?? recordString(context, 'day_of_week'),
  );
  const start = normalizeScheduleTimeValue(
    recordString(snapshot, 'start_time') ?? recordString(context, 'start_time'),
  );
  const end = normalizeScheduleTimeValue(
    recordString(snapshot, 'end_time') ?? recordString(context, 'end_time'),
  );
  const range = start && end ? `${start}-${end}` : null;
  return joinReferenceParts([day, range]);
}

function formatScheduleDayLabel(value: string | null | undefined) {
  switch ((value ?? '').trim().toUpperCase()) {
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
      return coalesceCatalogLabel(value);
  }
}

function normalizeScheduleTimeValue(value: string | null | undefined) {
  const normalized = value?.trim() ?? '';
  if (!normalized) {
    return null;
  }
  return /^\d{2}:\d{2}:\d{2}$/.test(normalized) ? normalized.slice(0, 5) : normalized;
}
