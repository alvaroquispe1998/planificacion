import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { createHash } from 'crypto';
import { InjectRepository } from '@nestjs/typeorm';
import { ILike, In, ObjectLiteral, Repository } from 'typeorm';
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
  PlanningCampusVcLocationMappingEntity,
  PlanningCyclePlanRuleEntity,
  PlanningOfferEntity,
  PlanningOfferStatusValues,
  PlanningSessionTypeValues,
  PlanningScheduleConflictV2Entity,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionKindValues,
  PlanningSubsectionScheduleEntity,
  PlanningVcLocationCodeValues,
  PlanningV2ConflictTypeValues,
  StudyTypeEntity,
} from '../entities/planning.entities';
import {
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcFacultyEntity,
  VcPeriodEntity,
  VcSectionEntity,
} from '../videoconference/videoconference.entity';
import {
  CreatePlanningCyclePlanRuleDto,
  CreatePlanningOfferDto,
  RecalculatePlanningVcMatchesDto,
  CreatePlanningSectionDto,
  CreatePlanningSubsectionDto,
  CreatePlanningSubsectionScheduleDto,
  UpdatePlanningCyclePlanRuleDto,
  UpdatePlanningCampusVcLocationMappingDto,
  UpdatePlanningOfferDto,
  UpdatePlanningSectionDto,
  UpdatePlanningSubsectionDto,
  UpdatePlanningSubsectionVcMatchDto,
  UpdatePlanningSubsectionScheduleDto,
} from './dto/planning.dto';

type PlanningOfferStatus = (typeof PlanningOfferStatusValues)[number];
type PlanningSessionType = (typeof PlanningSessionTypeValues)[number];
type PlanningSubsectionKind = (typeof PlanningSubsectionKindValues)[number];
type PlanningConflictType = (typeof PlanningV2ConflictTypeValues)[number];
type ConflictSeverity = (typeof ConflictSeverityValues)[number];
type DayOfWeek = (typeof DayOfWeekValues)[number];
type PlanningVcLocationCode = (typeof PlanningVcLocationCodeValues)[number];

const PLANNING_SHIFT_OPTIONS = ['DIURNO', 'MANANA', 'TARDE', 'NOCHE', 'NOCTURNO', 'TARDE/NOCHE'] as const;

type CourseCandidateFilters = {
  semester_id?: string;
  vc_period_id?: string;
  campus_id?: string;
  faculty_id?: string;
  academic_program_id?: string;
  cycle?: number;
  study_plan_id?: string;
};

type ConfiguredCycleFilters = {
  semester_id?: string;
  vc_period_id?: string;
  campus_id?: string;
  faculty_id?: string;
  academic_program_id?: string;
};

type PlanningVcMatchFilters = {
  semester_id?: string;
  campus_id?: string;
  faculty_id?: string;
  academic_program_id?: string;
  cycle?: number;
  study_plan_id?: string;
  query?: string;
};

type ChangeLogActor = {
  user_id?: string | null;
  username?: string | null;
  display_name?: string | null;
  ip_address?: string | null;
};

type ChangeLogFilters = {
  entity_type?: string;
  entity_id?: string;
  action?: string;
  query?: string;
  changed_by?: string;
  from?: string;
  to?: string;
  limit?: number;
  offer_id?: string;
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
  modalities: CourseModalityEntity[];
  buildings: BuildingEntity[];
  classrooms: ClassroomEntity[];
  conflicts: PlanningScheduleConflictV2Entity[];
  changeLogs: PlanningChangeLogEntity[];
  vcPeriods: VcPeriodEntity[];
  vcFaculties: VcFacultyEntity[];
  vcAcademicPrograms: VcAcademicProgramEntity[];
  vcCourses: VcCourseEntity[];
  vcSections: VcSectionEntity[];
  campusVcLocations: PlanningCampusVcLocationMappingEntity[];
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
    @InjectRepository(VcPeriodEntity)
    private readonly vcPeriodsRepo: Repository<VcPeriodEntity>,
    @InjectRepository(VcFacultyEntity)
    private readonly vcFacultiesRepo: Repository<VcFacultyEntity>,
    @InjectRepository(VcAcademicProgramEntity)
    private readonly vcAcademicProgramsRepo: Repository<VcAcademicProgramEntity>,
    @InjectRepository(VcCourseEntity)
    private readonly vcCoursesRepo: Repository<VcCourseEntity>,
    @InjectRepository(VcSectionEntity)
    private readonly vcSectionsRepo: Repository<VcSectionEntity>,
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
    @InjectRepository(PlanningCampusVcLocationMappingEntity)
    private readonly campusVcLocationMappingsRepo: Repository<PlanningCampusVcLocationMappingEntity>,
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
      vcPeriods,
      vcFaculties,
      vcAcademicPrograms,
      campusVcLocations,
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
        this.vcPeriodsRepo.find({ where: { is_active: true }, order: { text: 'DESC' } }),
        this.vcFacultiesRepo.find({ order: { name: 'ASC' } }),
        this.vcAcademicProgramsRepo.find({ order: { name: 'ASC' } }),
        this.campusVcLocationMappingsRepo.find({ order: { campus_id: 'ASC' } }),
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
      shift_options: PLANNING_SHIFT_OPTIONS.map((value) => ({ id: value, label: value })),
      vc_periods: vcPeriods,
      vc_faculties: vcFaculties,
      vc_academic_programs: vcAcademicPrograms,
      campus_vc_locations: campusVcLocations,
    };
  }

  async listConfiguredCycles(filters: ConfiguredCycleFilters) {
    await this.ensureDefaultCatalogs();
    const rules = await this.planRulesRepo.find({
      where: {
        is_active: true,
        ...(filters.semester_id ? { semester_id: filters.semester_id } : {}),
        ...(filters.vc_period_id ? { vc_period_id: filters.vc_period_id } : {}),
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
        ...(filters.vc_period_id ? { vc_period_id: filters.vc_period_id } : {}),
        ...(filters.campus_id ? { campus_id: filters.campus_id } : {}),
        ...(filters.faculty_id ? { faculty_id: filters.faculty_id } : {}),
        ...(filters.academic_program_id
          ? { academic_program_id: filters.academic_program_id }
          : {}),
      },
      order: { updated_at: 'DESC' },
    });
    const semesterIds = uniqueIds(rules.map((item) => item.semester_id));
    const vcPeriodIds = uniqueIds(rules.map((item) => item.vc_period_id));
    const programIds = uniqueIds(rules.map((item) => item.academic_program_id));
    const facultyIds = uniqueIds(rules.map((item) => item.faculty_id));
    const studyPlanIds = uniqueIds(rules.map((item) => item.study_plan_id));
    const campusIds = uniqueIds([
      ...offers.map((item) => item.campus_id),
      ...rules.map((item) => item.campus_id),
    ]);
    const [semesters, vcPeriods, programs, faculties, studyPlans, campuses] = await Promise.all([
      this.findManyByIds(this.semestersRepo, semesterIds),
      this.findManyByIds(this.vcPeriodsRepo, vcPeriodIds),
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
        vc_period_id: '',
        campus_id: '',
        academic_program_id: '',
        study_plan_id: course.study_plan_id,
        cycle: resolvedCycle,
      });
      expectedCourseCountByRuleKey.set(key, (expectedCourseCountByRuleKey.get(key) ?? 0) + 1);
    }

    const semesterMap = mapById(semesters);
    const vcPeriodMap = mapById(vcPeriods);
    const programMap = mapById(programs);
    const facultyMap = mapById(faculties);
    const planMap = mapById(studyPlans);
    const campusMap = mapById(campuses);
    const offersByRuleKey = new Map<string, PlanningOfferEntity[]>();
    for (const offer of offers) {
      const key = configuredCycleKey({
        semester_id: offer.semester_id,
        vc_period_id: offer.vc_period_id ?? '',
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
                vc_period_id: rule.vc_period_id ?? '',
                campus_id: rule.campus_id,
                academic_program_id: rule.academic_program_id,
                study_plan_id: rule.study_plan_id,
                cycle,
              }),
            ) ?? []
          : offers.filter(
              (offer) =>
                offer.semester_id === rule.semester_id &&
                (rule.vc_period_id ? offer.vc_period_id === rule.vc_period_id : true) &&
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
              vc_period_id: '',
              campus_id: '',
              academic_program_id: '',
              study_plan_id: rule.study_plan_id,
              cycle,
            }),
          ) ?? 0;
        return {
          id: rule.id,
          semester_id: rule.semester_id,
          vc_period_id: rule.vc_period_id ?? null,
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
          vc_period: vcPeriodMap.get(rule.vc_period_id ?? '') ?? null,
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
        const vcPeriodDelta = compareCatalogLabels(
          left.vc_period?.text ?? left.semester?.name,
          right.vc_period?.text ?? right.semester?.name,
        );
        if (vcPeriodDelta !== 0) {
          return vcPeriodDelta;
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

  async listPlanRules(
    semesterId?: string,
    campusId?: string,
    academicProgramId?: string,
    vcPeriodId?: string,
  ) {
    await this.ensureDefaultCatalogs();
    const rules = await this.planRulesRepo.find({
      where: {
        ...(semesterId ? { semester_id: semesterId } : {}),
        ...(vcPeriodId ? { vc_period_id: vcPeriodId } : {}),
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
      vc_period_id: emptyToNull(dto.vc_period_id),
      vc_faculty_id: emptyToNull(dto.vc_faculty_id),
      vc_academic_program_id: emptyToNull(dto.vc_academic_program_id),
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
    this.ensurePlanRuleEditable(current, { allowApproved: true });
    const next = {
      ...current,
      ...dto,
      vc_period_id:
        dto.vc_period_id !== undefined ? emptyToNull(dto.vc_period_id) : current.vc_period_id,
      vc_faculty_id:
        dto.vc_faculty_id !== undefined ? emptyToNull(dto.vc_faculty_id) : current.vc_faculty_id,
      vc_academic_program_id:
        dto.vc_academic_program_id !== undefined
          ? emptyToNull(dto.vc_academic_program_id)
          : current.vc_academic_program_id,
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
    await this.syncVcReferencesForRule(saved, actor);
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
    if (!['IN_REVIEW', 'APPROVED'].includes(current.workflow_status)) {
      throw new BadRequestException('Solo puedes mandar a correccion planes en revision o aprobados.');
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
      filters.vc_period_id ?? null,
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
            ...(planRule.vc_period_id ? { vc_period_id: planRule.vc_period_id } : {}),
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
      vc_period_id: dto.vc_period_id ?? null,
      campus_id: dto.campus_id,
      academic_program_id: dto.academic_program_id ?? null,
      study_plan_id: dto.study_plan_id,
      cycle: dto.cycle,
    });
    this.ensurePlanRuleEditable(planRule, { allowApproved: true });
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
    const vcPeriodId = planRule?.vc_period_id ?? null;
    const vcFacultyId = planRule?.vc_faculty_id ?? null;
    const vcAcademicProgramId = planRule?.vc_academic_program_id ?? null;
    const vcCourseId = await this.resolveVcCourseId({
      course_name: blueprint.course_name,
      course_code: blueprint.course_code,
      study_plan_id: dto.study_plan_id,
      vc_academic_program_id: vcAcademicProgramId,
    });
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
      vc_period_id: vcPeriodId,
      vc_faculty_id: vcFacultyId,
      vc_academic_program_id: vcAcademicProgramId,
      vc_course_id: vcCourseId,
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
    vcPeriodId?: string,
    campusId?: string,
    facultyId?: string,
    academicProgramId?: string,
    cycle?: number,
    studyPlanId?: string,
  ) {
    const offers = await this.offersRepo.find({
      where: {
        ...(semesterId ? { semester_id: semesterId } : {}),
        ...(vcPeriodId ? { vc_period_id: vcPeriodId } : {}),
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

  async listExpandedOffers(
    semesterId?: string,
    vcPeriodId?: string,
    campusId?: string,
    facultyId?: string,
    academicProgramId?: string,
    cycle?: number,
    studyPlanId?: string,
    search?: string,
  ) {
    const requestedSearch = normalizeSearchValue(search);
    const offers = await this.offersRepo.find({
      where: {
        ...(semesterId ? { semester_id: semesterId } : {}),
        ...(vcPeriodId ? { vc_period_id: vcPeriodId } : {}),
        ...(campusId ? { campus_id: campusId } : {}),
        ...(facultyId ? { faculty_id: facultyId } : {}),
        ...(academicProgramId ? { academic_program_id: academicProgramId } : {}),
        ...(cycle ? { cycle } : {}),
        ...(studyPlanId ? { study_plan_id: studyPlanId } : {}),
      },
      order: { updated_at: 'DESC', course_name: 'ASC' },
    });
    if (offers.length === 0) {
      return [];
    }

    const candidateOffers =
      requestedSearch && looksLikeWorkspaceCodeSearch(requestedSearch)
        ? offers.filter((offer) =>
            [offer.course_code, offer.course_name]
              .map((value) => normalizeSearchValue(value))
              .some((value) => value.includes(requestedSearch)),
          )
        : offers;
    const offersToExpand = candidateOffers.length > 0 ? candidateOffers : offers;

    const context = await this.buildContext(offersToExpand.map((item) => item.id), {
      skipChangeLogs: true,
      skipConflicts: true,
      preloadedOffers: offersToExpand,
    });
    const [semesters, campuses, faculties, programs, studyPlans] = await Promise.all([
      this.findManyByIds(this.semestersRepo, uniqueIds(offersToExpand.map((item) => item.semester_id))),
      this.findManyByIds(this.campusesRepo, uniqueIds(offersToExpand.map((item) => item.campus_id))),
      this.findManyByIds(this.facultiesRepo, uniqueIds(offersToExpand.map((item) => item.faculty_id))),
      this.findManyByIds(this.programsRepo, uniqueIds(offersToExpand.map((item) => item.academic_program_id))),
      this.findManyByIds(this.studyPlansRepo, uniqueIds(offersToExpand.map((item) => item.study_plan_id))),
    ]);
    const semesterMap = mapById(semesters);
    const campusMap = mapById(campuses);
    const facultyMap = mapById(faculties);
    const programMap = mapById(programs);
    const studyPlanMap = mapById(studyPlans);

    const expanded = offersToExpand.map((offer) => ({
      ...this.buildOfferDetail(offer, context),
      semester: semesterMap.get(offer.semester_id) ?? null,
      campus: campusMap.get(offer.campus_id) ?? null,
      faculty: facultyMap.get(offer.faculty_id ?? '') ?? null,
      academic_program: programMap.get(offer.academic_program_id ?? '') ?? null,
      study_plan: studyPlanMap.get(offer.study_plan_id) ?? null,
      change_log: [],
    }));

    if (!requestedSearch) {
      return expanded;
    }

    return expanded.filter((offer) => this.matchesExpandedOfferSearch(offer, requestedSearch));
  }

  async getOffer(id: string) {
    const offer = await this.requireEntity(this.offersRepo, id, 'planning_offer');
    return this.buildEditorOfferDetail(offer);
  }

  private async buildEditorOfferDetail(offer: PlanningOfferEntity) {
    const [sections, conflicts, planRule] = await Promise.all([
      this.sectionsRepo.find({
        where: { planning_offer_id: offer.id },
        order: { code: 'ASC' },
      }),
      this.conflictsRepo.find({
        where: { planning_offer_id: offer.id },
        order: { detected_at: 'DESC' },
      }),
      this.findPlanRuleForOfferContext({
        semester_id: offer.semester_id,
        vc_period_id: offer.vc_period_id ?? null,
        campus_id: offer.campus_id,
        academic_program_id: offer.academic_program_id ?? null,
        study_plan_id: offer.study_plan_id,
        cycle: offer.cycle,
      }),
    ]);

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

    const [teachers, modalities, buildings, classrooms] = await Promise.all([
      this.findManyByIds(
        this.teachersRepo,
        uniqueIds([
          ...sections.map((item) => item.teacher_id),
          ...subsections.map((item) => item.responsible_teacher_id),
          ...schedules.map((item) => item.teacher_id),
        ]),
      ),
      this.findManyByIds(
        this.courseModalitiesRepo,
        uniqueIds(subsections.map((item) => item.course_modality_id)),
      ),
      this.findManyByIds(
        this.buildingsRepo,
        uniqueIds([
          ...subsections.map((item) => item.building_id),
          ...schedules.map((item) => item.building_id),
        ]),
      ),
      this.findManyByIds(
        this.classroomsRepo,
        uniqueIds([
          ...subsections.map((item) => item.classroom_id),
          ...schedules.map((item) => item.classroom_id),
        ]),
      ),
    ]);

    const teacherMap = mapById(teachers);
    const subsectionMap = groupBy(subsections, (item) => item.planning_section_id);
    const scheduleMap = groupBy(schedules, (item) => item.planning_subsection_id);
    const sectionConflicts = groupBy(conflicts, (item) => item.planning_section_id ?? '__null__');
    const subsectionConflicts = groupBy(
      conflicts,
      (item) => item.planning_subsection_id ?? '__null__',
    );

    return {
      ...offer,
      section_count: sections.length,
      subsection_count: subsections.length,
      schedule_count: schedules.length,
      conflict_count: conflicts.length,
      sections: sections.map((section) => ({
        ...section,
        teacher: teacherMap.get(section.teacher_id ?? '') ?? null,
        subsections: (subsectionMap.get(section.id) ?? []).map((subsection) =>
          this.buildSubsectionDetail(subsection, {
            schedules: scheduleMap.get(subsection.id) ?? [],
            conflicts: subsectionConflicts.get(subsection.id) ?? [],
            teachers,
            modalities,
            buildings,
            classrooms,
          }),
        ),
        conflicts: sectionConflicts.get(section.id) ?? [],
      })),
      conflicts,
      plan_rule: planRule ?? null,
    };
  }

  async updateOffer(actor: ChangeLogActor | null | undefined, id: string, dto: UpdatePlanningOfferDto) {
    const current = await this.requireEntity(this.offersRepo, id, 'planning_offer');
    await this.assertOfferPlanEditable(current);
    const nextSourcePayloadJson =
      dto.vc_course_id !== undefined
        ? upsertOfferVcContextMetadata(current.source_payload_json, {
            vc_source: emptyToNull(dto.vc_course_id) ? 'manual_override' : 'sync_source',
            manual_vc_course_id: emptyToNull(dto.vc_course_id),
            vc_context_message: emptyToNull(dto.vc_course_id)
              ? 'Curso VC ajustado manualmente.'
              : 'Curso VC restablecido al contexto sincronizado.',
          })
        : current.source_payload_json;
    const next = this.offersRepo.create({
      ...current,
      ...dto,
      vc_course_id:
        dto.vc_course_id !== undefined ? emptyToNull(dto.vc_course_id) : current.vc_course_id,
      source_payload_json: nextSourcePayloadJson,
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
    await this.syncOfferVcReferences(saved.id, actor, { preserveManualCourse: true });
    return this.getOffer(id);
  }

  async createSection(actor: ChangeLogActor | null | undefined, offerId: string, dto: CreatePlanningSectionDto) {
    await this.ensureDefaultCatalogs();
    const offer = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    await this.assertOfferPlanEditable(offer);
    const now = new Date();
    const subsectionCount = Math.max(1, Math.trunc(dto.subsection_count));
    const modalityId = dto.course_modality_id ?? (await this.defaultCourseModalityId());
    const [sectionModality, campusVcLocation] = await Promise.all([
      modalityId ? this.courseModalitiesRepo.findOne({ where: { id: modalityId } }) : Promise.resolve(null),
      this.campusVcLocationMappingsRepo.findOne({ where: { campus_id: offer.campus_id } }),
    ]);
    const teacherId = emptyToNull(dto.teacher_id);
    const projectedVacancies =
      dto.projected_vacancies !== undefined ? Math.max(0, Math.trunc(dto.projected_vacancies)) : null;
    const requestedSectionCode =
      dto.code !== undefined ? normalizePlanningSectionCode(dto.code) : null;
    if (dto.code !== undefined && !requestedSectionCode) {
      throw new BadRequestException('Debes indicar un codigo valido para la seccion.');
    }

    const saved = await this.sectionsRepo.manager.transaction(async (manager) => {
      const existingSections = await manager.find(PlanningSectionEntity, {
        where: { planning_offer_id: offerId },
        order: { code: 'ASC' },
      });
      const suggestedSectionCode = nextSectionCodeValue(existingSections.map((item) => item.code));
      const sectionCode = requestedSectionCode ?? suggestedSectionCode;
      const duplicateSection = existingSections.find(
        (item) => normalizePlanningSectionCode(item.code) === sectionCode,
      );
      if (duplicateSection) {
        throw new BadRequestException(`La seccion ${sectionCode} ya existe para esta oferta.`);
      }
      const generatedExternalCode = buildPlanningSectionExternalCode(
        sectionCode,
        sectionModality?.code ?? null,
        campusVcLocation?.vc_location_code ?? null,
      );
      const section = manager.create(PlanningSectionEntity, {
        id: dto.id ?? newId(),
        planning_offer_id: offerId,
        code: sectionCode,
        external_code: emptyToNull(dto.external_code) ?? generatedExternalCode ?? null,
        source_section_id: null,
        source_payload_json: null,
        teacher_id: teacherId,
        course_modality_id: modalityId,
        projected_vacancies: projectedVacancies,
        is_cepea: Boolean(dto.is_cepea),
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

      const subsectionKinds = Array.from({ length: subsectionCount }, (_, index) =>
        resolveGeneratedSubsectionKind(offer.course_type, subsectionCount, index),
      );
      const subsections = Array.from({ length: subsectionCount }, (_, index) => {
        const subsectionCode = index === 0 ? sectionCode : `${sectionCode}${index}`;
        const kind = subsectionKinds[index];
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
          projected_vacancies: resolveSubsectionProjectedVacancies(
            projectedVacancies,
            subsectionKinds,
            index,
          ),
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
    await this.syncSectionVcMatches(saved.id, actor);
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
    const [teachers, modalities, buildings, classrooms, vcSections, campusVcLocations] = await Promise.all([
      this.findManyByIds(
        this.teachersRepo,
        uniqueIds([
          section.teacher_id,
          ...subsections.map((item) => item.responsible_teacher_id),
          ...schedules.map((item) => item.teacher_id),
        ]),
      ),
      this.findManyByIds(
        this.courseModalitiesRepo,
        uniqueIds([section.course_modality_id, ...subsections.map((item) => item.course_modality_id)]),
      ),
      this.findManyByIds(
        this.buildingsRepo,
        uniqueIds([
          ...subsections.map((item) => item.building_id),
          ...schedules.map((item) => item.building_id),
        ]),
      ),
      this.findManyByIds(
        this.classroomsRepo,
        uniqueIds([
          ...subsections.map((item) => item.classroom_id),
          ...schedules.map((item) => item.classroom_id),
        ]),
      ),
      this.findManyByIds(this.vcSectionsRepo, uniqueIds(subsections.map((item) => item.vc_section_id))),
      this.findManyByField(this.campusVcLocationMappingsRepo, 'campus_id', [offer.campus_id]),
    ]);
    const vcSectionMap = mapById(vcSections);
    const contextForExpectedName = {
      modalities,
      campusVcLocations,
    };

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
          vcSection: vcSectionMap.get(subsection.vc_section_id ?? '') ?? null,
          expectedVcSectionName: this.buildExpectedVcSectionNameForSubsection(
            subsection,
            section,
            offer,
            contextForExpectedName,
          ),
        }),
      ),
    };
  }

  async updateSection(actor: ChangeLogActor | null | undefined, id: string, dto: UpdatePlanningSectionDto) {
    const current = await this.requireEntity(this.sectionsRepo, id, 'planning_section');
    const currentOffer = await this.requireEntity(this.offersRepo, current.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(currentOffer);
    const normalizedCode =
      dto.code !== undefined ? normalizePlanningSectionCode(dto.code) : current.code;
    if (dto.code !== undefined && !normalizedCode) {
      throw new BadRequestException('Debes indicar un codigo valido para la seccion.');
    }
    const nextCode = normalizedCode ?? current.code;
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
      external_code:
        dto.external_code !== undefined ? emptyToNull(dto.external_code) : current.external_code,
      teacher_id:
        dto.teacher_id !== undefined ? emptyToNull(dto.teacher_id) : current.teacher_id,
      projected_vacancies:
        dto.projected_vacancies !== undefined
          ? Math.max(0, Math.trunc(dto.projected_vacancies))
          : current.projected_vacancies,
      is_cepea:
        dto.is_cepea !== undefined ? Boolean(dto.is_cepea) : current.is_cepea,
      updated_at: new Date(),
    });
    await this.sectionsRepo.save(next);
    if (dto.projected_vacancies !== undefined) {
      const sectionSubsections = await this.subsectionsRepo.find({
        where: { planning_section_id: id },
        order: { code: 'ASC' },
      });
      const subsectionBeforeMap = new Map(sectionSubsections.map((subsection) => [subsection.id, { ...subsection }]));
      const updatedSubsectionKinds = sectionSubsections.map((subsection) => subsection.kind);
      const updatedSubsections = sectionSubsections.map((subsection, index) =>
        this.subsectionsRepo.create({
          ...subsection,
          projected_vacancies: resolveSubsectionProjectedVacancies(
            next.projected_vacancies,
            updatedSubsectionKinds,
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
    await this.syncSectionVcMatches(id, actor);
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
        throw new BadRequestException(`El grupo ${code} ya existe.`);
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
    await this.syncSubsectionVcMatch(saved.id, actor);
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
    const [schedules, conflicts] = await Promise.all([
      this.schedulesRepo.find({
        where: { planning_subsection_id: id },
        order: { day_of_week: 'ASC', start_time: 'ASC' },
      }),
      this.conflictsRepo.find({
        where: { planning_subsection_id: id },
        order: { detected_at: 'DESC' },
      }),
    ]);
    const [teachers, modalities, buildings, classrooms, vcSections, campusVcLocations] =
      await Promise.all([
      this.findManyByIds(this.teachersRepo, uniqueIds([
        subsection.responsible_teacher_id,
        section.teacher_id,
        ...schedules.map((item) => item.teacher_id),
      ])),
      this.findManyByIds(this.courseModalitiesRepo, uniqueIds([subsection.course_modality_id])),
      this.findManyByIds(this.buildingsRepo, uniqueIds([subsection.building_id, ...schedules.map((item) => item.building_id)])),
      this.findManyByIds(this.classroomsRepo, uniqueIds([subsection.classroom_id, ...schedules.map((item) => item.classroom_id)])),
      this.findManyByIds(this.vcSectionsRepo, uniqueIds([subsection.vc_section_id])),
      this.findManyByField(this.campusVcLocationMappingsRepo, 'campus_id', [offer.campus_id]),
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
      vcSection: mapById(vcSections).get(subsection.vc_section_id ?? '') ?? null,
      expectedVcSectionName: this.buildExpectedVcSectionNameForSubsection(subsection, section, offer, {
        modalities,
        campusVcLocations,
      }),
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
        throw new BadRequestException(`El grupo ${nextCode} ya existe.`);
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
    await this.syncSubsectionVcMatch(id, actor);
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
    const classroomId = emptyToNull(dto.classroom_id);
    const buildingId = emptyToNull(dto.building_id);
    const classroom = classroomId
      ? await this.classroomsRepo.findOne({ where: { id: classroomId } })
      : null;
    await this.validateSubsectionLocationForOffer(
      offer,
      classroom?.building_id ?? buildingId ?? subsection.building_id ?? null,
      classroomId,
    );
    const now = new Date();
    const minutes = computeMinutesFromTimes(dto.start_time, dto.end_time);
    const schedule = this.schedulesRepo.create({
      id: dto.id ?? newId(),
      planning_subsection_id: subsectionId,
      day_of_week: dto.day_of_week,
      start_time: dto.start_time,
      end_time: dto.end_time,
      session_type: dto.session_type ?? resolveDefaultSessionTypeForGroup(subsection.kind),
      source_session_type_code: emptyToNull(dto.source_session_type_code),
      teacher_id: emptyToNull(dto.teacher_id),
      building_id: buildingId ?? classroom?.building_id ?? null,
      classroom_id: classroomId,
      source_schedule_id: null,
      source_payload_json: null,
      duration_minutes: minutes,
      academic_hours:
        dto.academic_hours !== undefined ? roundToTwo(dto.academic_hours) : roundToTwo(minutes / 50),
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
    const classroomId =
      dto.classroom_id !== undefined ? emptyToNull(dto.classroom_id) : current.classroom_id;
    const classroom = classroomId
      ? await this.classroomsRepo.findOne({ where: { id: classroomId } })
      : null;
    const buildingId =
      dto.building_id !== undefined
        ? emptyToNull(dto.building_id)
        : classroom?.building_id ?? current.building_id;
    await this.validateSubsectionLocationForOffer(
      offer,
      classroom?.building_id ?? buildingId ?? subsection.building_id ?? null,
      classroomId,
    );
    const next = this.schedulesRepo.create({
      ...current,
      ...dto,
      classroom_id: classroomId,
      building_id: classroom?.building_id ?? buildingId ?? null,
      teacher_id:
        dto.teacher_id !== undefined ? emptyToNull(dto.teacher_id) : current.teacher_id,
      source_session_type_code:
        dto.source_session_type_code !== undefined
          ? emptyToNull(dto.source_session_type_code)
          : current.source_session_type_code,
      duration_minutes: minutes,
      academic_hours:
        dto.academic_hours !== undefined ? roundToTwo(dto.academic_hours) : roundToTwo(minutes / 50),
      updated_at: new Date(),
    });
    await this.schedulesRepo.save(next);
    const saved = await this.requireEntity(this.schedulesRepo, id, 'planning_subsection_schedule');
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

  async listVcMatchRows(filters: PlanningVcMatchFilters = {}) {
    const baseWhere: any = {
      ...(filters.semester_id ? { semester_id: filters.semester_id } : {}),
      ...(filters.campus_id ? { campus_id: filters.campus_id } : {}),
      ...(filters.faculty_id ? { faculty_id: filters.faculty_id } : {}),
      ...(filters.academic_program_id ? { academic_program_id: filters.academic_program_id } : {}),
      ...(filters.cycle ? { cycle: filters.cycle } : {}),
      ...(filters.study_plan_id ? { study_plan_id: filters.study_plan_id } : {}),
    };

    const offers = await this.offersRepo.find({
      where: filters.query
        ? [
            { ...baseWhere, course_name: ILike(`%${filters.query}%`) },
            { ...baseWhere, course_code: ILike(`%${filters.query}%`) },
          ]
        : baseWhere,
      order: { course_name: 'ASC', updated_at: 'DESC' },
    });
    if (offers.length === 0) {
      return [];
    }

    const context = await this.buildContext(offers.map((item) => item.id), {
      skipChangeLogs: true,
      skipConflicts: true,
      preloadedOffers: offers,
    });
    const [semesters, campuses, faculties, academicPrograms, studyPlans, candidateVcSections] =
      await Promise.all([
        this.findManyByIds(this.semestersRepo, uniqueIds(offers.map((item) => item.semester_id))),
        this.findManyByIds(this.campusesRepo, uniqueIds(offers.map((item) => item.campus_id))),
        this.findManyByIds(this.facultiesRepo, uniqueIds(offers.map((item) => item.faculty_id))),
        this.findManyByIds(
          this.programsRepo,
          uniqueIds(offers.map((item) => item.academic_program_id)),
        ),
        this.findManyByIds(this.studyPlansRepo, uniqueIds(offers.map((item) => item.study_plan_id))),
        this.findManyByField(
          this.vcSectionsRepo,
          'course_id',
          uniqueIds(offers.map((item) => item.vc_course_id)),
        ),
      ]);

    const sectionsByOfferId = groupBy(context.sections, (item) => item.planning_offer_id);
    const subsectionsBySectionId = groupBy(context.subsections, (item) => item.planning_section_id);
    const semesterMap = mapById(semesters);
    const campusMap = mapById(campuses);
    const facultyMap = mapById(faculties);
    const programMap = mapById(academicPrograms);
    const studyPlanMap = mapById(studyPlans);
    const vcFacultyMap = mapById(context.vcFaculties);
    const vcAcademicProgramMap = mapById(context.vcAcademicPrograms);
    const vcCourseMap = mapById(context.vcCourses);
    const vcSectionMap = mapById([...context.vcSections, ...candidateVcSections]);
    const teacherMap = mapById(context.teachers);
    const candidateSectionsByCourseId = groupBy(candidateVcSections, (item) => item.course_id);

    const rows = offers
      .flatMap((offer) => {
        const offerVcContext = readOfferVcContextMetadata(offer.source_payload_json);
        const offerSections = sectionsByOfferId.get(offer.id) ?? [];
        return offerSections.flatMap((section) =>
          (subsectionsBySectionId.get(section.id) ?? []).map((subsection) => {
            const sectionVcContext = readSectionVcContextMetadata(section.source_payload_json);
            const expectedVcSectionName = this.buildExpectedVcSectionNameForSubsection(
              subsection,
              section,
              offer,
              {
                modalities: context.modalities,
                campusVcLocations: context.campusVcLocations,
              },
            );
            const expectedVcSectionNames = this.buildExpectedVcSectionNameCandidatesForSubsection(
              subsection,
              section,
              offer,
              {
                modalities: context.modalities,
                campusVcLocations: context.campusVcLocations,
              },
            );
            const sectionCandidates = candidateSectionsByCourseId.get(offer.vc_course_id ?? '__none__') ?? [];
            const suggestedMatches = expectedVcSectionNames.length
              ? this.findMatchingVcSections(sectionCandidates, expectedVcSectionNames)
              : [];

            // Priorizar concordancia desde la sincronización de Akademic (tabla sinc)
            const syncSourceId = sectionVcContext.source_vc_section_id;
            if (syncSourceId && !suggestedMatches.some((m) => m.id === syncSourceId)) {
              const syncRecord = vcSectionMap.get(syncSourceId);
              if (syncRecord) {
                suggestedMatches.unshift(syncRecord);
              }
            }
            return {
              id: subsection.id,
              semester: semesterMap.get(offer.semester_id) ?? null,
              campus: campusMap.get(offer.campus_id) ?? null,
              faculty: facultyMap.get(offer.faculty_id ?? '') ?? null,
              academic_program: programMap.get(offer.academic_program_id ?? '') ?? null,
              study_plan: studyPlanMap.get(offer.study_plan_id) ?? null,
              offer: {
                id: offer.id,
                cycle: offer.cycle,
                course_code: offer.course_code,
                course_name: offer.course_name,
                vc_course_id: offer.vc_course_id,
                vc_faculty_id: offer.vc_faculty_id,
                vc_academic_program_id: offer.vc_academic_program_id,
                vc_source: offerVcContext.vc_source,
                vc_context_message: offerVcContext.vc_context_message,
                source_vc_course_id: offerVcContext.source_vc_course_id,
                vc_course: vcCourseMap.get(offer.vc_course_id ?? '') ?? null,
                vc_faculty: vcFacultyMap.get(offer.vc_faculty_id ?? '') ?? null,
                vc_academic_program:
                  vcAcademicProgramMap.get(offer.vc_academic_program_id ?? '') ?? null,
              },
              section: {
                id: section.id,
                code: section.code,
              },
              subsection: {
                id: subsection.id,
                code: subsection.code,
                teacher_name: teacherMap.get(subsection.responsible_teacher_id ?? '')?.name ?? null,
                vc_section_id: subsection.vc_section_id,
                source_vc_section_id: sectionVcContext.source_vc_section_id,
                manual_vc_section_id:
                  sectionVcContext.manual_subsection_overrides[subsection.code] ?? null,
              },
              expected_vc_section_name: expectedVcSectionName,
              vc_section: vcSectionMap.get(subsection.vc_section_id ?? '') ?? null,
              vc_section_candidates: sectionCandidates,
              suggested_vc_sections: suggestedMatches,
              match_status: resolveDetailedVcMatchStatus(
                subsection.vc_section_id,
                vcSectionMap.get(subsection.vc_section_id ?? '') ?? null,
                offer.vc_course_id,
                expectedVcSectionName,
                suggestedMatches.length,
              ),
            };
          }),
        );
      });

    return rows.filter((r) => {
      const passesFilter = shouldIncludeVcMatchRow(r);
      if (!passesFilter) return false;

      if (filters.query) {
        const q = filters.query.toLowerCase();
        const teacherName = (r.subsection?.teacher_name || '').toLowerCase();
        const courseName = (r.offer?.course_name || '').toLowerCase();
        const courseCode = (r.offer?.course_code || '').toLowerCase();

        return (
          teacherName.includes(q) ||
          courseName.includes(q) ||
          courseCode.includes(q)
        );
      }

      return true;
    })
      .sort((left, right) => {
        const offerDelta = compareCatalogLabels(
          left.offer?.course_name ?? left.offer?.course_code,
          right.offer?.course_name ?? right.offer?.course_code,
        );
        if (offerDelta !== 0) {
          return offerDelta;
        }
        const sectionDelta = compareCatalogLabels(left.section?.code, right.section?.code);
        if (sectionDelta !== 0) {
          return sectionDelta;
        }
        return compareCatalogLabels(left.subsection?.code, right.subsection?.code);
      });
  }

  async updateSubsectionVcMatch(
    actor: ChangeLogActor | null | undefined,
    id: string,
    dto: UpdatePlanningSubsectionVcMatchDto,
  ) {
    const current = await this.requireEntity(this.subsectionsRepo, id, 'planning_subsection');
    const section = await this.requireEntity(this.sectionsRepo, current.planning_section_id, 'planning_section');
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    await this.assertOfferPlanEditable(offer);

    const nextVcSectionId = emptyToNull(dto.vc_section_id);
    if (nextVcSectionId) {
      const vcSection = await this.requireEntity(this.vcSectionsRepo, nextVcSectionId, 'vc_section');
      if (offer.vc_course_id && vcSection.course_id !== offer.vc_course_id) {
        throw new BadRequestException(
          'La seccion VC seleccionada no pertenece al curso VC asociado a la oferta.',
        );
      }
    }

    const next = this.subsectionsRepo.create({
      ...current,
      vc_section_id: nextVcSectionId,
      updated_at: new Date(),
    });
    await this.subsectionsRepo.save(next);
    const nextSection = this.sectionsRepo.create({
      ...section,
      source_payload_json: upsertSectionVcContextMetadata(section.source_payload_json, {
        manual_subsection_overrides: {
          ...readSectionVcContextMetadata(section.source_payload_json).manual_subsection_overrides,
          [current.code]: nextVcSectionId,
        },
        vc_context_message: nextVcSectionId
          ? 'Match VC ajustado manualmente para la subseccion.'
          : 'Match VC manual eliminado para la subseccion.',
      }),
      updated_at: new Date(),
    });
    await this.sectionsRepo.save(nextSection);
    const saved = await this.requireEntity(this.subsectionsRepo, id, 'planning_subsection');
    await this.logChange(
      'planning_subsection',
      id,
      'UPDATE',
      current,
      saved,
      { ...this.buildSubsectionLogContext(saved, section, offer), vc_match_action: 'manual_override' },
      actor,
    );
    await this.logChange(
      'planning_section',
      section.id,
      'UPDATE',
      section,
      nextSection,
      { ...this.buildSectionLogContext(nextSection, offer), vc_match_action: 'manual_override_metadata' },
      actor,
    );
    return this.getSubsection(id);
  }

  async upsertCampusVcLocationMapping(campusId: string, dto: UpdatePlanningCampusVcLocationMappingDto) {
    const campus = await this.requireEntity(this.campusesRepo, campusId, 'campus');
    if (!dto.vc_location_code) {
      throw new BadRequestException('Debes indicar el codigo VC para el campus.');
    }
    const now = new Date();
    const current = await this.campusVcLocationMappingsRepo.findOne({ where: { campus_id: campusId } });
    const next = this.campusVcLocationMappingsRepo.create({
      id: current?.id ?? newId(),
      campus_id: campus.id,
      vc_location_code: dto.vc_location_code,
      created_at: current?.created_at ?? now,
      updated_at: now,
    });
    return this.campusVcLocationMappingsRepo.save(next);
  }

  async recalculateVcMatches(actor: ChangeLogActor | null | undefined, dto: RecalculatePlanningVcMatchesDto) {
    const offerIds = dto.subsection_ids?.length
      ? await this.resolveOfferIdsFromSubsectionIds(dto.subsection_ids)
      : await this.resolveOfferIdsForVcRecalculation(dto);
    let updatedOfferCount = 0;
    let updatedSubsectionCount = 0;
    for (const offerId of offerIds) {
      const result = await this.syncOfferVcReferences(offerId, actor);
      updatedOfferCount += result.offer_updated ? 1 : 0;
      updatedSubsectionCount += result.updated_subsection_count;
    }
    return {
      offer_count: offerIds.length,
      updated_offer_count: updatedOfferCount,
      updated_subsection_count: updatedSubsectionCount,
    };
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

    return conflicts
      .map((conflict) => {
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
    })
      .filter((conflict) => Boolean(conflict.meeting_a && conflict.meeting_b));
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

  private defaultCatalogsEnsured = false;

  private async ensureDefaultCatalogs() {
    if (this.defaultCatalogsEnsured) {
      return;
    }
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
    this.defaultCatalogsEnsured = true;
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
      vc_period_id?: string | null;
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
        ...(candidate.vc_period_id ? { vc_period_id: candidate.vc_period_id } : {}),
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
    const [
      semesters,
      campuses,
      programs,
      plans,
      faculties,
      vcPeriods,
      vcFaculties,
      vcAcademicPrograms,
    ] = await Promise.all([
      this.findManyByIds(this.semestersRepo, uniqueIds(rules.map((item) => item.semester_id))),
      this.findManyByIds(this.campusesRepo, uniqueIds(rules.map((item) => item.campus_id))),
      this.findManyByIds(this.programsRepo, uniqueIds(rules.map((item) => item.academic_program_id))),
      this.findManyByIds(this.studyPlansRepo, uniqueIds(rules.map((item) => item.study_plan_id))),
      this.findManyByIds(this.facultiesRepo, uniqueIds(rules.map((item) => item.faculty_id))),
      this.findManyByIds(this.vcPeriodsRepo, uniqueIds(rules.map((item) => item.vc_period_id))),
      this.findManyByIds(this.vcFacultiesRepo, uniqueIds(rules.map((item) => item.vc_faculty_id))),
      this.findManyByIds(
        this.vcAcademicProgramsRepo,
        uniqueIds(rules.map((item) => item.vc_academic_program_id)),
      ),
    ]);
    const semesterMap = mapById(semesters);
    const campusMap = mapById(campuses);
    const programMap = mapById(programs);
    const planMap = mapById(plans);
    const facultyMap = mapById(faculties);
    const vcPeriodMap = mapById(vcPeriods);
    const vcFacultyMap = mapById(vcFaculties);
    const vcAcademicProgramMap = mapById(vcAcademicPrograms);
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
        vc_period: vcPeriodMap.get(rule.vc_period_id ?? '') ?? null,
        vc_faculty: vcFacultyMap.get(rule.vc_faculty_id ?? '') ?? null,
        vc_academic_program: vcAcademicProgramMap.get(rule.vc_academic_program_id ?? '') ?? null,
      };
    });
  }

  private async enrichPlanRule(rule: PlanningCyclePlanRuleEntity) {
    const [item] = await this.enrichPlanRules([rule]);
    return item;
  }

  private async findMatchingRules(
    semesterId: string,
    vcPeriodId: string | null,
    campusId: string,
    academicProgramId: string,
    cycle: number,
  ) {
    const rules = await this.planRulesRepo.find({
      where: {
        semester_id: semesterId,
        ...(vcPeriodId ? { vc_period_id: vcPeriodId } : {}),
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
    const vcCourseIdByStudyPlanCourseId = new Map<string, string | null>();
    for (const blueprint of blueprints) {
      vcCourseIdByStudyPlanCourseId.set(
        blueprint.study_plan_course_id,
        await this.resolveVcCourseId({
          course_name: blueprint.course_name,
          course_code: blueprint.course_code,
          study_plan_id: rule.study_plan_id,
          vc_academic_program_id: rule.vc_academic_program_id,
        }),
      );
    }
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
          vc_period_id: rule.vc_period_id ?? null,
          vc_faculty_id: rule.vc_faculty_id ?? null,
          vc_academic_program_id: rule.vc_academic_program_id ?? null,
          vc_course_id: vcCourseIdByStudyPlanCourseId.get(item.study_plan_course_id) ?? null,
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
    const context = await this.buildContext(offers.map((item) => item.id), { skipChangeLogs: true });
    return offers.map((offer) => this.buildOfferSummary(offer, context));
  }

  private async buildContext(
    offerIds: string[],
    options?: { skipChangeLogs?: boolean; skipConflicts?: boolean; preloadedOffers?: PlanningOfferEntity[] },
  ): Promise<PlanningContext> {
    if (offerIds.length === 0) {
      return {
        offers: [],
        sections: [],
        subsections: [],
        schedules: [],
        teachers: [],
        modalities: [],
        buildings: [],
        classrooms: [],
        conflicts: [],
        changeLogs: [],
        vcPeriods: [],
        vcFaculties: [],
        vcAcademicPrograms: [],
        vcCourses: [],
        vcSections: [],
        campusVcLocations: [],
      };
    }

    // --- Phase 1: Fetch offers + sections + conflicts in parallel ---
    const [offers, sections, conflicts] = await Promise.all([
      options?.preloadedOffers
        ? Promise.resolve(options.preloadedOffers)
        : this.offersRepo.find({ where: { id: In(offerIds) } }),
      this.sectionsRepo.find({
        where: { planning_offer_id: In(offerIds) },
        order: { code: 'ASC' },
      }),
      options?.skipConflicts
        ? Promise.resolve([] as PlanningScheduleConflictV2Entity[])
        : this.conflictsRepo.find({
            where: { planning_offer_id: In(offerIds) },
            order: { detected_at: 'DESC' },
          }),
    ]);

    // --- Phase 2: Fetch subsections (needs section IDs) ---
    const sectionIds = sections.map((item) => item.id);
    const subsections = sectionIds.length
      ? await this.subsectionsRepo.find({
          where: { planning_section_id: In(sectionIds) },
          order: { code: 'ASC' },
        })
      : [];

    // --- Phase 3: Fetch schedules + all lookups in parallel ---
    const subsectionIds = subsections.map((item) => item.id);

    const [
      schedules,
      teachers,
      modalities,
      buildings,
      classrooms,
      vcPeriods,
      vcFaculties,
      vcAcademicPrograms,
      vcCourses,
      vcSections,
      campusVcLocations,
    ] = await Promise.all([
      subsectionIds.length
        ? this.schedulesRepo.find({
            where: { planning_subsection_id: In(subsectionIds) },
            order: { day_of_week: 'ASC', start_time: 'ASC' },
          })
        : Promise.resolve([] as PlanningSubsectionScheduleEntity[]),
      this.findManyByIds(
        this.teachersRepo,
        uniqueIds([
          ...sections.map((item) => item.teacher_id),
          ...subsections.map((item) => item.responsible_teacher_id),
        ]),
      ),
      this.findManyByIds(
        this.courseModalitiesRepo,
        uniqueIds(subsections.map((item) => item.course_modality_id)),
      ),
      this.findManyByIds(
        this.buildingsRepo,
        uniqueIds(subsections.map((item) => item.building_id)),
      ),
      this.findManyByIds(
        this.classroomsRepo,
        uniqueIds(subsections.map((item) => item.classroom_id)),
      ),
      this.findManyByIds(this.vcPeriodsRepo, uniqueIds(offers.map((item) => item.vc_period_id))),
      this.findManyByIds(this.vcFacultiesRepo, uniqueIds(offers.map((item) => item.vc_faculty_id))),
      this.findManyByIds(
        this.vcAcademicProgramsRepo,
        uniqueIds(offers.map((item) => item.vc_academic_program_id)),
      ),
      this.findManyByIds(this.vcCoursesRepo, uniqueIds(offers.map((item) => item.vc_course_id))),
      this.findManyByIds(this.vcSectionsRepo, uniqueIds(subsections.map((item) => item.vc_section_id))),
      this.findManyByField(
        this.campusVcLocationMappingsRepo,
        'campus_id',
        uniqueIds(offers.map((item) => item.campus_id)),
      ),
    ]);

    // Supplement with schedule-specific teacher/building/classroom IDs
    const existingTeacherIds = new Set(teachers.map((t) => t.id));
    const existingBuildingIds = new Set(buildings.map((b) => b.id));
    const existingClassroomIds = new Set(classrooms.map((c) => c.id));
    const extraTeacherIds = uniqueIds(schedules.map((item) => item.teacher_id)).filter((id) => !existingTeacherIds.has(id));
    const extraBuildingIds = uniqueIds(schedules.map((item) => item.building_id)).filter((id) => !existingBuildingIds.has(id));
    const extraClassroomIds = uniqueIds(schedules.map((item) => item.classroom_id)).filter((id) => !existingClassroomIds.has(id));

    let allTeachers = teachers;
    let allBuildings = buildings;
    let allClassrooms = classrooms;

    if (extraTeacherIds.length > 0 || extraBuildingIds.length > 0 || extraClassroomIds.length > 0) {
      const [et, eb, ec] = await Promise.all([
        extraTeacherIds.length > 0 ? this.findManyByIds(this.teachersRepo, extraTeacherIds) : Promise.resolve([]),
        extraBuildingIds.length > 0 ? this.findManyByIds(this.buildingsRepo, extraBuildingIds) : Promise.resolve([]),
        extraClassroomIds.length > 0 ? this.findManyByIds(this.classroomsRepo, extraClassroomIds) : Promise.resolve([]),
      ]);
      allTeachers = [...teachers, ...et];
      allBuildings = [...buildings, ...eb];
      allClassrooms = [...classrooms, ...ec];
    }

    let changeLogs: PlanningChangeLogEntity[] = [];
    if (!options?.skipChangeLogs) {
      const entityIds = uniqueIds([
        ...offerIds,
        ...sectionIds,
        ...subsectionIds,
        ...schedules.map((item) => item.id),
      ]);
      changeLogs = await this.changeLogsRepo.find({
        where: { entity_id: In(entityIds) },
        order: { changed_at: 'DESC' },
      });
    }

    return {
      offers,
      sections,
      subsections,
      schedules,
      teachers: allTeachers,
      modalities,
      buildings: allBuildings,
      classrooms: allClassrooms,
      conflicts,
      changeLogs,
      vcPeriods,
      vcFaculties,
      vcAcademicPrograms,
      vcCourses,
      vcSections,
      campusVcLocations,
    };
  }

  private buildOfferSummary(offer: PlanningOfferEntity, context: PlanningContext) {
    const sections = context.sections.filter((item) => item.planning_offer_id === offer.id);
    const sectionIds = sections.map((item) => item.id);
    const subsections = context.subsections.filter((item) => sectionIds.includes(item.planning_section_id));
    const subsectionIds = subsections.map((item) => item.id);
    const schedules = context.schedules.filter((item) => subsectionIds.includes(item.planning_subsection_id));
    const conflicts = context.conflicts.filter((item) => item.planning_offer_id === offer.id);
    const vcPeriodMap = mapById(context.vcPeriods);
    const vcFacultyMap = mapById(context.vcFaculties);
    const vcAcademicProgramMap = mapById(context.vcAcademicPrograms);
    const vcCourseMap = mapById(context.vcCourses);
    const campusVcLocationMap = new Map(
      context.campusVcLocations.map((item) => [item.campus_id, item]),
    );
    const vcContext = readOfferVcContextMetadata(offer.source_payload_json);
    return {
      ...offer,
      vc_period: vcPeriodMap.get(offer.vc_period_id ?? '') ?? null,
      vc_faculty: vcFacultyMap.get(offer.vc_faculty_id ?? '') ?? null,
      vc_academic_program: vcAcademicProgramMap.get(offer.vc_academic_program_id ?? '') ?? null,
      vc_course: vcCourseMap.get(offer.vc_course_id ?? '') ?? null,
      vc_source: vcContext.vc_source,
      vc_context_message: vcContext.vc_context_message,
      campus_vc_location: campusVcLocationMap.get(offer.campus_id) ?? null,
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
    const vcSectionMap = mapById(context.vcSections);
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
            modalities: context.modalities,
            buildings: context.buildings,
            classrooms: context.classrooms,
            vcSection: vcSectionMap.get(subsection.vc_section_id ?? '') ?? null,
            expectedVcSectionName: this.buildExpectedVcSectionNameForSubsection(subsection, section, offer, {
              modalities: context.modalities,
              campusVcLocations: context.campusVcLocations,
            }),
          }),
        ),
        conflicts: sectionConflicts.get(section.id) ?? [],
      })),
      conflicts: context.conflicts.filter((item) => item.planning_offer_id === offer.id),
      change_log: context.changeLogs,
    };
  }

  private matchesExpandedOfferSearch(offer: any, search: string) {
    const sections = Array.isArray(offer?.sections) ? offer.sections : [];
    const subsections = sections.flatMap((section: any) =>
      Array.isArray(section?.subsections) ? section.subsections : [],
    );
    const schedules = subsections.flatMap((subsection: any) =>
      Array.isArray(subsection?.schedules) ? subsection.schedules : [],
    );
    const haystack = normalizeSearchValue(
      [
        offer?.course_code,
        offer?.course_name,
        offer?.academic_program?.code,
        offer?.academic_program?.name,
        offer?.study_plan?.name,
        offer?.campus?.name,
        ...sections.flatMap((section: any) => [
          section?.code,
          section?.external_code,
          section?.teacher?.full_name,
          section?.teacher?.name,
        ]),
        ...subsections.flatMap((subsection: any) => [
          subsection?.code,
          subsection?.denomination,
          subsection?.responsible_teacher?.full_name,
          subsection?.responsible_teacher?.name,
          subsection?.classroom?.code,
          subsection?.classroom?.name,
          subsection?.building?.name,
        ]),
        ...schedules.flatMap((schedule: any) => [
          schedule?.teacher?.full_name,
          schedule?.teacher?.name,
          schedule?.classroom?.code,
          schedule?.classroom?.name,
          schedule?.building?.name,
        ]),
      ]
        .filter(Boolean)
        .join(' '),
    );
    return haystack.includes(search);
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
        const subsectionSchedules = schedulesBySubsectionId.get(subsection.id) ?? [];
        if (
          this.subsectionHasRequiredConfiguration(
            subsection,
            subsectionSchedules,
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
    schedules: PlanningSubsectionScheduleEntity[],
  ) {
    const effectiveTeacherIds = uniqueIds([
      subsection.responsible_teacher_id,
      ...schedules.map((item) => item.teacher_id),
    ]);
    return Boolean(
      effectiveTeacherIds.length > 0 &&
        subsection.shift &&
        subsection.course_modality_id &&
        schedules.length > 0,
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
        : 'El plan no puede enviarse a revision hasta que todos sus cursos tengan al menos una seccion y todos sus grupos configurados.',
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
      vcSection?: VcSectionEntity | null;
      expectedVcSectionName?: string | null;
    },
  ) {
    const teacherMap = mapById(input.teachers);
    const modalityMap = mapById(input.modalities ?? []);
    const buildingMap = mapById(input.buildings);
    const classroomMap = mapById(input.classrooms);
    const ownSchedules = input.schedules.filter((item) => item.planning_subsection_id === subsection.id);
    const sectionVcContext = readSectionVcContextMetadata(input.section?.source_payload_json);
    const scheduleTeacherIds = uniqueIds(ownSchedules.map((item) => item.teacher_id));
    const scheduleBuildingIds = uniqueIds(ownSchedules.map((item) => item.building_id));
    const scheduleClassroomIds = uniqueIds(ownSchedules.map((item) => item.classroom_id));
    const effectiveTeacherId =
      scheduleTeacherIds.length === 1 ? scheduleTeacherIds[0] : subsection.responsible_teacher_id;
    const effectiveBuildingId =
      scheduleBuildingIds.length === 1 ? scheduleBuildingIds[0] : subsection.building_id;
    const effectiveClassroomId =
      scheduleClassroomIds.length === 1 ? scheduleClassroomIds[0] : subsection.classroom_id;
    return {
      ...subsection,
      section: input.section ?? null,
      offer: input.offer ?? null,
      responsible_teacher_id: effectiveTeacherId ?? null,
      building_id: effectiveBuildingId ?? null,
      classroom_id: effectiveClassroomId ?? null,
      responsible_teacher: teacherMap.get(effectiveTeacherId ?? '') ?? null,
      modality: modalityMap.get(subsection.course_modality_id ?? '') ?? null,
      building: buildingMap.get(effectiveBuildingId ?? '') ?? null,
      classroom: classroomMap.get(effectiveClassroomId ?? '') ?? null,
      vc_section: input.vcSection ?? null,
      expected_vc_section_name: input.expectedVcSectionName ?? null,
      vc_source:
        sectionVcContext.manual_subsection_overrides[subsection.code]
          ? 'manual_override'
          : sectionVcContext.source_vc_section_id
            ? 'sync_source'
            : null,
      vc_context_message: sectionVcContext.vc_context_message,
      vc_match_status: resolveVcMatchStatus(subsection.vc_section_id, input.expectedVcSectionName, input.vcSection),
      schedules: ownSchedules.map((schedule) => ({
        ...schedule,
        teacher: teacherMap.get(schedule.teacher_id ?? '') ?? null,
        building: buildingMap.get(schedule.building_id ?? '') ?? null,
        classroom: classroomMap.get(schedule.classroom_id ?? '') ?? null,
      })),
      conflicts: input.conflicts.filter((item) => item.planning_subsection_id === subsection.id),
    };
  }

  private async findPlanRuleForOfferContext(input: {
    semester_id: string;
    vc_period_id?: string | null;
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
        ...(input.vc_period_id ? { vc_period_id: input.vc_period_id } : {}),
        campus_id: input.campus_id ?? undefined,
        academic_program_id: input.academic_program_id,
        study_plan_id: input.study_plan_id,
        cycle: input.cycle,
        is_active: true,
      },
      order: { created_at: 'DESC' },
    });
  }

  async getPlanRuleForOfferContext(input: {
    semester_id: string;
    vc_period_id?: string | null;
    campus_id: string | null;
    academic_program_id: string | null;
    study_plan_id: string;
    cycle: number;
  }) {
    return this.findPlanRuleForOfferContext(input);
  }

  private ensurePlanRuleEditable(
    rule: PlanningCyclePlanRuleEntity | null,
    options?: { allowApproved?: boolean },
  ) {
    if (!rule) {
      return;
    }
    const allowApproved = Boolean(options?.allowApproved);
    if (rule.workflow_status === 'IN_REVIEW' || (!allowApproved && rule.workflow_status === 'APPROVED')) {
      throw new BadRequestException(
        'Este plan esta bloqueado por workflow. Solo puede editarse si vuelve a correccion.',
      );
    }
  }

  private async assertOfferPlanEditable(offer: PlanningOfferEntity) {
    const rule = await this.findPlanRuleForOfferContext({
      semester_id: offer.semester_id,
      vc_period_id: offer.vc_period_id ?? null,
      campus_id: offer.campus_id,
      academic_program_id: offer.academic_program_id ?? null,
      study_plan_id: offer.study_plan_id,
      cycle: offer.cycle,
    });
    this.ensurePlanRuleEditable(rule, { allowApproved: true });
    return rule;
  }

  private async syncVcReferencesForRule(
    rule: PlanningCyclePlanRuleEntity,
    actor?: ChangeLogActor | null,
  ) {
    const offers = await this.offersRepo.find({
      where: {
        semester_id: rule.semester_id,
        ...(rule.vc_period_id ? { vc_period_id: rule.vc_period_id } : {}),
        campus_id: rule.campus_id ?? undefined,
        academic_program_id: rule.academic_program_id,
        study_plan_id: rule.study_plan_id,
        cycle: rule.cycle,
      },
    });
    for (const offer of offers) {
      await this.syncOfferVcReferences(offer.id, actor);
    }
  }

  private async syncOfferVcReferences(
    offerId: string,
    actor?: ChangeLogActor | null,
    options?: { preserveManualCourse?: boolean; restoreSourceContext?: boolean; preserveManualSection?: boolean },
  ) {
    const current = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    const rule = await this.findPlanRuleForOfferContext({
      semester_id: current.semester_id,
      vc_period_id: current.vc_period_id ?? null,
      campus_id: current.campus_id,
      academic_program_id: current.academic_program_id ?? null,
      study_plan_id: current.study_plan_id,
      cycle: current.cycle,
    });
    const nextVcPeriodId = rule?.vc_period_id ?? current.vc_period_id ?? null;
    const sourceContext = await this.resolveOfferSourceVcContext(current);
    const restoreSourceContext = options?.restoreSourceContext !== false;
    const preserveManualCourse = options?.preserveManualCourse !== false;
    const nextVcFacultyId = sourceContext.source_vc_faculty_id ?? null;
    const nextVcAcademicProgramId = sourceContext.source_vc_academic_program_id ?? null;
    const fallbackVcCourseId = await this.resolveVcCourseIdFromSourceContext({
      course_name: sourceContext.source_course_name ?? current.course_name,
      course_code: sourceContext.source_course_code ?? current.course_code,
      vc_academic_program_id: nextVcAcademicProgramId,
    });
    let nextVcCourseId = fallbackVcCourseId;
    if (preserveManualCourse && sourceContext.manual_vc_course_id) {
      nextVcCourseId = sourceContext.manual_vc_course_id;
    } else if (restoreSourceContext && sourceContext.source_vc_course_id) {
      nextVcCourseId = sourceContext.source_vc_course_id;
    }

    const nextSourcePayloadJson =
      sourceContext.has_source_context || sourceContext.manual_vc_course_id
        ? upsertOfferVcContextMetadata(current.source_payload_json, {
            vc_source:
              preserveManualCourse && sourceContext.manual_vc_course_id
                ? 'manual_override'
                : sourceContext.has_source_context
                  ? 'sync_source'
                  : 'fallback_match',
            source_vc_faculty_id: sourceContext.source_vc_faculty_id,
            source_vc_academic_program_id: sourceContext.source_vc_academic_program_id,
            source_vc_course_id: sourceContext.source_vc_course_id,
            manual_vc_course_id:
              preserveManualCourse && sourceContext.manual_vc_course_id
                ? sourceContext.manual_vc_course_id
                : null,
            vc_context_message:
              preserveManualCourse && sourceContext.manual_vc_course_id
                ? 'Curso VC ajustado manualmente.'
                : sourceContext.has_source_context
                  ? 'Contexto AV preservado desde la sincronizacion.'
                  : 'Contexto AV pendiente de resolver desde sincronizacion.',
          })
        : current.source_payload_json;

    const next = this.offersRepo.create({
      ...current,
      vc_period_id: nextVcPeriodId,
      vc_faculty_id: nextVcFacultyId,
      vc_academic_program_id: nextVcAcademicProgramId,
      vc_course_id: nextVcCourseId,
      source_payload_json: nextSourcePayloadJson,
      updated_at: new Date(),
    });

    let offerUpdated = false;
    if (
      current.vc_period_id !== next.vc_period_id ||
      current.vc_faculty_id !== next.vc_faculty_id ||
      current.vc_academic_program_id !== next.vc_academic_program_id ||
      current.vc_course_id !== next.vc_course_id
    ) {
      await this.offersRepo.save(next);
      const saved = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
      await this.logChange(
        'planning_offer',
        offerId,
        'UPDATE',
        current,
        saved,
        { ...this.buildOfferLogContext(saved), vc_match_action: 'sync_offer_refs' },
        actor,
      );
      offerUpdated = true;
    }

    const sections = await this.sectionsRepo.find({ where: { planning_offer_id: offerId } });
    let updatedSubsectionCount = 0;
    for (const section of sections) {
      updatedSubsectionCount += await this.syncSectionVcMatches(section.id, actor, {
        restoreSourceContext,
        preserveManualSection: options?.preserveManualSection !== false,
      });
    }

    return {
      offer_updated: offerUpdated,
      updated_subsection_count: updatedSubsectionCount,
    };
  }

  private async syncSectionVcMatches(
    sectionId: string,
    actor?: ChangeLogActor | null,
    options?: { restoreSourceContext?: boolean; preserveManualSection?: boolean },
  ) {
    const subsections = await this.subsectionsRepo.find({
      where: { planning_section_id: sectionId },
      order: { code: 'ASC' },
    });
    let updatedCount = 0;
    for (const subsection of subsections) {
      updatedCount += await this.syncSubsectionVcMatch(subsection.id, actor, options);
    }
    return updatedCount;
  }

  private async syncSubsectionVcMatch(
    subsectionId: string,
    actor?: ChangeLogActor | null,
    options?: { restoreSourceContext?: boolean; preserveManualSection?: boolean },
  ) {
    const current = await this.requireEntity(this.subsectionsRepo, subsectionId, 'planning_subsection');
    const section = await this.requireEntity(this.sectionsRepo, current.planning_section_id, 'planning_section');
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    const sectionVcContext = readSectionVcContextMetadata(section.source_payload_json);
    const modalities = await this.findManyByIds(this.courseModalitiesRepo, [current.course_modality_id]);
    const campusVcLocations = await this.findManyByField(
      this.campusVcLocationMappingsRepo,
      'campus_id',
      [offer.campus_id],
    );
    const expectedVcSectionName = this.buildExpectedVcSectionNameForSubsection(current, section, offer, {
      modalities,
      campusVcLocations,
    });
    const expectedVcSectionNames = this.buildExpectedVcSectionNameCandidatesForSubsection(
      current,
      section,
      offer,
      {
        modalities,
        campusVcLocations,
      },
    );
    const candidateSections = offer.vc_course_id
      ? await this.findManyByField(this.vcSectionsRepo, 'course_id', [offer.vc_course_id])
      : [];
    const suggestedMatches = expectedVcSectionNames.length
      ? this.findMatchingVcSections(candidateSections, expectedVcSectionNames)
      : [];
    const currentVcSection =
      current.vc_section_id
        ? await this.vcSectionsRepo.findOne({ where: { id: current.vc_section_id } })
        : null;
    const restoreSourceContext = options?.restoreSourceContext !== false;
    const preserveManualSection = options?.preserveManualSection !== false;
    const manualOverrideSectionId =
      preserveManualSection
        ? sectionVcContext.manual_subsection_overrides[current.code] ?? null
        : null;
    const manualOverrideSection =
      manualOverrideSectionId
        ? await this.vcSectionsRepo.findOne({ where: { id: manualOverrideSectionId } })
        : null;
    const sourceVcSectionId = sectionVcContext.source_vc_section_id;
    const sourceVcSection =
      sourceVcSectionId
        ? await this.vcSectionsRepo.findOne({ where: { id: sourceVcSectionId } })
        : null;

    let nextVcSectionId = current.vc_section_id ?? null;
    if (
      manualOverrideSection &&
      (!offer.vc_course_id || manualOverrideSection.course_id === offer.vc_course_id)
    ) {
      nextVcSectionId = manualOverrideSection.id;
    } else if (restoreSourceContext && sourceVcSection) {
      // Confiamos en la concordancia de origen (Akademic). 
      // Si hay un desfase de curso, el estado de match en la UI lo marcará para revisión manual.
      nextVcSectionId = sourceVcSection.id;
    } else if (currentVcSection && offer.vc_course_id && currentVcSection.course_id !== offer.vc_course_id) {
      nextVcSectionId = suggestedMatches.length === 1 ? suggestedMatches[0].id : null;
    } else if (!currentVcSection) {
      nextVcSectionId = suggestedMatches.length === 1 ? suggestedMatches[0].id : null;
    }

    if (nextVcSectionId === current.vc_section_id) {
      return 0;
    }

    const next = this.subsectionsRepo.create({
      ...current,
      vc_section_id: nextVcSectionId,
      updated_at: new Date(),
    });
    await this.subsectionsRepo.save(next);
    const saved = await this.requireEntity(this.subsectionsRepo, subsectionId, 'planning_subsection');
    await this.logChange(
      'planning_subsection',
      subsectionId,
      'UPDATE',
      current,
      saved,
      { ...this.buildSubsectionLogContext(saved, section, offer), vc_match_action: 'auto_sync' },
      actor,
    );
    return 1;
  }

  private async resolveOfferSourceVcContext(offer: PlanningOfferEntity) {
    const vcContext = readOfferVcContextMetadata(offer.source_payload_json);
    const rawSourceFacultyId = vcContext.source_vc_faculty_id ?? extractSourceFacultyIdFromPayload(offer.source_payload_json);
    const rawSourceAcademicProgramId =
      vcContext.source_vc_academic_program_id ?? extractSourceAcademicProgramIdFromPayload(offer.source_payload_json);
    const rawSourceCourseId =
      vcContext.source_vc_course_id ??
      extractSourceCourseIdFromPayload(offer.source_payload_json) ??
      offer.source_course_id;
    const rawSourceFacultyName = extractSourceFacultyNameFromPayload(offer.source_payload_json);
    const rawSourceAcademicProgramName =
      extractSourceAcademicProgramNameFromPayload(offer.source_payload_json);
    const rawSourceCourseName =
      extractSourceCourseNameFromPayload(offer.source_payload_json) ?? offer.course_name;
    const rawSourceCourseCode =
      extractSourceCourseCodeFromPayload(offer.source_payload_json) ?? offer.course_code;
    const manualVcCourseId = vcContext.manual_vc_course_id;

    let sourceCourse = rawSourceCourseId
      ? await this.vcCoursesRepo.findOne({ where: { id: rawSourceCourseId } })
      : null;
    let sourceAcademicProgram = sourceCourse?.program_id
      ? await this.vcAcademicProgramsRepo.findOne({ where: { id: sourceCourse.program_id } })
      : rawSourceAcademicProgramId
        ? await this.vcAcademicProgramsRepo.findOne({ where: { id: rawSourceAcademicProgramId } })
        : null;
    let sourceFaculty = sourceAcademicProgram?.faculty_id
      ? await this.vcFacultiesRepo.findOne({ where: { id: sourceAcademicProgram.faculty_id } })
      : rawSourceFacultyId
        ? await this.vcFacultiesRepo.findOne({ where: { id: rawSourceFacultyId } })
        : null;

    if (!sourceFaculty && rawSourceFacultyName) {
      sourceFaculty = await this.matchVcFacultyByName(rawSourceFacultyName);
    }
    if (!sourceAcademicProgram && rawSourceAcademicProgramName) {
      sourceAcademicProgram = await this.matchVcAcademicProgramByName(
        rawSourceAcademicProgramName,
        sourceFaculty?.id ?? null,
      );
      if (sourceAcademicProgram && !sourceFaculty) {
        sourceFaculty =
          (await this.vcFacultiesRepo.findOne({ where: { id: sourceAcademicProgram.faculty_id } })) ?? null;
      }
    }
    if (!sourceCourse && sourceAcademicProgram?.id) {
      const resolvedCourseId = await this.resolveVcCourseIdFromSourceContext({
        course_name: rawSourceCourseName,
        course_code: rawSourceCourseCode,
        vc_academic_program_id: sourceAcademicProgram.id,
      });
      sourceCourse = resolvedCourseId
        ? await this.vcCoursesRepo.findOne({ where: { id: resolvedCourseId } })
        : null;
      if (sourceCourse && !sourceAcademicProgram) {
        sourceAcademicProgram =
          (await this.vcAcademicProgramsRepo.findOne({ where: { id: sourceCourse.program_id } })) ?? null;
      }
      if (sourceCourse && sourceAcademicProgram && !sourceFaculty) {
        sourceFaculty =
          (await this.vcFacultiesRepo.findOne({ where: { id: sourceAcademicProgram.faculty_id } })) ?? null;
      }
    }

    return {
      has_source_context: Boolean(sourceCourse || sourceAcademicProgram || sourceFaculty),
      source_vc_faculty_id: sourceFaculty?.id ?? null,
      source_vc_academic_program_id: sourceAcademicProgram?.id ?? null,
      source_vc_course_id: sourceCourse?.id ?? null,
      manual_vc_course_id: manualVcCourseId,
      source_course_name: rawSourceCourseName ?? null,
      source_course_code: rawSourceCourseCode ?? null,
    };
  }

  private async matchVcFacultyByName(name: string | null | undefined) {
    const variants = buildCatalogNameMatchVariants(name);
    if (variants.length === 0) {
      return null;
    }
    const faculties = await this.vcFacultiesRepo.find({ order: { name: 'ASC' } });
    const matches = faculties.filter((item) => catalogNameMatches(item.name, variants));
    return matches.length === 1 ? matches[0] : null;
  }

  private async matchVcAcademicProgramByName(name: string | null | undefined, facultyId: string | null) {
    const variants = buildCatalogNameMatchVariants(name);
    if (variants.length === 0) {
      return null;
    }
    const programs = await this.vcAcademicProgramsRepo.find({
      where: facultyId ? { faculty_id: facultyId } : {},
      order: { name: 'ASC' },
    });
    const matches = programs.filter((item) => catalogNameMatches(item.name, variants));
    return matches.length === 1 ? matches[0] : null;
  }

  private async resolveVcCourseIdFromSourceContext(input: {
    course_name: string | null | undefined;
    course_code: string | null | undefined;
    vc_academic_program_id: string | null;
  }) {
    if (!input.vc_academic_program_id) {
      return null;
    }
    const candidates = await this.vcCoursesRepo.find({
      where: { program_id: input.vc_academic_program_id },
      order: { name: 'ASC' },
    });
    const normalizedCode = normalizeMatchValue(input.course_code);
    if (normalizedCode) {
      const codeMatches = candidates.filter(
        (item) => normalizeMatchValue(item.code) === normalizedCode,
      );
      if (codeMatches.length === 1) {
        return codeMatches[0].id;
      }
    }

    const normalizedCourseName = normalizeCourseNameForMatch(input.course_name);
    if (!normalizedCourseName) {
      return null;
    }
    const nameMatches = candidates.filter(
      (item) => normalizeCourseNameForMatch(item.name) === normalizedCourseName,
    );
    if (nameMatches.length === 1) {
      return nameMatches[0].id;
    }
    if (nameMatches.length > 1 && normalizedCode) {
      const refinedByCode = nameMatches.filter(
        (item) => normalizeMatchValue(item.code) === normalizedCode,
      );
      return refinedByCode.length === 1 ? refinedByCode[0].id : null;
    }
    return null;
  }

  private async resolveVcCourseId(input: {
    course_name: string | null | undefined;
    course_code: string | null | undefined;
    study_plan_id: string;
    vc_academic_program_id: string | null;
  }) {
    const normalizedCourseName = normalizeCourseNameForMatch(input.course_name);
    if (!normalizedCourseName || !input.vc_academic_program_id) {
      return null;
    }
    const [studyPlan, candidates] = await Promise.all([
      this.studyPlansRepo.findOne({ where: { id: input.study_plan_id } }),
      this.vcCoursesRepo.find({
        where: { program_id: input.vc_academic_program_id },
        order: { name: 'ASC' },
      }),
    ]);
    const nameMatches = candidates.filter(
      (item) => normalizeCourseNameForMatch(item.name) === normalizedCourseName,
    );
    if (nameMatches.length === 0) {
      return null;
    }

    const normalizedStudyPlanYear = normalizeMatchValue(studyPlan?.year);
    if (normalizedStudyPlanYear) {
      const yearMatches = nameMatches.filter((item) =>
        vcCourseCodeMatchesStudyPlanYear(item.code, normalizedStudyPlanYear),
      );
      if (yearMatches.length === 1) {
        return yearMatches[0].id;
      }
      if (yearMatches.length > 1) {
        return null;
      }
    }

    const normalizedCourseCode = normalizeMatchValue(input.course_code);
    if (normalizedCourseCode) {
      const directCodeMatches = nameMatches.filter(
        (item) => normalizeMatchValue(item.code) === normalizedCourseCode,
      );
      if (directCodeMatches.length === 1) {
        return directCodeMatches[0].id;
      }
      if (directCodeMatches.length > 1) {
        return null;
      }
    }

    return nameMatches.length === 1 ? nameMatches[0].id : null;
  }

  private buildExpectedVcSectionNameForSubsection(
    subsection: PlanningSubsectionEntity,
    section: PlanningSectionEntity,
    offer: PlanningOfferEntity,
    input: { modalities?: CourseModalityEntity[]; campusVcLocations?: PlanningCampusVcLocationMappingEntity[] },
  ) {
    const directNames = this.buildExpectedVcSectionNameCandidatesForSubsection(
      subsection,
      section,
      offer,
      input,
    );
    return directNames[0] ?? null;
  }

  private buildExpectedVcSectionNameCandidatesForSubsection(
    subsection: PlanningSubsectionEntity,
    section: PlanningSectionEntity,
    offer: PlanningOfferEntity,
    input: { modalities?: CourseModalityEntity[]; campusVcLocations?: PlanningCampusVcLocationMappingEntity[] },
  ) {
    const directCandidates = [
      extractSourceSectionNameFromPayload(section.source_payload_json),
      section.external_code,
      section.code,
    ]
      .map((item) => `${item ?? ''}`.trim())
      .filter(Boolean);
    if (directCandidates.length > 0) {
      return uniqueIds(directCandidates);
    }

    const campusVcLocation = (input.campusVcLocations ?? []).find(
      (item) => item.campus_id === offer.campus_id,
    );
    if (!campusVcLocation) {
      return [];
    }
    const modality = (input.modalities ?? []).find((item) => item.id === subsection.course_modality_id) ?? null;
    const modalityCode = `${modality?.code ?? ''}`.toUpperCase();
    const modalityPrefix = modalityCode.includes('VIRTUAL') ? 'V' : 'P';
    const sectionLetters = `${section.code ?? ''}`.trim().toUpperCase().replace(/[^A-Z]/g, '');
    if (!sectionLetters) {
      return [];
    }
    const numericSuffixMatch = `${subsection.code ?? ''}`.trim().toUpperCase().match(/(\d+)$/);
    const numericSuffix =
      numericSuffixMatch && numericSuffixMatch[1] !== '0' ? numericSuffixMatch[1] : '';
    return [`${sectionLetters}${modalityPrefix}${numericSuffix} - ${campusVcLocation.vc_location_code}`];
  }

  private findMatchingVcSections(candidates: VcSectionEntity[], expectedName: string | string[]) {
    const normalizedExpectedNames = uniqueIds(
      (Array.isArray(expectedName) ? expectedName : [expectedName])
        .map((item) => normalizeMatchValue(item))
        .filter(Boolean),
    );
    if (normalizedExpectedNames.length === 0) {
      return [];
    }
    return candidates.filter((item) =>
      normalizedExpectedNames.includes(normalizeMatchValue(item.name)),
    );
  }

  private async resolveOfferIdsFromSubsectionIds(subsectionIds: string[]) {
    const subsections = await this.findManyByIds(this.subsectionsRepo, subsectionIds);
    const sections = await this.findManyByIds(
      this.sectionsRepo,
      uniqueIds(subsections.map((item) => item.planning_section_id)),
    );
    return uniqueIds(sections.map((item) => item.planning_offer_id));
  }

  private async resolveOfferIdsForVcRecalculation(dto: RecalculatePlanningVcMatchesDto) {
    const offers = await this.offersRepo.find({
      where: {
        ...(dto.semester_id ? { semester_id: dto.semester_id } : {}),
        ...(dto.campus_id ? { campus_id: dto.campus_id } : {}),
        ...(dto.faculty_id ? { faculty_id: dto.faculty_id } : {}),
        ...(dto.academic_program_id ? { academic_program_id: dto.academic_program_id } : {}),
        ...(dto.cycle ? { cycle: dto.cycle } : {}),
        ...(dto.study_plan_id ? { study_plan_id: dto.study_plan_id } : {}),
        ...(dto.offer_id ? { id: dto.offer_id } : {}),
      },
    });
    return uniqueIds(offers.map((item) => item.id));
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
        'En cursos teorico practico debe existir al menos un grupo teorico y uno practico.',
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
    let candidate = sectionCode;
    while (existing.has(candidate)) {
      counter += 1;
      candidate = `${sectionCode}${counter}`;
    }
    return candidate;
  }

  async rebuildOfferConflictsAndStatus(offerId: string, actor?: ChangeLogActor | null) {
    await this.rebuildConflictsForSemesterByOffer(offerId, actor);
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

        const teacherA = a.teacher_id ?? subsectionA.responsible_teacher_id;
        const teacherB = b.teacher_id ?? subsectionB.responsible_teacher_id;
        if (teacherA && teacherA === teacherB) {
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
              teacherA,
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
              teacherB,
              undefined,
              {
                other_offer_id: offerA.id,
                other_section_id: sectionA.id,
                other_subsection_id: subsectionA.id,
              },
            ),
          );
        }

        const classroomA = a.classroom_id ?? subsectionA.classroom_id;
        const classroomB = b.classroom_id ?? subsectionB.classroom_id;
        if (classroomA && classroomA === classroomB) {
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
              classroomA,
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
              classroomB,
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
      vc_period_id: rule.vc_period_id ?? null,
      vc_faculty_id: rule.vc_faculty_id ?? null,
      vc_academic_program_id: rule.vc_academic_program_id ?? null,
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
      vc_period_id: offer.vc_period_id ?? null,
      vc_faculty_id: offer.vc_faculty_id ?? null,
      vc_academic_program_id: offer.vc_academic_program_id ?? null,
      vc_course_id: offer.vc_course_id ?? null,
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
      session_type: schedule.session_type,
      teacher_id: schedule.teacher_id ?? null,
      building_id: schedule.building_id ?? null,
      classroom_id: schedule.classroom_id ?? null,
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
        subsectionCode ? `Grupo ${subsectionCode}` : null,
        scheduleLabel ? `Horario ${scheduleLabel}` : null,
      ]);
    }
    if (log.entity_type === 'planning_subsection') {
      return joinReferenceParts([
        offerLabel,
        sectionCode ? `Seccion ${sectionCode}` : null,
        subsectionCode ? `Grupo ${subsectionCode}` : null,
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
        subsectionCode ? `Grupo ${subsectionCode}` : null,
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
        'La vacante del grupo no puede ser mayor a la vacante de la seccion.',
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
        changed_from_ip: actor?.ip_address ?? null,
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
  subsectionKinds: Array<PlanningSubsectionKind | null | undefined>,
  index: number,
) {
  if (sectionProjectedVacancies === null || sectionProjectedVacancies === undefined) {
    return null;
  }
  const total = Math.max(0, Math.trunc(sectionProjectedVacancies));
  const kinds = subsectionKinds.map((item) => item ?? null);
  const count = Math.max(1, Math.trunc(kinds.length));
  if (count === 1) {
    return total;
  }

  const theoryIndexes = kinds
    .map((kind, position) => ({ kind, position }))
    .filter((item) => item.kind === 'THEORY')
    .map((item) => item.position);
  const practiceIndexes = kinds
    .map((kind, position) => ({ kind, position }))
    .filter((item) => item.kind === 'PRACTICE')
    .map((item) => item.position);

  if (
    theoryIndexes.length === 1 &&
    practiceIndexes.length >= 1 &&
    kinds.every((kind) => kind === 'THEORY' || kind === 'PRACTICE')
  ) {
    if (index === theoryIndexes[0]) {
      return total;
    }
    const practicePosition = practiceIndexes.indexOf(index);
    return splitIntegerEvenly(total, practiceIndexes.length, practicePosition);
  }

  return splitIntegerEvenly(total, count, index);
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

function normalizeMatchValue(value: string | null | undefined) {
  return `${value ?? ''}`
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();
}

function normalizeCourseNameForMatch(value: string | null | undefined) {
  const raw = `${value ?? ''}`.trim();
  if (!raw) {
    return '';
  }
  const parts = raw.split(/\s-\s/);
  const courseNameOnly = parts.length > 1 ? parts.slice(1).join(' - ') : raw;
  return normalizeMatchValue(courseNameOnly);
}

function buildCatalogNameMatchVariants(value: string | null | undefined) {
  const raw = `${value ?? ''}`.replace(/\s+/g, ' ').trim();
  if (!raw) {
    return [];
  }
  const variants = new Set<string>([normalizeMatchValue(raw)]);
  const parts = raw.split(/\s-\s/);
  if (parts.length > 1) {
    const head = normalizeMatchValue(parts[0]);
    if (/^[A-Z0-9/_-]+$/.test(head)) {
      variants.add(normalizeMatchValue(parts.slice(1).join(' - ')));
    }
  }
  return [...variants].filter(Boolean);
}

function catalogNameMatches(value: string | null | undefined, variants: string[]) {
  if (!variants.length) {
    return false;
  }
  return buildCatalogNameMatchVariants(value).some((item) => variants.includes(item));
}

function vcCourseCodeMatchesStudyPlanYear(
  vcCourseCode: string | null | undefined,
  normalizedStudyPlanYear: string,
) {
  const normalizedCode = normalizeMatchValue(vcCourseCode);
  if (!normalizedCode || !normalizedStudyPlanYear) {
    return false;
  }
  return (
    normalizedCode === normalizedStudyPlanYear ||
    normalizedCode.startsWith(`${normalizedStudyPlanYear}-`)
  );
}

function resolveVcMatchStatus(
  vcSectionId: string | null | undefined,
  expectedVcSectionName: string | null | undefined,
  vcSection: VcSectionEntity | null | undefined,
) {
  if (vcSectionId && vcSection) {
    return 'MATCHED';
  }
  if (!expectedVcSectionName) {
    return 'UNMATCHED';
  }
  return 'UNMATCHED';
}

function resolveDetailedVcMatchStatus(
  vcSectionId: string | null | undefined,
  currentVcSection: { id?: string | null; course_id?: string | null } | null | undefined,
  currentVcCourseId: string | null | undefined,
  expectedVcSectionName: string | null | undefined,
  suggestedMatchCount: number,
) {
  if (
    vcSectionId &&
    currentVcSection?.id &&
    (!currentVcCourseId || currentVcSection.course_id === currentVcCourseId)
  ) {
    return 'MATCHED';
  }
  if (!expectedVcSectionName) {
    return 'UNMATCHED';
  }
  return suggestedMatchCount > 1 ? 'AMBIGUOUS' : 'UNMATCHED';
}

function shouldIncludeVcMatchRow(row: any) {
  const offer = row.offer;
  if (!offer) {
    return true;
  }

  // 1. Filtrar cursos de prueba (test courses)
  const courseName = (offer.course_name ?? '').toUpperCase();
  const courseCode = (offer.course_code ?? '').toUpperCase();
  if (
    courseName.includes('PRUEBA') ||
    courseName.includes('TEST') ||
    courseCode.includes('PRUEBA') ||
    courseCode.includes('TEST')
  ) {
    return false;
  }

  // 2. Solo mostrar los que literalmente faltan por hacer match o requieren atención
  // Si el estado ya es MATCHED, lo ocultamos para simplificar la vista del usuario
  if (row.match_status === 'MATCHED') {
    return false;
  }

  // Incluimos cualquier otro estado (UNMATCHED, AMBIGUOUS) que requiera intervención manual
  return true;
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
    return ['MIXED', 'MIXED_PRACTICE_THEORY'];
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
    return ['MIXED', 'MIXED_PRACTICE_THEORY'].includes(normalizedKinds[0] ?? '');
  }
  return normalizedKinds.includes('THEORY') && normalizedKinds.includes('PRACTICE');
}

function buildSubsectionKindErrorMessage(
  courseType: string | null | undefined,
  subsectionCount: number,
) {
  if (courseType === 'TEORICO') {
    return 'Este curso solo admite grupos teoricos.';
  }
  if (courseType === 'PRACTICO') {
    return 'Este curso solo admite grupos practicos.';
  }
  if (Math.max(1, subsectionCount) <= 1) {
    return 'Si solo existe un grupo en un curso teorico practico, debe ser teorico practico o practico teorico.';
  }
  return 'En cursos teorico practico solo puedes usar grupos teoricos o practicos.';
}

function resolveDefaultSessionTypeForGroup(kind: PlanningSubsectionKind | null | undefined): PlanningSessionType {
  if (kind === 'THEORY') {
    return 'THEORY';
  }
  if (kind === 'PRACTICE') {
    return 'PRACTICE';
  }
  return 'OTHER';
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

function normalizePlanningSectionCode(value: string | null | undefined) {
  const normalized = `${value ?? ''}`
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '')
    .replace(/[^A-Z0-9_-]/g, '');
  return normalized || null;
}

function buildPlanningSectionExternalCode(
  sectionCode: string | null | undefined,
  modalityCode: string | null | undefined,
  vcLocationCode: string | null | undefined,
) {
  const sectionLetters = `${sectionCode ?? ''}`
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, '');
  const location = `${vcLocationCode ?? ''}`.trim().toUpperCase();
  if (!sectionLetters || !location) {
    return null;
  }
  const normalizedModality = `${modalityCode ?? ''}`.trim().toUpperCase();
  const modalityPrefix = normalizedModality.includes('VIRTUAL') ? 'V' : 'P';
  return `${sectionLetters}${modalityPrefix} - ${location}`;
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
  vc_period_id: string;
  campus_id: string;
  academic_program_id: string;
  study_plan_id: string;
  cycle: number;
}) {
  return [
    input.semester_id,
    input.vc_period_id,
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

function payloadPickString(value: Record<string, unknown> | null | undefined, ...paths: string[]) {
  const payload = asRecord(value);
  if (!payload) {
    return null;
  }
  for (const path of paths) {
    const resolved = path.split('.').reduce<unknown>((acc, key) => {
      if (!acc || typeof acc !== 'object' || Array.isArray(acc)) {
        return undefined;
      }
      return (acc as Record<string, unknown>)[key];
    }, payload);
    const normalized = `${resolved ?? ''}`.trim();
    if (normalized) {
      return normalized;
    }
  }
  return null;
}

function readOfferVcContextMetadata(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  const payload = asRecord(sourcePayloadJson) ?? {};
  const rawContext = asRecord(payload.__vc_context);
  return {
    vc_source: recordString(rawContext, 'vc_source'),
    vc_context_message: recordString(rawContext, 'vc_context_message'),
    source_vc_faculty_id:
      recordString(rawContext, 'source_vc_faculty_id') ?? extractSourceFacultyIdFromPayload(payload),
    source_vc_academic_program_id:
      recordString(rawContext, 'source_vc_academic_program_id') ?? extractSourceAcademicProgramIdFromPayload(payload),
    source_vc_course_id:
      recordString(rawContext, 'source_vc_course_id') ?? extractSourceCourseIdFromPayload(payload),
    manual_vc_course_id: recordString(rawContext, 'manual_vc_course_id'),
  };
}

function readSectionVcContextMetadata(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  const payload = asRecord(sourcePayloadJson) ?? {};
  const rawContext = asRecord(payload.__vc_context);
  const rawManualOverrides = asRecord(rawContext?.manual_subsection_overrides);
  return {
    vc_source: recordString(rawContext, 'vc_source'),
    vc_context_message: recordString(rawContext, 'vc_context_message'),
    source_vc_section_id:
      recordString(rawContext, 'source_vc_section_id') ?? extractSourceSectionIdFromPayload(payload),
    manual_subsection_overrides: Object.fromEntries(
      Object.entries(rawManualOverrides ?? {})
        .map(([key, value]) => [key, emptyToNull(`${value ?? ''}`)])
        .filter(([, value]) => Boolean(value)),
    ) as Record<string, string>,
  };
}

function upsertOfferVcContextMetadata(
  sourcePayloadJson: Record<string, unknown> | null | undefined,
  patch: Record<string, unknown>,
) {
  const payload = asRecord(sourcePayloadJson) ?? {};
  const currentContext = asRecord(payload.__vc_context) ?? {};
  return {
    ...payload,
    __vc_context: {
      ...currentContext,
      ...patch,
    },
  };
}

function upsertSectionVcContextMetadata(
  sourcePayloadJson: Record<string, unknown> | null | undefined,
  patch: Record<string, unknown>,
) {
  const payload = asRecord(sourcePayloadJson) ?? {};
  const currentContext = asRecord(payload.__vc_context) ?? {};
  return {
    ...payload,
    __vc_context: {
      ...currentContext,
      ...patch,
    },
  };
}

function extractSourceCourseIdFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  return payloadPickString(sourcePayloadJson, 'id', 'course.id');
}

function extractSourceAcademicProgramIdFromPayload(
  sourcePayloadJson: Record<string, unknown> | null | undefined,
) {
  return payloadPickString(
    sourcePayloadJson,
    'career.id',
    'careerId',
    'programId',
    'program.id',
    'detail.career.id',
    'detail.careerId',
  );
}

function extractSourceFacultyIdFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  return payloadPickString(
    sourcePayloadJson,
    'career.facultyId',
    'career.faculty.id',
    'facultyId',
    'faculty.id',
    'detail.career.facultyId',
    'detail.career.faculty.id',
  );
}

function extractSourceFacultyNameFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  return payloadPickString(
    sourcePayloadJson,
    'career.faculty.name',
    'career.facultyName',
    'faculty.name',
    'facultyName',
    'detail.career.faculty.name',
    'detail.career.facultyName',
  );
}

function extractSourceAcademicProgramNameFromPayload(
  sourcePayloadJson: Record<string, unknown> | null | undefined,
) {
  return payloadPickString(
    sourcePayloadJson,
    'career.name',
    'careerName',
    'program.name',
    'programName',
    'detail.career.name',
    'detail.careerName',
  );
}

function extractSourceCourseNameFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  return payloadPickString(sourcePayloadJson, 'name', 'course.name', 'courseName');
}

function extractSourceCourseCodeFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  return payloadPickString(sourcePayloadJson, 'code', 'course.code', 'courseCode', 'short_code', 'shortCode');
}

function extractSourceSectionNameFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  return payloadPickString(
    sourcePayloadJson,
    'name',
    'description',
    'code',
    'section.name',
    'section.description',
    'section.code',
  );
}

function extractSourceSectionIdFromPayload(sourcePayloadJson: Record<string, unknown> | null | undefined) {
  return payloadPickString(sourcePayloadJson, 'id', 'section.id', 'sectionId');
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

function looksLikeWorkspaceCodeSearch(value: string | null | undefined) {
  const normalized = normalizeSearchValue(value).replace(/\s+/g, '');
  if (normalized.length < 4) {
    return false;
  }
  return /[a-z]/.test(normalized) && /\d/.test(normalized);
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
