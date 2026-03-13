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
        return {
          id: rule.id,
          semester_id: rule.semester_id,
          campus_id: rule.campus_id,
          faculty_id: rule.faculty_id,
          academic_program_id: rule.academic_program_id,
          study_plan_id: rule.study_plan_id,
          cycle,
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
        };
      })
      .filter((row) =>
        filters.campus_id ? row.campus_ids.includes(filters.campus_id) : true,
      );
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

  async createPlanRule(dto: CreatePlanningCyclePlanRuleDto) {
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
      created_at: now,
      updated_at: now,
    });
    const saved = await this.planRulesRepo.save(entity);
    await this.logChange('planning_cycle_plan_rule', saved.id, 'CREATE', null, saved);
    const offerSync = await this.ensureCycleOffersForRule(saved);
    return {
      ...(await this.enrichPlanRule(saved)),
      created_offer_count: offerSync.created,
      existing_offer_count: offerSync.existing,
      total_offer_count: offerSync.total,
    };
  }

  async updatePlanRule(id: string, dto: UpdatePlanningCyclePlanRuleDto) {
    const current = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
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
    await this.logChange('planning_cycle_plan_rule', id, 'UPDATE', current, saved);
    return this.enrichPlanRule(saved);
  }

  async deletePlanRule(id: string) {
    const current = await this.requireEntity(this.planRulesRepo, id, 'planning_cycle_plan_rule');
    await this.planRulesRepo.delete({ id });
    await this.logChange('planning_cycle_plan_rule', id, 'DELETE', current, null);
    return { deleted: true, id };
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
    const offers = await this.offersRepo.find({
      where: {
        semester_id: filters.semester_id,
        campus_id: filters.campus_id ?? undefined,
      },
    });
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

  async createOffer(dto: CreatePlanningOfferDto) {
    await this.ensureDefaultCatalogs();
    await this.ensureStudyPlanMatchesContext(
      dto.study_plan_id,
      dto.academic_program_id ?? null,
      dto.faculty_id ?? null,
      dto.cycle,
    );
    const existing = await this.offersRepo.findOne({
      where: {
        semester_id: dto.semester_id,
        campus_id: dto.campus_id || undefined,
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
    await this.logChange('planning_offer', saved.id, 'CREATE', null, saved);
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
    return this.buildOfferDetail(offer, context);
  }

  async updateOffer(id: string, dto: UpdatePlanningOfferDto) {
    const current = await this.requireEntity(this.offersRepo, id, 'planning_offer');
    const next = this.offersRepo.create({
      ...current,
      ...dto,
      updated_at: new Date(),
    });
    await this.offersRepo.save(next);
    const saved = await this.requireEntity(this.offersRepo, id, 'planning_offer');
    await this.logChange('planning_offer', id, 'UPDATE', current, saved);
    return this.getOffer(id);
  }

  async createSection(offerId: string, dto: CreatePlanningSectionDto) {
    await this.ensureDefaultCatalogs();
    const offer = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    const now = new Date();
    const subsectionCount = Math.max(1, Math.trunc(dto.subsection_count));
    const modalityId = dto.course_modality_id ?? (await this.defaultCourseModalityId());
    const teacherId = emptyToNull(dto.teacher_id);

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
          responsible_teacher_id: null,
          building_id: null,
          classroom_id: null,
          capacity_snapshot: null,
          shift: null,
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

    await this.logChange('planning_section', saved.id, 'CREATE', null, saved, { offer_id: offerId });
    const createdSubsections = await this.subsectionsRepo.find({
      where: { planning_section_id: saved.id },
      order: { code: 'ASC' },
    });
    for (const subsection of createdSubsections) {
      await this.logChange('planning_subsection', subsection.id, 'CREATE', null, subsection, {
        offer_id: offerId,
        section_id: saved.id,
      });
    }
    await this.refreshOfferStatus(offerId);
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
      this.findManyByIds(this.courseModalitiesRepo, uniqueIds([section.course_modality_id])),
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
          buildings,
          classrooms,
          conflicts,
        }),
      ),
    };
  }

  async updateSection(id: string, dto: UpdatePlanningSectionDto) {
    const current = await this.requireEntity(this.sectionsRepo, id, 'planning_section');
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
      updated_at: new Date(),
    });
    await this.sectionsRepo.save(next);
    const saved = await this.requireEntity(this.sectionsRepo, id, 'planning_section');
    await this.logChange('planning_section', id, 'UPDATE', current, saved);
    await this.refreshOfferStatus(saved.planning_offer_id);
    return this.getSection(id);
  }

  async createSubsection(sectionId: string, dto: CreatePlanningSubsectionDto) {
    const section = await this.requireEntity(this.sectionsRepo, sectionId, 'planning_section');
    const offer = await this.requireEntity(this.offersRepo, section.planning_offer_id, 'planning_offer');
    const code = dto.code?.trim().toUpperCase() || (await this.nextSubsectionCode(sectionId, section.code));
    const existing = await this.subsectionsRepo.findOne({
      where: { planning_section_id: sectionId, code },
    });
    if (existing) {
      throw new BadRequestException(`La subseccion ${code} ya existe.`);
    }
    const classroom = dto.classroom_id
      ? await this.classroomsRepo.findOne({ where: { id: dto.classroom_id } })
      : null;
    const assigned = resolveAssignedHours(dto.kind, offer);
    const now = new Date();
    const subsection = this.subsectionsRepo.create({
      id: dto.id ?? newId(),
      planning_section_id: sectionId,
      code,
      kind: dto.kind,
      responsible_teacher_id: emptyToNull(dto.responsible_teacher_id),
      building_id: emptyToNull(dto.building_id) ?? classroom?.building_id ?? null,
      classroom_id: emptyToNull(dto.classroom_id),
      capacity_snapshot: dto.capacity_snapshot ?? classroom?.capacity ?? null,
      shift: emptyToNull(dto.shift),
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
    await this.logChange('planning_subsection', saved.id, 'CREATE', null, saved, { section_id: sectionId });
    await this.refreshOfferStatus(section.planning_offer_id);
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
    const [schedules, conflicts, teachers, buildings, classrooms] = await Promise.all([
      this.schedulesRepo.find({
        where: { planning_subsection_id: id },
        order: { day_of_week: 'ASC', start_time: 'ASC' },
      }),
      this.conflictsRepo.find({
        where: { planning_subsection_id: id },
        order: { detected_at: 'DESC' },
      }),
      this.findManyByIds(this.teachersRepo, uniqueIds([subsection.responsible_teacher_id, section.teacher_id])),
      this.findManyByIds(this.buildingsRepo, uniqueIds([subsection.building_id])),
      this.findManyByIds(this.classroomsRepo, uniqueIds([subsection.classroom_id])),
    ]);

    return this.buildSubsectionDetail(subsection, {
      section,
      offer,
      schedules,
      conflicts,
      teachers,
      buildings,
      classrooms,
    });
  }

  async updateSubsection(id: string, dto: UpdatePlanningSubsectionDto) {
    const current = await this.requireEntity(this.subsectionsRepo, id, 'planning_subsection');
    const section = await this.requireEntity(
      this.sectionsRepo,
      current.planning_section_id,
      'planning_section',
    );
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
    const next = this.subsectionsRepo.create({
      ...current,
      ...dto,
      code: nextCode,
      responsible_teacher_id:
        dto.responsible_teacher_id !== undefined
          ? emptyToNull(dto.responsible_teacher_id)
          : current.responsible_teacher_id,
      building_id: classroom?.building_id ?? buildingId ?? null,
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
      assigned_total_hours: roundToTwo(
        numberValue(dto.assigned_theoretical_hours, current.assigned_theoretical_hours) +
          numberValue(dto.assigned_practical_hours, current.assigned_practical_hours) +
          numberValue(dto.assigned_virtual_hours, current.assigned_virtual_hours) +
          numberValue(dto.assigned_seminar_hours, current.assigned_seminar_hours),
      ),
      updated_at: new Date(),
    });
    await this.subsectionsRepo.save(next);
    const saved = await this.requireEntity(this.subsectionsRepo, id, 'planning_subsection');
    await this.logChange('planning_subsection', id, 'UPDATE', current, saved);
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id);
    return this.getSubsection(id);
  }

  async createSubsectionSchedule(subsectionId: string, dto: CreatePlanningSubsectionScheduleDto) {
    const subsection = await this.requireEntity(
      this.subsectionsRepo,
      subsectionId,
      'planning_subsection',
    );
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
    await this.logChange('planning_subsection_schedule', saved.id, 'CREATE', null, saved, {
      subsection_id: subsectionId,
    });
    const section = await this.requireEntity(
      this.sectionsRepo,
      subsection.planning_section_id,
      'planning_section',
    );
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id);
    return this.getSubsection(subsectionId);
  }

  async updateSubsectionSchedule(id: string, dto: UpdatePlanningSubsectionScheduleDto) {
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
    await this.logChange('planning_subsection_schedule', id, 'UPDATE', current, saved);
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
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id);
    return this.getSubsection(subsection.id);
  }

  async deleteSubsectionSchedule(id: string) {
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
    await this.schedulesRepo.delete({ id });
    await this.logChange('planning_subsection_schedule', id, 'DELETE', current, null);
    await this.rebuildConflictsForSemesterByOffer(section.planning_offer_id);
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
    return conflicts;
  }

  async listChangeLog(entityType?: string, entityId?: string) {
    const logs = await this.changeLogsRepo.find({
      where: {
        ...(entityType ? { entity_type: entityType } : {}),
        ...(entityId ? { entity_id: entityId } : {}),
      },
      order: { changed_at: 'DESC' },
      take: 200,
    });
    return logs;
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

  private async ensureCycleOffersForRule(rule: PlanningCyclePlanRuleEntity) {
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
      await this.logChange('planning_offer', offer.id, 'CREATE', null, offer, {
        source: 'create_plan_rule',
        plan_rule_id: rule.id,
      });
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

  private buildSubsectionDetail(
    subsection: PlanningSubsectionEntity,
    input: {
      section?: PlanningSectionEntity;
      offer?: PlanningOfferEntity;
      schedules: PlanningSubsectionScheduleEntity[];
      conflicts: PlanningScheduleConflictV2Entity[];
      teachers: TeacherEntity[];
      buildings: BuildingEntity[];
      classrooms: ClassroomEntity[];
    },
  ) {
    const teacherMap = mapById(input.teachers);
    const buildingMap = mapById(input.buildings);
    const classroomMap = mapById(input.classrooms);
    const ownSchedules = input.schedules.filter((item) => item.planning_subsection_id === subsection.id);
    return {
      ...subsection,
      section: input.section ?? null,
      offer: input.offer ?? null,
      responsible_teacher: teacherMap.get(subsection.responsible_teacher_id ?? '') ?? null,
      building: buildingMap.get(subsection.building_id ?? '') ?? null,
      classroom: classroomMap.get(subsection.classroom_id ?? '') ?? null,
      schedules: ownSchedules,
      conflicts: input.conflicts.filter((item) => item.planning_subsection_id === subsection.id),
    };
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

  private async rebuildConflictsForSemesterByOffer(offerId: string) {
    const offer = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    const offers = await this.offersRepo.find({ where: { semester_id: offer.semester_id } });
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
    await this.conflictsRepo.delete({ semester_id: offer.semester_id });
    const conflicts = this.calculateConflictsForDataset(offer.semester_id, offers, sections, subsections, schedules);
    if (conflicts.length) {
      await this.conflictsRepo.save(conflicts);
    }
    for (const item of offers) {
      await this.refreshOfferStatus(item.id, conflicts);
    }
  }

  private async refreshOfferStatus(offerId: string, cachedConflicts?: PlanningScheduleConflictV2Entity[]) {
    const offer = await this.requireEntity(this.offersRepo, offerId, 'planning_offer');
    const sectionCount = await this.sectionsRepo.count({ where: { planning_offer_id: offerId } });
    const conflicts =
      cachedConflicts?.filter((item) => item.planning_offer_id === offerId) ??
      (await this.conflictsRepo.find({ where: { planning_offer_id: offerId } }));
    let status: PlanningOfferStatus = 'DRAFT';
    if (conflicts.length > 0) {
      status = 'OBSERVED';
    } else if (sectionCount > 0) {
      status = 'ACTIVE';
    }
    if (status !== offer.status) {
      const before = { ...offer };
      offer.status = status;
      offer.updated_at = new Date();
      await this.offersRepo.save(offer);
      await this.logChange('planning_offer', offer.id, 'UPDATE', before, offer, {
        reason: 'status_refresh',
      });
    }
  }

  private calculateConflictsForDataset(
    semesterId: string,
    offers: PlanningOfferEntity[],
    sections: PlanningSectionEntity[],
    subsections: PlanningSubsectionEntity[],
    schedules: PlanningSubsectionScheduleEntity[],
  ) {
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
              },
            ),
          );
        }
      }
    }

    return dedupePlanningConflicts(conflicts);
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

  private async logChange(
    entityType: string,
    entityId: string,
    action: 'CREATE' | 'UPDATE' | 'DELETE',
    beforeValue: unknown,
    afterValue: unknown,
    context?: Record<string, unknown>,
  ) {
    await this.changeLogsRepo.save(
      this.changeLogsRepo.create({
        id: newId(),
        entity_type: entityType,
        entity_id: entityId,
        action,
        before_json: toLogJson(beforeValue),
        after_json: toLogJson(afterValue),
        changed_by: 'SYSTEM',
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
  const minutes = Math.max(0, toMinutes(end) - toMinutes(start));
  if (minutes <= 0) {
    throw new BadRequestException('La hora fin debe ser mayor que la hora inicio.');
  }
  return minutes;
}

function toMinutes(value: string) {
  const [hours = '0', minutes = '0'] = value.split(':');
  return Number(hours) * 60 + Number(minutes);
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
