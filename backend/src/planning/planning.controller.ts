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
import { CurrentAuthUser } from '../auth/current-auth-user.decorator';
import { AuthService } from '../auth/auth.service';
import { RequirePermissions } from '../auth/permissions.decorator';
import type { AuthenticatedRequestUser } from '../auth/auth.service';
import {
  BulkAssignClassroomDto,
  BulkAssignTeacherDto,
  BulkDuplicateDto,
  CreatePlanningCyclePlanRuleDto,
  CreatePlanningOfferDto,
  CreatePlanningSectionDto,
  CreatePlanningSubsectionDto,
  CreatePlanningSubsectionScheduleDto,
  CreateClassGroupDto,
  CreateClassGroupTeacherDto,
  CreateClassMeetingDto,
  CreateClassOfferingDto,
  CreateClassTeacherDto,
  CreateCourseSectionHourRequirementDto,
  UpdatePlanningCyclePlanRuleDto,
  UpdatePlanningOfferDto,
  UpdatePlanningSectionDto,
  UpdatePlanningSubsectionDto,
  UpdatePlanningSubsectionScheduleDto,
  UpdateClassGroupDto,
  UpdateClassGroupTeacherDto,
  UpdateClassMeetingDto,
  UpdateClassOfferingDto,
  UpdateClassTeacherDto,
  UpdateCourseSectionHourRequirementDto,
  UpdatePlanningWorkspaceRowDto,
} from './dto/planning.dto';
import { PlanningManualService } from './planning-manual.service';
import { PlanningService } from './planning.service';

@Controller('planning')
@RequirePermissions(WINDOW_PERMISSIONS.PLANNING)
export class PlanningController {
  constructor(
    private readonly planningService: PlanningService,
    private readonly planningManualService: PlanningManualService,
    private readonly authService: AuthService,
  ) {}

  @Get('catalog/filters')
  async listCatalogFilters(@CurrentAuthUser() authUser: AuthenticatedRequestUser) {
    const result = await this.planningManualService.listCatalogFilters();
    if (authUser.is_global) {
      return result;
    }

    const faculties = result.faculties.filter((item: any) =>
      this.matchesScope(authUser, item.faculty_id ?? item.id ?? null, item.academic_program_id ?? null),
    );
    const academicPrograms = result.academic_programs.filter((item: any) =>
      this.matchesScope(authUser, item.faculty_id ?? null, item.id ?? item.academic_program_id ?? null),
    );
    const studyPlans = result.study_plans.filter((item: any) =>
      this.matchesScope(authUser, item.faculty_id ?? null, item.academic_program_id ?? null),
    );
    const planRules = result.plan_rules.filter((item: any) =>
      this.matchesScope(authUser, item.faculty_id ?? null, item.academic_program_id ?? null),
    );

    return {
      ...result,
      faculties,
      academic_programs: academicPrograms,
      study_plans: studyPlans,
      plan_rules: planRules,
    };
  }

  @Get('plan-rules')
  listPlanRules(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('academic_program_id') academicProgramId?: string,
  ) {
    if (academicProgramId) {
      this.authService.assertScopeAccess(authUser, null, academicProgramId);
    }
    return this.planningManualService
      .listPlanRules(semesterId, campusId, academicProgramId)
      .then((rows) =>
        this.authService.filterByScope(authUser, rows, (item: any) => ({
          faculty_id: item.faculty_id ?? null,
          academic_program_id: item.academic_program_id ?? null,
        })),
      );
  }

  @Get('configured-cycles')
  listConfiguredCycles(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('faculty_id') facultyId?: string,
    @Query('academic_program_id') academicProgramId?: string,
  ) {
    if (facultyId || academicProgramId) {
      this.authService.assertScopeAccess(authUser, facultyId, academicProgramId);
    }
    return this.planningManualService.listConfiguredCycles({
      semester_id: semesterId,
      campus_id: campusId,
      faculty_id: facultyId,
      academic_program_id: academicProgramId,
    }).then((rows) =>
      this.authService.filterByScope(authUser, rows, (item: any) => ({
        faculty_id: item.faculty_id ?? null,
        academic_program_id: item.academic_program_id ?? null,
      })),
    );
  }

  @Post('plan-rules')
  createPlanRule(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Body() dto: CreatePlanningCyclePlanRuleDto,
  ) {
    this.authService.assertScopeAccess(authUser, dto.faculty_id, dto.academic_program_id);
    return this.planningManualService.createPlanRule(dto);
  }

  @Patch('plan-rules/:id')
  async updatePlanRule(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: UpdatePlanningCyclePlanRuleDto,
  ) {
    const current = await this.planningManualService.listPlanRules();
    const rule = current.find((item) => item.id === id);
    if (rule) {
      this.authService.assertScopeAccess(authUser, rule.faculty_id ?? null, rule.academic_program_id ?? null);
    }
    if (dto.faculty_id || dto.academic_program_id) {
      this.authService.assertScopeAccess(authUser, dto.faculty_id, dto.academic_program_id);
    }
    return this.planningManualService.updatePlanRule(id, dto);
  }

  @Delete('plan-rules/:id')
  async deletePlanRule(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
  ) {
    const current = await this.planningManualService.listPlanRules();
    const rule = current.find((item) => item.id === id);
    if (rule) {
      this.authService.assertScopeAccess(authUser, rule.faculty_id ?? null, rule.academic_program_id ?? null);
    }
    return this.planningManualService.deletePlanRule(id);
  }

  @Get('course-candidates')
  listCourseCandidates(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('faculty_id') facultyId?: string,
    @Query('academic_program_id') academicProgramId?: string,
    @Query('cycle') cycle?: string,
    @Query('study_plan_id') studyPlanId?: string,
  ) {
    if (facultyId || academicProgramId) {
      this.authService.assertScopeAccess(authUser, facultyId, academicProgramId);
    }
    return this.planningManualService.listCourseCandidates({
      semester_id: semesterId,
      campus_id: campusId,
      faculty_id: facultyId,
      academic_program_id: academicProgramId,
      cycle: cycle ? Number(cycle) : undefined,
      study_plan_id: studyPlanId,
    });
  }

  @Post('offers')
  createOffer(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Body() dto: CreatePlanningOfferDto,
  ) {
    this.authService.assertScopeAccess(authUser, dto.faculty_id, dto.academic_program_id);
    return this.planningManualService.createOffer(dto);
  }

  @Get('offers')
  listOffers(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('faculty_id') facultyId?: string,
    @Query('academic_program_id') academicProgramId?: string,
    @Query('cycle') cycle?: string,
    @Query('study_plan_id') studyPlanId?: string,
  ) {
    if (facultyId || academicProgramId) {
      this.authService.assertScopeAccess(authUser, facultyId, academicProgramId);
    }
    return this.planningManualService.listOffers(
      semesterId,
      campusId,
      facultyId,
      academicProgramId,
      cycle ? Number(cycle) : undefined,
      studyPlanId,
    ).then((rows) =>
      this.authService.filterByScope(authUser, rows, (item: any) => ({
        faculty_id: item.faculty_id ?? null,
        academic_program_id: item.academic_program_id ?? null,
      })),
    );
  }

  @Get('offers/:id')
  async getOffer(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
  ) {
    const offer = await this.planningManualService.getOffer(id);
    this.authService.assertScopeAccess(authUser, offer.faculty_id ?? null, offer.academic_program_id ?? null);
    return offer;
  }

  @Patch('offers/:id')
  async updateOffer(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: UpdatePlanningOfferDto,
  ) {
    const offer = await this.planningManualService.getOffer(id);
    this.authService.assertScopeAccess(authUser, offer.faculty_id ?? null, offer.academic_program_id ?? null);
    return this.planningManualService.updateOffer(id, dto);
  }

  @Post('offers/:id/sections')
  async createSection(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: CreatePlanningSectionDto,
  ) {
    const offer = await this.planningManualService.getOffer(id);
    this.authService.assertScopeAccess(authUser, offer.faculty_id ?? null, offer.academic_program_id ?? null);
    return this.planningManualService.createSection(id, dto);
  }

  @Get('sections/:id')
  async getSection(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
  ) {
    const section = await this.planningManualService.getSection(id);
    this.authService.assertScopeAccess(
      authUser,
      section.offer?.faculty_id ?? null,
      section.offer?.academic_program_id ?? null,
    );
    return section;
  }

  @Patch('sections/:id')
  async updateSection(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: UpdatePlanningSectionDto,
  ) {
    const section = await this.planningManualService.getSection(id);
    this.authService.assertScopeAccess(
      authUser,
      section.offer?.faculty_id ?? null,
      section.offer?.academic_program_id ?? null,
    );
    return this.planningManualService.updateSection(id, dto);
  }

  @Post('sections/:id/subsections')
  async createSubsection(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: CreatePlanningSubsectionDto,
  ) {
    const section = await this.planningManualService.getSection(id);
    this.authService.assertScopeAccess(
      authUser,
      section.offer?.faculty_id ?? null,
      section.offer?.academic_program_id ?? null,
    );
    return this.planningManualService.createSubsection(id, dto);
  }

  @Get('subsections/:id')
  async getSubsection(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
  ) {
    const subsection = await this.planningManualService.getSubsection(id);
    this.authService.assertScopeAccess(
      authUser,
      subsection.offer?.faculty_id ?? null,
      subsection.offer?.academic_program_id ?? null,
    );
    return subsection;
  }

  @Patch('subsections/:id')
  async updateSubsection(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: UpdatePlanningSubsectionDto,
  ) {
    const subsection = await this.planningManualService.getSubsection(id);
    this.authService.assertScopeAccess(
      authUser,
      subsection.offer?.faculty_id ?? null,
      subsection.offer?.academic_program_id ?? null,
    );
    return this.planningManualService.updateSubsection(id, dto);
  }

  @Post('subsections/:id/schedules')
  createSubsectionSchedule(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: CreatePlanningSubsectionScheduleDto,
  ) {
    return this.planningManualService.getSubsection(id).then((subsection) => {
      this.authService.assertScopeAccess(
        authUser,
        subsection.offer?.faculty_id ?? null,
        subsection.offer?.academic_program_id ?? null,
      );
      return this.planningManualService.createSubsectionSchedule(id, dto);
    });
  }

  @Patch('subsection-schedules/:id')
  updateSubsectionSchedule(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: UpdatePlanningSubsectionScheduleDto,
  ) {
    return this.planningManualService.getSubsectionBySchedule(id).then((schedule) => {
      this.authService.assertScopeAccess(
        authUser,
        schedule.offer?.faculty_id ?? null,
        schedule.offer?.academic_program_id ?? null,
      );
      return this.planningManualService.updateSubsectionSchedule(id, dto);
    });
  }

  @Delete('subsection-schedules/:id')
  deleteSubsectionSchedule(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
  ) {
    return this.planningManualService.getSubsectionBySchedule(id).then((schedule) => {
      this.authService.assertScopeAccess(
        authUser,
        schedule.offer?.faculty_id ?? null,
        schedule.offer?.academic_program_id ?? null,
      );
      return this.planningManualService.deleteSubsectionSchedule(id);
    });
  }

  @Get('conflicts')
  @RequirePermissions(WINDOW_PERMISSIONS.CONFLICTS)
  listConflictsManual(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
    @Query('offer_id') offerId?: string,
  ) {
    return this.planningManualService.listConflicts(semesterId, offerId).then((rows) =>
      this.authService.filterByScope(authUser, rows, (item: any) => ({
        faculty_id: item.offer?.faculty_id ?? null,
        academic_program_id: item.offer?.academic_program_id ?? null,
      })),
    );
  }

  @Get('change-log')
  listChangeLog(
    @Query('entity_type') entityType?: string,
    @Query('entity_id') entityId?: string,
  ) {
    return this.planningManualService.listChangeLog(entityType, entityId);
  }

  @Get('class-offerings')
  listClassOfferings(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
  ) {
    return this.planningService.listClassOfferings(semesterId).then((rows) =>
      this.authService.filterByScope(authUser, rows, (item: any) => ({
        faculty_id: null,
        academic_program_id: item.academic_program_id ?? null,
      })),
    );
  }

  @Get('workspace')
  listWorkspace(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('academic_program_id') academicProgramId?: string,
    @Query('study_plan_id') studyPlanId?: string,
    @Query('delivery_modality_id') deliveryModalityId?: string,
    @Query('shift_id') shiftId?: string,
    @Query('search') search?: string,
  ) {
    if (academicProgramId) {
      this.authService.assertScopeAccess(authUser, null, academicProgramId);
    }
    return this.planningService.listWorkspace({
      semesterId,
      campusId,
      academicProgramId,
      studyPlanId,
      deliveryModalityId,
      shiftId,
      search,
    }).then((result) => ({
      ...result,
      summaries: this.authService.filterByScope(authUser, result.summaries, (item: any) => ({
        faculty_id: null,
        academic_program_id: item.academic_program_id ?? null,
      })),
      rows: this.authService.filterByScope(authUser, result.rows, (item: any) => ({
        faculty_id: null,
        academic_program_id: item.academic_program_id ?? null,
      })),
    }));
  }

  @Patch('workspace/rows/:rowId')
  updateWorkspaceRow(@Param('rowId') rowId: string, @Body() dto: UpdatePlanningWorkspaceRowDto) {
    return this.planningService.updateWorkspaceRow(rowId, dto);
  }

  @Post('workspace/bulk-assign-teacher')
  bulkAssignTeacher(@Body() dto: BulkAssignTeacherDto) {
    return this.planningService.bulkAssignTeacher(dto);
  }

  @Post('workspace/bulk-assign-classroom')
  bulkAssignClassroom(@Body() dto: BulkAssignClassroomDto) {
    return this.planningService.bulkAssignClassroom(dto);
  }

  @Post('workspace/bulk-duplicate')
  bulkDuplicate(@Body() dto: BulkDuplicateDto) {
    return this.planningService.bulkDuplicate(dto);
  }

  @Get('class-offerings/:id')
  async getClassOffering(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
  ) {
    const offering = await this.planningService.getClassOffering(id);
    this.authService.assertScopeAccess(authUser, null, offering.academic_program_id ?? null);
    return offering;
  }

  @Post('class-offerings')
  createClassOffering(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Body() dto: CreateClassOfferingDto,
  ) {
    this.authService.assertScopeAccess(authUser, null, dto.academic_program_id);
    return this.planningService.createClassOffering(dto);
  }

  @Patch('class-offerings/:id')
  async updateClassOffering(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
    @Body() dto: UpdateClassOfferingDto,
  ) {
    const offering = await this.planningService.getClassOffering(id);
    this.authService.assertScopeAccess(
      authUser,
      null,
      dto.academic_program_id ?? offering.academic_program_id ?? null,
    );
    return this.planningService.updateClassOffering(id, dto);
  }

  @Delete('class-offerings/:id')
  async deleteClassOffering(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Param('id') id: string,
  ) {
    const offering = await this.planningService.getClassOffering(id);
    this.authService.assertScopeAccess(authUser, null, offering.academic_program_id ?? null);
    return this.planningService.deleteClassOffering(id);
  }

  @Get('class-groups')
  listClassGroups(@Query('class_offering_id') classOfferingId?: string) {
    return this.planningService.listClassGroups(classOfferingId);
  }

  @Post('class-groups')
  createClassGroup(@Body() dto: CreateClassGroupDto) {
    return this.planningService.createClassGroup(dto);
  }

  @Patch('class-groups/:id')
  updateClassGroup(@Param('id') id: string, @Body() dto: UpdateClassGroupDto) {
    return this.planningService.updateClassGroup(id, dto);
  }

  @Delete('class-groups/:id')
  deleteClassGroup(@Param('id') id: string) {
    return this.planningService.deleteClassGroup(id);
  }

  @Get('class-meetings')
  listClassMeetings(@Query('class_offering_id') classOfferingId?: string) {
    return this.planningService.listClassMeetings(classOfferingId);
  }

  @Post('class-meetings')
  createClassMeeting(@Body() dto: CreateClassMeetingDto) {
    return this.planningService.createClassMeeting(dto);
  }

  @Patch('class-meetings/:id')
  updateClassMeeting(@Param('id') id: string, @Body() dto: UpdateClassMeetingDto) {
    return this.planningService.updateClassMeeting(id, dto);
  }

  @Delete('class-meetings/:id')
  deleteClassMeeting(@Param('id') id: string) {
    return this.planningService.deleteClassMeeting(id);
  }

  @Get('class-teachers')
  listClassTeachers(@Query('class_offering_id') classOfferingId?: string) {
    return this.planningService.listClassTeachers(classOfferingId);
  }

  @Post('class-teachers')
  createClassTeacher(@Body() dto: CreateClassTeacherDto) {
    return this.planningService.createClassTeacher(dto);
  }

  @Patch('class-teachers/:id')
  updateClassTeacher(@Param('id') id: string, @Body() dto: UpdateClassTeacherDto) {
    return this.planningService.updateClassTeacher(id, dto);
  }

  @Delete('class-teachers/:id')
  deleteClassTeacher(@Param('id') id: string) {
    return this.planningService.deleteClassTeacher(id);
  }

  @Get('class-group-teachers')
  listClassGroupTeachers(@Query('class_group_id') classGroupId?: string) {
    return this.planningService.listClassGroupTeachers(classGroupId);
  }

  @Post('class-group-teachers')
  createClassGroupTeacher(@Body() dto: CreateClassGroupTeacherDto) {
    return this.planningService.createClassGroupTeacher(dto);
  }

  @Patch('class-group-teachers/:id')
  updateClassGroupTeacher(@Param('id') id: string, @Body() dto: UpdateClassGroupTeacherDto) {
    return this.planningService.updateClassGroupTeacher(id, dto);
  }

  @Delete('class-group-teachers/:id')
  deleteClassGroupTeacher(@Param('id') id: string) {
    return this.planningService.deleteClassGroupTeacher(id);
  }

  @Get('course-section-hour-requirements')
  listHourRequirements(@Query('course_section_id') courseSectionId?: string) {
    return this.planningService.listHourRequirements(courseSectionId);
  }

  @Post('course-section-hour-requirements')
  createHourRequirement(@Body() dto: CreateCourseSectionHourRequirementDto) {
    return this.planningService.createHourRequirement(dto);
  }

  @Patch('course-section-hour-requirements/:id')
  updateHourRequirement(
    @Param('id') id: string,
    @Body() dto: UpdateCourseSectionHourRequirementDto,
  ) {
    return this.planningService.updateHourRequirement(id, dto);
  }

  @Delete('course-section-hour-requirements/:id')
  deleteHourRequirement(@Param('id') id: string) {
    return this.planningService.deleteHourRequirement(id);
  }

  @Post('hours-validation/:classOfferingId')
  validateHours(@Param('classOfferingId') classOfferingId: string) {
    return this.planningService.validateHours(classOfferingId);
  }

  @Get('schedule-conflicts')
  @RequirePermissions(WINDOW_PERMISSIONS.CONFLICTS)
  listScheduleConflicts(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Query('semester_id') semesterId?: string,
  ) {
    return this.planningService.listConflicts(semesterId).then((rows) =>
      this.authService.filterByScope(authUser, rows, (item: any) => ({
        faculty_id: null,
        academic_program_id: item.academic_program_id ?? null,
      })),
    );
  }

  @Post('schedule-conflicts/detect/:semesterId')
  @RequirePermissions(WINDOW_PERMISSIONS.CONFLICTS)
  detectScheduleConflicts(@Param('semesterId') semesterId: string) {
    return this.planningService.detectConflicts(semesterId);
  }

  private matchesScope(
    authUser: AuthenticatedRequestUser,
    facultyId?: string | null,
    academicProgramId?: string | null,
  ) {
    try {
      this.authService.assertScopeAccess(authUser, facultyId, academicProgramId);
      return true;
    } catch {
      return false;
    }
  }
}
