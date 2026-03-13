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
export class PlanningController {
  constructor(
    private readonly planningService: PlanningService,
    private readonly planningManualService: PlanningManualService,
  ) {}

  @Get('catalog/filters')
  listCatalogFilters() {
    return this.planningManualService.listCatalogFilters();
  }

  @Get('plan-rules')
  listPlanRules(
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('academic_program_id') academicProgramId?: string,
  ) {
    return this.planningManualService.listPlanRules(semesterId, campusId, academicProgramId);
  }

  @Get('configured-cycles')
  listConfiguredCycles(
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('faculty_id') facultyId?: string,
    @Query('academic_program_id') academicProgramId?: string,
  ) {
    return this.planningManualService.listConfiguredCycles({
      semester_id: semesterId,
      campus_id: campusId,
      faculty_id: facultyId,
      academic_program_id: academicProgramId,
    });
  }

  @Post('plan-rules')
  createPlanRule(@Body() dto: CreatePlanningCyclePlanRuleDto) {
    return this.planningManualService.createPlanRule(dto);
  }

  @Patch('plan-rules/:id')
  updatePlanRule(@Param('id') id: string, @Body() dto: UpdatePlanningCyclePlanRuleDto) {
    return this.planningManualService.updatePlanRule(id, dto);
  }

  @Delete('plan-rules/:id')
  deletePlanRule(@Param('id') id: string) {
    return this.planningManualService.deletePlanRule(id);
  }

  @Get('course-candidates')
  listCourseCandidates(
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('faculty_id') facultyId?: string,
    @Query('academic_program_id') academicProgramId?: string,
    @Query('cycle') cycle?: string,
    @Query('study_plan_id') studyPlanId?: string,
  ) {
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
  createOffer(@Body() dto: CreatePlanningOfferDto) {
    return this.planningManualService.createOffer(dto);
  }

  @Get('offers')
  listOffers(
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('faculty_id') facultyId?: string,
    @Query('academic_program_id') academicProgramId?: string,
    @Query('cycle') cycle?: string,
    @Query('study_plan_id') studyPlanId?: string,
  ) {
    return this.planningManualService.listOffers(
      semesterId,
      campusId,
      facultyId,
      academicProgramId,
      cycle ? Number(cycle) : undefined,
      studyPlanId,
    );
  }

  @Get('offers/:id')
  getOffer(@Param('id') id: string) {
    return this.planningManualService.getOffer(id);
  }

  @Patch('offers/:id')
  updateOffer(@Param('id') id: string, @Body() dto: UpdatePlanningOfferDto) {
    return this.planningManualService.updateOffer(id, dto);
  }

  @Post('offers/:id/sections')
  createSection(@Param('id') id: string, @Body() dto: CreatePlanningSectionDto) {
    return this.planningManualService.createSection(id, dto);
  }

  @Get('sections/:id')
  getSection(@Param('id') id: string) {
    return this.planningManualService.getSection(id);
  }

  @Patch('sections/:id')
  updateSection(@Param('id') id: string, @Body() dto: UpdatePlanningSectionDto) {
    return this.planningManualService.updateSection(id, dto);
  }

  @Post('sections/:id/subsections')
  createSubsection(@Param('id') id: string, @Body() dto: CreatePlanningSubsectionDto) {
    return this.planningManualService.createSubsection(id, dto);
  }

  @Get('subsections/:id')
  getSubsection(@Param('id') id: string) {
    return this.planningManualService.getSubsection(id);
  }

  @Patch('subsections/:id')
  updateSubsection(@Param('id') id: string, @Body() dto: UpdatePlanningSubsectionDto) {
    return this.planningManualService.updateSubsection(id, dto);
  }

  @Post('subsections/:id/schedules')
  createSubsectionSchedule(
    @Param('id') id: string,
    @Body() dto: CreatePlanningSubsectionScheduleDto,
  ) {
    return this.planningManualService.createSubsectionSchedule(id, dto);
  }

  @Patch('subsection-schedules/:id')
  updateSubsectionSchedule(
    @Param('id') id: string,
    @Body() dto: UpdatePlanningSubsectionScheduleDto,
  ) {
    return this.planningManualService.updateSubsectionSchedule(id, dto);
  }

  @Delete('subsection-schedules/:id')
  deleteSubsectionSchedule(@Param('id') id: string) {
    return this.planningManualService.deleteSubsectionSchedule(id);
  }

  @Get('conflicts')
  listConflictsManual(
    @Query('semester_id') semesterId?: string,
    @Query('offer_id') offerId?: string,
  ) {
    return this.planningManualService.listConflicts(semesterId, offerId);
  }

  @Get('change-log')
  listChangeLog(
    @Query('entity_type') entityType?: string,
    @Query('entity_id') entityId?: string,
  ) {
    return this.planningManualService.listChangeLog(entityType, entityId);
  }

  @Get('class-offerings')
  listClassOfferings(@Query('semester_id') semesterId?: string) {
    return this.planningService.listClassOfferings(semesterId);
  }

  @Get('workspace')
  listWorkspace(
    @Query('semester_id') semesterId?: string,
    @Query('campus_id') campusId?: string,
    @Query('academic_program_id') academicProgramId?: string,
    @Query('study_plan_id') studyPlanId?: string,
    @Query('delivery_modality_id') deliveryModalityId?: string,
    @Query('shift_id') shiftId?: string,
    @Query('search') search?: string,
  ) {
    return this.planningService.listWorkspace({
      semesterId,
      campusId,
      academicProgramId,
      studyPlanId,
      deliveryModalityId,
      shiftId,
      search,
    });
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
  getClassOffering(@Param('id') id: string) {
    return this.planningService.getClassOffering(id);
  }

  @Post('class-offerings')
  createClassOffering(@Body() dto: CreateClassOfferingDto) {
    return this.planningService.createClassOffering(dto);
  }

  @Patch('class-offerings/:id')
  updateClassOffering(@Param('id') id: string, @Body() dto: UpdateClassOfferingDto) {
    return this.planningService.updateClassOffering(id, dto);
  }

  @Delete('class-offerings/:id')
  deleteClassOffering(@Param('id') id: string) {
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
  listScheduleConflicts(@Query('semester_id') semesterId?: string) {
    return this.planningService.listConflicts(semesterId);
  }

  @Post('schedule-conflicts/detect/:semesterId')
  detectScheduleConflicts(@Param('semesterId') semesterId: string) {
    return this.planningService.detectConflicts(semesterId);
  }
}
