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
import { PlanningService } from './planning.service';

@Controller('planning')
export class PlanningController {
  constructor(private readonly planningService: PlanningService) {}

  @Get('class-offerings')
  listClassOfferings() {
    return this.planningService.listClassOfferings();
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
