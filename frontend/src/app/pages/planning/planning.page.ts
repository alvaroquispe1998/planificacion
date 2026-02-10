import { CommonModule, NgClass } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-planning-page',
  standalone: true,
  imports: [CommonModule, FormsModule, NgClass],
  templateUrl: './planning.page.html',
  styleUrl: './planning.page.css',
})
export class PlanningPageComponent implements OnInit {
  offerings: any[] = [];
  groups: any[] = [];
  meetings: any[] = [];
  hoursValidation: any | null = null;
  selectedOfferingId = '';

  // UI State
  showCreateOffering = false;
  showCreateGroup = false;
  showCreateMeeting = false;

  // Catalogs
  semesters: any[] = [];
  campuses: any[] = [];
  programs: any[] = [];
  courses: any[] = [];
  teachers: any[] = [];
  classrooms: any[] = [];
  studyPlans: any[] = [];
  courseSections: any[] = [];

  offeringForm = {
    id: this.makeId(),
    semester_id: '',
    study_plan_id: '',
    academic_program_id: '',
    course_id: '',
    course_section_id: '',
    campus_id: '',
    delivery_modality_id: 'PRESENCIAL', // Default
    shift_id: 'M', // Default
    projected_vacancies: 0,
    status: true,
  };

  groupForm = {
    id: this.makeId(),
    class_offering_id: '',
    group_type: 'THEORY',
    code: 'T',
    capacity: 0,
    note: '',
  };

  meetingForm = {
    id: this.makeId(),
    class_offering_id: '',
    class_group_id: '',
    day_of_week: 'LUNES',
    start_time: '08:00:00',
    end_time: '09:30:00',
    minutes: 90,
    academic_hours: 2,
    classroom_id: '',
  };

  // Maps for quick lookup
  courseMap = new Map<string, any>();
  semesterMap = new Map<string, any>();
  teacherMap = new Map<string, any>();
  classroomMap = new Map<string, any>();

  // Teacher Assignments
  groupTeachers: any[] = [];
  selectedGroupIdForTeacher = '';
  teacherAssignmentForm = {
    teacher_id: '',
    role: 'PRIMARY',
    is_primary: true
  };

  constructor(private readonly api: ApiService) { }

  ngOnInit(): void {
    this.loadCatalogs();
    this.loadOfferings();
  }

  loadCatalogs() {
    this.api.listSemesters().subscribe((rows) => {
      this.semesters = rows;
      this.semesterMap = new Map(rows.map(s => [s.id, s]));
      if (rows.length > 0) this.offeringForm.semester_id = rows[0].id;
    });
    this.api.listCampuses().subscribe((rows) => this.campuses = rows);
    this.api.listPrograms().subscribe((rows) => this.programs = rows);
    this.api.listCourses().subscribe((rows) => {
      this.courses = rows;
      this.courseMap = new Map(rows.map(c => [c.id, c]));
    });
    this.api.listTeachers().subscribe((rows) => {
      this.teachers = rows;
      this.teacherMap = new Map(rows.map(t => [t.id, t]));
    });
    this.api.listClassrooms().subscribe((rows) => {
      this.classrooms = rows;
      this.classroomMap = new Map(rows.map(c => [c.id, c]));
    });
    this.api.listStudyPlans().subscribe((rows) => this.studyPlans = rows);
    this.api.listCourseSections().subscribe((rows) => this.courseSections = rows);
  }

  loadOfferings() {
    this.api.listClassOfferings().subscribe((rows) => {
      this.offerings = rows;
      if (!this.selectedOfferingId && rows.length > 0) {
        this.selectOffering(rows[0].id);
      }
    });
  }

  selectOffering(classOfferingId: string) {
    this.selectedOfferingId = classOfferingId;
    this.groupForm.class_offering_id = classOfferingId;
    this.meetingForm.class_offering_id = classOfferingId;
    this.loadGroups();
    this.loadMeetings();
    this.hoursValidation = null;
    this.groupTeachers = []; // clear until groups loaded
  }

  loadGroups() {
    if (!this.selectedOfferingId) {
      this.groups = [];
      return;
    }
    this.api.listClassGroups(this.selectedOfferingId).subscribe((rows) => {
      this.groups = rows;
      if (rows.length > 0) {
        this.meetingForm.class_group_id = rows[0].id;
      }
      this.loadGroupTeachers(); // Load teachers for these groups
    });
  }

  loadMeetings() {
    if (!this.selectedOfferingId) {
      this.meetings = [];
      return;
    }
    this.api.listClassMeetings(this.selectedOfferingId).subscribe((rows) => {
      this.meetings = rows;
    });
  }

  loadGroupTeachers() {
    // We fetch all teachers for the groups of this offering
    // Ideally we would filter by offering but the API filters by group or generic.
    // We will loop groups and fetch? Or fetch all for offering if API supported it.
    // API supports 'class_group_id'. We have multiple.
    // Better: GET all class-group-teachers without filter? No, too many.
    // We will just fetch for each group or use a new 'listByOffering' endpoint if we had one.
    // For now, let's just fetch for EACH group. Slow but works for MVP.
    this.groupTeachers = [];
    this.groups.forEach(g => {
      this.api.listClassGroupTeachers(g.id).subscribe(teachers => {
        this.groupTeachers = [...this.groupTeachers, ...teachers];
      });
    });
  }

  assignTeacher(groupId: string) {
    if (!this.teacherAssignmentForm.teacher_id) return;

    const payload = {
      id: crypto.randomUUID(),
      class_group_id: groupId,
      teacher_id: this.teacherAssignmentForm.teacher_id,
      role: this.teacherAssignmentForm.role,
      is_primary: this.teacherAssignmentForm.is_primary,
      assigned_from: new Date().toISOString(),
      assigned_to: null
    };

    this.api.assignTeacherToGroup(payload).subscribe(() => {
      this.teacherAssignmentForm.teacher_id = ''; // reset
      this.loadGroupTeachers();
      // maybe refresh meetings too if needed
    });
  }

  removeTeacher(assignmentId: string) {
    if (!confirm('Quitar docente?')) return;
    this.api.removeTeacherFromGroup(assignmentId).subscribe(() => {
      this.loadGroupTeachers();
    });
  }

  createOffering() {
    this.api.createClassOffering(this.offeringForm).subscribe(() => {
      this.offeringForm.id = this.makeId();
      this.loadOfferings();
      this.showCreateOffering = false;
    });
  }

  createGroup() {
    if (!this.groupForm.class_offering_id) {
      this.groupForm.class_offering_id = this.selectedOfferingId;
    }
    this.api.createClassGroup(this.groupForm).subscribe(() => {
      this.groupForm.id = this.makeId();
      this.loadGroups();
      this.showCreateGroup = false;
    });
  }

  prepareMeetingForm(groupId: string) {
    this.meetingForm.id = this.makeId();
    this.meetingForm.class_group_id = groupId;
    this.meetingForm.class_offering_id = this.selectedOfferingId;
    this.showCreateMeeting = true;
  }

  createMeeting() {
    if (!this.meetingForm.class_offering_id) {
      this.meetingForm.class_offering_id = this.selectedOfferingId;
    }
    this.api.createClassMeeting(this.meetingForm).subscribe(() => {
      this.meetingForm.id = this.makeId();
      this.loadMeetings();
      this.showCreateMeeting = false;
    });
  }

  runHoursValidation() {
    if (!this.selectedOfferingId) {
      return;
    }
    this.api.validateHours(this.selectedOfferingId).subscribe((result) => {
      this.hoursValidation = result;
    });
  }

  private makeId() {
    return crypto.randomUUID();
  }
}
