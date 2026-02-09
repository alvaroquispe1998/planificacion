import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-planning-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './planning.page.html',
  styleUrl: './planning.page.css',
})
export class PlanningPageComponent implements OnInit {
  offerings: any[] = [];
  groups: any[] = [];
  meetings: any[] = [];
  hoursValidation: any | null = null;
  selectedOfferingId = '';

  offeringForm = {
    id: this.makeId(),
    semester_id: '',
    study_plan_id: '',
    academic_program_id: '',
    course_id: '',
    course_section_id: '',
    campus_id: '',
    delivery_modality_id: '',
    shift_id: '',
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

  constructor(private readonly api: ApiService) {}

  ngOnInit(): void {
    this.loadOfferings();
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

  createOffering() {
    this.api.createClassOffering(this.offeringForm).subscribe(() => {
      this.offeringForm.id = this.makeId();
      this.loadOfferings();
    });
  }

  createGroup() {
    if (!this.groupForm.class_offering_id) {
      this.groupForm.class_offering_id = this.selectedOfferingId;
    }
    this.api.createClassGroup(this.groupForm).subscribe(() => {
      this.groupForm.id = this.makeId();
      this.loadGroups();
    });
  }

  createMeeting() {
    if (!this.meetingForm.class_offering_id) {
      this.meetingForm.class_offering_id = this.selectedOfferingId;
    }
    this.api.createClassMeeting(this.meetingForm).subscribe(() => {
      this.meetingForm.id = this.makeId();
      this.loadMeetings();
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
