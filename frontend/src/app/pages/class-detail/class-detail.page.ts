import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, of, switchMap } from 'rxjs';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-class-detail-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './class-detail.page.html',
  styleUrl: './class-detail.page.css',
})
export class ClassDetailPageComponent implements OnInit {
  classOfferingId = '';

  offering: any | null = null;
  meetings: any[] = [];
  teachers: any[] = [];
  videoConferences: any[] = [];
  meetingInstances: any[] = [];
  selectedMeetingInstanceId = '';

  hoursValidation: any | null = null;
  teacherMetrics: any[] = [];
  summaries: any[] = [];
  matches: any[] = [];
  syllabusSessions: any[] = [];

  constructor(
    private readonly route: ActivatedRoute,
    private readonly api: ApiService,
  ) {}

  ngOnInit(): void {
    this.route.paramMap
      .pipe(
        switchMap((params) => {
          this.classOfferingId = params.get('id') ?? '';
          if (!this.classOfferingId) {
            return of(null);
          }
          return forkJoin({
            offering: this.api.getClassOffering(this.classOfferingId),
            meetings: this.api.listClassMeetings(this.classOfferingId),
            teachers: this.api.listClassTeachers(this.classOfferingId),
            videoConferences: this.api.listVideoConferences(this.classOfferingId),
            hoursValidation: this.api.validateHours(this.classOfferingId),
            syllabusSessions: this.api.listSyllabusSessions(this.classOfferingId),
          });
        }),
      )
      .subscribe((result) => {
        if (!result) {
          return;
        }
        this.offering = result.offering;
        this.meetings = result.meetings;
        this.teachers = result.teachers;
        this.videoConferences = result.videoConferences;
        this.hoursValidation = result.hoursValidation;
        this.syllabusSessions = result.syllabusSessions;

        this.loadInstances();
      });
  }

  selectMeetingInstance(meetingInstanceId: string) {
    this.selectedMeetingInstanceId = meetingInstanceId;
    this.refreshMeetingAnalysis();
  }

  runSummary() {
    if (!this.selectedMeetingInstanceId) {
      return;
    }
    this.api.generateSummary(this.selectedMeetingInstanceId).subscribe(() => {
      this.refreshMeetingAnalysis();
    });
  }

  runMatch() {
    if (!this.selectedMeetingInstanceId || this.syllabusSessions.length === 0) {
      return;
    }
    this.api.runMatch(this.selectedMeetingInstanceId, this.syllabusSessions[0].id).subscribe(() => {
      this.refreshMeetingAnalysis();
    });
  }

  private loadInstances() {
    const requests = this.videoConferences.map((conference) =>
      this.api.listMeetingInstances(conference.id),
    );

    if (requests.length === 0) {
      this.meetingInstances = [];
      return;
    }

    forkJoin(requests).subscribe((rows) => {
      this.meetingInstances = rows.flat();
      if (this.meetingInstances.length > 0) {
        this.selectMeetingInstance(this.meetingInstances[0].id);
      }
    });
  }

  private refreshMeetingAnalysis() {
    if (!this.selectedMeetingInstanceId) {
      return;
    }

    forkJoin({
      teacherMetrics: this.api.listTeacherMetrics(this.selectedMeetingInstanceId),
      summaries: this.api.listSummaries(this.selectedMeetingInstanceId),
      matches: this.api.listMatches(this.selectedMeetingInstanceId),
    }).subscribe((result) => {
      this.teacherMetrics = result.teacherMetrics;
      this.summaries = result.summaries;
      this.matches = result.matches;
    });
  }
}
