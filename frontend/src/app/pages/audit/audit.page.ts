import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-audit-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './audit.page.html',
  styleUrl: './audit.page.css',
})
export class AuditPageComponent {
  videoConferenceId = '';
  meetingInstanceId = '';

  meetingInstances: any[] = [];
  participants: any[] = [];
  teacherMetrics: any[] = [];
  transcripts: any[] = [];
  summaries: any[] = [];
  lastAction: any | null = null;

  constructor(private readonly api: ApiService) {}

  loadInstances() {
    this.api.listMeetingInstances(this.videoConferenceId || undefined).subscribe((rows) => {
      this.meetingInstances = rows;
      if (rows.length > 0 && !this.meetingInstanceId) {
        this.selectInstance(rows[0].id);
      }
    });
  }

  selectInstance(id: string) {
    this.meetingInstanceId = id;
    this.refreshInstanceData();
  }

  refreshInstanceData() {
    if (!this.meetingInstanceId) {
      return;
    }
    this.api.listParticipants(this.meetingInstanceId).subscribe((rows) => (this.participants = rows));
    this.api.listTeacherMetrics(this.meetingInstanceId).subscribe((rows) => (this.teacherMetrics = rows));
    this.api.listTranscripts(this.meetingInstanceId).subscribe((rows) => (this.transcripts = rows));
    this.api.listSummaries(this.meetingInstanceId).subscribe((rows) => (this.summaries = rows));
  }

  recomputeTeacherMetrics() {
    if (!this.meetingInstanceId) {
      return;
    }
    this.api.recomputeTeacherMetrics(this.meetingInstanceId).subscribe((result) => {
      this.lastAction = result;
      this.refreshInstanceData();
    });
  }

  generateSummary() {
    if (!this.meetingInstanceId) {
      return;
    }
    this.api.generateSummary(this.meetingInstanceId).subscribe((result) => {
      this.lastAction = result;
      this.refreshInstanceData();
    });
  }
}
