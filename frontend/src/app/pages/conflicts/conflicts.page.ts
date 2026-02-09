import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-conflicts-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './conflicts.page.html',
  styleUrl: './conflicts.page.css',
})
export class ConflictsPageComponent {
  semesterId = '';
  conflicts: any[] = [];
  lastRun: any | null = null;

  constructor(private readonly api: ApiService) {}

  loadConflicts() {
    this.api.listScheduleConflicts(this.semesterId || undefined).subscribe((rows) => {
      this.conflicts = rows;
    });
  }

  detectConflicts() {
    if (!this.semesterId) {
      return;
    }
    this.api.detectConflicts(this.semesterId).subscribe((result) => {
      this.lastRun = result;
      this.loadConflicts();
    });
  }
}
