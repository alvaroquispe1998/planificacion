import { CommonModule } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { ApiService } from '../../core/api.service';

type SyncResult = {
  source_system: string;
  semester_id: string | null;
  replace_semester_applied: boolean;
  totals: {
    received: number;
    processed: number;
    created: number;
    updated: number;
    deduplicated: number;
  };
  imported_at: string;
};

@Component({
  selector: 'app-sync-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './sync.page.html',
  styleUrl: './sync.page.css',
})
export class SyncPageComponent implements OnInit {
  payloadText = JSON.stringify(this.buildExamplePayload(), null, 2);
  replaceSemester = true;
  isSubmitting = false;
  errorMessage = '';
  successMessage = '';
  result: SyncResult | null = null;
  currentOfferingsCount = 0;

  constructor(private readonly api: ApiService) {}

  ngOnInit() {
    this.refreshCurrentOfferings();
  }

  loadExamplePayload() {
    this.payloadText = JSON.stringify(this.buildExamplePayload(), null, 2);
    this.errorMessage = '';
    this.successMessage = '';
  }

  clearPayload() {
    this.payloadText = '';
    this.errorMessage = '';
    this.successMessage = '';
  }

  sync() {
    this.errorMessage = '';
    this.successMessage = '';
    this.result = null;

    let parsedPayload: Record<string, unknown>;
    try {
      parsedPayload = JSON.parse(this.payloadText) as Record<string, unknown>;
    } catch {
      this.errorMessage = 'El JSON no es valido. Corrige el formato antes de sincronizar.';
      return;
    }

    const payload = {
      ...parsedPayload,
      replace_semester: this.replaceSemester,
    };

    this.isSubmitting = true;
    this.api.syncAkademic(payload).subscribe({
      next: (response) => {
        this.isSubmitting = false;
        this.result = response;
        this.successMessage = 'Sincronizacion completada. Ya puedes continuar con planificacion.';
        this.refreshCurrentOfferings();
      },
      error: (error: HttpErrorResponse) => {
        this.isSubmitting = false;
        this.errorMessage = this.extractErrorMessage(error);
      },
    });
  }

  private refreshCurrentOfferings() {
    this.api.listClassOfferings().subscribe((rows) => {
      this.currentOfferingsCount = rows.length;
    });
  }

  private extractErrorMessage(error: HttpErrorResponse) {
    const message = error.error?.message;
    if (Array.isArray(message)) {
      return message.join(' | ');
    }
    if (typeof message === 'string' && message.trim()) {
      return message;
    }
    return 'No fue posible sincronizar. Revisa el payload y vuelve a intentar.';
  }

  private buildExamplePayload() {
    return {
      source_system: 'AKADEMIC',
      semester_id: '2026-1',
      class_offerings: [
        {
          id: 'off-2026-1-progra',
          semester_id: '2026-1',
          study_plan_id: 'sp-isi-2024',
          academic_program_id: 'prog-isi',
          course_id: 'course-prog-1',
          course_section_id: 'sec-prog-1-a',
          campus_id: 'campus-main',
          delivery_modality_id: 'mod-presencial',
          shift_id: 'turno-manana',
          projected_vacancies: 45,
          status: true,
        },
      ],
      class_groups: [
        {
          id: 'grp-2026-1-progra-t',
          class_offering_id: 'off-2026-1-progra',
          group_type: 'THEORY',
          code: 'T1',
          capacity: 45,
          note: 'Grupo teoria',
        },
      ],
      class_meetings: [
        {
          id: 'meet-2026-1-progra-lun',
          class_offering_id: 'off-2026-1-progra',
          class_group_id: 'grp-2026-1-progra-t',
          day_of_week: 'LUNES',
          start_time: '08:00:00',
          end_time: '09:30:00',
          minutes: 90,
          academic_hours: 2,
          classroom_id: 'aula-b201',
        },
      ],
      class_teachers: [
        {
          id: 'tch-2026-1-progra-ana',
          class_offering_id: 'off-2026-1-progra',
          teacher_id: 'doc-ana-001',
          role: 'TITULAR',
          is_primary: true,
        },
      ],
      course_section_hour_requirements: [
        {
          id: 'hr-2026-1-progra',
          course_section_id: 'sec-prog-1-a',
          course_format: 'T',
          theory_hours_academic: 4,
          practice_hours_academic: 0,
          lab_hours_academic: 0,
          academic_minutes_per_hour: 50,
          notes: 'Carga academica objetivo',
        },
      ],
    };
  }
}
