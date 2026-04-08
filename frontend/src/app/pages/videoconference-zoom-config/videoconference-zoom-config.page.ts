import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { ApiService } from '../../core/api.service';

type ZoomConfigForm = {
  id: string;
  accountId: string;
  clientId: string;
  clientSecret: string;
  maxConcurrent: number;
  pageSize: number;
  timezone: string;
  created_at: string | null;
  updated_at: string | null;
};

type ZoomConfigTestResult = {
  ok: boolean;
  accountId?: string;
  userCount?: number;
  reason?: string;
};

@Component({
  selector: 'app-videoconference-zoom-config-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './videoconference-zoom-config.page.html',
  styleUrl: './videoconference-zoom-config.page.css',
})
export class VideoconferenceZoomConfigPageComponent implements OnInit {
  loading = true;
  saving = false;
  testing = false;
  message = '';
  error = '';
  showSecret = false;
  lastTest: ZoomConfigTestResult | null = null;
  form: ZoomConfigForm = this.createEmptyForm();

  private initialFingerprint = '';

  constructor(
    private readonly api: ApiService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadPage();
  }

  get isDirty() {
    return this.fingerprint(this.form) !== this.initialFingerprint;
  }

  get isConfigured() {
    return Boolean(
      this.form.accountId.trim() &&
        this.form.clientId.trim() &&
        this.form.clientSecret.trim(),
    );
  }

  get completenessLabel() {
    return this.isConfigured ? 'Completa' : 'Pendiente';
  }

  get completenessClass() {
    return this.isConfigured ? 'state-ok' : 'state-warn';
  }

  loadPage() {
    this.loading = true;
    this.error = '';
    this.message = '';
    this.lastTest = null;

    this.api.getZoomConfig().subscribe({
      next: (response) => {
        this.form = this.toForm(response);
        this.initialFingerprint = this.fingerprint(this.form);
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo cargar la configuracion Zoom.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  save() {
    if (this.saving) {
      return;
    }

    this.saving = true;
    this.error = '';
    this.message = '';
    this.lastTest = null;

    this.api.updateZoomConfig(this.toPayload(this.form)).subscribe({
      next: (response) => {
        this.form = this.toForm(response);
        this.initialFingerprint = this.fingerprint(this.form);
        this.message = 'Configuracion Zoom guardada.';
        this.saving = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo guardar la configuracion Zoom.';
        this.saving = false;
        this.cdr.detectChanges();
      },
    });
  }

  testConnection() {
    if (this.testing || this.saving) {
      return;
    }

    if (this.isDirty) {
      this.saveAndThenTest();
      return;
    }

    this.runTestRequest();
  }

  reload() {
    if (this.loading || this.saving || this.testing) {
      return;
    }
    this.loadPage();
  }

  private saveAndThenTest() {
    this.saving = true;
    this.testing = true;
    this.error = '';
    this.message = '';
    this.lastTest = null;

    this.api.updateZoomConfig(this.toPayload(this.form)).subscribe({
      next: (response) => {
        this.form = this.toForm(response);
        this.initialFingerprint = this.fingerprint(this.form);
        this.message = 'Configuracion Zoom guardada.';
        this.saving = false;
        this.runTestRequest();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo guardar la configuracion Zoom.';
        this.saving = false;
        this.testing = false;
        this.cdr.detectChanges();
      },
    });
  }

  private runTestRequest() {
    this.testing = true;
    this.error = '';
    this.lastTest = null;

    this.api.testZoomConfig().subscribe({
      next: (response) => {
        this.lastTest = {
          ok: Boolean(response?.ok),
          accountId: response?.accountId,
          userCount: Number(response?.userCount ?? 0),
          reason: response?.reason ?? undefined,
        };
        this.testing = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo probar la conexion Zoom.';
        this.testing = false;
        this.cdr.detectChanges();
      },
    });
  }

  private toPayload(form: ZoomConfigForm) {
    return {
      accountId: form.accountId.trim(),
      clientId: form.clientId.trim(),
      clientSecret: form.clientSecret.trim(),
      maxConcurrent: Math.max(1, Number(form.maxConcurrent || 1)),
      pageSize: Math.max(1, Number(form.pageSize || 20)),
      timezone: form.timezone.trim(),
    };
  }

  private toForm(value: any): ZoomConfigForm {
    return {
      id: `${value?.id ?? ''}`,
      accountId: `${value?.accountId ?? ''}`,
      clientId: `${value?.clientId ?? ''}`,
      clientSecret: `${value?.clientSecret ?? ''}`,
      maxConcurrent: Math.max(1, Number(value?.maxConcurrent ?? 2)),
      pageSize: Math.max(1, Number(value?.pageSize ?? 20)),
      timezone: `${value?.timezone ?? 'America/Lima'}`,
      created_at: value?.created_at ?? null,
      updated_at: value?.updated_at ?? null,
    };
  }

  private createEmptyForm(): ZoomConfigForm {
    return {
      id: '',
      accountId: '',
      clientId: '',
      clientSecret: '',
      maxConcurrent: 2,
      pageSize: 20,
      timezone: 'America/Lima',
      created_at: null,
      updated_at: null,
    };
  }

  private fingerprint(form: ZoomConfigForm) {
    return JSON.stringify(this.toPayload(form));
  }
}
