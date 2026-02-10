import { CommonModule } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { ChangeDetectorRef, Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../../core/api.service';

type SyncResource = {
  code: string;
  label: string;
  source: string;
  module_code: string;
  module_label: string;
  module_description: string;
  module_order: number;
  resource_order: number;
};

type SyncResourceModule = {
  code: string;
  label: string;
  description: string;
  order: number;
  resources: SyncResource[];
};

@Component({
  selector: 'app-settings-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './settings.page.html',
  styleUrl: './settings.page.css',
})
export class SettingsPageComponent implements OnInit, OnDestroy {
  sources: any[] = [];
  resources: SyncResource[] = [];
  resourceModules: SyncResourceModule[] = [];
  jobs: any[] = [];

  selectedSourceCode = 'MATRICULA';
  cookieText = '';
  renewalUrl = '';
  renewalSteps: string[] = [];

  mode: 'FULL' | 'INCREMENTAL' = 'FULL';
  selectedResources = new Set<string>();

  isLoading = false;
  isSavingCookie = false;
  isSyncing = false;
  feedback = '';
  errorMessage = '';
  syncResult: any | null = null;

  /** Tracks which individual sources are currently being validated */
  validatingSourceCodes = new Set<string>();

  /** Sync elapsed time tracking */
  syncElapsedSeconds = 0;
  syncTotalCount = 0;
  private syncTimer: ReturnType<typeof setInterval> | null = null;
  private syncPollInterval: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly api: ApiService,
    private readonly cdr: ChangeDetectorRef,
    private readonly zone: NgZone,
  ) { }

  ngOnInit() {
    this.loadInitialData();
  }

  ngOnDestroy() {
    this.clearSyncTimer();
  }

  get isValidatingAny(): boolean {
    return this.validatingSourceCodes.size > 0;
  }

  isSourceValidating(code: string): boolean {
    return this.validatingSourceCodes.has(code);
  }

  loadInitialData() {
    this.isLoading = true;
    this.errorMessage = '';

    // Load resources (static list)
    this.api.listSyncResources().subscribe({
      next: (resources) => {
        this.resources = resources as SyncResource[];
        this.resourceModules = this.buildResourceModules(this.resources);
        if (this.selectedResources.size === 0) {
          this.resources.forEach((item) => this.selectedResources.add(item.code));
        }
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.handleError(err);
        this.cdr.detectChanges();
      },
    });

    // Phase 1: Load sources WITHOUT probe → instant display
    this.api.listSyncSources(false).subscribe({
      next: (sources) => {
        this.zone.run(() => {
          this.sources = sources;
          if (
            !this.sources.some((item) => item.code === this.selectedSourceCode) &&
            this.sources.length > 0
          ) {
            this.selectedSourceCode = this.sources[0].code;
          }
          this.onSelectedSourceChange();
          this.isLoading = false;
          this.cdr.detectChanges();

          // Phase 2: Auto-validate each source individually in background
          this.validateAllSourcesInBackground();
        });
      },
      error: (err) => {
        this.zone.run(() => {
          this.isLoading = false;
          this.handleError(err);
          this.cdr.detectChanges();
        });
      },
    });

    this.refreshJobs();
  }

  /** Validates each source individually so each card updates as soon as its validation completes */
  private validateAllSourcesInBackground() {
    if (this.sources.length === 0) return;

    for (const source of this.sources) {
      this.validatingSourceCodes.add(source.code);
    }
    this.cdr.detectChanges();

    for (const source of this.sources) {
      this.validateSingleSourceInBackground(source.code);
    }
  }

  private validateSingleSourceInBackground(code: string) {
    this.api.validateSyncSource(code).subscribe({
      next: (result) => {
        this.zone.run(() => {
          this.validatingSourceCodes.delete(code);
          this.updateSourceFromValidation(code, result);
          if (this.validatingSourceCodes.size === 0) {
            this.feedback = 'Estado de sesiones actualizado.';
          }
          this.cdr.detectChanges();
        });
      },
      error: () => {
        this.zone.run(() => {
          this.validatingSourceCodes.delete(code);
          const idx = this.sources.findIndex((s) => s.code === code);
          if (idx >= 0) {
            this.sources[idx] = {
              ...this.sources[idx],
              session_status: 'ERROR',
              needs_renewal: true,
              error_last: 'Error de conexion al validar.',
            };
          }
          if (this.validatingSourceCodes.size === 0) {
            this.feedback = 'Validacion completada con errores.';
          }
          this.cdr.detectChanges();
        });
      },
    });
  }

  private updateSourceFromValidation(code: string, result: any) {
    const idx = this.sources.findIndex((s) => s.code === code);
    if (idx < 0) return;
    const current = this.sources[idx];

    if (result.ok) {
      this.sources[idx] = {
        ...current,
        session_status: 'ACTIVE',
        last_validated_at: new Date().toISOString(),
        needs_renewal: false,
        error_last: null,
      };
    } else {
      const status = current.session_status === 'MISSING' ? 'MISSING' : 'EXPIRED';
      this.sources[idx] = {
        ...current,
        session_status: status,
        last_validated_at: new Date().toISOString(),
        needs_renewal: true,
        error_last: result.reason ?? null,
      };
    }
  }

  refreshSourceStatus() {
    this.feedback = '';
    this.errorMessage = '';
    this.validateAllSourcesInBackground();
  }

  saveCookie() {
    if (!this.selectedSourceCode || !this.cookieText.trim()) {
      this.errorMessage = 'Selecciona una fuente y pega la cookie.';
      return;
    }
    this.isSavingCookie = true;
    this.feedback = '';
    this.errorMessage = '';
    this.cdr.detectChanges();

    const payload: Record<string, unknown> = { cookie_text: this.cookieText.trim() };

    this.api.upsertSyncCookie(this.selectedSourceCode, payload).subscribe({
      next: () => {
        this.zone.run(() => {
          this.isSavingCookie = false;
          this.feedback = `Cookie guardada para ${this.selectedSourceCode}. Validando...`;
          this.cdr.detectChanges();
          // Auto-validate the source after saving cookie
          this.validatingSourceCodes.add(this.selectedSourceCode);
          this.cdr.detectChanges();
          this.validateSingleSourceInBackground(this.selectedSourceCode);
        });
      },
      error: (err) => {
        this.zone.run(() => {
          this.isSavingCookie = false;
          this.handleError(err);
          this.cdr.detectChanges();
        });
      },
    });
  }

  validateSelectedSource() {
    if (!this.selectedSourceCode) return;
    const code = this.selectedSourceCode;
    this.validatingSourceCodes.add(code);
    this.feedback = '';
    this.errorMessage = '';
    this.cdr.detectChanges();

    this.api.validateSyncSource(code).subscribe({
      next: (result) => {
        this.zone.run(() => {
          this.validatingSourceCodes.delete(code);
          this.updateSourceFromValidation(code, result);
          this.feedback = result.ok
            ? `Sesion valida en ${code}.`
            : `Sesion invalida en ${code}: ${result.reason ?? 'sin detalle'}.`;
          this.cdr.detectChanges();
        });
      },
      error: (err) => {
        this.zone.run(() => {
          this.validatingSourceCodes.delete(code);
          this.handleError(err);
          this.cdr.detectChanges();
        });
      },
    });
  }

  runSync() {
    const resources = [...this.selectedResources];
    if (resources.length === 0) {
      this.errorMessage = 'Selecciona al menos un recurso para sincronizar.';
      return;
    }

    this.isSyncing = true;
    this.syncElapsedSeconds = 0;
    this.syncTotalCount = resources.length;
    this.feedback = '';
    this.errorMessage = '';
    this.syncResult = null;
    this.cdr.detectChanges();

    // Start elapsed time counter
    this.syncTimer = setInterval(() => {
      this.zone.run(() => {
        this.syncElapsedSeconds++;
        this.cdr.detectChanges();
      });
    }, 1000);

    // Poll jobs every 3s so the user sees live progress
    this.syncPollInterval = setInterval(() => {
      this.zone.run(() => {
        this.refreshJobs();
      });
    }, 3000);

    this.api.runExternalSync({ mode: this.mode, resources }).subscribe({
      next: (result) => {
        this.zone.run(() => {
          this.clearSyncTimer();
          this.isSyncing = false;
          this.syncResult = result;
          const elapsed = this.formatElapsed(this.syncElapsedSeconds);
          this.feedback =
            `Sincronizacion completada en ${elapsed}. ` +
            `${result.completed ?? 0} exitosos, ${result.failed ?? 0} fallidos.`;
          this.refreshJobs();
          this.cdr.detectChanges();
        });
      },
      error: (err) => {
        this.zone.run(() => {
          this.clearSyncTimer();
          this.isSyncing = false;
          this.handleError(err);
          this.refreshJobs();
          this.cdr.detectChanges();
        });
      },
    });
  }

  refreshJobs() {
    this.api.listSyncJobs(20).subscribe({
      next: (jobs) => {
        this.zone.run(() => {
          this.jobs = jobs;
          this.cdr.detectChanges();
        });
      },
      error: (err) => {
        this.zone.run(() => {
          this.handleError(err);
          this.cdr.detectChanges();
        });
      },
    });
  }

  formatElapsed(seconds: number): string {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  }

  toggleResource(resourceCode: string, checked: boolean) {
    if (checked) {
      this.selectedResources.add(resourceCode);
      return;
    }
    this.selectedResources.delete(resourceCode);
  }

  isResourceSelected(resourceCode: string) {
    return this.selectedResources.has(resourceCode);
  }

  toggleModuleResources(module: SyncResourceModule, checked: boolean) {
    for (const resource of module.resources) {
      this.toggleResource(resource.code, checked);
    }
  }

  isModuleFullySelected(module: SyncResourceModule) {
    if (!module.resources.length) return false;
    return module.resources.every((item) => this.selectedResources.has(item.code));
  }

  isModulePartiallySelected(module: SyncResourceModule) {
    const selected = module.resources.filter((item) => this.selectedResources.has(item.code)).length;
    return selected > 0 && selected < module.resources.length;
  }

  selectedCountForModule(module: SyncResourceModule) {
    return module.resources.filter((item) => this.selectedResources.has(item.code)).length;
  }

  sourceNeedsRenewal(source: any) {
    return Boolean(source?.needs_renewal);
  }

  onSelectedSourceChange() {
    const source = this.sources.find((item) => item.code === this.selectedSourceCode);
    if (!source) {
      this.renewalUrl = '';
      this.renewalSteps = [];
      this.cookieText = '';
      return;
    }
    this.renewalUrl = source.login_url || source.base_url || '';
    this.renewalSteps = this.getRenewalSteps(source.code);
    this.loadSourceCookie(source.code);
  }

  private loadSourceCookie(code: string) {
    this.api.getSyncCookie(code).subscribe({
      next: (result) => {
        this.zone.run(() => {
          if (result.has_cookie && this.selectedSourceCode === code) {
            this.cookieText = result.cookie_text ?? '';
          } else if (this.selectedSourceCode === code) {
            this.cookieText = '';
          }
          this.cdr.detectChanges();
        });
      },
      error: () => {
        this.zone.run(() => {
          if (this.selectedSourceCode === code) {
            this.cookieText = '';
          }
          this.cdr.detectChanges();
        });
      },
    });
  }

  startCookieRenewal(source: any) {
    this.selectedSourceCode = source.code;
    this.onSelectedSourceChange();
    this.feedback = `Renovacion preparada para ${source.code}. Abre la URL, inicia sesion y copia el header Cookie.`;
    this.errorMessage = '';
    setTimeout(() => {
      document.getElementById('cookie-panel')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 20);
  }

  async copyRenewalUrl() {
    if (!this.renewalUrl) {
      this.errorMessage = 'No hay URL de renovacion disponible para la fuente seleccionada.';
      return;
    }
    try {
      await navigator.clipboard.writeText(this.renewalUrl);
      this.feedback = 'URL de renovacion copiada al portapapeles.';
      this.errorMessage = '';
    } catch {
      this.errorMessage = 'No se pudo copiar la URL. Copiala manualmente.';
    }
  }

  private clearSyncTimer() {
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }
    if (this.syncPollInterval) {
      clearInterval(this.syncPollInterval);
      this.syncPollInterval = null;
    }
  }

  private getRenewalSteps(sourceCode: string) {
    const source = sourceCode.toUpperCase();
    if (source === 'AULAVIRTUAL') {
      return [
        'Abre la URL de Aula Virtual e inicia sesion.',
        'En DevTools abre Network y recarga la pagina.',
        'Selecciona una request a /web/conference/... y copia el header Cookie.',
        'Pega el valor en este formulario y guarda.',
      ];
    }
    if (source === 'DOCENTE') {
      return [
        'Abre Docente e inicia sesion.',
        'Desde Network, elige una request a /admin/cursos/get.',
        'Copia el header Cookie completo.',
        'Pega y guarda la cookie.',
      ];
    }
    if (source === 'INTRANET') {
      return [
        'Abre Intranet e inicia sesion.',
        'Busca una request a /admin/docentes/get.',
        'Copia el header Cookie y pegalo aqui.',
        'Guarda y valida la sesion.',
      ];
    }
    return [
      'Abre Matricula e inicia sesion.',
      'En Network, selecciona una request a /admin/.../get.',
      'Copia el header Cookie.',
      'Pega y guarda la cookie.',
    ];
  }

  private buildResourceModules(resources: SyncResource[]): SyncResourceModule[] {
    const modulesMap = new Map<string, SyncResourceModule>();
    for (const resource of resources) {
      if (!modulesMap.has(resource.module_code)) {
        modulesMap.set(resource.module_code, {
          code: resource.module_code,
          label: resource.module_label,
          description: resource.module_description,
          order: resource.module_order,
          resources: [],
        });
      }
      modulesMap.get(resource.module_code)?.resources.push(resource);
    }

    return [...modulesMap.values()]
      .sort((a, b) => a.order - b.order || a.label.localeCompare(b.label))
      .map((module) => ({
        ...module,
        resources: [...module.resources].sort(
          (a, b) => a.resource_order - b.resource_order || a.label.localeCompare(b.label),
        ),
      }));
  }

  private handleError(err: HttpErrorResponse) {
    const message = err.error?.message;
    if (Array.isArray(message)) {
      this.errorMessage = message.join(' | ');
      return;
    }
    this.errorMessage = message || 'Ocurrio un error en la configuracion de sincronizacion.';
  }
}
