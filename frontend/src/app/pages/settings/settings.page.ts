import { CommonModule } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { Component, OnInit } from '@angular/core';
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
export class SettingsPageComponent implements OnInit {
  sources: any[] = [];
  resources: SyncResource[] = [];
  resourceModules: SyncResourceModule[] = [];
  jobs: any[] = [];

  selectedSourceCode = 'MATRICULA';
  cookieText = '';
  expiresAt = '';
  renewalUrl = '';
  renewalSteps: string[] = [];

  mode: 'FULL' | 'INCREMENTAL' = 'FULL';
  selectedResources = new Set<string>();

  isLoading = false;
  isSavingCookie = false;
  isValidating = false;
  isSyncing = false;
  feedback = '';
  errorMessage = '';
  syncResult: any | null = null;

  constructor(private readonly api: ApiService) {}

  ngOnInit() {
    this.loadInitialData();
  }

  loadInitialData() {
    this.isLoading = true;
    this.errorMessage = '';
    this.api.listSyncResources().subscribe({
      next: (resources) => {
        this.resources = resources as SyncResource[];
        this.resourceModules = this.buildResourceModules(this.resources);
        if (this.selectedResources.size === 0) {
          this.resources.forEach((item) => this.selectedResources.add(item.code));
        }
      },
      error: (err) => this.handleError(err),
    });

    this.api.listSyncSources(false).subscribe({
      next: (sources) => {
        this.sources = sources;
        if (!this.sources.some((item) => item.code === this.selectedSourceCode) && this.sources.length > 0) {
          this.selectedSourceCode = this.sources[0].code;
        }
        this.onSelectedSourceChange();
        this.isLoading = false;
      },
      error: (err) => {
        this.isLoading = false;
        this.handleError(err);
      },
    });

    this.refreshJobs();
  }

  refreshSourceStatus() {
    this.isValidating = true;
    this.feedback = '';
    this.errorMessage = '';
    this.api.listSyncSources(true).subscribe({
      next: (sources) => {
        this.sources = sources;
        this.isValidating = false;
        this.feedback = 'Estado de sesiones actualizado.';
      },
      error: (err) => {
        this.isValidating = false;
        this.handleError(err);
      },
    });
  }

  saveCookie() {
    if (!this.selectedSourceCode || !this.cookieText.trim()) {
      this.errorMessage = 'Selecciona una fuente y pega la cookie.';
      return;
    }
    this.isSavingCookie = true;
    this.feedback = '';
    this.errorMessage = '';

    const payload: Record<string, unknown> = { cookie_text: this.cookieText.trim() };
    if (this.expiresAt.trim()) {
      payload['expires_at'] = new Date(this.expiresAt).toISOString();
    }

    this.api.upsertSyncCookie(this.selectedSourceCode, payload).subscribe({
      next: () => {
        this.isSavingCookie = false;
        this.feedback = `Cookie guardada para ${this.selectedSourceCode}.`;
        this.refreshSourceStatus();
      },
      error: (err) => {
        this.isSavingCookie = false;
        this.handleError(err);
      },
    });
  }

  validateSelectedSource() {
    if (!this.selectedSourceCode) {
      return;
    }
    this.isValidating = true;
    this.feedback = '';
    this.errorMessage = '';
    this.api.validateSyncSource(this.selectedSourceCode).subscribe({
      next: (result) => {
        this.isValidating = false;
        this.feedback = result.ok
          ? `Sesion valida en ${this.selectedSourceCode}.`
          : `Sesion invalida en ${this.selectedSourceCode}: ${result.reason ?? 'sin detalle'}.`;
        this.refreshSourceStatus();
      },
      error: (err) => {
        this.isValidating = false;
        this.handleError(err);
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
    this.feedback = '';
    this.errorMessage = '';
    this.syncResult = null;
    this.api.runExternalSync({ mode: this.mode, resources }).subscribe({
      next: (result) => {
        this.isSyncing = false;
        this.syncResult = result;
        this.feedback = 'Sincronizacion ejecutada.';
        this.refreshJobs();
        this.refreshSourceStatus();
      },
      error: (err) => {
        this.isSyncing = false;
        this.handleError(err);
      },
    });
  }

  refreshJobs() {
    this.api.listSyncJobs(20).subscribe({
      next: (jobs) => {
        this.jobs = jobs;
      },
      error: (err) => this.handleError(err),
    });
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
    if (!module.resources.length) {
      return false;
    }
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
      return;
    }
    this.renewalUrl = source.login_url || source.base_url || '';
    this.renewalSteps = this.getRenewalSteps(source.code);
  }

  startCookieRenewal(source: any) {
    this.selectedSourceCode = source.code;
    this.renewalUrl = source.login_url || source.base_url || '';
    this.renewalSteps = this.getRenewalSteps(source.code);
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
