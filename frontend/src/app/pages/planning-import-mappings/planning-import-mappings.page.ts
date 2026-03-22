import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { finalize, forkJoin } from 'rxjs';
import type { Subscription } from 'rxjs';
import { ApiService } from '../../core/api.service';

@Component({
  selector: 'app-planning-import-mappings-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './planning-import-mappings.page.html',
  styleUrl: './planning-import-mappings.page.css',
})
export class PlanningImportMappingsPageComponent implements OnInit, OnDestroy {
  loading = true;
  saving = false;
  error = '';
  message = '';

  namespaceFilter = '';
  search = '';
  aliases: any[] = [];
  catalog: any = {};
  readonly namespaceLabels: Record<string, string> = {
    campus: 'Sede o local',
    faculty_code: 'Facultad',
    academic_program_code: 'Programa academico',
    study_plan_code: 'Plan de estudios',
    course_code: 'Curso',
    course_modality: 'Modalidad del curso',
    shift: 'Turno',
    building: 'Pabellon o edificio',
    classroom: 'Aula',
    laboratory: 'Laboratorio',
  };

  editingId = '';
  private routeSubscription: Subscription | null = null;
  private bootstrapLoaded = false;
  form = {
    namespace: '',
    source_value: '',
    target_id: '',
    target_label: '',
    is_active: true,
    notes: '',
  };

  constructor(
    private readonly api: ApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.routeSubscription = this.route.queryParamMap.subscribe((params) => {
      this.applyRoutePrefill(params.get('namespace') ?? '', params.get('source_value') ?? '');
      if (!this.bootstrapLoaded) {
        this.loadBootstrap();
        return;
      }
      this.reloadAliases();
    });
  }

  ngOnDestroy(): void {
    this.routeSubscription?.unsubscribe();
    this.routeSubscription = null;
  }

  get availableTargets() {
    switch (this.form.namespace) {
      case 'campus':
        return this.catalog.campuses ?? [];
      case 'faculty_code':
        return this.catalog.faculties ?? [];
      case 'academic_program_code':
        return this.catalog.academic_programs ?? [];
      case 'study_plan_code':
        return this.catalog.study_plans ?? [];
      case 'course_code':
        return this.catalog.course_targets ?? [];
      case 'course_modality':
        return this.catalog.course_modalities ?? [];
      case 'shift':
        return this.catalog.shift_options ?? [];
      case 'building':
        return this.catalog.buildings ?? [];
      case 'classroom':
      case 'laboratory':
        return this.catalog.classrooms ?? [];
      default:
        return [];
    }
  }

  namespaceLabel(namespace: string) {
    return this.namespaceLabels[String(namespace ?? '').trim()] ?? this.humanizeCode(namespace);
  }

  loadBootstrap() {
    this.loading = true;
    this.error = '';
    forkJoin({
      catalog: this.api.getPlanningImportAliasCatalog(),
      aliases: this.api.listPlanningImportAliases({
        namespace: this.namespaceFilter,
        search: this.search,
      }),
    })
      .pipe(
        finalize(() => {
          this.loading = false;
          this.cdr.detectChanges();
        }),
      )
      .subscribe({
        next: ({ catalog, aliases }) => {
          this.bootstrapLoaded = true;
          this.catalog = catalog ?? {};
          this.aliases = aliases ?? [];
          this.applyRoutePrefill(
            this.route.snapshot.queryParamMap.get('namespace') ?? '',
            this.route.snapshot.queryParamMap.get('source_value') ?? '',
          );
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo cargar la pantalla de mapeos.';
          this.cdr.detectChanges();
        },
      });
  }

  reloadAliases() {
    this.loading = true;
    this.api
      .listPlanningImportAliases({
        namespace: this.namespaceFilter,
        search: this.search,
      })
      .pipe(
        finalize(() => {
          this.loading = false;
          this.cdr.detectChanges();
        }),
      )
      .subscribe({
        next: (aliases) => {
          this.aliases = aliases ?? [];
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudieron cargar los mapeos.';
          this.cdr.detectChanges();
        },
      });
  }

  onNamespaceChange() {
    this.form.target_id = '';
    this.form.target_label = '';
    this.namespaceFilter = this.form.namespace;
    this.reloadAliases();
  }

  onTargetChange() {
    const selected = this.availableTargets.find((item: any) => item.id === this.form.target_id);
    this.form.target_label = selected?.label ?? this.form.target_id;
  }

  save() {
    this.saving = true;
    this.error = '';
    this.message = '';
    const request$ = this.editingId
      ? this.api.updatePlanningImportAlias(this.editingId, {
          target_id: this.form.target_id,
          target_label: this.form.target_label,
          is_active: this.form.is_active,
          notes: this.form.notes,
        })
      : this.api.createPlanningImportAlias({
          namespace: this.form.namespace,
          source_value: this.form.source_value,
          target_id: this.form.target_id,
          target_label: this.form.target_label,
          is_active: this.form.is_active,
          notes: this.form.notes,
        });

    request$
      .pipe(
        finalize(() => {
          this.saving = false;
          this.cdr.detectChanges();
        }),
      )
      .subscribe({
        next: (saved) => {
          this.upsertAlias(saved);
          this.message = this.editingId
            ? 'Mapeo actualizado correctamente.'
            : 'Mapeo creado correctamente.';
          this.resetForm();
          this.reloadAliases();
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo guardar el mapeo.';
          this.cdr.detectChanges();
        },
      });
  }

  edit(item: any) {
    this.editingId = item.id;
    this.form = {
      namespace: item.namespace,
      source_value: item.source_value,
      target_id: item.target_id,
      target_label: item.target_label ?? '',
      is_active: item.is_active,
      notes: item.notes ?? '',
    };
  }

  resetForm() {
    this.applyRoutePrefill(
      this.route.snapshot.queryParamMap.get('namespace') ?? '',
      this.route.snapshot.queryParamMap.get('source_value') ?? '',
    );
  }

  backToBatch() {
    const batch = this.route.snapshot.queryParamMap.get('batch');
    if (!batch) {
      this.router.navigate(['/planning/imports']);
      return;
    }
    this.router.navigate(['/planning/imports'], {
      queryParams: { batch },
    });
  }

  private applyRoutePrefill(namespace: string, sourceValue: string) {
    const normalizedNamespace = namespace.trim();
    const normalizedSourceValue = sourceValue.trim();
    this.editingId = '';
    this.namespaceFilter = normalizedNamespace;
    this.form = {
      namespace: normalizedNamespace,
      source_value: normalizedSourceValue,
      target_id: '',
      target_label: '',
      is_active: true,
      notes: '',
    };
  }

  private upsertAlias(saved: any) {
    const savedId = String(saved?.id ?? '').trim();
    if (!savedId) {
      return;
    }
    const existingIndex = this.aliases.findIndex((item) => String(item?.id ?? '').trim() === savedId);
    if (existingIndex >= 0) {
      this.aliases[existingIndex] = {
        ...this.aliases[existingIndex],
        ...saved,
      };
      this.aliases = [...this.aliases];
      return;
    }
    this.aliases = [saved, ...this.aliases];
  }

  private humanizeCode(value: string) {
    return String(value ?? '')
      .trim()
      .toLowerCase()
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (letter) => letter.toUpperCase());
  }
}
