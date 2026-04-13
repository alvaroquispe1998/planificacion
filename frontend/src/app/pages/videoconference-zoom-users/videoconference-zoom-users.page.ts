import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { ApiService } from '../../core/api.service';
import { DialogService } from '../../core/dialog.service';

type ZoomPoolItem = {
  id?: string;
  zoom_user_id: string;
  sort_order: number;
  is_active: boolean;
  name: string | null;
  email: string | null;
  license_status: 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN';
  license_label: string;
  is_licensed: boolean | null;
};

type ZoomPoolUser = {
  id: string;
  name: string | null;
  email: string | null;
  in_pool: boolean;
  license_status: 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN';
  license_label: string;
  is_licensed: boolean | null;
};

type ZoomPoolResponse = {
  group?: {
    id: string;
    name: string;
    code: string;
    is_active: boolean;
  };
  items: ZoomPoolItem[];
  users: ZoomPoolUser[];
  license_sync_ok: boolean;
  license_sync_error: string | null;
};

type ZoomGroup = {
  id: string;
  name: string;
  code: string;
  is_active: boolean;
  is_default?: boolean;
  members_count?: number;
  active_members_count?: number;
};

type LicenseFilter = 'ALL' | 'NOT_FOUND' | 'BASIC' | 'LICENSED';

@Component({
  selector: 'app-videoconference-zoom-users-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './videoconference-zoom-users.page.html',
  styleUrl: './videoconference-zoom-users.page.css',
})
export class VideoconferenceZoomUsersPageComponent implements OnInit {
  loading = true;
  saving = false;
  managingGroups = false;
  message = '';
  error = '';
  search = '';
  availableLicenseFilter: LicenseFilter = 'ALL';
  licenseSyncOk = true;
  licenseSyncError = '';

  poolItems: ZoomPoolItem[] = [];
  users: ZoomPoolUser[] = [];
  groups: ZoomGroup[] = [];
  selectedGroupId = '';

  private initialFingerprint = '[]';

  constructor(
    private readonly api: ApiService,
    private readonly cdr: ChangeDetectorRef,
    private readonly dialog: DialogService,
  ) {}

  ngOnInit(): void {
    this.loadPage();
  }

  get hasUsers() {
    return this.users.length > 0;
  }

  get selectedGroup() {
    return this.groups.find((item) => item.id === this.selectedGroupId) ?? null;
  }

  get hasSelectedGroup() {
    return Boolean(this.selectedGroupId);
  }

  get selectedCount() {
    return this.poolItems.length;
  }

  get activeCount() {
    return this.poolItems.filter((item) => item.is_active).length;
  }

  get inactiveCount() {
    return Math.max(0, this.poolItems.length - this.activeCount);
  }

  get availableCount() {
    const selected = new Set(this.poolItems.map((item) => item.zoom_user_id));
    return this.users.filter((user) => !selected.has(user.id)).length;
  }

  get filteredAvailableUsers() {
    const selected = new Set(this.poolItems.map((item) => item.zoom_user_id));
    const search = this.normalize(this.search);
    return this.users.filter((user) => {
      if (selected.has(user.id)) {
        return false;
      }
      if (this.availableLicenseFilter !== 'ALL' && this.licenseFilterKey(user) !== this.availableLicenseFilter) {
        return false;
      }
      if (!search) {
        return true;
      }
      return this.normalize(this.userLabel(user)).includes(search);
    });
  }

  get isDirty() {
    return this.snapshotFingerprint(this.poolItems) !== this.initialFingerprint;
  }

  loadPage() {
    this.loading = true;
    this.error = '';
    this.message = '';
    this.api.listZoomGroups().subscribe({
      next: (groups) => {
        this.groups = Array.isArray(groups) ? groups : [];
        if (!this.groups.length) {
          this.poolItems = [];
          this.users = [];
          this.selectedGroupId = '';
          this.loading = false;
          this.cdr.detectChanges();
          return;
        }
        if (!this.selectedGroupId || !this.groups.some((item) => item.id === this.selectedGroupId)) {
          this.selectedGroupId = this.groups[0]?.id || '';
        }
        this.loadSelectedGroupPool();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo cargar los grupos Zoom.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  onGroupChange(value: string) {
    this.selectedGroupId = value;
    this.loadSelectedGroupPool();
  }

  private loadSelectedGroupPool() {
    if (!this.selectedGroupId) {
      this.poolItems = [];
      this.users = [];
      this.loading = false;
      this.cdr.detectChanges();
      return;
    }
    this.loading = true;
    this.api.getZoomGroupPool(this.selectedGroupId).subscribe({
      next: (response) => {
        this.applyResponse(response);
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo cargar el pool del grupo Zoom.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  async createGroup() {
    const name = window.prompt('Nombre del grupo Zoom');
    if (!name?.trim()) {
      return;
    }
    const code = window.prompt('Codigo del grupo (opcional)');
    this.managingGroups = true;
    this.error = '';
    this.message = '';
    this.api.createZoomGroup({
      name: name.trim(),
      code: code?.trim() || undefined,
      is_active: true,
    }).subscribe({
      next: (created) => {
        this.managingGroups = false;
        this.selectedGroupId = created?.id || this.selectedGroupId;
        this.message = 'Grupo Zoom creado.';
        this.loadPage();
      },
      error: (err) => {
        this.managingGroups = false;
        this.error = err?.error?.message ?? 'No se pudo crear el grupo Zoom.';
        this.cdr.detectChanges();
      },
    });
  }

  async editSelectedGroup() {
    if (!this.selectedGroup) {
      return;
    }
    const current = this.selectedGroup;
    const nextName = window.prompt('Nombre del grupo Zoom', current.name || '');
    if (!nextName?.trim()) {
      return;
    }
    const nextCode = window.prompt('Codigo del grupo', current.code || '') ?? current.code;
    this.managingGroups = true;
    this.error = '';
    this.message = '';
    this.api.updateZoomGroup(current.id, {
      name: nextName.trim(),
      code: nextCode.trim(),
    }).subscribe({
      next: () => {
        this.managingGroups = false;
        this.message = 'Grupo Zoom actualizado.';
        this.loadPage();
      },
      error: (err) => {
        this.managingGroups = false;
        this.error = err?.error?.message ?? 'No se pudo actualizar el grupo Zoom.';
        this.cdr.detectChanges();
      },
    });
  }

  async toggleSelectedGroupActive() {
    if (!this.selectedGroup) {
      return;
    }
    const target = this.selectedGroup;
    this.managingGroups = true;
    this.error = '';
    this.message = '';
    this.api.updateZoomGroup(target.id, {
      is_active: !target.is_active,
    }).subscribe({
      next: () => {
        this.managingGroups = false;
        this.message = 'Estado del grupo Zoom actualizado.';
        this.loadPage();
      },
      error: (err) => {
        this.managingGroups = false;
        this.error = err?.error?.message ?? 'No se pudo actualizar el grupo Zoom.';
        this.cdr.detectChanges();
      },
    });
  }

  async deleteSelectedGroup() {
    if (!this.selectedGroup) {
      return;
    }
    const target = this.selectedGroup;
    const confirmed = await this.dialog.confirm({
      title: 'Eliminar grupo Zoom',
      message: `Se eliminara el grupo ${target.name}. Esta accion no elimina usuarios Zoom, solo su agrupacion. Deseas continuar?`,
      confirmLabel: 'Eliminar',
      cancelLabel: 'Cancelar',
      tone: 'danger',
    });
    if (!confirmed) {
      return;
    }

    this.managingGroups = true;
    this.error = '';
    this.message = '';
    this.api.deleteZoomGroup(target.id).subscribe({
      next: () => {
        this.managingGroups = false;
        this.message = 'Grupo Zoom eliminado.';
        this.selectedGroupId = '';
        this.loadPage();
      },
      error: (err) => {
        this.managingGroups = false;
        this.error = err?.error?.message ?? 'No se pudo eliminar el grupo Zoom.';
        this.cdr.detectChanges();
      },
    });
  }

  async addUser(user: ZoomPoolUser) {
    if (this.poolItems.some((item) => item.zoom_user_id === user.id)) {
      return;
    }

    if (!this.canAddUser(user)) {
      return;
    }

    if (this.licenseFilterKey(user) === 'BASIC') {
      const confirmed = await this.dialog.confirm({
        title: 'Usuario Zoom con licencia basica',
        message: `${this.userLabel(user)} solo tiene licencia basica. Si lo agregas al pool, al generar el sistema te advertira para decidir si deseas usarlo. Deseas continuar de todos modos?`,
        confirmLabel: 'Agregar igual',
        cancelLabel: 'Cancelar',
      });
      if (!confirmed) {
        return;
      }
    }

    this.poolItems = this.reindexPool([
      ...this.poolItems,
      {
        zoom_user_id: user.id,
        sort_order: this.poolItems.length + 1,
        is_active: true,
        name: user.name,
        email: user.email,
        license_status: user.license_status,
        license_label: user.license_label,
        is_licensed: user.is_licensed,
      },
    ]);
  }

  removeUser(zoomUserId: string) {
    this.poolItems = this.reindexPool(
      this.poolItems.filter((item) => item.zoom_user_id !== zoomUserId),
    );
  }

  moveUp(index: number) {
    if (index <= 0) {
      return;
    }
    const items = [...this.poolItems];
    const current = items[index];
    items[index] = items[index - 1];
    items[index - 1] = current;
    this.poolItems = this.reindexPool(items);
  }

  moveDown(index: number) {
    if (index >= this.poolItems.length - 1) {
      return;
    }
    const items = [...this.poolItems];
    const current = items[index];
    items[index] = items[index + 1];
    items[index + 1] = current;
    this.poolItems = this.reindexPool(items);
  }

  async save() {
    if (this.saving) {
      return;
    }
    if (!this.selectedGroupId) {
      this.error = 'Selecciona un grupo Zoom para guardar su pool.';
      this.cdr.detectChanges();
      return;
    }

    const activeUnlicensed = this.poolItems.filter(
      (item) => item.is_active && item.is_licensed !== true,
    );
    if (activeUnlicensed.length > 0) {
      const confirmed = await this.dialog.confirm({
        title: 'Pool con usuarios sin licencia verificada',
        message: `Hay ${activeUnlicensed.length} usuario(s) activos con licencia basica o no verificada. Al generar, el sistema te mostrara una advertencia para decidir si deseas usarlos. Deseas guardar el pool de todos modos?`,
        confirmLabel: 'Guardar igual',
        cancelLabel: 'Cancelar',
      });
      if (!confirmed) {
        return;
      }
    }

    this.saving = true;
    this.error = '';
    this.message = '';
    this.api
      .updateZoomGroupPool(this.selectedGroupId, {
        items: this.poolItems.map((item, index) => ({
          zoom_user_id: item.zoom_user_id,
          sort_order: index + 1,
          is_active: Boolean(item.is_active),
        })),
      })
      .subscribe({
        next: (response) => {
          this.applyResponse(response);
          this.message = 'Pool de usuarios Zoom actualizado.';
          this.saving = false;
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.error = err?.error?.message ?? 'No se pudo guardar el pool de usuarios Zoom.';
          this.saving = false;
          this.cdr.detectChanges();
        },
      });
  }

  reload() {
    if (this.loading || this.saving || this.managingGroups) {
      return;
    }
    this.loadPage();
  }

  canAddUser(user: ZoomPoolUser) {
    return this.licenseFilterKey(user) !== 'NOT_FOUND';
  }

  licenseFilterKey(user: { license_status: 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN' }) {
    switch (user.license_status) {
      case 'LICENSED':
      case 'ON_PREM':
        return 'LICENSED' as const;
      case 'BASIC':
        return 'BASIC' as const;
      default:
        return 'NOT_FOUND' as const;
    }
  }

  userLabel(user: { id?: string; zoom_user_id?: string; name: string | null; email: string | null }) {
    const identifier = user.id || user.zoom_user_id || '';
    const head = user.name?.trim() || user.email?.trim() || identifier;
    const tail = user.name?.trim() && user.email?.trim() ? user.email.trim() : identifier;
    return tail && tail !== head ? `${head} | ${tail}` : head;
  }

  private applyResponse(response: ZoomPoolResponse) {
    if (response?.group?.id) {
      this.selectedGroupId = response.group.id;
    }
    const users = Array.isArray(response?.users) ? response.users : [];
    const userMap = new Map(users.map((user) => [user.id, user]));
    const items = Array.isArray(response?.items) ? response.items : [];

    this.users = users
      .map((user) => ({
        id: user.id,
        name: user.name ?? null,
        email: user.email ?? null,
        in_pool: Boolean(user.in_pool),
        license_status: user.license_status ?? 'UNKNOWN',
        license_label: user.license_label ?? 'No verificado',
        is_licensed: user.is_licensed ?? null,
      }))
      .sort((left, right) => this.userLabel(left).localeCompare(this.userLabel(right)));

    this.poolItems = this.reindexPool(
      items
        .map((item) => {
          const user = userMap.get(item.zoom_user_id);
          return {
            id: item.id,
            zoom_user_id: item.zoom_user_id,
            sort_order: Number(item.sort_order ?? 0),
            is_active: Boolean(item.is_active),
            name: item.name ?? user?.name ?? null,
            email: item.email ?? user?.email ?? null,
            license_status: item.license_status ?? user?.license_status ?? 'UNKNOWN',
            license_label: item.license_label ?? user?.license_label ?? 'No verificado',
            is_licensed: item.is_licensed ?? user?.is_licensed ?? null,
          } satisfies ZoomPoolItem;
        })
        .sort((left, right) => left.sort_order - right.sort_order),
    );

    this.licenseSyncOk = response?.license_sync_ok !== false;
    this.licenseSyncError = response?.license_sync_error ?? '';
    this.initialFingerprint = this.snapshotFingerprint(this.poolItems);
  }

  licenseBadgeClass(item: { license_status: 'LICENSED' | 'BASIC' | 'ON_PREM' | 'UNKNOWN' }) {
    switch (item.license_status) {
      case 'LICENSED':
      case 'ON_PREM':
        return 'license-badge-ok';
      case 'BASIC':
        return 'license-badge-warning';
      default:
        return 'license-badge-unknown';
    }
  }

  private reindexPool(items: ZoomPoolItem[]) {
    return items.map((item, index) => ({
      ...item,
      sort_order: index + 1,
    }));
  }

  private snapshotFingerprint(items: ZoomPoolItem[]) {
    return JSON.stringify(
      items.map((item, index) => ({
        zoom_user_id: item.zoom_user_id,
        sort_order: index + 1,
        is_active: Boolean(item.is_active),
      })),
    );
  }

  private normalize(value: string | null | undefined) {
    return `${value ?? ''}`
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim()
      .toUpperCase();
  }
}
