import { CommonModule } from '@angular/common';
import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../../core/api.service';

type ZoomPoolItem = {
  id?: string;
  zoom_user_id: string;
  sort_order: number;
  is_active: boolean;
  name: string | null;
  email: string | null;
};

type ZoomPoolUser = {
  id: string;
  name: string | null;
  email: string | null;
  in_pool: boolean;
};

type ZoomPoolResponse = {
  items: ZoomPoolItem[];
  users: ZoomPoolUser[];
};

@Component({
  selector: 'app-videoconference-zoom-users-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './videoconference-zoom-users.page.html',
  styleUrl: './videoconference-zoom-users.page.css',
})
export class VideoconferenceZoomUsersPageComponent implements OnInit {
  loading = true;
  saving = false;
  message = '';
  error = '';
  search = '';

  poolItems: ZoomPoolItem[] = [];
  users: ZoomPoolUser[] = [];

  private initialFingerprint = '[]';

  constructor(
    private readonly api: ApiService,
    private readonly cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadPage();
  }

  get hasUsers() {
    return this.users.length > 0;
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
    this.api.getZoomPool().subscribe({
      next: (response) => {
        this.applyResponse(response);
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        this.error = err?.error?.message ?? 'No se pudo cargar la configuracion de usuarios Zoom.';
        this.loading = false;
        this.cdr.detectChanges();
      },
    });
  }

  addUser(user: ZoomPoolUser) {
    if (this.poolItems.some((item) => item.zoom_user_id === user.id)) {
      return;
    }
    this.poolItems = this.reindexPool([
      ...this.poolItems,
      {
        zoom_user_id: user.id,
        sort_order: this.poolItems.length + 1,
        is_active: true,
        name: user.name,
        email: user.email,
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

  save() {
    if (this.saving) {
      return;
    }
    this.saving = true;
    this.error = '';
    this.message = '';
    this.api
      .updateZoomPool({
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
    if (this.loading || this.saving) {
      return;
    }
    this.loadPage();
  }

  userLabel(user: { id?: string; zoom_user_id?: string; name: string | null; email: string | null }) {
    const identifier = user.id || user.zoom_user_id || '';
    const head = user.name?.trim() || user.email?.trim() || identifier;
    const tail = user.name?.trim() && user.email?.trim() ? user.email.trim() : identifier;
    return tail && tail !== head ? `${head} | ${tail}` : head;
  }

  private applyResponse(response: ZoomPoolResponse) {
    const users = Array.isArray(response?.users) ? response.users : [];
    const userMap = new Map(users.map((user) => [user.id, user]));
    const items = Array.isArray(response?.items) ? response.items : [];

    this.users = users
      .map((user) => ({
        id: user.id,
        name: user.name ?? null,
        email: user.email ?? null,
        in_pool: Boolean(user.in_pool),
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
          } satisfies ZoomPoolItem;
        })
        .sort((left, right) => left.sort_order - right.sort_order),
    );

    this.initialFingerprint = this.snapshotFingerprint(this.poolItems);
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
