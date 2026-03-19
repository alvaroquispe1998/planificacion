import { AsyncPipe, CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { filter } from 'rxjs';
import { NavigationEnd, Router, RouterLink, RouterOutlet } from '@angular/router';
import { AppDialogComponent } from './components/app-dialog/app-dialog.component';
import { AuthService } from './core/auth.service';
import {
  ModuleNavItem,
  SecondaryNavItem,
  isNavChildActive,
  moduleDefaultPath,
  resolveModuleFromUrl,
  visibleChildren,
  visibleNavGroups,
} from './core/navigation';

@Component({
  selector: 'app-root',
  imports: [CommonModule, AsyncPipe, RouterOutlet, RouterLink, AppDialogComponent],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  readonly session$;
  readonly ready$;
  currentUrl = '';
  expandedModules: Record<string, boolean> = {};
  readonly iconPaths: Record<string, string> = {
    clipboard:
      'M9 4.75A2.25 2.25 0 0 1 11.25 2.5h1.5A2.25 2.25 0 0 1 15 4.75V5h1.25A2.75 2.75 0 0 1 19 7.75v8.5A2.75 2.75 0 0 1 16.25 19h-8.5A2.75 2.75 0 0 1 5 16.25v-8.5A2.75 2.75 0 0 1 7.75 5H9v-.25Zm1.5.25h3v-.25a.75.75 0 0 0-.75-.75h-1.5a.75.75 0 0 0-.75.75V5Zm-2.75 2A.75.75 0 0 0 7 7.75v8.5c0 .414.336.75.75.75h8.5a.75.75 0 0 0 .75-.75v-8.5a.75.75 0 0 0-.75-.75h-8.5Z',
    layers:
      'M12 3.75 4.5 7.5 12 11.25 19.5 7.5 12 3.75Zm-6.375 6.28L12 13.22l6.375-3.19.9 1.8L12 15.47l-7.275-3.64.9-1.8Zm0 3.75L12 16.97l6.375-3.19.9 1.8L12 19.22l-7.275-3.64.9-1.8Z',
    shuffle:
      'M16.28 4H20v3.72h-1.75V6.98h-2.07l-2.64 3.3 1.15 1.43-1.37 1.1-1.28-1.6-2.2 2.76H4V12.2h4.22l2.63-3.3-2.63-3.28H4V3.85h6.02l2.08 2.6L14.74 4h1.54Zm1.97 12.02V15.3H20V19h-3.7v-1.75h1.94l-3.5-4.37 1.37-1.1 3.61 4.24ZM4 18.15v-1.77h4.22l2.01-2.53 1.38 1.1-2.53 3.2H4Z',
    history:
      'M12 4a8 8 0 1 1-7.75 10H6v2H1.75V11.75h2v2.18A10 10 0 1 0 12 2c2.35 0 4.5.8 6.2 2.14l-1.28 1.52A7.96 7.96 0 0 0 12 4Zm-.75 3.25h1.5v4.19l3.1 1.79-.75 1.3-3.85-2.22V7.25Z',
    camera:
      'M7.5 6.5 9 4.5h6l1.5 2H18A2.5 2.5 0 0 1 20.5 9v7A2.5 2.5 0 0 1 18 18.5H6A2.5 2.5 0 0 1 3.5 16V9A2.5 2.5 0 0 1 6 6.5h1.5ZM12 8a4.5 4.5 0 1 0 0 9 4.5 4.5 0 0 0 0-9Zm0 2a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5Z',
    spark:
      'M11.25 3 8.9 8.15 3.75 10.5l5.15 2.35L11.25 18l2.35-5.15 5.15-2.35-5.15-2.35L11.25 3Zm6 11 1 2.2L20.5 17l-2.25.8-1 2.2-1-2.2L14 17l2.25-.8 1-2.2Z',
    search:
      'M10.5 4a6.5 6.5 0 0 1 5.11 10.52l4.19 4.2-1.41 1.4-4.2-4.19A6.5 6.5 0 1 1 10.5 4Zm0 2a4.5 4.5 0 1 0 0 9 4.5 4.5 0 0 0 0-9Z',
    link:
      'M8.8 14.2a3 3 0 0 1 0-4.24l2.47-2.47 1.41 1.41-2.47 2.47a1 1 0 1 0 1.41 1.41l2.47-2.47 1.41 1.41-2.47 2.47a3 3 0 0 1-4.23.02Zm2.52 2.3 2.47-2.47-1.41-1.41-2.47 2.47a1 1 0 1 1-1.41-1.41l2.47-2.47-1.41-1.41-2.47 2.47a3 3 0 0 0 4.23 4.23Z',
    refresh:
      'M18.5 12a6.5 6.5 0 0 0-11.13-4.6V5.75H5V11h5.25V8.75H8.33A4.5 4.5 0 1 1 7.5 12H5.5a6.5 6.5 0 1 0 13-1h-2Z',
    shield:
      'M12 3.5 5.5 6v4.22c0 4.15 2.7 7.99 6.5 9.28 3.8-1.3 6.5-5.13 6.5-9.28V6L12 3.5Zm0 2.14 4.5 1.73v2.85c0 3.15-1.97 6.1-4.5 7.22-2.53-1.12-4.5-4.07-4.5-7.22V7.37L12 5.64Z',
    key:
      'M9.5 13a4.5 4.5 0 1 1 3.89-2.24l6.36-.01v2h-1.5v1.5h-1.5v1.5h-2.25v-2.13l-1.11.01A4.48 4.48 0 0 1 9.5 13Zm0-2a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5Z',
    users:
      'M8.75 10.5a3.25 3.25 0 1 1 0-6.5 3.25 3.25 0 0 1 0 6.5Zm6.5 1.25a2.75 2.75 0 1 1 0-5.5 2.75 2.75 0 0 1 0 5.5ZM3.5 18a4.75 4.75 0 0 1 9.5 0v.5h-9.5V18Zm10.5.5V18c0-.94-.24-1.82-.67-2.58A3.98 3.98 0 0 1 20.5 18v.5H14Z',
    badge:
      'M12 3.5 14.2 8h4.8l-3.6 3.3.95 4.95L12 13.9l-4.35 2.35.95-4.95L5 8h4.8L12 3.5Zm0 12.65 3.02 1.64-.66-3.46 2.52-2.3h-3.37L12 8.8l-1.51 3.23H7.12l2.52 2.3-.66 3.46L12 16.15Z',
    chevron:
      'm8.47 9.53 3.53 3.53 3.53-3.53 1.06 1.06-4.59 4.59-4.59-4.59 1.06-1.06Z',
  };

  constructor(
    readonly auth: AuthService,
    private readonly router: Router,
  ) {
    this.session$ = this.auth.session$;
    this.ready$ = this.auth.ready$;
    this.currentUrl = this.router.url;
    this.router.events.pipe(filter((event): event is NavigationEnd => event instanceof NavigationEnd)).subscribe((event) => {
      this.currentUrl = event.urlAfterRedirects;
      this.ensureCurrentModuleExpanded();
    });
    this.ensureCurrentModuleExpanded();
  }

  logout() {
    this.auth.logout().subscribe();
  }

  getRoleNames(session: { roles: Array<{ name: string }> }) {
    return session.roles.map((role) => role.name).join(', ');
  }

  get navGroups() {
    return visibleNavGroups(this.auth.snapshot?.windows ?? [], this.auth.snapshot?.permissions ?? []);
  }

  get currentModule() {
    return resolveModuleFromUrl(this.currentUrl);
  }

  get currentSecondaryNav() {
    const module = this.findCurrentModule();
    if (!module) {
      return [];
    }
    return this.visibleModuleChildren(module);
  }

  modulePath(module: ModuleNavItem) {
    return moduleDefaultPath(module, this.auth.snapshot?.windows ?? [], this.auth.snapshot?.permissions ?? []);
  }

  isModuleActive(module: ModuleNavItem) {
    return this.currentModule === module.key;
  }

  isSecondaryActive(item: SecondaryNavItem) {
    return isNavChildActive(item, this.currentUrl);
  }

  visibleModuleChildren(module: ModuleNavItem) {
    return visibleChildren(module, this.auth.snapshot?.windows ?? [], this.auth.snapshot?.permissions ?? []);
  }

  isModuleExpanded(module: ModuleNavItem) {
    return this.expandedModules[module.key] ?? this.isModuleActive(module);
  }

  toggleModule(module: ModuleNavItem) {
    const currentlyExpanded = this.isModuleExpanded(module);
    this.expandedModules = {
      ...this.expandedModules,
      [module.key]: !currentlyExpanded,
    };
  }

  iconPath(name: string) {
    return this.iconPaths[name] ?? this.iconPaths['layers'];
  }

  private findCurrentModule() {
    const currentModule = this.currentModule;
    if (!currentModule) {
      return null;
    }
    for (const group of this.navGroups) {
      const found = group.modules.find((module) => module.key === currentModule);
      if (found) {
        return found;
      }
    }
    return null;
  }

  private ensureCurrentModuleExpanded() {
    const currentModule = this.currentModule;
    if (!currentModule) {
      return;
    }
    this.expandedModules = {
      ...this.expandedModules,
      [currentModule]: true,
    };
  }
}
