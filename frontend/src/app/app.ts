import { AsyncPipe, CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { AuthService } from './core/auth.service';
import { APP_NAV_ITEMS, NavItem } from './core/navigation';

@Component({
  selector: 'app-root',
  imports: [CommonModule, AsyncPipe, RouterOutlet, RouterLink, RouterLinkActive],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  readonly session$;
  readonly ready$;
  readonly navItems = APP_NAV_ITEMS;

  constructor(readonly auth: AuthService) {
    this.session$ = this.auth.session$;
    this.ready$ = this.auth.ready$;
  }

  logout() {
    this.auth.logout().subscribe();
  }

  getRoleNames(session: { roles: Array<{ name: string }> }) {
    return session.roles.map((role) => role.name).join(', ');
  }

  canAccessNav(item: NavItem) {
    if (item.window) {
      return this.auth.hasWindow(item.window);
    }
    if (item.permission) {
      return this.auth.hasPermission(item.permission);
    }
    return true;
  }
}
