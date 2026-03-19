import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { SecurityAdminPanelComponent } from '../settings/security-admin-panel.component';

@Component({
  selector: 'app-security-page',
  standalone: true,
  imports: [CommonModule, SecurityAdminPanelComponent],
  templateUrl: './security.page.html',
  styleUrl: './security.page.css',
})
export class SecurityPageComponent {
  activeView: 'users' | 'roles' = 'users';

  constructor(private readonly route: ActivatedRoute) {
    this.route.data.subscribe((data) => {
      this.activeView = data['securityView'] === 'roles' ? 'roles' : 'users';
    });
  }

  get title() {
    return this.activeView === 'roles' ? 'Roles y privilegios' : 'Usuarios y accesos';
  }

  get description() {
    return this.activeView === 'roles'
      ? 'Configura ventanas, acciones y privilegios disponibles para cada rol del sistema.'
      : 'Administra cuentas, activacion y alcance por facultad o programa academico.';
  }
}
