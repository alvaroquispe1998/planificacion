import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { SecurityAdminPanelComponent } from '../settings/security-admin-panel.component';

@Component({
  selector: 'app-security-page',
  standalone: true,
  imports: [CommonModule, SecurityAdminPanelComponent],
  templateUrl: './security.page.html',
  styleUrl: './security.page.css',
})
export class SecurityPageComponent {}
