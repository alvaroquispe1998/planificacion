import { Routes } from '@angular/router';
import { loginRedirectGuard, permissionGuard, windowGuard } from './core/auth.guard';
import { AuditPageComponent } from './pages/audit/audit.page';
import { ClassDetailPageComponent } from './pages/class-detail/class-detail.page';
import { ConflictsPageComponent } from './pages/conflicts/conflicts.page';
import { LoginPageComponent } from './pages/login/login.page';
import { PlanningPageComponent } from './pages/planning/planning.page';
import { PlanningCycleDetailPageComponent } from './pages/planning-cycle-detail/planning-cycle-detail.page';
import { PlanningCycleEditorPageComponent } from './pages/planning-cycle-editor/planning-cycle-editor.page';
import { PlanningOfferSectionsPageComponent } from './pages/planning-offer-sections/planning-offer-sections.page';
import { SecurityPageComponent } from './pages/security/security.page';
import { SettingsPageComponent } from './pages/settings/settings.page';

import { VideoconferencesPageComponent } from './pages/videoconferences/videoconferences.page';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'planning' },
  { path: 'login', component: LoginPageComponent, canActivate: [loginRedirectGuard] },
  {
    path: 'planning',
    component: PlanningPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.planning' },
  },
  {
    path: 'planning/cycle-detail',
    component: PlanningCycleDetailPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.planning' },
  },
  {
    path: 'planning/cycle-editor',
    component: PlanningCycleEditorPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.planning' },
  },
  {
    path: 'planning/offers/:offerId/sections',
    component: PlanningOfferSectionsPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.planning' },
  },
  {
    path: 'conflicts',
    component: ConflictsPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.conflicts' },
  },
  {
    path: 'audit',
    component: AuditPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.audit' },
  },
  {
    path: 'settings',
    component: SettingsPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.settings' },
  },
  {
    path: 'security',
    component: SecurityPageComponent,
    canActivate: [permissionGuard],
    data: { requiredPermission: 'action.users.manage' },
  },
  {
    path: 'class-detail/:id',
    component: ClassDetailPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.planning' },
  },
  {
    path: 'videoconferences',
    component: VideoconferencesPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.videoconferences' },
  },
];
