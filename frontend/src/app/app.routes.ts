import { Routes } from '@angular/router';
import { loginRedirectGuard, permissionGuard, securityLandingGuard, windowGuard } from './core/auth.guard';
import { AuditPageComponent } from './pages/audit/audit.page';
import { ClassDetailPageComponent } from './pages/class-detail/class-detail.page';
import { ConflictsPageComponent } from './pages/conflicts/conflicts.page';
import { LoginPageComponent } from './pages/login/login.page';
import { PlanningPageComponent } from './pages/planning/planning.page';
import { PlanningCycleDetailPageComponent } from './pages/planning-cycle-detail/planning-cycle-detail.page';
import { PlanningCycleEditorPageComponent } from './pages/planning-cycle-editor/planning-cycle-editor.page';
import { PlanningChangeLogPageComponent } from './pages/planning-change-log/planning-change-log.page';
import { PlanningOfferSectionsPageComponent } from './pages/planning-offer-sections/planning-offer-sections.page';
import { PlanningImportMappingsPageComponent } from './pages/planning-import-mappings/planning-import-mappings.page';
import { PlanningImportsPageComponent } from './pages/planning-imports/planning-imports.page';
import { PlanningVcMatchPageComponent } from './pages/planning-vc-match/planning-vc-match.page';
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
    path: 'planning/change-log',
    component: PlanningChangeLogPageComponent,
    canActivate: [windowGuard, permissionGuard],
    data: {
      requiredWindow: 'window.planning',
      requiredPermission: 'action.planning.change_log.view',
    },
  },
  {
    path: 'planning/vc-match',
    component: PlanningVcMatchPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.planning' },
  },
  {
    path: 'planning/imports',
    component: PlanningImportsPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.planning' },
  },
  {
    path: 'planning/import-mappings',
    component: PlanningImportMappingsPageComponent,
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
    path: 'planning/conflicts',
    component: ConflictsPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.conflicts' },
  },
  {
    path: 'videoconferences/audit',
    component: AuditPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.audit' },
  },
  {
    path: 'integrations/sync',
    component: SettingsPageComponent,
    canActivate: [windowGuard],
    data: { requiredWindow: 'window.settings' },
  },
  {
    path: 'admin/security/users',
    component: SecurityPageComponent,
    canActivate: [windowGuard, permissionGuard],
    data: {
      requiredWindow: 'window.security',
      requiredPermission: 'action.users.manage',
      securityView: 'users',
    },
  },
  {
    path: 'admin/security/roles',
    component: SecurityPageComponent,
    canActivate: [windowGuard, permissionGuard],
    data: {
      requiredWindow: 'window.security',
      requiredPermissions: ['action.roles.manage', 'action.permissions.manage'],
      securityView: 'roles',
    },
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
  { path: 'conflicts', pathMatch: 'full', redirectTo: 'planning/conflicts' },
  { path: 'audit', pathMatch: 'full', redirectTo: 'videoconferences/audit' },
  { path: 'settings', pathMatch: 'full', redirectTo: 'integrations/sync' },
  { path: 'admin/security', pathMatch: 'full', canActivate: [securityLandingGuard], component: SecurityPageComponent },
  { path: 'security', pathMatch: 'full', canActivate: [securityLandingGuard], component: SecurityPageComponent },
];
