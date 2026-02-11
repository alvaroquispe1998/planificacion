import { Routes } from '@angular/router';
import { AuditPageComponent } from './pages/audit/audit.page';
import { ClassDetailPageComponent } from './pages/class-detail/class-detail.page';
import { ConflictsPageComponent } from './pages/conflicts/conflicts.page';
import { PlanningPageComponent } from './pages/planning/planning.page';
import { SettingsPageComponent } from './pages/settings/settings.page';

import { VideoconferencesPageComponent } from './pages/videoconferences/videoconferences.page';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'planning' },
  { path: 'planning', component: PlanningPageComponent },
  { path: 'conflicts', component: ConflictsPageComponent },
  { path: 'audit', component: AuditPageComponent },
  { path: 'settings', component: SettingsPageComponent },
  { path: 'class-detail/:id', component: ClassDetailPageComponent },
  { path: 'videoconferences', component: VideoconferencesPageComponent },
];
