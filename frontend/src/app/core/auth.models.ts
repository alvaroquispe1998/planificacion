export type WindowCode =
  | 'window.planning'
  | 'window.conflicts'
  | 'window.audit'
  | 'window.videoconferences'
  | 'window.settings';

export type PermissionCode =
  | WindowCode
  | 'action.users.manage'
  | 'action.roles.manage'
  | 'action.permissions.manage'
  | 'action.settings.manage'
  | 'action.planning.change_log.view'
  | 'action.planning.plan.submit_review'
  | 'action.planning.plan.review_decide';

export type AccessScope = {
  assignment_id: string;
  role_id: string;
  role_code: string;
  role_name: string;
  faculty_id: string | null;
  faculty_name: string | null;
  academic_program_id: string | null;
  academic_program_name: string | null;
  is_global: boolean;
};

export type RoleSummary = {
  id: string;
  code: string;
  name: string;
};

export type CurrentUser = {
  id: string;
  username: string;
  display_name: string;
  email: string | null;
  is_active?: boolean;
};

export type AuthResponse = {
  access_token: string;
  refresh_token: string;
  user: CurrentUser;
  roles: RoleSummary[];
  role_assignments: AccessScope[];
  permissions: PermissionCode[];
  scopes: AccessScope[];
  windows: WindowCode[];
};

export type SessionState = {
  accessToken: string;
  refreshToken: string;
  user: CurrentUser;
  roles: RoleSummary[];
  scopes: AccessScope[];
  permissions: PermissionCode[];
  windows: WindowCode[];
};
