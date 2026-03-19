export const WINDOW_PERMISSIONS = {
  PLANNING: 'window.planning',
  CONFLICTS: 'window.conflicts',
  AUDIT: 'window.audit',
  VIDEOCONFERENCES: 'window.videoconferences',
  SETTINGS: 'window.settings',
} as const;

export const ACTION_PERMISSIONS = {
  USERS_MANAGE: 'action.users.manage',
  ROLES_MANAGE: 'action.roles.manage',
  PERMISSIONS_MANAGE: 'action.permissions.manage',
  SETTINGS_MANAGE: 'action.settings.manage',
  PLANNING_CHANGE_LOG_VIEW: 'action.planning.change_log.view',
  PLANNING_PLAN_SUBMIT_REVIEW: 'action.planning.plan.submit_review',
  PLANNING_PLAN_REVIEW_DECIDE: 'action.planning.plan.review_decide',
} as const;

export const ROLE_CODES = {
  ADMIN: 'ADMIN',
  ADMINISTRATIVE: 'ADMINISTRATIVE',
  IT_SUPPORT: 'IT_SUPPORT',
} as const;

export const AUTH_PUBLIC_KEY = 'auth:public';
export const AUTH_PERMISSIONS_KEY = 'auth:permissions';

export const ACCESS_TOKEN_DEFAULT_EXPIRES = '15m';
export const REFRESH_TOKEN_DEFAULT_EXPIRES_DAYS = 7;

export const DEFAULT_ADMIN_USERNAME = 'admin';
export const DEFAULT_ADMIN_PASSWORD = 'admin123';

export const PERMISSION_SEEDS = [
  {
    code: WINDOW_PERMISSIONS.PLANNING,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de planificacion.',
    display_name: 'Planificacion',
    group_key: 'planning',
    parent_window_code: null,
    sort_order: 10,
  },
  {
    code: WINDOW_PERMISSIONS.CONFLICTS,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de cruces.',
    display_name: 'Cruces',
    group_key: 'conflicts',
    parent_window_code: null,
    sort_order: 20,
  },
  {
    code: WINDOW_PERMISSIONS.AUDIT,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de auditoria Zoom.',
    display_name: 'Auditoria Zoom',
    group_key: 'audit',
    parent_window_code: null,
    sort_order: 30,
  },
  {
    code: WINDOW_PERMISSIONS.VIDEOCONFERENCES,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de videoconferencias.',
    display_name: 'Videoconferencias',
    group_key: 'videoconferences',
    parent_window_code: null,
    sort_order: 40,
  },
  {
    code: WINDOW_PERMISSIONS.SETTINGS,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de configuracion.',
    display_name: 'Configuracion',
    group_key: 'settings',
    parent_window_code: null,
    sort_order: 50,
  },
  {
    code: ACTION_PERMISSIONS.USERS_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion de usuarios.',
    display_name: 'Gestionar usuarios',
    group_key: 'global_admin',
    parent_window_code: null,
    sort_order: 100,
  },
  {
    code: ACTION_PERMISSIONS.ROLES_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion de roles.',
    display_name: 'Gestionar roles',
    group_key: 'global_admin',
    parent_window_code: null,
    sort_order: 110,
  },
  {
    code: ACTION_PERMISSIONS.PERMISSIONS_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion de privilegios.',
    display_name: 'Gestionar privilegios',
    group_key: 'global_admin',
    parent_window_code: null,
    sort_order: 120,
  },
  {
    code: ACTION_PERMISSIONS.SETTINGS_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion operativa de configuracion.',
    display_name: 'Gestion operativa de configuracion',
    group_key: 'settings',
    parent_window_code: WINDOW_PERMISSIONS.SETTINGS,
    sort_order: 130,
  },
  {
    code: ACTION_PERMISSIONS.PLANNING_CHANGE_LOG_VIEW,
    type: 'ACTION' as const,
    description: 'Consulta del historial de cambios de planificacion manual.',
    display_name: 'Ver historial de cambios',
    group_key: 'planning',
    parent_window_code: WINDOW_PERMISSIONS.PLANNING,
    sort_order: 140,
  },
  {
    code: ACTION_PERMISSIONS.PLANNING_PLAN_SUBMIT_REVIEW,
    type: 'ACTION' as const,
    description: 'Enviar planes de planificacion a revision.',
    display_name: 'Enviar plan a revision',
    group_key: 'planning',
    parent_window_code: WINDOW_PERMISSIONS.PLANNING,
    sort_order: 150,
  },
  {
    code: ACTION_PERMISSIONS.PLANNING_PLAN_REVIEW_DECIDE,
    type: 'ACTION' as const,
    description: 'Aprobar planes o mandarlos a correccion.',
    display_name: 'Aprobar o corregir plan',
    group_key: 'planning',
    parent_window_code: WINDOW_PERMISSIONS.PLANNING,
    sort_order: 160,
  },
] as const;

export const ROLE_SEEDS = [
  {
    code: ROLE_CODES.ADMIN,
    name: 'Administrador',
    permissionCodes: [...PERMISSION_SEEDS.map((item) => item.code)],
  },
  {
    code: ROLE_CODES.ADMINISTRATIVE,
    name: 'Administrativo',
    permissionCodes: [WINDOW_PERMISSIONS.PLANNING],
  },
  {
    code: ROLE_CODES.IT_SUPPORT,
    name: 'Soporte TI',
    permissionCodes: [
      WINDOW_PERMISSIONS.PLANNING,
      WINDOW_PERMISSIONS.CONFLICTS,
      WINDOW_PERMISSIONS.AUDIT,
      WINDOW_PERMISSIONS.VIDEOCONFERENCES,
    ],
  },
] as const;
