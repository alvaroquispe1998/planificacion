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
  },
  {
    code: WINDOW_PERMISSIONS.CONFLICTS,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de cruces.',
  },
  {
    code: WINDOW_PERMISSIONS.AUDIT,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de auditoria Zoom.',
  },
  {
    code: WINDOW_PERMISSIONS.VIDEOCONFERENCES,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de videoconferencias.',
  },
  {
    code: WINDOW_PERMISSIONS.SETTINGS,
    type: 'WINDOW' as const,
    description: 'Acceso a la ventana de configuracion.',
  },
  {
    code: ACTION_PERMISSIONS.USERS_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion de usuarios.',
  },
  {
    code: ACTION_PERMISSIONS.ROLES_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion de roles.',
  },
  {
    code: ACTION_PERMISSIONS.PERMISSIONS_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion de privilegios.',
  },
  {
    code: ACTION_PERMISSIONS.SETTINGS_MANAGE,
    type: 'ACTION' as const,
    description: 'Gestion operativa de configuracion.',
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
