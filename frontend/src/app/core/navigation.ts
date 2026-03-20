import { PermissionCode, WindowCode } from './auth.models';

export type AppModuleKey = 'planning' | 'videoconferences' | 'integrations' | 'administration';

type NavMatch = {
  exact?: string[];
  prefix?: string[];
};

export type SecondaryNavItem = {
  label: string;
  path: string;
  icon: string;
  window?: WindowCode;
  permission?: PermissionCode;
  match: NavMatch;
};

export type ModuleNavItem = {
  key: AppModuleKey;
  label: string;
  hint: string;
  icon: string;
  children: SecondaryNavItem[];
};

export type NavGroup = {
  label: string;
  modules: ModuleNavItem[];
};

export const APP_NAV_GROUPS: NavGroup[] = [
  {
    label: 'Academico',
    modules: [
      {
        key: 'planning',
        label: 'Planificacion',
        hint: 'Planes, cruces y trazabilidad',
        icon: 'clipboard',
        children: [
          {
            label: 'Planes',
            path: '/planning',
            icon: 'layers',
            window: 'window.planning',
            match: {
              exact: ['/planning'],
              prefix: ['/planning/cycle-detail', '/planning/cycle-editor', '/planning/offers', '/class-detail'],
            },
          },
          {
            label: 'Cruces',
            path: '/planning/conflicts',
            icon: 'shuffle',
            window: 'window.conflicts',
            match: {
              exact: ['/planning/conflicts', '/conflicts'],
            },
          },
          {
            label: 'Match VC',
            path: '/planning/vc-match',
            icon: 'link',
            window: 'window.planning',
            match: {
              exact: ['/planning/vc-match'],
            },
          },
          {
            label: 'Historial',
            path: '/planning/change-log',
            icon: 'history',
            window: 'window.planning',
            permission: 'action.planning.change_log.view',
            match: {
              exact: ['/planning/change-log'],
            },
          },
        ],
      },
      {
        key: 'videoconferences',
        label: 'Videoconferencias',
        hint: 'Gestion y auditoria Zoom',
        icon: 'camera',
        children: [
          {
            label: 'Gestion',
            path: '/videoconferences',
            icon: 'spark',
            window: 'window.videoconferences',
            match: {
              exact: ['/videoconferences'],
            },
          },
          {
            label: 'Auditoria Zoom',
            path: '/videoconferences/audit',
            icon: 'search',
            window: 'window.audit',
            match: {
              exact: ['/videoconferences/audit', '/audit'],
            },
          },
        ],
      },
    ],
  },
  {
    label: 'Integraciones',
    modules: [
      {
        key: 'integrations',
        label: 'Sincronizacion',
        hint: 'Preparacion y fuentes Akademic',
        icon: 'link',
        children: [
          {
            label: 'Sincronizacion Akademic',
            path: '/integrations/sync',
            icon: 'refresh',
            window: 'window.settings',
            match: {
              exact: ['/integrations/sync', '/settings'],
            },
          },
        ],
      },
    ],
  },
  {
    label: 'Administracion',
    modules: [
      {
        key: 'administration',
        label: 'Seguridad',
        hint: 'Usuarios, roles y privilegios',
        icon: 'shield',
        children: [
          {
            label: 'Usuarios',
            path: '/admin/security/users',
            icon: 'users',
            permission: 'action.users.manage',
            match: {
              exact: ['/admin/security/users', '/admin/security', '/security'],
            },
          },
          {
            label: 'Roles',
            path: '/admin/security/roles',
            icon: 'badge',
            permission: 'action.users.manage',
            match: {
              exact: ['/admin/security/roles'],
            },
          },
        ],
      },
    ],
  },
];

export function canAccessNavItem(
  item: Pick<SecondaryNavItem, 'window' | 'permission'>,
  windows: WindowCode[],
  permissions: PermissionCode[],
) {
  const windowAllowed = !item.window || windows.includes(item.window);
  const permissionAllowed = !item.permission || permissions.includes(item.permission);
  return windowAllowed && permissionAllowed;
}

export function visibleChildren(
  module: ModuleNavItem,
  windows: WindowCode[],
  permissions: PermissionCode[],
) {
  return module.children.filter((item) => canAccessNavItem(item, windows, permissions));
}

export function isModuleVisible(
  module: ModuleNavItem,
  windows: WindowCode[],
  permissions: PermissionCode[],
) {
  return visibleChildren(module, windows, permissions).length > 0;
}

export function visibleNavGroups(
  windows: WindowCode[],
  permissions: PermissionCode[],
) {
  return APP_NAV_GROUPS.map((group) => ({
    ...group,
    modules: group.modules.filter((module) => isModuleVisible(module, windows, permissions)),
  })).filter((group) => group.modules.length > 0);
}

export function moduleDefaultPath(
  module: ModuleNavItem,
  windows: WindowCode[],
  permissions: PermissionCode[],
) {
  return visibleChildren(module, windows, permissions)[0]?.path ?? '/planning';
}

export function isNavChildActive(item: SecondaryNavItem, url: string) {
  const normalized = normalizeUrlPath(url);
  const exactMatch = (item.match.exact ?? []).some((path) => normalizeUrlPath(path) === normalized);
  const prefixMatch = (item.match.prefix ?? []).some((path) => normalized.startsWith(normalizeUrlPath(path)));
  return exactMatch || prefixMatch;
}

export function resolveModuleFromUrl(url: string): AppModuleKey | null {
  for (const group of APP_NAV_GROUPS) {
    for (const module of group.modules) {
      if (module.children.some((item) => isNavChildActive(item, url))) {
        return module.key;
      }
    }
  }
  return null;
}

export function firstAllowedPath(
  windows: WindowCode[],
  permissions: PermissionCode[],
) {
  for (const group of APP_NAV_GROUPS) {
    for (const module of group.modules) {
      if (isModuleVisible(module, windows, permissions)) {
        return moduleDefaultPath(module, windows, permissions);
      }
    }
  }
  return '/planning';
}

function normalizeUrlPath(url: string) {
  const [path] = `${url || ''}`.split(/[?#]/);
  return path.endsWith('/') && path.length > 1 ? path.slice(0, -1) : path;
}
