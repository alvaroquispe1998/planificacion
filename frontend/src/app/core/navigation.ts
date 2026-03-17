import { PermissionCode, WindowCode } from './auth.models';

export type NavItem = {
  label: string;
  path: string;
  window?: WindowCode;
  permission?: PermissionCode;
};

export const APP_NAV_ITEMS: NavItem[] = [
  { label: 'Planificacion', path: '/planning', window: 'window.planning' },
  { label: 'Cruces', path: '/conflicts', window: 'window.conflicts' },
  { label: 'Auditoria Zoom', path: '/audit', window: 'window.audit' },
  { label: 'Videoconferencias', path: '/videoconferences', window: 'window.videoconferences' },
  { label: 'Sincronizacion', path: '/settings', window: 'window.settings' },
  { label: 'Seguridad', path: '/security', permission: 'action.users.manage' },
];

export function firstAllowedPath(windows: WindowCode[]) {
  return (
    APP_NAV_ITEMS.find((item) => item.window && windows.includes(item.window))?.path ?? '/planning'
  );
}

export function canManageSecurity(permissions: PermissionCode[]) {
  return permissions.includes('action.users.manage');
}
