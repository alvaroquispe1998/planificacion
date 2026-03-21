import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from './auth.service';
import { PermissionCode, WindowCode } from './auth.models';

function redirectToFirstAllowedPath(
  auth: AuthService,
  router: Router,
  currentUrl?: string,
) {
  const fallbackPath = auth.firstAllowedPath();
  if (!fallbackPath || fallbackPath === currentUrl) {
    auth.clearLocalSession();
    return router.createUrlTree(['/login'], {
      queryParams: { reason: 'no-access' },
    });
  }

  return router.createUrlTree([fallbackPath]);
}

export const authGuard: CanActivateFn = (_route, state) => {
  const auth = inject(AuthService);
  const router = inject(Router);

  if (!auth.isAuthenticated()) {
    return router.createUrlTree(['/login'], {
      queryParams: { redirect: state.url },
    });
  }

  return true;
};

export const windowGuard: CanActivateFn = (route, state) => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const requiredWindow = route.data?.['requiredWindow'] as WindowCode | undefined;

  if (!auth.isAuthenticated()) {
    return router.createUrlTree(['/login'], {
      queryParams: { redirect: state.url },
    });
  }

  if (!requiredWindow || auth.hasWindow(requiredWindow)) {
    return true;
  }

  return redirectToFirstAllowedPath(auth, router, state.url);
};

export const permissionGuard: CanActivateFn = (route, state) => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const requiredPermission = route.data?.['requiredPermission'] as PermissionCode | undefined;
  const requiredPermissions = (route.data?.['requiredPermissions'] as PermissionCode[] | undefined) ?? [];

  if (!auth.isAuthenticated()) {
    return router.createUrlTree(['/login'], {
      queryParams: { redirect: state.url },
    });
  }

  const singlePermissionAllowed = !requiredPermission || auth.hasPermission(requiredPermission);
  const allPermissionsAllowed =
    requiredPermissions.length === 0 ||
    requiredPermissions.every((permission) => auth.hasPermission(permission));

  if (singlePermissionAllowed && allPermissionsAllowed) {
    return true;
  }

  return redirectToFirstAllowedPath(auth, router, state.url);
};

export const loginRedirectGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);

  if (auth.isAuthenticated()) {
    const fallbackPath = auth.firstAllowedPath();
    if (!fallbackPath) {
      auth.clearLocalSession();
      return true;
    }
    return router.createUrlTree([fallbackPath]);
  }

  return true;
};

export const securityLandingGuard: CanActivateFn = (_route, state) => {
  const auth = inject(AuthService);
  const router = inject(Router);

  if (!auth.isAuthenticated()) {
    return router.createUrlTree(['/login'], {
      queryParams: { redirect: state.url },
    });
  }

  const hasSecurityWindow = auth.hasWindow('window.security');
  const canManageUsers = hasSecurityWindow && auth.hasPermission('action.users.manage');
  const canManageRoles =
    hasSecurityWindow &&
    auth.hasPermission('action.roles.manage') &&
    auth.hasPermission('action.permissions.manage');

  if (canManageUsers) {
    return router.createUrlTree(['/admin/security/users']);
  }
  if (canManageRoles) {
    return router.createUrlTree(['/admin/security/roles']);
  }

  return redirectToFirstAllowedPath(auth, router, state.url);
};
