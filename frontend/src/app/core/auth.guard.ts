import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from './auth.service';
import { PermissionCode, WindowCode } from './auth.models';

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

  return router.createUrlTree([auth.firstAllowedPath()]);
};

export const permissionGuard: CanActivateFn = (route, state) => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const requiredPermission = route.data?.['requiredPermission'] as PermissionCode | undefined;

  if (!auth.isAuthenticated()) {
    return router.createUrlTree(['/login'], {
      queryParams: { redirect: state.url },
    });
  }

  if (!requiredPermission || auth.hasPermission(requiredPermission)) {
    return true;
  }

  return router.createUrlTree([auth.firstAllowedPath()]);
};

export const loginRedirectGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);

  if (auth.isAuthenticated()) {
    return router.createUrlTree([auth.firstAllowedPath()]);
  }

  return true;
};
