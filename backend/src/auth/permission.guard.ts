import { CanActivate, ExecutionContext, ForbiddenException, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { AUTH_PERMISSIONS_KEY } from './auth.constants';
import type { AuthenticatedRequestUser } from './auth.service';

@Injectable()
export class PermissionGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext) {
    const requiredPermissions =
      this.reflector.getAllAndOverride<string[]>(AUTH_PERMISSIONS_KEY, [
        context.getHandler(),
        context.getClass(),
      ]) ?? [];

    if (requiredPermissions.length === 0) {
      return true;
    }

    const request = context.switchToHttp().getRequest();
    const authUser = request.authUser as AuthenticatedRequestUser | undefined;
    if (!authUser) {
      throw new ForbiddenException('No se pudo resolver el perfil de acceso.');
    }

    const missingPermissions = requiredPermissions.filter(
      (permission) => !authUser.permissions.includes(permission),
    );
    if (missingPermissions.length > 0) {
      throw new ForbiddenException(
        `No tienes privilegios para esta accion. Faltan: ${missingPermissions.join(', ')}`,
      );
    }

    return true;
  }
}
