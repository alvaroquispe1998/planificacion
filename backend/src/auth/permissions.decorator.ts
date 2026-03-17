import { SetMetadata } from '@nestjs/common';
import { AUTH_PERMISSIONS_KEY } from './auth.constants';

export const RequirePermissions = (...permissions: string[]) =>
  SetMetadata(AUTH_PERMISSIONS_KEY, permissions);
