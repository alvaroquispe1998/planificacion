import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { AUTH_PUBLIC_KEY } from './auth.constants';
import { AuthService } from './auth.service';

@Injectable()
export class AccessTokenGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly authService: AuthService,
  ) {}

  async canActivate(context: ExecutionContext) {
    const isPublic = this.reflector.getAllAndOverride<boolean>(AUTH_PUBLIC_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);
    if (isPublic) {
      return true;
    }

    const request = context.switchToHttp().getRequest();
    const authHeader = `${request.headers.authorization ?? ''}`.trim();
    if (!authHeader.startsWith('Bearer ')) {
      throw new UnauthorizedException('Sesion requerida.');
    }

    const token = authHeader.slice('Bearer '.length).trim();
    if (!token) {
      throw new UnauthorizedException('Token invalido.');
    }

    request.authUser = {
      ...(await this.authService.authenticateAccessToken(token)),
      request_ip: resolveRequestIp(request),
    };
    return true;
  }
}

function resolveRequestIp(request: any) {
  const forwardedFor = `${request?.headers?.['x-forwarded-for'] ?? ''}`
    .split(',')
    .map((item) => item.trim())
    .find(Boolean);
  const candidates = [
    forwardedFor,
    request?.headers?.['cf-connecting-ip'],
    request?.headers?.['x-real-ip'],
    request?.headers?.['true-client-ip'],
    request?.ip,
    request?.socket?.remoteAddress,
  ];
  for (const candidate of candidates) {
    const normalized = `${candidate ?? ''}`.trim();
    if (normalized) {
      return normalized;
    }
  }
  return null;
}
