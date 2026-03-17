import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { JwtModule } from '@nestjs/jwt';
import { TypeOrmModule } from '@nestjs/typeorm';
import {
  AcademicProgramEntity,
  FacultyEntity,
} from '../entities/catalog-sync.entities';
import {
  AuthPermissionEntity,
  AuthRefreshTokenEntity,
  AuthRoleEntity,
  AuthRolePermissionEntity,
  AuthUserEntity,
  AuthUserRoleAssignmentEntity,
} from '../entities/auth.entities';
import { AccessTokenGuard } from './access-token.guard';
import { AuthAdminController } from './auth-admin.controller';
import { AuthAdminService } from './auth-admin.service';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { PermissionGuard } from './permission.guard';

@Module({
  imports: [
    JwtModule.register({}),
    TypeOrmModule.forFeature([
      AuthUserEntity,
      AuthRoleEntity,
      AuthPermissionEntity,
      AuthRolePermissionEntity,
      AuthUserRoleAssignmentEntity,
      AuthRefreshTokenEntity,
      FacultyEntity,
      AcademicProgramEntity,
    ]),
  ],
  controllers: [AuthController, AuthAdminController],
  providers: [
    AuthService,
    AuthAdminService,
    {
      provide: APP_GUARD,
      useClass: AccessTokenGuard,
    },
    {
      provide: APP_GUARD,
      useClass: PermissionGuard,
    },
  ],
  exports: [AuthService],
})
export class AuthModule {}
