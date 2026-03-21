import { Body, Controller, Get, Param, Patch, Post, Put } from '@nestjs/common';
import { ACTION_PERMISSIONS, WINDOW_PERMISSIONS } from './auth.constants';
import { AuthAdminService } from './auth-admin.service';
import {
  ReplaceRolePermissionsDto,
  ReplaceUserAssignmentsDto,
  UpsertAuthRoleDto,
  UpsertAuthUserDto,
} from './dto/auth.dto';
import { RequirePermissions } from './permissions.decorator';

@Controller('auth/admin')
export class AuthAdminController {
  constructor(private readonly authAdminService: AuthAdminService) {}

  @Get('users')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.USERS_MANAGE)
  listUsers() {
    return this.authAdminService.listUsers();
  }

  @Post('users')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.USERS_MANAGE)
  createUser(@Body() dto: UpsertAuthUserDto) {
    return this.authAdminService.createUser(dto);
  }

  @Patch('users/:id')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.USERS_MANAGE)
  updateUser(@Param('id') id: string, @Body() dto: UpsertAuthUserDto) {
    return this.authAdminService.updateUser(id, dto);
  }

  @Put('users/:id/assignments')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.USERS_MANAGE)
  updateAssignments(@Param('id') id: string, @Body() dto: ReplaceUserAssignmentsDto) {
    return this.authAdminService.replaceUserAssignments(id, dto);
  }

  @Get('roles')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.ROLES_MANAGE)
  listRoles() {
    return this.authAdminService.listRoles();
  }

  @Post('roles')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.ROLES_MANAGE)
  createRole(@Body() dto: UpsertAuthRoleDto) {
    return this.authAdminService.createRole(dto);
  }

  @Patch('roles/:id')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.ROLES_MANAGE)
  updateRole(@Param('id') id: string, @Body() dto: UpsertAuthRoleDto) {
    return this.authAdminService.updateRole(id, dto);
  }

  @Put('roles/:id/permissions')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.PERMISSIONS_MANAGE)
  updateRolePermissions(@Param('id') id: string, @Body() dto: ReplaceRolePermissionsDto) {
    return this.authAdminService.replaceRolePermissions(id, dto);
  }

  @Get('permissions')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.PERMISSIONS_MANAGE)
  listPermissions() {
    return this.authAdminService.listPermissions();
  }

  @Get('scopes/catalog')
  @RequirePermissions(WINDOW_PERMISSIONS.SECURITY, ACTION_PERMISSIONS.USERS_MANAGE)
  listScopeCatalog() {
    return this.authAdminService.getScopeCatalog();
  }
}
