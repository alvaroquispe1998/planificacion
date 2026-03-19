import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import * as bcrypt from 'bcrypt';
import { Repository } from 'typeorm';
import { newId } from '../common';
import {
  AcademicProgramEntity,
  FacultyEntity,
} from '../entities/catalog-sync.entities';
import {
  AuthPermissionEntity,
  AuthRoleEntity,
  AuthRolePermissionEntity,
  AuthUserEntity,
  AuthUserRoleAssignmentEntity,
} from '../entities/auth.entities';
import { AuthService } from './auth.service';
import {
  ReplaceRolePermissionsDto,
  ReplaceUserAssignmentsDto,
  UpsertAuthRoleDto,
  UpsertAuthUserDto,
} from './dto/auth.dto';

@Injectable()
export class AuthAdminService {
  constructor(
    @InjectRepository(AuthUserEntity)
    private readonly usersRepo: Repository<AuthUserEntity>,
    @InjectRepository(AuthRoleEntity)
    private readonly rolesRepo: Repository<AuthRoleEntity>,
    @InjectRepository(AuthPermissionEntity)
    private readonly permissionsRepo: Repository<AuthPermissionEntity>,
    @InjectRepository(AuthRolePermissionEntity)
    private readonly rolePermissionsRepo: Repository<AuthRolePermissionEntity>,
    @InjectRepository(AuthUserRoleAssignmentEntity)
    private readonly assignmentsRepo: Repository<AuthUserRoleAssignmentEntity>,
    @InjectRepository(FacultyEntity)
    private readonly facultiesRepo: Repository<FacultyEntity>,
    @InjectRepository(AcademicProgramEntity)
    private readonly programsRepo: Repository<AcademicProgramEntity>,
    private readonly authService: AuthService,
  ) {}

  async listUsers() {
    const [users, roles, assignments, faculties, programs] = await Promise.all([
      this.usersRepo.find({ order: { username: 'ASC' } }),
      this.rolesRepo.find({ order: { name: 'ASC' } }),
      this.assignmentsRepo.find({ order: { created_at: 'ASC' } }),
      this.facultiesRepo.find({ order: { name: 'ASC' } }),
      this.programsRepo.find({ order: { name: 'ASC' } }),
    ]);
    const roleMap = new Map(roles.map((item) => [item.id, item]));
    const facultyMap = new Map(faculties.map((item) => [item.id, item]));
    const programMap = new Map(programs.map((item) => [item.id, item]));

    return users.map((user) => ({
      id: user.id,
      username: user.username,
      display_name: user.display_name,
      email: user.email,
      is_active: user.is_active,
      created_at: user.created_at,
      updated_at: user.updated_at,
      assignments: assignments
        .filter((item) => item.user_id === user.id)
        .map((assignment) => ({
          id: assignment.id,
          role_id: assignment.role_id,
          role_code: roleMap.get(assignment.role_id)?.code ?? null,
          role_name: roleMap.get(assignment.role_id)?.name ?? null,
          faculty_id: assignment.faculty_id,
          faculty_name: assignment.faculty_id
            ? facultyMap.get(assignment.faculty_id)?.name ?? null
            : null,
          academic_program_id: assignment.academic_program_id,
          academic_program_name: assignment.academic_program_id
            ? programMap.get(assignment.academic_program_id)?.name ?? null
            : null,
          is_active: assignment.is_active,
          is_global: !assignment.faculty_id && !assignment.academic_program_id,
        })),
    }));
  }

  async createUser(dto: UpsertAuthUserDto) {
    const normalizedUsername = dto.username.trim().toLowerCase();
    const existing = await this.usersRepo.findOne({ where: { username: normalizedUsername } });
    if (existing) {
      throw new BadRequestException('El username ya existe.');
    }
    if (!dto.password?.trim()) {
      throw new BadRequestException('La password es obligatoria al crear un usuario.');
    }
    const now = new Date();
    const user = await this.usersRepo.save(
      this.usersRepo.create({
        id: newId(),
        username: normalizedUsername,
        password_hash: await bcrypt.hash(dto.password, 10),
        display_name: dto.display_name.trim(),
        email: emptyToNull(dto.email),
        is_active: dto.is_active ?? true,
        created_at: now,
        updated_at: now,
      }),
    );
    return this.serializeUser(user);
  }

  async updateUser(id: string, dto: UpsertAuthUserDto) {
    const user = await this.requireUser(id);
    const normalizedUsername = dto.username.trim().toLowerCase();
    const duplicate = await this.usersRepo.findOne({ where: { username: normalizedUsername } });
    if (duplicate && duplicate.id !== user.id) {
      throw new BadRequestException('El username ya existe.');
    }
    user.username = normalizedUsername;
    user.display_name = dto.display_name.trim();
    user.email = emptyToNull(dto.email);
    user.is_active = dto.is_active ?? user.is_active;
    user.updated_at = new Date();
    if (dto.password?.trim()) {
      user.password_hash = await bcrypt.hash(dto.password, 10);
    }
    await this.usersRepo.save(user);
    return this.serializeUser(user);
  }

  async listRoles() {
    const [roles, permissions, mappings] = await Promise.all([
      this.rolesRepo.find({ order: { name: 'ASC' } }),
      this.permissionsRepo.find({ order: { sort_order: 'ASC', type: 'ASC', code: 'ASC' } }),
      this.rolePermissionsRepo.find(),
    ]);
    const permissionMap = new Map(permissions.map((item) => [item.id, item]));
    return roles.map((role) => ({
      id: role.id,
      code: role.code,
      name: role.name,
      is_active: role.is_active,
      is_editable: role.is_editable,
      created_at: role.created_at,
      updated_at: role.updated_at,
      permissions: mappings
        .filter((item) => item.role_id === role.id)
        .map((item) => permissionMap.get(item.permission_id))
        .filter((item): item is AuthPermissionEntity => Boolean(item))
        .map((permission) => ({
          id: permission.id,
          code: permission.code,
          type: permission.type,
          description: permission.description,
          display_name: permission.display_name,
          group_key: permission.group_key,
          parent_window_code: permission.parent_window_code,
          sort_order: permission.sort_order,
          is_active: permission.is_active,
        })),
    }));
  }

  async createRole(dto: UpsertAuthRoleDto) {
    const code = normalizeCode(dto.code);
    const existing = await this.rolesRepo.findOne({ where: { code } });
    if (existing) {
      throw new BadRequestException('El codigo del rol ya existe.');
    }
    const now = new Date();
    return this.rolesRepo.save(
      this.rolesRepo.create({
        id: newId(),
        code,
        name: dto.name.trim(),
        is_active: dto.is_active ?? true,
        is_editable: dto.is_editable ?? true,
        created_at: now,
        updated_at: now,
      }),
    );
  }

  async updateRole(id: string, dto: UpsertAuthRoleDto) {
    const role = await this.requireRole(id);
    const code = normalizeCode(dto.code);
    const duplicate = await this.rolesRepo.findOne({ where: { code } });
    if (duplicate && duplicate.id !== role.id) {
      throw new BadRequestException('El codigo del rol ya existe.');
    }
    role.code = code;
    role.name = dto.name.trim();
    role.is_active = dto.is_active ?? role.is_active;
    role.is_editable = dto.is_editable ?? role.is_editable;
    role.updated_at = new Date();
    await this.rolesRepo.save(role);
    return role;
  }

  listPermissions() {
    return this.permissionsRepo.find({ order: { sort_order: 'ASC', type: 'ASC', code: 'ASC' } });
  }

  async replaceRolePermissions(roleId: string, dto: ReplaceRolePermissionsDto) {
    await this.requireRole(roleId);
    const permissions = dto.permission_ids.length
      ? await this.permissionsRepo.find({
          where: dto.permission_ids.map((id) => ({ id })),
        })
      : [];
    if (permissions.length !== dto.permission_ids.length) {
      throw new BadRequestException('Uno o mas privilegios no existen.');
    }

    const permissionMap = new Map(permissions.map((item) => [item.id, item]));
    const parentCodes = [...new Set(
      permissions
        .map((permission) => permission.parent_window_code)
        .filter((code): code is string => Boolean(code)),
    )];
    const missingParentCodes = parentCodes.filter(
      (code) => !permissions.some((permission) => permission.code === code),
    );
    const parentPermissions = missingParentCodes.length
      ? await this.permissionsRepo.find({ where: missingParentCodes.map((code) => ({ code })) })
      : [];
    for (const permission of parentPermissions) {
      permissionMap.set(permission.id, permission);
    }
    const normalizedPermissions = [...permissionMap.values()];

    await this.rolePermissionsRepo.delete({ role_id: roleId });
    const now = new Date();
    if (normalizedPermissions.length > 0) {
      await this.rolePermissionsRepo.save(
        normalizedPermissions.map((permission) =>
          this.rolePermissionsRepo.create({
            id: newId(),
            role_id: roleId,
            permission_id: permission.id,
            created_at: now,
          }),
        ),
      );
    }
    return { updated: true, role_id: roleId };
  }

  async replaceUserAssignments(userId: string, dto: ReplaceUserAssignmentsDto) {
    await this.requireUser(userId);
    const roleIds = [...new Set(dto.assignments.map((item) => item.role_id))];
    const roles = roleIds.length
      ? await this.rolesRepo.find({ where: roleIds.map((id) => ({ id })) })
      : [];
    const roleMap = new Map(roles.map((item) => [item.id, item]));
    if (roles.length !== roleIds.length) {
      throw new BadRequestException('Uno o mas roles no existen.');
    }

    for (const assignment of dto.assignments) {
      const role = roleMap.get(assignment.role_id);
      if (!role) {
        throw new BadRequestException('Rol invalido.');
      }
      await this.authService.validateAssignmentScope(
        role.code,
        assignment.faculty_id,
        assignment.academic_program_id,
      );
    }

    const programs = await this.programsRepo.find({
      where: dto.assignments
        .map((item) => item.academic_program_id)
        .filter((item): item is string => Boolean(item))
        .map((id) => ({ id })),
    });
    const programMap = new Map(programs.map((item) => [item.id, item]));

    await this.assignmentsRepo.delete({ user_id: userId });
    const now = new Date();
    await this.assignmentsRepo.save(
      dto.assignments.map((assignment) => {
        const program = assignment.academic_program_id
          ? programMap.get(assignment.academic_program_id)
          : null;
        return this.assignmentsRepo.create({
          id: newId(),
          user_id: userId,
          role_id: assignment.role_id,
          faculty_id: emptyToNull(assignment.faculty_id) ?? program?.faculty_id ?? null,
          academic_program_id: emptyToNull(assignment.academic_program_id),
          is_active: assignment.is_active ?? true,
          created_at: now,
          updated_at: now,
        });
      }),
    );
    return { updated: true, user_id: userId };
  }

  async getScopeCatalog() {
    const [faculties, academicPrograms, roles] = await Promise.all([
      this.facultiesRepo.find({ order: { name: 'ASC' } }),
      this.programsRepo.find({ order: { name: 'ASC' } }),
      this.rolesRepo.find({ where: { is_active: true }, order: { name: 'ASC' } }),
    ]);
    return {
      faculties,
      academic_programs: academicPrograms,
      roles,
    };
  }

  private async requireUser(id: string) {
    const user = await this.usersRepo.findOne({ where: { id } });
    if (!user) {
      throw new NotFoundException('El usuario no existe.');
    }
    return user;
  }

  private async requireRole(id: string) {
    const role = await this.rolesRepo.findOne({ where: { id } });
    if (!role) {
      throw new NotFoundException('El rol no existe.');
    }
    return role;
  }

  private serializeUser(user: AuthUserEntity) {
    return {
      id: user.id,
      username: user.username,
      display_name: user.display_name,
      email: user.email,
      is_active: user.is_active,
      created_at: user.created_at,
      updated_at: user.updated_at,
    };
  }
}

function emptyToNull(value: string | null | undefined) {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function normalizeCode(value: string) {
  return value.trim().replace(/\s+/g, '_').toUpperCase();
}
