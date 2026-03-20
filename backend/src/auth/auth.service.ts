import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  OnModuleInit,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { JwtService } from '@nestjs/jwt';
import * as bcrypt from 'bcrypt';
import * as crypto from 'crypto';
import { Repository } from 'typeorm';
import { newId } from '../common';
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
import {
  ACCESS_TOKEN_DEFAULT_EXPIRES,
  DEFAULT_ADMIN_PASSWORD,
  DEFAULT_ADMIN_USERNAME,
  PERMISSION_SEEDS,
  REFRESH_TOKEN_DEFAULT_EXPIRES_DAYS,
  ROLE_CODES,
  ROLE_SEEDS,
} from './auth.constants';
import { LoginDto, RefreshTokenDto } from './dto/auth.dto';

type JwtAccessPayload = {
  sub: string;
  username: string;
  type: 'access';
};

type JwtRefreshPayload = {
  sub: string;
  sid: string;
  username: string;
  type: 'refresh';
};

type AccessScope = {
  assignment_id: string;
  role_id: string;
  role_code: string;
  role_name: string;
  faculty_id: string | null;
  faculty_name: string | null;
  academic_program_id: string | null;
  academic_program_name: string | null;
  is_global: boolean;
};

export type AuthenticatedRequestUser = {
  id: string;
  username: string;
  display_name: string;
  email: string | null;
  permissions: string[];
  windows: string[];
  roles: Array<{ id: string; code: string; name: string }>;
  scopes: AccessScope[];
  allowed_faculty_ids: string[];
  allowed_academic_program_ids: string[];
  is_global: boolean;
  is_admin: boolean;
};

@Injectable()
export class AuthService implements OnModuleInit {
  private readonly logger = new Logger(AuthService.name);

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
    @InjectRepository(AuthRefreshTokenEntity)
    private readonly refreshTokensRepo: Repository<AuthRefreshTokenEntity>,
    @InjectRepository(FacultyEntity)
    private readonly facultiesRepo: Repository<FacultyEntity>,
    @InjectRepository(AcademicProgramEntity)
    private readonly programsRepo: Repository<AcademicProgramEntity>,
    private readonly jwtService: JwtService,
    private readonly configService: ConfigService,
  ) {}

  async onModuleInit() {
    await this.ensureSecuritySeedData();
  }

  async login(dto: LoginDto) {
    const username = dto.username.trim().toLowerCase();
    const user = await this.usersRepo.findOne({ where: { username } });
    if (!user || !user.is_active) {
      throw new UnauthorizedException('Credenciales invalidas.');
    }

    const valid = await bcrypt.compare(dto.password, user.password_hash);
    if (!valid) {
      throw new UnauthorizedException('Credenciales invalidas.');
    }

    const [authUser, tokens] = await Promise.all([
      this.buildAuthenticatedUser(user.id, user),
      this.issueTokens(user.id, user.username),
    ]);
    return {
      ...tokens,
      user: this.serializeUser(user),
      roles: authUser.roles,
      role_assignments: authUser.scopes,
      permissions: authUser.permissions,
      scopes: authUser.scopes,
      windows: authUser.windows,
    };
  }

  async refresh(dto: RefreshTokenDto) {
    const payload = await this.verifyRefreshToken(dto.refresh_token);
    const session = await this.refreshTokensRepo.findOne({ where: { id: payload.sid } });
    if (!session || session.user_id !== payload.sub) {
      throw new UnauthorizedException('Refresh token invalido.');
    }
    if (session.revoked_at || session.expires_at.getTime() <= Date.now()) {
      throw new UnauthorizedException('Refresh token expirado.');
    }

    const tokenHash = hashToken(dto.refresh_token);
    if (tokenHash !== session.token_hash) {
      throw new UnauthorizedException('Refresh token invalido.');
    }

    session.revoked_at = new Date();
    session.updated_at = new Date();
    await this.refreshTokensRepo.save(session);

    const user = await this.requireActiveUser(payload.sub);
    const authUser = await this.buildAuthenticatedUser(user.id);
    const tokens = await this.issueTokens(user.id, user.username);
    return {
      ...tokens,
      user: this.serializeUser(user),
      roles: authUser.roles,
      role_assignments: authUser.scopes,
      permissions: authUser.permissions,
      scopes: authUser.scopes,
      windows: authUser.windows,
    };
  }

  async logout(userId: string, refreshToken?: string) {
    if (refreshToken?.trim()) {
      try {
        const payload = await this.verifyRefreshToken(refreshToken);
        const session = await this.refreshTokensRepo.findOne({ where: { id: payload.sid } });
        if (session && session.user_id === userId && !session.revoked_at) {
          session.revoked_at = new Date();
          session.updated_at = new Date();
          await this.refreshTokensRepo.save(session);
        }
      } catch {
        // Ignore invalid local token during logout.
      }
      return { logged_out: true };
    }

    const sessions = await this.refreshTokensRepo.find({
      where: { user_id: userId },
      order: { created_at: 'DESC' },
    });
    const now = new Date();
    for (const session of sessions) {
      if (!session.revoked_at) {
        session.revoked_at = now;
        session.updated_at = now;
      }
    }
    if (sessions.length > 0) {
      await this.refreshTokensRepo.save(sessions);
    }
    return { logged_out: true };
  }

  async me(userId: string) {
    const user = await this.requireActiveUser(userId);
    const authUser = await this.buildAuthenticatedUser(user.id);
    return {
      user: this.serializeUser(user),
      roles: authUser.roles,
      role_assignments: authUser.scopes,
      permissions: authUser.permissions,
      scopes: authUser.scopes,
      windows: authUser.windows,
    };
  }

  async authenticateAccessToken(token: string) {
    try {
      const payload = await this.jwtService.verifyAsync<JwtAccessPayload>(token, {
        secret: this.jwtSecret,
      });
      if (payload.type !== 'access') {
        throw new UnauthorizedException('Token de acceso invalido.');
      }
      return this.buildAuthenticatedUser(payload.sub);
    } catch (error) {
      if (error instanceof UnauthorizedException) {
        throw error;
      }
      throw new UnauthorizedException('Sesion invalida o expirada.');
    }
  }

  async buildAuthenticatedUser(userId: string, cachedUser?: AuthUserEntity): Promise<AuthenticatedRequestUser> {
    // Fetch user + assignments + all roles + all permissions + all role-permissions in parallel
    // This avoids sequential DB round-trips that were causing slow logins
    const [user, assignments, allRoles, allPermissions, allRolePermissions, allFaculties, allPrograms] = await Promise.all([
      cachedUser ? Promise.resolve(cachedUser) : this.requireActiveUser(userId),
      this.assignmentsRepo.find({
        where: { user_id: userId, is_active: true },
        order: { created_at: 'ASC' },
      }),
      this.rolesRepo.find({ where: { is_active: true } }),
      this.permissionsRepo.find({ where: { is_active: true } }),
      this.rolePermissionsRepo.find(),
      this.facultiesRepo.find(),
      this.programsRepo.find(),
    ]);

    if (assignments.length === 0) {
      throw new ForbiddenException('El usuario no tiene roles activos asignados.');
    }

    const roleMap = new Map(allRoles.map((item) => [item.id, item]));
    const facultyMap = new Map(allFaculties.map((item) => [item.id, item]));
    const programMap = new Map(allPrograms.map((item) => [item.id, item]));
    const permissionMap = new Map(allPermissions.map((item) => [item.id, item]));

    // Build role-permission lookup from in-memory data
    const rolePermissionsByRole = new Map<string, string[]>();
    for (const rp of allRolePermissions) {
      const arr = rolePermissionsByRole.get(rp.role_id) || [];
      arr.push(rp.permission_id);
      rolePermissionsByRole.set(rp.role_id, arr);
    }

    const activeRoleIds = new Set(
      assignments.map((item) => item.role_id).filter((roleId) => roleMap.has(roleId)),
    );

    // Gather all permission codes from active roles
    const permissionCodeSet = new Set<string>();
    for (const roleId of activeRoleIds) {
      const permIds = rolePermissionsByRole.get(roleId) || [];
      for (const permId of permIds) {
        const perm = permissionMap.get(permId);
        if (perm) {
          permissionCodeSet.add(perm.code);
        }
      }
    }
    const permissionCodes = [...permissionCodeSet].sort();

    const scopes: AccessScope[] = assignments
      .map((assignment) => {
        const role = roleMap.get(assignment.role_id);
        if (!role) {
          return null;
        }
        const program = assignment.academic_program_id
          ? programMap.get(assignment.academic_program_id)
          : null;
        const effectiveFacultyId = assignment.faculty_id ?? program?.faculty_id ?? null;
        const faculty = effectiveFacultyId ? facultyMap.get(effectiveFacultyId) : null;
        return {
          assignment_id: assignment.id,
          role_id: role.id,
          role_code: role.code,
          role_name: role.name,
          faculty_id: effectiveFacultyId,
          faculty_name: faculty?.name ?? null,
          academic_program_id: assignment.academic_program_id ?? null,
          academic_program_name: program?.name ?? null,
          is_global: !assignment.faculty_id && !assignment.academic_program_id,
        };
      })
      .filter((item): item is AccessScope => Boolean(item));

    const roleSummaries = [
      ...new Map(
        assignments
          .map((a) => roleMap.get(a.role_id))
          .filter((r): r is AuthRoleEntity => Boolean(r))
          .map((role) => [role.id, { id: role.id, code: role.code, name: role.name }]),
      ).values(),
    ];
    const isAdmin = roleSummaries.some((item) => item.code === ROLE_CODES.ADMIN);
    const isGlobal = isAdmin || scopes.some((item) => item.is_global);

    return {
      id: user.id,
      username: user.username,
      display_name: user.display_name,
      email: user.email,
      permissions: permissionCodes,
      windows: permissionCodes.filter((item) => item.startsWith('window.')),
      roles: roleSummaries,
      scopes,
      allowed_faculty_ids: uniqueIds(scopes.map((item) => item.faculty_id)),
      allowed_academic_program_ids: uniqueIds(scopes.map((item) => item.academic_program_id)),
      is_global: isGlobal,
      is_admin: isAdmin,
    };
  }

  assertScopeAccess(
    authUser: AuthenticatedRequestUser,
    facultyId?: string | null,
    academicProgramId?: string | null,
  ) {
    if (authUser.is_global) {
      return;
    }

    const normalizedFacultyId = facultyId ?? null;
    const normalizedProgramId = academicProgramId ?? null;
    const allowed = authUser.scopes.some((scope) => {
      if (scope.is_global) {
        return true;
      }
      if (normalizedProgramId && scope.academic_program_id) {
        return (
          scope.academic_program_id === normalizedProgramId &&
          (!normalizedFacultyId || !scope.faculty_id || scope.faculty_id === normalizedFacultyId)
        );
      }
      if (normalizedFacultyId && scope.faculty_id) {
        return scope.faculty_id === normalizedFacultyId;
      }
      return false;
    });

    if (!allowed) {
      throw new ForbiddenException('No tienes alcance para esta facultad/carrera.');
    }
  }

  filterByScope<T>(
    authUser: AuthenticatedRequestUser,
    items: T[],
    getScope: (item: T) => { faculty_id?: string | null; academic_program_id?: string | null },
  ) {
    if (authUser.is_global) {
      return items;
    }
    return items.filter((item) => {
      const scope = getScope(item);
      try {
        this.assertScopeAccess(authUser, scope.faculty_id, scope.academic_program_id);
        return true;
      } catch {
        return false;
      }
    });
  }

  async validateAssignmentScope(
    roleCode: string,
    facultyId?: string | null,
    academicProgramId?: string | null,
  ) {
    const normalizedFacultyId = emptyToNull(facultyId);
    const normalizedProgramId = emptyToNull(academicProgramId);
    const allowsGlobal = roleCode === ROLE_CODES.ADMIN || roleCode === ROLE_CODES.IT_SUPPORT;

    if (!allowsGlobal && !normalizedFacultyId && !normalizedProgramId) {
      throw new BadRequestException('Este rol requiere alcance por facultad o carrera.');
    }

    if (normalizedProgramId) {
      const program = await this.programsRepo.findOne({ where: { id: normalizedProgramId } });
      if (!program) {
        throw new BadRequestException('La carrera/programa no existe.');
      }
      if (!program.faculty_id) {
        throw new BadRequestException('La carrera/programa no tiene facultad asociada.');
      }
      if (normalizedFacultyId && program.faculty_id !== normalizedFacultyId) {
        throw new BadRequestException(
          'La carrera/programa no pertenece a la facultad seleccionada.',
        );
      }
    }

    if (normalizedFacultyId) {
      const faculty = await this.facultiesRepo.findOne({ where: { id: normalizedFacultyId } });
      if (!faculty) {
        throw new BadRequestException('La facultad no existe.');
      }
    }
  }

  private async ensureSecuritySeedData() {
    for (const seed of PERMISSION_SEEDS) {
      const existing = await this.permissionsRepo.findOne({ where: { code: seed.code } });
      if (existing) {
        let changed = false;
        if (existing.type !== seed.type) {
          existing.type = seed.type;
          changed = true;
        }
        if (existing.description !== seed.description) {
          existing.description = seed.description;
          changed = true;
        }
        if ((existing.display_name ?? null) !== (seed.display_name ?? null)) {
          existing.display_name = seed.display_name ?? null;
          changed = true;
        }
        if ((existing.group_key ?? null) !== (seed.group_key ?? null)) {
          existing.group_key = seed.group_key ?? null;
          changed = true;
        }
        if ((existing.parent_window_code ?? null) !== (seed.parent_window_code ?? null)) {
          existing.parent_window_code = seed.parent_window_code ?? null;
          changed = true;
        }
        if (Number(existing.sort_order ?? 0) !== Number(seed.sort_order ?? 0)) {
          existing.sort_order = Number(seed.sort_order ?? 0);
          changed = true;
        }
        if (!existing.is_active) {
          existing.is_active = true;
          changed = true;
        }
        if (changed) {
          existing.updated_at = new Date();
          await this.permissionsRepo.save(existing);
        }
        continue;
      }

      const now = new Date();
      await this.permissionsRepo.save(
        this.permissionsRepo.create({
          id: newId(),
          code: seed.code,
          type: seed.type,
          description: seed.description,
          display_name: seed.display_name ?? null,
          group_key: seed.group_key ?? null,
          parent_window_code: seed.parent_window_code ?? null,
          sort_order: Number(seed.sort_order ?? 0),
          is_active: true,
          created_at: now,
          updated_at: now,
        }),
      );
    }

    const permissionMap = new Map((await this.permissionsRepo.find()).map((item) => [item.code, item]));
    for (const seed of ROLE_SEEDS) {
      let role = await this.rolesRepo.findOne({ where: { code: seed.code } });
      const now = new Date();
      if (!role) {
        role = await this.rolesRepo.save(
          this.rolesRepo.create({
            id: newId(),
            code: seed.code,
            name: seed.name,
            is_active: true,
            is_editable: true,
            created_at: now,
            updated_at: now,
          }),
        );
      }

      const existingMappings = await this.rolePermissionsRepo.find({ where: { role_id: role.id } });
      if (existingMappings.length === 0) {
        const mappings = seed.permissionCodes
          .map((code) => permissionMap.get(code))
          .filter((item): item is AuthPermissionEntity => Boolean(item))
          .map((permission) =>
            this.rolePermissionsRepo.create({
              id: newId(),
              role_id: role!.id,
              permission_id: permission.id,
              created_at: now,
            }),
          );
        if (mappings.length > 0) {
          await this.rolePermissionsRepo.save(mappings);
        }
      }
    }

    const userCount = await this.usersRepo.count();
    if (userCount > 0) {
      return;
    }

    const username = this.configService
      .get<string>('AUTH_BOOTSTRAP_ADMIN_USERNAME', DEFAULT_ADMIN_USERNAME)
      .trim()
      .toLowerCase();
    const password = this.configService.get<string>(
      'AUTH_BOOTSTRAP_ADMIN_PASSWORD',
      DEFAULT_ADMIN_PASSWORD,
    );
    const now = new Date();
    const adminUser = await this.usersRepo.save(
      this.usersRepo.create({
        id: newId(),
        username,
        password_hash: await bcrypt.hash(password, 10),
        display_name: 'Administrador Principal',
        email: null,
        is_active: true,
        created_at: now,
        updated_at: now,
      }),
    );

    const adminRole = await this.rolesRepo.findOne({ where: { code: ROLE_CODES.ADMIN } });
    if (!adminRole) {
      throw new BadRequestException('No se pudo inicializar el rol administrador.');
    }

    await this.assignmentsRepo.save(
      this.assignmentsRepo.create({
        id: newId(),
        user_id: adminUser.id,
        role_id: adminRole.id,
        faculty_id: null,
        academic_program_id: null,
        is_active: true,
        created_at: now,
        updated_at: now,
      }),
    );

    this.logger.warn(
      `Usuario administrador inicial creado. username=${username} password=${password}`,
    );
  }

  private async requireActiveUser(userId: string) {
    const user = await this.usersRepo.findOne({ where: { id: userId } });
    if (!user || !user.is_active) {
      throw new UnauthorizedException('El usuario no existe o esta inactivo.');
    }
    return user;
  }

  private async issueTokens(userId: string, username: string) {
    const refreshId = newId();
    const refreshPayload: JwtRefreshPayload = {
      sub: userId,
      sid: refreshId,
      username,
      type: 'refresh',
    };
    const refreshToken = await this.jwtService.signAsync(refreshPayload, {
      secret: this.jwtSecret,
      expiresIn: `${this.refreshExpiresDays}d` as never,
    });
    const accessPayload: JwtAccessPayload = {
      sub: userId,
      username,
      type: 'access',
    };
    const accessToken = await this.jwtService.signAsync(accessPayload, {
      secret: this.jwtSecret,
      expiresIn: this.accessExpiresIn as never,
    });

    const now = new Date();
    await this.refreshTokensRepo.save(
      this.refreshTokensRepo.create({
        id: refreshId,
        user_id: userId,
        token_hash: hashToken(refreshToken),
        expires_at: new Date(now.getTime() + this.refreshExpiresDays * 24 * 60 * 60 * 1000),
        revoked_at: null,
        created_at: now,
        updated_at: now,
      }),
    );

    return {
      access_token: accessToken,
      refresh_token: refreshToken,
    };
  }

  private async verifyRefreshToken(token: string) {
    try {
      const payload = await this.jwtService.verifyAsync<JwtRefreshPayload>(token, {
        secret: this.jwtSecret,
      });
      if (payload.type !== 'refresh') {
        throw new UnauthorizedException('Refresh token invalido.');
      }
      return payload;
    } catch (error) {
      if (error instanceof UnauthorizedException) {
        throw error;
      }
      throw new UnauthorizedException('Refresh token invalido o expirado.');
    }
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

  private get jwtSecret() {
    return this.configService.get<string>('AUTH_JWT_SECRET', 'uai-dev-secret');
  }

  private get accessExpiresIn() {
    return this.configService.get<string>(
      'AUTH_ACCESS_TOKEN_EXPIRES_IN',
      ACCESS_TOKEN_DEFAULT_EXPIRES,
    );
  }

  private get refreshExpiresDays() {
    return Number(
      this.configService.get<string>(
        'AUTH_REFRESH_TOKEN_EXPIRES_DAYS',
        String(REFRESH_TOKEN_DEFAULT_EXPIRES_DAYS),
      ),
    );
  }
}

function uniqueIds(values: Array<string | null | undefined>) {
  return [...new Set(values.filter((item): item is string => Boolean(item)))];
}

function emptyToNull(value: string | null | undefined) {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function hashToken(token: string): string {
  return crypto.createHash('sha256').update(token).digest('hex');
}
