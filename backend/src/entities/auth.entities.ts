import { Column, Entity, Index, PrimaryColumn } from 'typeorm';

export const AuthPermissionTypeValues = ['WINDOW', 'ACTION'] as const;

@Entity({ name: 'auth_users' })
@Index(['username'], { unique: true })
export class AuthUserEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 80 })
  username!: string;

  @Column({ type: 'varchar', length: 255 })
  password_hash!: string;

  @Column({ type: 'varchar', length: 150 })
  display_name!: string;

  @Column({ type: 'varchar', length: 150, nullable: true })
  email!: string | null;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'auth_roles' })
@Index(['code'], { unique: true })
export class AuthRoleEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 80 })
  code!: string;

  @Column({ type: 'varchar', length: 120 })
  name!: string;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'boolean', default: true })
  is_editable!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'auth_permissions' })
@Index(['code'], { unique: true })
export class AuthPermissionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 120 })
  code!: string;

  @Column({ type: 'enum', enum: AuthPermissionTypeValues })
  type!: (typeof AuthPermissionTypeValues)[number];

  @Column({ type: 'varchar', length: 180 })
  description!: string;

  @Column({ type: 'varchar', length: 120, nullable: true })
  display_name!: string | null;

  @Column({ type: 'varchar', length: 80, nullable: true })
  group_key!: string | null;

  @Column({ type: 'varchar', length: 120, nullable: true })
  parent_window_code!: string | null;

  @Column({ type: 'int', default: 0 })
  sort_order!: number;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'auth_role_permissions' })
@Index(['role_id', 'permission_id'], { unique: true })
export class AuthRolePermissionEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  role_id!: string;

  @Column({ type: 'varchar', length: 36 })
  permission_id!: string;

  @Column({ type: 'datetime' })
  created_at!: Date;
}

@Entity({ name: 'auth_user_role_assignments' })
@Index(['user_id'])
@Index(['role_id'])
@Index(['faculty_id'])
@Index(['academic_program_id'])
export class AuthUserRoleAssignmentEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  user_id!: string;

  @Column({ type: 'varchar', length: 36 })
  role_id!: string;

  @Column({ type: 'varchar', length: 36, nullable: true })
  faculty_id!: string | null;

  @Column({ type: 'varchar', length: 36, nullable: true })
  academic_program_id!: string | null;

  @Column({ type: 'boolean', default: true })
  is_active!: boolean;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}

@Entity({ name: 'auth_refresh_tokens' })
@Index(['user_id'])
export class AuthRefreshTokenEntity {
  @PrimaryColumn({ type: 'varchar', length: 36 })
  id!: string;

  @Column({ type: 'varchar', length: 36 })
  user_id!: string;

  @Column({ type: 'varchar', length: 255 })
  token_hash!: string;

  @Column({ type: 'datetime' })
  expires_at!: Date;

  @Column({ type: 'datetime', nullable: true })
  revoked_at!: Date | null;

  @Column({ type: 'datetime' })
  created_at!: Date;

  @Column({ type: 'datetime' })
  updated_at!: Date;
}
