import {
  ArrayMaxSize,
  ArrayNotEmpty,
  IsArray,
  IsBoolean,
  IsEmail,
  IsNotEmpty,
  IsOptional,
  IsString,
  Length,
  MinLength,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';

export class LoginDto {
  @IsString()
  @IsNotEmpty()
  username!: string;

  @IsString()
  @IsNotEmpty()
  password!: string;
}

export class RefreshTokenDto {
  @IsString()
  @IsNotEmpty()
  refresh_token!: string;
}

export class LogoutDto {
  @IsOptional()
  @IsString()
  refresh_token?: string;
}

export class UpsertAuthUserDto {
  @IsString()
  @Length(3, 80)
  username!: string;

  @IsOptional()
  @IsString()
  @MinLength(6)
  password?: string;

  @IsString()
  @Length(2, 150)
  display_name!: string;

  @IsOptional()
  @IsEmail()
  email?: string | null;

  @IsOptional()
  @IsBoolean()
  is_active?: boolean;
}

export class UpsertAuthRoleDto {
  @IsString()
  @Length(2, 80)
  code!: string;

  @IsString()
  @Length(2, 120)
  name!: string;

  @IsOptional()
  @IsBoolean()
  is_active?: boolean;

  @IsOptional()
  @IsBoolean()
  is_editable?: boolean;
}

export class ReplaceRolePermissionsDto {
  @IsArray()
  @ArrayMaxSize(200)
  @IsString({ each: true })
  permission_ids!: string[];
}

export class UserRoleAssignmentUpsertDto {
  @IsString()
  @IsNotEmpty()
  role_id!: string;

  @IsOptional()
  @IsString()
  faculty_id?: string | null;

  @IsOptional()
  @IsString()
  academic_program_id?: string | null;

  @IsOptional()
  @IsBoolean()
  is_active?: boolean;
}

export class ReplaceUserAssignmentsDto {
  @IsArray()
  @ArrayNotEmpty()
  @ArrayMaxSize(50)
  @ValidateNested({ each: true })
  @Type(() => UserRoleAssignmentUpsertDto)
  assignments!: UserRoleAssignmentUpsertDto[];
}
