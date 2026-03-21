import { Body, Controller, Get, Post } from '@nestjs/common';
import { CurrentAuthUser } from './current-auth-user.decorator';
import { AuthService } from './auth.service';
import { LoginDto, LogoutDto, RefreshTokenDto } from './dto/auth.dto';
import { Public } from './public.decorator';
import type { AuthenticatedRequestUser } from './auth.service';

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Public()
  @Post('login')
  login(@Body() dto: LoginDto) {
    return this.authService.login(dto);
  }

  @Public()
  @Post('refresh')
  refresh(@Body() dto: RefreshTokenDto) {
    return this.authService.refresh(dto);
  }

  @Post('logout')
  logout(
    @CurrentAuthUser() authUser: AuthenticatedRequestUser,
    @Body() dto: LogoutDto,
  ) {
    return this.authService.logout(authUser.id, dto.refresh_token);
  }

  @Get('me')
  me(@CurrentAuthUser() authUser: AuthenticatedRequestUser) {
    return this.authService.me(authUser);
  }
}
