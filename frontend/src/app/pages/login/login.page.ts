import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { finalize } from 'rxjs';
import { AuthService } from '../../core/auth.service';

@Component({
  selector: 'app-login-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './login.page.html',
  styleUrl: './login.page.css',
})
export class LoginPageComponent {
  username = '';
  password = '';
  isSubmitting = false;
  errorMessage = '';

  constructor(
    private readonly auth: AuthService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
  ) {
    if (this.route.snapshot.queryParamMap.get('reason') === 'no-access') {
      this.errorMessage =
        'Tu usuario no tiene una ventana habilitada para ingresar. Revisa sus permisos.';
    }
  }

  submit() {
    if (!this.username.trim() || !this.password.trim()) {
      this.errorMessage = 'Ingresa usuario y password.';
      return;
    }

    this.isSubmitting = true;
    this.errorMessage = '';
    this.auth
      .login(this.username, this.password)
      .pipe(finalize(() => (this.isSubmitting = false)))
      .subscribe({
      next: () => {
        const redirect = this.normalizeRedirect(this.route.snapshot.queryParamMap.get('redirect'));
        const target = redirect || this.auth.firstAllowedPath();
        if (!target) {
          this.auth.clearLocalSession();
          this.errorMessage =
            'Tu usuario no tiene una ventana habilitada para ingresar. Revisa sus permisos.';
          return;
        }
        void this.router.navigateByUrl(target).catch(() => {
          this.auth.clearLocalSession();
          this.errorMessage =
            'El login fue valido, pero no se pudo abrir una ruta permitida para este usuario.';
        });
      },
      error: (error) => {
        this.errorMessage =
          error?.error?.message ??
          (error?.name === 'TimeoutError'
            ? 'La validacion del login demoro demasiado. Verifica el backend e intenta otra vez.'
            : 'No se pudo iniciar sesion. Verifica tus credenciales.');
      },
    });
  }

  private normalizeRedirect(value: string | null) {
    if (!value) {
      return null;
    }
    return value.startsWith('/') ? value : null;
  }
}
