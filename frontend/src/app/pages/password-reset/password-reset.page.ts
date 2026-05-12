import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { AuthService } from '../../core/auth.service';

@Component({
  selector: 'app-password-reset-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './password-reset.page.html',
  styleUrl: './password-reset.page.css',
})
export class PasswordResetPageComponent {
  currentPassword = '';
  newPassword = '';
  confirmPassword = '';
  saving = false;
  successMessage = '';
  errorMessage = '';
  showCurrent = false;
  showNew = false;
  showConfirm = false;

  constructor(private readonly auth: AuthService) {}

  get passwordChecks() {
    const value = this.newPassword;
    return [
      { label: 'Al menos 8 caracteres', valid: value.length >= 8 },
      { label: 'Una letra mayúscula', valid: /[A-Z]/.test(value) },
      { label: 'Una letra minúscula', valid: /[a-z]/.test(value) },
      { label: 'Un número', valid: /\d/.test(value) },
      { label: 'Sin espacios', valid: value.length > 0 && !/\s/.test(value) },
    ];
  }

  get newPasswordValid() {
    return this.passwordChecks.every((item) => item.valid);
  }

  get passwordsMatch() {
    return this.confirmPassword.length > 0 && this.newPassword === this.confirmPassword;
  }

  get canSubmit() {
    return Boolean(this.currentPassword) && this.newPasswordValid && this.passwordsMatch && !this.saving;
  }

  submit() {
    this.successMessage = '';
    this.errorMessage = '';
    if (!this.canSubmit) {
      this.errorMessage = 'Completa las validaciones antes de guardar.';
      return;
    }
    if (this.currentPassword === this.newPassword) {
      this.errorMessage = 'La nueva contraseña debe ser diferente a la actual.';
      return;
    }

    this.saving = true;
    this.auth.changePassword(this.currentPassword, this.newPassword).subscribe({
      next: () => {
        this.saving = false;
        this.successMessage = 'Contraseña actualizada correctamente.';
        this.currentPassword = '';
        this.newPassword = '';
        this.confirmPassword = '';
      },
      error: (err) => {
        this.saving = false;
        this.errorMessage = err?.error?.message ?? 'No se pudo actualizar la contraseña.';
      },
    });
  }
}
