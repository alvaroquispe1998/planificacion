import { AsyncPipe, CommonModule, NgClass } from '@angular/common';
import { Component } from '@angular/core';
import { DialogService } from '../../core/dialog.service';

@Component({
  selector: 'app-dialog',
  standalone: true,
  imports: [CommonModule, AsyncPipe, NgClass],
  templateUrl: './app-dialog.component.html',
  styleUrl: './app-dialog.component.css',
})
export class AppDialogComponent {
  readonly dialog$;

  constructor(private readonly dialogService: DialogService) {
    this.dialog$ = this.dialogService.dialog$;
  }

  accept() {
    this.dialogService.close(true);
  }

  cancel() {
    this.dialogService.close(false);
  }
}
