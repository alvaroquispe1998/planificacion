import { AsyncPipe, CommonModule, NgClass } from '@angular/common';
import { Component, OnDestroy } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import { DialogService } from '../../core/dialog.service';

@Component({
  selector: 'app-dialog',
  standalone: true,
  imports: [CommonModule, AsyncPipe, NgClass, FormsModule],
  templateUrl: './app-dialog.component.html',
  styleUrl: './app-dialog.component.css',
})
export class AppDialogComponent implements OnDestroy {
  readonly dialog$;
  promptValue = '';
  private currentDialogId: number | null = null;
  private readonly dialogSub: Subscription;

  constructor(private readonly dialogService: DialogService) {
    this.dialog$ = this.dialogService.dialog$;
    this.dialogSub = this.dialog$.subscribe((dialog) => {
      if (!dialog) {
        this.currentDialogId = null;
        this.promptValue = '';
        return;
      }
      if (dialog.id !== this.currentDialogId) {
        this.currentDialogId = dialog.id;
        this.promptValue = dialog.inputValue ?? '';
      }
    });
  }

  ngOnDestroy() {
    this.dialogSub.unsubscribe();
  }

  accept(kind: 'alert' | 'confirm' | 'prompt') {
    this.dialogService.close(kind === 'prompt' ? this.promptValue : true);
  }

  cancel() {
    this.dialogService.close(false);
  }
}
