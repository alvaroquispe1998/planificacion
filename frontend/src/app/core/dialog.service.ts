import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

export type DialogTone = 'default' | 'danger' | 'success';

export type DialogRequest = {
  id: number;
  kind: 'alert' | 'confirm';
  title: string;
  message: string;
  confirmLabel: string;
  cancelLabel: string;
  tone: DialogTone;
};

type PendingDialog = DialogRequest & {
  resolve: (value: boolean) => void;
};

type DialogOptions = {
  title?: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  tone?: DialogTone;
};

@Injectable({ providedIn: 'root' })
export class DialogService {
  private readonly dialogSubject = new BehaviorSubject<DialogRequest | null>(null);
  readonly dialog$ = this.dialogSubject.asObservable();

  private readonly queue: PendingDialog[] = [];
  private activeDialog: PendingDialog | null = null;
  private nextId = 1;

  alert(messageOrOptions: string | DialogOptions) {
    const options = this.normalizeOptions(messageOrOptions, {
      title: 'Aviso',
      confirmLabel: 'Entendido',
      cancelLabel: '',
      tone: 'default',
    });
    return this.enqueue({
      kind: 'alert',
      title: options.title,
      message: options.message,
      confirmLabel: options.confirmLabel,
      cancelLabel: '',
      tone: options.tone,
    }).then(() => undefined);
  }

  confirm(messageOrOptions: string | DialogOptions) {
    const options = this.normalizeOptions(messageOrOptions, {
      title: 'Confirmar accion',
      confirmLabel: 'Aceptar',
      cancelLabel: 'Cancelar',
      tone: 'default',
    });
    return this.enqueue({
      kind: 'confirm',
      title: options.title,
      message: options.message,
      confirmLabel: options.confirmLabel,
      cancelLabel: options.cancelLabel,
      tone: options.tone,
    });
  }

  close(accepted: boolean) {
    if (!this.activeDialog) {
      return;
    }
    const current = this.activeDialog;
    this.activeDialog = null;
    this.dialogSubject.next(null);
    current.resolve(accepted);
    this.flushQueue();
  }

  private enqueue(request: Omit<DialogRequest, 'id'>) {
    return new Promise<boolean>((resolve) => {
      this.queue.push({
        ...request,
        id: this.nextId++,
        resolve,
      });
      this.flushQueue();
    });
  }

  private flushQueue() {
    if (this.activeDialog || this.queue.length === 0) {
      return;
    }
    this.activeDialog = this.queue.shift() ?? null;
    this.dialogSubject.next(this.activeDialog);
  }

  private normalizeOptions(
    value: string | DialogOptions,
    defaults: Required<Pick<DialogOptions, 'title' | 'confirmLabel' | 'cancelLabel' | 'tone'>>,
  ) {
    if (typeof value === 'string') {
      return {
        ...defaults,
        message: value,
      };
    }
    return {
      title: value.title ?? defaults.title,
      message: value.message,
      confirmLabel: value.confirmLabel ?? defaults.confirmLabel,
      cancelLabel: value.cancelLabel ?? defaults.cancelLabel,
      tone: value.tone ?? defaults.tone,
    };
  }
}
