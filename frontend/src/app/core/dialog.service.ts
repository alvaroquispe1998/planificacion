import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

export type DialogTone = 'default' | 'danger' | 'success';

export type DialogRequest = {
  id: number;
  kind: 'alert' | 'confirm' | 'prompt';
  title: string;
  message: string;
  confirmLabel: string;
  cancelLabel: string;
  tone: DialogTone;
  inputLabel?: string;
  inputPlaceholder?: string;
  inputValue?: string;
  maxLength?: number;
};

type PendingDialog = DialogRequest & {
  resolve: (value: boolean | string | null) => void;
};

type DialogOptions = {
  title?: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  tone?: DialogTone;
};

type DialogPromptOptions = DialogOptions & {
  inputLabel?: string;
  inputPlaceholder?: string;
  inputValue?: string;
  maxLength?: number;
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
    }).then((value) => value === true);
  }

  prompt(messageOrOptions: string | DialogPromptOptions) {
    const options = this.normalizeOptions(messageOrOptions, {
      title: 'Confirmar accion',
      confirmLabel: 'Aceptar',
      cancelLabel: 'Cancelar',
      tone: 'default',
    });
    const promptOptions: Partial<DialogPromptOptions> = typeof messageOrOptions === 'string' ? {} : messageOrOptions;
    return this.enqueue({
      kind: 'prompt',
      title: options.title,
      message: options.message,
      confirmLabel: options.confirmLabel,
      cancelLabel: options.cancelLabel,
      tone: options.tone,
      inputLabel: promptOptions.inputLabel,
      inputPlaceholder: promptOptions.inputPlaceholder,
      inputValue: promptOptions.inputValue ?? '',
      maxLength: promptOptions.maxLength,
    }).then((value) => typeof value === 'string' ? value : null);
  }

  close(value: boolean | string | null) {
    if (!this.activeDialog) {
      return;
    }
    const current = this.activeDialog;
    this.activeDialog = null;
    this.dialogSubject.next(null);
    current.resolve(value);
    this.flushQueue();
  }

  private enqueue(request: Omit<DialogRequest, 'id'>) {
    return new Promise<boolean | string | null>((resolve) => {
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
