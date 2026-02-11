import { CommonModule } from '@angular/common';
import { Component, EventEmitter, HostListener, Input, Output, ElementRef } from '@angular/core';
import { FormsModule } from '@angular/forms';

export interface MultiSelectOption {
    id: string;
    label: string;
}

@Component({
    selector: 'app-multi-select',
    standalone: true,
    imports: [CommonModule, FormsModule],
    templateUrl: './multi-select.component.html',
    styleUrls: ['./multi-select.component.css']
})
export class MultiSelectComponent {
    @Input() label: string = '';
    @Input() placeholder: string = 'Seleccionar...';
    @Input() options: MultiSelectOption[] = [];
    @Input() disabled: boolean = false;

    // Two-way binding for selectedIds
    @Input() selectedIds: string[] = [];
    @Output() selectedIdsChange = new EventEmitter<string[]>();
    @Output() selectionChange = new EventEmitter<string[]>();
    // Backward compatibility for existing templates using (change)
    @Output() change = new EventEmitter<string[]>();

    isOpen = false;
    searchText = '';

    constructor(private elementRef: ElementRef) { }

    toggleDropdown() {
        if (!this.disabled) {
            this.isOpen = !this.isOpen;
            if (!this.isOpen) {
                this.searchText = '';
            }
        }
    }

    closeDropdown() {
        this.isOpen = false;
        this.searchText = '';
    }

    @HostListener('document:click', ['$event'])
    onClickOutside(event: Event) {
        if (!this.elementRef.nativeElement.contains(event.target)) {
            this.closeDropdown();
        }
    }

    get filteredOptions() {
        if (!this.searchText) return this.options;
        return this.options.filter(opt =>
            opt.label.toLowerCase().includes(this.searchText.toLowerCase())
        );
    }

    isSelected(id: string): boolean {
        return this.selectedIds.includes(id);
    }

    toggleOption(id: string) {
        if (this.isSelected(id)) {
            this.selectedIds = this.selectedIds.filter(selId => selId !== id);
        } else {
            this.selectedIds = [...this.selectedIds, id];
        }
        // Ensure we handle the change properly
        this.emitChange();
    }

    toggleAll(event: any) {
        if (event.target.checked) {
            // Select all visible (filtered) options
            const visibleIds = this.filteredOptions.map(opt => opt.id);
            // Combine with existing selected
            this.selectedIds = Array.from(new Set([...this.selectedIds, ...visibleIds]));
        } else {
            // Deselect all visible options
            const visibleIds = this.filteredOptions.map(opt => opt.id);
            this.selectedIds = this.selectedIds.filter(id => !visibleIds.includes(id));
        }
        this.emitChange();
    }

    isAllSelected(): boolean {
        if (!this.filteredOptions.length) return false;
        return this.filteredOptions.every(opt => this.isSelected(opt.id));
    }

    isIndeterminate(): boolean {
        if (!this.filteredOptions.length) return false;
        const selectedCount = this.filteredOptions.filter(opt => this.isSelected(opt.id)).length;
        return selectedCount > 0 && selectedCount < this.filteredOptions.length;
    }

    get displayValue(): string {
        if (!this.selectedIds.length) return this.placeholder;
        if (this.selectedIds.length === this.options.length && this.options.length > 0) return 'Todos seleccionados';

        // Find labels for selected IDs
        const selectedLabels = this.options
            .filter(opt => this.selectedIds.includes(opt.id))
            .map(opt => opt.label);

        if (selectedLabels.length <= 2) {
            return selectedLabels.join(', ');
        }
        return `${selectedLabels.length} seleccionados`;
    }

    emitChange() {
        this.selectedIdsChange.emit(this.selectedIds);
        this.selectionChange.emit(this.selectedIds);
        this.change.emit(this.selectedIds);
    }
}
