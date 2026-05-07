import { CommonModule } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ManualMeeting, VideoconferenceCreatorApiService } from '../../services/videoconference-creator-api.service';

@Component({
    selector: 'app-videoconference-creator-drafts-page',
    standalone: true,
    imports: [CommonModule],
    templateUrl: './videoconference-creator-drafts.page.html',
    styleUrl: './videoconference-creator-drafts.page.css',
})
export class VideoconferenceCreatorDraftsPageComponent implements OnInit {
    drafts: ManualMeeting[] = [];
    loading = true;
    approvingId = '';
    error = '';

    constructor(
        private readonly api: VideoconferenceCreatorApiService,
        private readonly router: Router,
    ) { }

    ngOnInit(): void {
        this.load();
    }

    load(): void {
        this.loading = true;
        this.api.listDrafts().subscribe({
            next: (drafts) => {
                this.drafts = drafts;
                this.loading = false;
            },
            error: () => {
                this.error = 'No se pudieron cargar los borradores.';
                this.loading = false;
            },
        });
    }

    approveBackup(id: string): void {
        this.approvingId = id;
        this.error = '';
        this.api.approveDraft(id).subscribe({
            next: () => {
                this.approvingId = '';
                this.load();
            },
            error: (err) => {
                this.approvingId = '';
                this.error = err?.error?.message ?? 'Error al aprobar el borrador.';
            },
        });
    }

    openDetail(id: string): void {
        this.router.navigate(['/videoconferences/creator', id]);
    }
}
