import { Injectable, Logger } from '@nestjs/common';
import { Interval } from '@nestjs/schedule';
import { AuditService } from './audit.service';

/** Runs every 5 minutes and attempts to reconcile CREATED_UNMATCHED meetings
 *  with their Zoom counterpart. This covers the case where the immediate
 *  post-creation match failed (e.g. the meeting was already live and Zoom
 *  returned duration=0) without requiring manual intervention from the UI. */
@Injectable()
export class ReconcileTaskService {
    private readonly logger = new Logger(ReconcileTaskService.name);
    private running = false;

    constructor(private readonly auditService: AuditService) {}

    @Interval(5 * 60 * 1000) // every 5 minutes
    async reconcileUnmatched() {
        if (this.running) {
            return;
        }
        this.running = true;
        try {
            const result = await this.auditService.syncPendingPlanningVideoconferences({
                status: 'CREATED_UNMATCHED',
                hide_inherited: true,
                limit: 20,
            });
            if (result.processed > 0) {
                this.logger.log(
                    `Auto-reconcile: processed=${result.processed} reconciled=${result.reconciled} synced=${result.synced} errors=${result.errors}`,
                );
            }
        } catch (err) {
            this.logger.error('Auto-reconcile failed', err instanceof Error ? err.stack : String(err));
        } finally {
            this.running = false;
        }
    }
}
