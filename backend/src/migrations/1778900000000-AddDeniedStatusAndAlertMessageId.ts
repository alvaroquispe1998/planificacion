import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddDeniedStatusAndAlertMessageId1778900000000 implements MigrationInterface {
    name = 'AddDeniedStatusAndAlertMessageId1778900000000';

    public async up(queryRunner: QueryRunner): Promise<void> {
        // 1. Add DENIED to the status enum
        await queryRunner.query(
            `ALTER TABLE \`manual_videoconferences\`
             MODIFY COLUMN \`status\` ENUM('CREATED','DRAFT_NO_HOST','APPROVED_WITH_BACKUP','DENIED','ERROR','CANCELLED')
             NOT NULL DEFAULT 'CREATED'`,
        );

        // 2. Add ti_alert_message_id column (idempotent)
        const [row] = await queryRunner.query(
            `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME   = 'manual_videoconferences'
               AND COLUMN_NAME  = 'ti_alert_message_id'
             LIMIT 1`,
        );
        if (!row) {
            await queryRunner.query(
                `ALTER TABLE \`manual_videoconferences\`
                 ADD \`ti_alert_message_id\` varchar(512) NULL`,
            );
        }
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        // Restore enum without DENIED
        await queryRunner.query(
            `ALTER TABLE \`manual_videoconferences\`
             MODIFY COLUMN \`status\` ENUM('CREATED','DRAFT_NO_HOST','APPROVED_WITH_BACKUP','ERROR','CANCELLED')
             NOT NULL DEFAULT 'CREATED'`,
        );

        const [row] = await queryRunner.query(
            `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME   = 'manual_videoconferences'
               AND COLUMN_NAME  = 'ti_alert_message_id'
             LIMIT 1`,
        );
        if (row) {
            await queryRunner.query(
                `ALTER TABLE \`manual_videoconferences\` DROP COLUMN \`ti_alert_message_id\``,
            );
        }
    }
}
