import { MigrationInterface, QueryRunner } from "typeorm";

export class AddVideoconferenceCreatorTables1778195371356 implements MigrationInterface {
    name = 'AddVideoconferenceCreatorTables1778195371356'

    public async up(queryRunner: QueryRunner): Promise<void> {
        // Eliminar el índice solo si existe (puede no existir en producción)
        const [idx] = await queryRunner.query(
            `SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME = 'planning_subsection_videoconferences'
               AND INDEX_NAME = 'IDX_673d12c4e0b28e57da86b66a22'
             LIMIT 1`
        );
        if (idx) {
            await queryRunner.query(`DROP INDEX \`IDX_673d12c4e0b28e57da86b66a22\` ON \`planning_subsection_videoconferences\``);
        }
        await queryRunner.query(`CREATE TABLE IF NOT EXISTS \`manual_videoconferences\` (\`id\` varchar(36) NOT NULL, \`created_by_user_id\` varchar(36) NOT NULL, \`zoom_group_id\` varchar(36) NOT NULL, \`assigned_zoom_user_id\` varchar(36) NULL, \`backup_zoom_user_id\` varchar(36) NULL, \`type\` enum ('UNIQUE', 'WEEKLY') NOT NULL, \`topic\` varchar(300) NOT NULL, \`agenda\` text NULL, \`start_time\` datetime NOT NULL, \`end_time\` datetime NOT NULL, \`duration_minutes\` int UNSIGNED NOT NULL, \`recurrence_json\` json NULL, \`status\` enum ('CREATED', 'DRAFT_NO_HOST', 'APPROVED_WITH_BACKUP', 'ERROR', 'CANCELLED') NOT NULL DEFAULT 'CREATED', \`zoom_meeting_id\` varchar(50) NULL, \`join_url\` varchar(512) NULL, \`start_url\` varchar(1024) NULL, \`zoom_payload_json\` json NULL, \`zoom_response_json\` json NULL, \`error_message\` text NULL, \`created_at\` datetime NOT NULL, \`updated_at\` datetime NOT NULL, INDEX \`IDX_e5be634be8c8bb278a59b314c5\` (\`start_time\`), INDEX \`IDX_8304851251f059c6d8037be0f8\` (\`status\`), INDEX \`IDX_2ad921f4681737bee33736b6cd\` (\`zoom_group_id\`), INDEX \`IDX_b8b8ed173b702af5b4c6125d8a\` (\`created_by_user_id\`), PRIMARY KEY (\`id\`)) ENGINE=InnoDB`);
        await queryRunner.query(`CREATE TABLE IF NOT EXISTS \`manual_videoconference_user_zoom_groups\` (\`id\` varchar(36) NOT NULL, \`user_id\` varchar(36) NOT NULL, \`zoom_group_id\` varchar(36) NOT NULL, \`is_active\` tinyint NOT NULL DEFAULT 1, \`created_at\` datetime NOT NULL, \`updated_at\` datetime NOT NULL, INDEX \`IDX_d218cf6de8f42e582784a29d28\` (\`user_id\`), UNIQUE INDEX \`IDX_2cca2fd1c7ffb68953e61b482b\` (\`user_id\`, \`zoom_group_id\`), PRIMARY KEY (\`id\`)) ENGINE=InnoDB`);
        // Agregar columnas solo si no existen
        const [mcol] = await queryRunner.query(
            `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'meeting_instances' AND COLUMN_NAME = 'manual_videoconference_id'`
        );
        if (!mcol) await queryRunner.query(`ALTER TABLE \`meeting_instances\` ADD \`manual_videoconference_id\` varchar(36) NULL`);
        const [tcol] = await queryRunner.query(
            `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zoom_config' AND COLUMN_NAME = 'ti_alert_emails'`
        );
        if (!tcol) await queryRunner.query(`ALTER TABLE \`zoom_config\` ADD \`ti_alert_emails\` text NULL`);
        const [bcol] = await queryRunner.query(
            `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zoom_groups' AND COLUMN_NAME = 'backup_zoom_user_id'`
        );
        if (!bcol) await queryRunner.query(`ALTER TABLE \`zoom_groups\` ADD \`backup_zoom_user_id\` varchar(36) NULL`);
        // Recrear índice como no-unique si no existe ya
        const [idx2] = await queryRunner.query(
            `SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME = 'planning_subsection_videoconferences'
               AND INDEX_NAME = 'IDX_673d12c4e0b28e57da86b66a22'
               AND NON_UNIQUE = 1
             LIMIT 1`
        );
        if (!idx2) {
            await queryRunner.query(`CREATE INDEX \`IDX_673d12c4e0b28e57da86b66a22\` ON \`planning_subsection_videoconferences\` (\`planning_subsection_schedule_id\`, \`conference_date\`)`);
        }
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP INDEX \`IDX_673d12c4e0b28e57da86b66a22\` ON \`planning_subsection_videoconferences\``);
        await queryRunner.query(`ALTER TABLE \`zoom_groups\` DROP COLUMN \`backup_zoom_user_id\``);
        await queryRunner.query(`ALTER TABLE \`zoom_config\` DROP COLUMN \`ti_alert_emails\``);
        await queryRunner.query(`ALTER TABLE \`meeting_instances\` DROP COLUMN \`manual_videoconference_id\``);
        await queryRunner.query(`DROP INDEX \`IDX_2cca2fd1c7ffb68953e61b482b\` ON \`manual_videoconference_user_zoom_groups\``);
        await queryRunner.query(`DROP INDEX \`IDX_d218cf6de8f42e582784a29d28\` ON \`manual_videoconference_user_zoom_groups\``);
        await queryRunner.query(`DROP TABLE \`manual_videoconference_user_zoom_groups\``);
        await queryRunner.query(`DROP INDEX \`IDX_b8b8ed173b702af5b4c6125d8a\` ON \`manual_videoconferences\``);
        await queryRunner.query(`DROP INDEX \`IDX_2ad921f4681737bee33736b6cd\` ON \`manual_videoconferences\``);
        await queryRunner.query(`DROP INDEX \`IDX_8304851251f059c6d8037be0f8\` ON \`manual_videoconferences\``);
        await queryRunner.query(`DROP INDEX \`IDX_e5be634be8c8bb278a59b314c5\` ON \`manual_videoconferences\``);
        await queryRunner.query(`DROP TABLE \`manual_videoconferences\``);
        await queryRunner.query(`CREATE UNIQUE INDEX \`IDX_673d12c4e0b28e57da86b66a22\` ON \`planning_subsection_videoconferences\` (\`planning_subsection_schedule_id\`, \`conference_date\`)`);
    }

}
