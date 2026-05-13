import { MigrationInterface, QueryRunner } from "typeorm";

export class AddZoomGroupBackupGroup1778629000000 implements MigrationInterface {
    name = 'AddZoomGroupBackupGroup1778629000000'

    public async up(queryRunner: QueryRunner): Promise<void> {
        const [column] = await queryRunner.query(
            `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME = 'zoom_groups'
               AND COLUMN_NAME = 'backup_zoom_group_id'
             LIMIT 1`,
        );
        if (!column) {
            await queryRunner.query(`ALTER TABLE \`zoom_groups\` ADD \`backup_zoom_group_id\` varchar(36) NULL`);
        }
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        const [column] = await queryRunner.query(
            `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME = 'zoom_groups'
               AND COLUMN_NAME = 'backup_zoom_group_id'
             LIMIT 1`,
        );
        if (column) {
            await queryRunner.query(`ALTER TABLE \`zoom_groups\` DROP COLUMN \`backup_zoom_group_id\``);
        }
    }
}
