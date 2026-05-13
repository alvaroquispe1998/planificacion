import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddMsGraphConfigToZoomConfig1778800000000 implements MigrationInterface {
    name = 'AddMsGraphConfigToZoomConfig1778800000000';

    private readonly columns = [
        { name: 'ms_tenant_id',      def: 'varchar(100)  NULL' },
        { name: 'ms_client_id',      def: 'varchar(100)  NULL' },
        { name: 'ms_client_secret',  def: 'varchar(512)  NULL' },
        { name: 'mail_ti_recipient', def: 'varchar(255)  NULL' },
        { name: 'system_public_url', def: 'varchar(512)  NULL' },
    ];

    public async up(queryRunner: QueryRunner): Promise<void> {
        for (const col of this.columns) {
            const [row] = await queryRunner.query(
                `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE()
                   AND TABLE_NAME   = 'zoom_config'
                   AND COLUMN_NAME  = '${col.name}'
                 LIMIT 1`,
            );
            if (!row) {
                await queryRunner.query(
                    `ALTER TABLE \`zoom_config\` ADD \`${col.name}\` ${col.def}`,
                );
            }
        }
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        for (const col of [...this.columns].reverse()) {
            const [row] = await queryRunner.query(
                `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE()
                   AND TABLE_NAME   = 'zoom_config'
                   AND COLUMN_NAME  = '${col.name}'
                 LIMIT 1`,
            );
            if (row) {
                await queryRunner.query(
                    `ALTER TABLE \`zoom_config\` DROP COLUMN \`${col.name}\``,
                );
            }
        }
    }
}
