import 'reflect-metadata';
import { existsSync, readFileSync } from 'fs';
import { DataSource } from 'typeorm';
import { appEntities } from '../config/typeorm.config';

type ResetOptions = {
  apply: boolean;
  confirm: string;
  wipeConfig: boolean;
};

type TablePlan = {
  name: string;
  category: 'runtime' | 'config';
};

const REQUIRED_CONFIRMATION = 'RESET_PLANNING_WORKSPACE';

const RUNTIME_TABLES: TablePlan[] = [
  { name: 'videoconference_generation_batch_results', category: 'runtime' },
  { name: 'videoconference_generation_batches', category: 'runtime' },
  { name: 'planning_subsection_videoconference_overrides', category: 'runtime' },
  { name: 'planning_subsection_videoconferences', category: 'runtime' },
  { name: 'planning_subsection_schedule_vc_inheritances', category: 'runtime' },
  { name: 'planning_schedule_conflicts_v2', category: 'runtime' },
  { name: 'planning_change_logs', category: 'runtime' },
  { name: 'planning_subsection_schedules', category: 'runtime' },
  { name: 'planning_subsections', category: 'runtime' },
  { name: 'planning_sections', category: 'runtime' },
  { name: 'planning_offers', category: 'runtime' },
  { name: 'planning_import_row_issues', category: 'runtime' },
  { name: 'planning_import_scope_decisions', category: 'runtime' },
  { name: 'planning_import_rows', category: 'runtime' },
  { name: 'planning_import_batches', category: 'runtime' },
];

const CONFIG_TABLES: TablePlan[] = [
  { name: 'planning_cycle_plan_rules', category: 'config' },
  { name: 'planning_campus_vc_location_mappings', category: 'config' },
  { name: 'planning_import_alias_mappings', category: 'config' },
];

async function bootstrap() {
  loadDotEnv();
  const options = parseArgs(process.argv.slice(2));
  const tables = options.wipeConfig ? [...RUNTIME_TABLES, ...CONFIG_TABLES] : [...RUNTIME_TABLES];

  const dataSource = new DataSource({
    type: 'mysql',
    host: process.env.DB_HOST ?? 'localhost',
    port: Number(process.env.DB_PORT ?? '3306'),
    username: process.env.DB_USER ?? 'root',
    password: process.env.DB_PASSWORD ?? 'root',
    database: process.env.DB_NAME ?? 'uai_planning',
    entities: appEntities,
    synchronize: false,
    timezone: 'Z',
  });

  await dataSource.initialize();

  try {
    const before = await collectCounts(dataSource, tables);
    printPlan(options, before);

    if (!options.apply) {
      console.log('');
      console.log('Dry run completado. No se elimino nada.');
      console.log(
        `Para ejecutar de verdad usa: npm run reset:planning-workspace -- --apply --confirm ${REQUIRED_CONFIRMATION}`,
      );
      if (!options.wipeConfig) {
        console.log(
          'Si tambien quieres borrar reglas/mapas/aliases de planificacion agrega: --wipe-config',
        );
      }
      return;
    }

    if (options.confirm !== REQUIRED_CONFIRMATION) {
      throw new Error(
        `Confirmacion invalida. Debes pasar --confirm ${REQUIRED_CONFIRMATION} para ejecutar la limpieza.`,
      );
    }

    const queryRunner = dataSource.createQueryRunner();
    await queryRunner.connect();
    try {
      await queryRunner.startTransaction();
      await queryRunner.query('SET FOREIGN_KEY_CHECKS = 0');
      for (const table of tables) {
        if (!(await tableExists(dataSource, table.name))) {
          continue;
        }
        await queryRunner.query(`DELETE FROM ${table.name}`);
      }
      await queryRunner.query('SET FOREIGN_KEY_CHECKS = 1');
      await queryRunner.commitTransaction();
    } catch (error) {
      try {
        await queryRunner.query('SET FOREIGN_KEY_CHECKS = 1');
      } catch {
        // no-op
      }
      await queryRunner.rollbackTransaction();
      throw error;
    } finally {
      await queryRunner.release();
    }

    const after = await collectCounts(dataSource, tables);
    console.log('');
    console.log('Limpieza completada.');
    console.log(JSON.stringify({ after }, null, 2));
  } finally {
    await dataSource.destroy();
  }
}

function parseArgs(args: string[]): ResetOptions {
  let apply = false;
  let wipeConfig = false;
  let confirm = '';

  for (let index = 0; index < args.length; index += 1) {
    const current = args[index];
    if (current === '--apply') {
      apply = true;
      continue;
    }
    if (current === '--wipe-config') {
      wipeConfig = true;
      continue;
    }
    if (current === '--confirm') {
      confirm = args[index + 1] ?? '';
      index += 1;
      continue;
    }
  }

  return { apply, confirm, wipeConfig };
}

async function collectCounts(dataSource: DataSource, tables: TablePlan[]) {
  const counts: Record<string, number> = {};
  for (const table of tables) {
    if (!(await tableExists(dataSource, table.name))) {
      counts[table.name] = -1;
      continue;
    }
    const rows = await dataSource.query(`SELECT COUNT(*) AS qty FROM ${table.name}`);
    counts[table.name] = Number(rows?.[0]?.qty ?? 0);
  }
  return counts;
}

function printPlan(options: ResetOptions, counts: Record<string, number>) {
  const runtimeSummary = summarize(RUNTIME_TABLES, counts);
  const configSummary = summarize(CONFIG_TABLES, counts);

  console.log(
    JSON.stringify(
      {
        mode: options.apply ? 'APPLY' : 'DRY_RUN',
        wipe_config: options.wipeConfig,
        runtime_tables: runtimeSummary,
        config_tables: options.wipeConfig ? configSummary : 'preserved',
      },
      null,
      2,
    ),
  );
}

function summarize(tables: TablePlan[], counts: Record<string, number>) {
  return {
    total_rows: tables.reduce((sum, table) => sum + Math.max(counts[table.name] ?? 0, 0), 0),
    tables: tables.map((table) => ({
      table: table.name,
      rows: counts[table.name] ?? 0,
      exists: (counts[table.name] ?? 0) >= 0,
    })),
  };
}

async function tableExists(dataSource: DataSource, tableName: string) {
  const rows = await dataSource.query(
    `SELECT COUNT(*) AS qty
     FROM information_schema.tables
     WHERE table_schema = DATABASE()
       AND table_name = ?`,
    [tableName],
  );
  return Number(rows?.[0]?.qty ?? 0) > 0;
}

function loadDotEnv() {
  if (!existsSync('.env')) {
    return;
  }
  const lines = readFileSync('.env', 'utf-8').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const separator = trimmed.indexOf('=');
    if (separator <= 0) {
      continue;
    }
    const key = trimmed.slice(0, separator).trim();
    const value = trimmed.slice(separator + 1).trim();
    if (!(key in process.env)) {
      process.env[key] = value;
    }
  }
}

bootstrap().catch((error) => {
  console.error(error);
  process.exit(1);
});
