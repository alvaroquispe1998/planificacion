/**
 * DataSource para TypeORM CLI (migraciones).
 * Uso:
 *   npm run migration:generate -- src/migrations/NombreMigracion
 *   npm run migration:run
 *   npm run migration:revert
 *
 * Variables de entorno requeridas (o usa el archivo .env del proyecto):
 *   DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
 */
import 'reflect-metadata';
import * as dotenv from 'dotenv';
import { DataSource } from 'typeorm';
import { appEntities } from './typeorm.config';

dotenv.config();

export const AppDataSource = new DataSource({
  type: 'mysql',
  host: process.env.DB_HOST ?? 'localhost',
  port: Number(process.env.DB_PORT ?? 3306),
  username: process.env.DB_USER ?? 'root',
  password: process.env.DB_PASSWORD ?? 'root',
  database: process.env.DB_NAME ?? 'uai_planning',
  entities: appEntities,
  migrations: ['src/migrations/*.ts'],
  synchronize: false,
  timezone: 'Z',
});
