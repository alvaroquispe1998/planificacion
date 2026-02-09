import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuditModule } from './audit/audit.module';
import { PlanningModule } from './planning/planning.module';
import { SettingsModule } from './settings/settings.module';
import { SyncModule } from './sync/sync.module';
import { SyllabusModule } from './syllabus/syllabus.module';
import { AppController } from './app.controller';
import { buildTypeOrmConfig } from './config/typeorm.config';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => buildTypeOrmConfig(configService),
    }),
    PlanningModule,
    SettingsModule,
    SyncModule,
    AuditModule,
    SyllabusModule,
  ],
  controllers: [AppController],
})
export class AppModule {}
