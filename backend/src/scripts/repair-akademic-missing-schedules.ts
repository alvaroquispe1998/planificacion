import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../app.module';
import { PlanningImportService } from '../planning/planning-import.service';
import { PlanningManualService } from '../planning/planning-manual.service';

async function bootstrap() {
  const semesterId = process.argv[2];
  if (!semesterId) {
    throw new Error('Debes indicar el semester_id como primer argumento.');
  }

  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ['error', 'warn'],
  });

  try {
    const planningImportService = app.get(PlanningImportService);
    const planningManualService = app.get(PlanningManualService);

    const result = await planningImportService.repairMissingAkademicSchedulesForSemester(
      semesterId,
      {
        username: 'SCRIPT',
        display_name: 'Repair Akademic Missing Schedules',
      },
    );

    await (planningManualService as any).rebuildConflictsForSemester(semesterId, {
      username: 'SCRIPT',
      display_name: 'Repair Akademic Missing Schedules',
    });

    console.log(JSON.stringify(result, null, 2));
  } finally {
    await app.close();
  }
}

bootstrap().catch((error) => {
  console.error(error);
  process.exit(1);
});
