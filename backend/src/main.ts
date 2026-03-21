import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ConfigService } from '@nestjs/config';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  // ✅ IMPORTANTE: proxy (nginx, apache, cloudflare, etc.)
  app.getHttpAdapter().getInstance().set('trust proxy', true);
  
  // ✅ CORS dinámico
  const configService = app.get(ConfigService);
  const corsEnv = configService.get<string>('CORS_ORIGINS');
  if (corsEnv && `${corsEnv}`.trim() !== '') {
    if (corsEnv === '*') {
      app.enableCors();
    } else {
      const origins = corsEnv.split(',').map((s) => s.trim()).filter(Boolean);
      app.enableCors({ origin: origins.length === 1 ? origins[0] : origins });
    }
  } else {
    app.enableCors();
  }

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  await app.listen(process.env.PORT ?? 3000);
}

bootstrap();
