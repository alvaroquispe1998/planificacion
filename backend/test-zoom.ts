import { NestFactory } from '@nestjs/core';
import { AppModule } from './src/app.module';
import { ZoomAccountService } from './src/videoconference/zoom-account.service';

async function bootstrap() {
  try {
    const app = await NestFactory.createApplicationContext(AppModule);
    const zoomService = app.get(ZoomAccountService);
    
    console.log('Fetching meeting 89795676909...');
    // Fetch raw directly to see the exact payload before mapMeetingSummary
    const rawData = await zoomService['fetchZoomJsonOrNull'](`/meetings/89795676909`);
    console.log('\n--- RAW ZOOM API DATA ---');
    console.log(JSON.stringify(rawData, null, 2));
    
    console.log('\n--- MAP MEETING SUMMARY ---');
    const mapped = zoomService.getMeeting('89795676909');
    console.log(JSON.stringify(await mapped, null, 2));
    
    await app.close();
    process.exit(0);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

bootstrap();
