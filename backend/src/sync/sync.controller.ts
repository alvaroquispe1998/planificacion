import { Body, Controller, Post } from '@nestjs/common';
import { SyncAkademicDto } from './dto/sync-akademic.dto';
import { SyncService } from './sync.service';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Post('akademic')
  syncAkademic(@Body() dto: SyncAkademicDto) {
    return this.syncService.syncAkademic(dto);
  }
}
