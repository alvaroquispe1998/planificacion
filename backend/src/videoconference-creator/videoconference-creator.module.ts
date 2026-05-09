import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import {
    MeetingInstanceEntity,
    MeetingParticipantEntity,
    MeetingRecordingEntity,
    ZoomUserEntity,
} from '../entities/audit.entities';
import { AuthUserEntity } from '../entities/auth.entities';
import {
    ManualVideoconferenceEntity,
    ManualVideoconferenceUserZoomGroupEntity,
} from '../entities/videoconference-creator.entities';
import {
    ZoomConfigEntity,
    ZoomGroupEntity,
    ZoomGroupUserEntity,
} from '../videoconference/videoconference.entity';
import { AuthModule } from '../auth/auth.module';
import { VideoconferenceModule } from '../videoconference/videoconference.module';
import { VideoconferenceCreatorController } from './videoconference-creator.controller';
import { VideoconferenceCreatorService } from './videoconference-creator.service';

@Module({
    imports: [
        AuthModule,
        VideoconferenceModule,
        TypeOrmModule.forFeature([
            ManualVideoconferenceEntity,
            ManualVideoconferenceUserZoomGroupEntity,
            ZoomGroupEntity,
            ZoomGroupUserEntity,
            ZoomUserEntity,
            MeetingInstanceEntity,
            MeetingParticipantEntity,
            MeetingRecordingEntity,
            ZoomConfigEntity,
            AuthUserEntity,
        ]),
    ],
    controllers: [VideoconferenceCreatorController],
    providers: [VideoconferenceCreatorService],
})
export class VideoconferenceCreatorModule { }
