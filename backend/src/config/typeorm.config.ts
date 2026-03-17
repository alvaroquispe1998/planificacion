import { existsSync } from 'fs';
import { Socket } from 'net';
import { ConfigService } from '@nestjs/config';
import { DataSourceOptions } from 'typeorm';
import {
  AuthPermissionEntity,
  AuthRefreshTokenEntity,
  AuthRoleEntity,
  AuthRolePermissionEntity,
  AuthUserEntity,
  AuthUserRoleAssignmentEntity,
} from '../entities/auth.entities';
import {
  ClassZoomMeetingEntity,
  MeetingAttendanceSegmentEntity,
  MeetingInstanceEntity,
  MeetingParticipantEntity,
  MeetingRecordingEntity,
  MeetingTeacherMetricEntity,
  MeetingTranscriptEntity,
  VideoConferenceEntity,
  ZoomUserEntity,
} from '../entities/audit.entities';
import {
  ClassGroupEntity,
  ClassGroupTeacherEntity,
  ClassMeetingEntity,
  ClassOfferingEntity,
  ClassTeacherEntity,
  CourseSectionHourRequirementEntity,
  CourseModalityEntity,
  PlanningChangeLogEntity,
  PlanningCyclePlanRuleEntity,
  PlanningOfferEntity,
  PlanningScheduleConflictV2Entity,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionScheduleEntity,
  ScheduleConflictEntity,
  StudyTypeEntity,
} from '../entities/planning.entities';
import {
  ClassSyllabusKeywordEntity,
  ClassSyllabusSessionEntity,
  MeetingSummaryEntity,
  MeetingSyllabusMatchEntity,
} from '../entities/syllabus.entities';
import {
  AcademicProgramCampusEntity,
  AcademicProgramEntity,
  BuildingEntity,
  CampusEntity,
  ClassroomSectionScheduleEntity,
  ClassroomEntity,
  ClassroomTypeEntity,
  CourseEntity,
  CourseSectionEntity,
  ExternalSessionEntity,
  ExternalSourceEntity,
  FacultyEntity,
  SectionEntity,
  SemesterEntity,
  StudyPlanCourseDetailEntity,
  StudyPlanCourseEntity,
  StudyPlanEntity,
  SyncJobEntity,
  SyncLogEntity,
  TeacherEntity,
} from '../entities/catalog-sync.entities';

import {
  VideoconferenceEntity,
  VcPeriodEntity,
  VcFacultyEntity,
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcSectionEntity,
} from '../videoconference/videoconference.entity';

export const appEntities = [
  AuthUserEntity,
  AuthRoleEntity,
  AuthPermissionEntity,
  AuthRolePermissionEntity,
  AuthUserRoleAssignmentEntity,
  AuthRefreshTokenEntity,
  VideoconferenceEntity,
  VcPeriodEntity,
  VcFacultyEntity,
  VcAcademicProgramEntity,
  VcCourseEntity,
  VcSectionEntity,
  ClassOfferingEntity,
  ClassGroupEntity,
  ClassMeetingEntity,
  ClassTeacherEntity,
  ClassGroupTeacherEntity,
  CourseSectionHourRequirementEntity,
  ScheduleConflictEntity,
  StudyTypeEntity,
  CourseModalityEntity,
  PlanningCyclePlanRuleEntity,
  PlanningOfferEntity,
  PlanningSectionEntity,
  PlanningSubsectionEntity,
  PlanningSubsectionScheduleEntity,
  PlanningScheduleConflictV2Entity,
  PlanningChangeLogEntity,
  ZoomUserEntity,
  VideoConferenceEntity,
  ClassZoomMeetingEntity,
  MeetingInstanceEntity,
  MeetingParticipantEntity,
  MeetingAttendanceSegmentEntity,
  MeetingTeacherMetricEntity,
  MeetingRecordingEntity,
  MeetingTranscriptEntity,
  ClassSyllabusSessionEntity,
  ClassSyllabusKeywordEntity,
  MeetingSyllabusMatchEntity,
  MeetingSummaryEntity,
  SemesterEntity,
  CampusEntity,
  FacultyEntity,
  AcademicProgramEntity,
  StudyPlanEntity,
  StudyPlanCourseEntity,
  StudyPlanCourseDetailEntity,
  SectionEntity,
  CourseEntity,
  TeacherEntity,
  ClassroomTypeEntity,
  BuildingEntity,
  ClassroomEntity,
  ClassroomSectionScheduleEntity,
  AcademicProgramCampusEntity,
  CourseSectionEntity,
  ExternalSourceEntity,
  ExternalSessionEntity,
  SyncJobEntity,
  SyncLogEntity,
];

type DatabaseCandidate = {
  host: string;
  port: number;
  label: string;
};

const DATABASE_PROBE_TIMEOUT_MS = 800;
const DEFAULT_COMPOSE_MYSQL_PORT = 3307;

function readConfig(configService: ConfigService, key: string, fallback: string): string {
  return configService.get<string>(key) ?? fallback;
}

function isContainerEnvironment() {
  return existsSync('/.dockerenv') || Boolean(process.env.KUBERNETES_SERVICE_HOST);
}

function parsePort(value: string, fallback: number): number {
  const port = Number(value);
  return Number.isFinite(port) && port > 0 ? port : fallback;
}

function dedupeCandidates(candidates: DatabaseCandidate[]) {
  const seen = new Set<string>();
  return candidates.filter((candidate) => {
    const key = `${candidate.host}:${candidate.port}`;
    if (seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function buildDatabaseCandidates(configService: ConfigService): DatabaseCandidate[] {
  const configuredHost = readConfig(configService, 'DB_HOST', 'localhost');
  const configuredPort = parsePort(readConfig(configService, 'DB_PORT', '3306'), 3306);
  const publishedComposePort = parsePort(
    readConfig(configService, 'MYSQL_HOST_PORT', String(DEFAULT_COMPOSE_MYSQL_PORT)),
    DEFAULT_COMPOSE_MYSQL_PORT,
  );
  const runningInContainer = isContainerEnvironment();

  if (!runningInContainer && configuredHost === 'mysql') {
    return dedupeCandidates([
      { host: '127.0.0.1', port: publishedComposePort, label: 'docker compose mysql published port' },
      { host: 'localhost', port: publishedComposePort, label: 'docker compose mysql published port' },
      { host: '127.0.0.1', port: configuredPort, label: 'local mysql fallback' },
      { host: 'localhost', port: configuredPort, label: 'local mysql fallback' },
      { host: configuredHost, port: configuredPort, label: 'configured service host' },
    ]);
  }

  if (runningInContainer && ['localhost', '127.0.0.1'].includes(configuredHost)) {
    return dedupeCandidates([
      { host: 'mysql', port: 3306, label: 'docker compose service host' },
      { host: configuredHost, port: configuredPort, label: 'configured local host' },
    ]);
  }

  return [{ host: configuredHost, port: configuredPort, label: 'configured host' }];
}

async function canReachDatabase(host: string, port: number) {
  return new Promise<boolean>((resolve) => {
    const socket = new Socket();

    const finish = (result: boolean) => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(result);
    };

    socket.setTimeout(DATABASE_PROBE_TIMEOUT_MS);
    socket.once('connect', () => finish(true));
    socket.once('timeout', () => finish(false));
    socket.once('error', () => finish(false));
    socket.connect(port, host);
  });
}

async function resolveDatabaseEndpoint(configService: ConfigService) {
  const candidates = buildDatabaseCandidates(configService);

  for (const candidate of candidates) {
    if (await canReachDatabase(candidate.host, candidate.port)) {
      return candidate;
    }
  }

  return candidates[0];
}

export async function buildTypeOrmConfig(configService: ConfigService): Promise<DataSourceOptions> {
  const configuredHost = readConfig(configService, 'DB_HOST', 'localhost');
  const configuredPort = parsePort(readConfig(configService, 'DB_PORT', '3306'), 3306);
  const resolvedEndpoint = await resolveDatabaseEndpoint(configService);

  if (
    resolvedEndpoint.host !== configuredHost ||
    resolvedEndpoint.port !== configuredPort
  ) {
    console.warn(
      `[TypeOrmConfig] DB host ${configuredHost}:${configuredPort} no estuvo disponible al arrancar. ` +
      `Se usara ${resolvedEndpoint.host}:${resolvedEndpoint.port} (${resolvedEndpoint.label}).`,
    );
  }

  return {
    type: 'mysql',
    host: resolvedEndpoint.host,
    port: resolvedEndpoint.port,
    username: readConfig(configService, 'DB_USER', 'root'),
    password: readConfig(configService, 'DB_PASSWORD', 'root'),
    database: readConfig(configService, 'DB_NAME', 'uai_planning'),
    entities: appEntities,
    synchronize: readConfig(configService, 'DB_SYNCHRONIZE', 'true') === 'true',
    timezone: 'Z',
  };
}
