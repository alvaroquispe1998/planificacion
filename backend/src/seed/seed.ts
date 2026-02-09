import { existsSync, readFileSync } from 'fs';
import { DataSource } from 'typeorm';
import {
  ClassZoomMeetingEntity,
  MeetingAttendanceSegmentEntity,
  MeetingInstanceEntity,
  MeetingParticipantEntity,
  MeetingRecordingEntity,
  MeetingTeacherMetricEntity,
  MeetingTranscriptEntity,
  VideoConferenceEntity,
} from '../entities/audit.entities';
import {
  ClassGroupEntity,
  ClassGroupTeacherEntity,
  ClassMeetingEntity,
  ClassOfferingEntity,
  ClassTeacherEntity,
  CourseSectionHourRequirementEntity,
  ScheduleConflictEntity,
} from '../entities/planning.entities';
import {
  ClassSyllabusKeywordEntity,
  ClassSyllabusSessionEntity,
  MeetingSummaryEntity,
  MeetingSyllabusMatchEntity,
} from '../entities/syllabus.entities';

const dataSource = new DataSource({
  type: 'mysql',
  host: process.env.DB_HOST ?? 'localhost',
  port: Number(process.env.DB_PORT ?? '3306'),
  username: process.env.DB_USER ?? 'root',
  password: process.env.DB_PASSWORD ?? 'root',
  database: process.env.DB_NAME ?? 'uai_planning',
  entities: [
    ClassOfferingEntity,
    ClassGroupEntity,
    ClassMeetingEntity,
    ClassTeacherEntity,
    ClassGroupTeacherEntity,
    CourseSectionHourRequirementEntity,
    ScheduleConflictEntity,
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
  ],
  synchronize: false,
  timezone: 'Z',
});

async function bootstrap() {
  loadDotEnv();
  await dataSource.initialize();
  await waitForTables(['class_offerings', 'meeting_instances', 'class_syllabus_sessions']);

  const now = new Date('2026-01-15T12:00:00.000Z');

  const semesterId = '11111111-1111-1111-1111-111111111111';
  const offeringAId = '22222222-2222-2222-2222-222222222221';
  const offeringBId = '22222222-2222-2222-2222-222222222222';
  const teacherId = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1';
  const classroomId = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1';

  const offeringsRepo = dataSource.getRepository(ClassOfferingEntity);
  const groupsRepo = dataSource.getRepository(ClassGroupEntity);
  const meetingsRepo = dataSource.getRepository(ClassMeetingEntity);
  const classTeachersRepo = dataSource.getRepository(ClassTeacherEntity);
  const groupTeachersRepo = dataSource.getRepository(ClassGroupTeacherEntity);
  const hourRequirementsRepo = dataSource.getRepository(CourseSectionHourRequirementEntity);
  const conflictsRepo = dataSource.getRepository(ScheduleConflictEntity);

  const conferencesRepo = dataSource.getRepository(VideoConferenceEntity);
  const classZoomMeetingsRepo = dataSource.getRepository(ClassZoomMeetingEntity);
  const instancesRepo = dataSource.getRepository(MeetingInstanceEntity);
  const participantsRepo = dataSource.getRepository(MeetingParticipantEntity);
  const segmentsRepo = dataSource.getRepository(MeetingAttendanceSegmentEntity);
  const teacherMetricsRepo = dataSource.getRepository(MeetingTeacherMetricEntity);
  const recordingsRepo = dataSource.getRepository(MeetingRecordingEntity);
  const transcriptsRepo = dataSource.getRepository(MeetingTranscriptEntity);

  const syllabusSessionsRepo = dataSource.getRepository(ClassSyllabusSessionEntity);
  const syllabusKeywordsRepo = dataSource.getRepository(ClassSyllabusKeywordEntity);
  const matchesRepo = dataSource.getRepository(MeetingSyllabusMatchEntity);
  const summariesRepo = dataSource.getRepository(MeetingSummaryEntity);

  await offeringsRepo.upsert(
    [
      {
        id: offeringAId,
        semester_id: semesterId,
        study_plan_id: 'sp-00000000-0000-0000-0000-00000000001',
        academic_program_id: 'prog-0000-0000-0000-0000000000001',
        course_id: 'course-000-0000-0000-000000000000001',
        course_section_id: 'sec-00000000-0000-0000-0000-00000000001',
        campus_id: 'camp-00000000-0000-0000-0000-00000000001',
        delivery_modality_id: 'moda-00000000-0000-0000-0000-00000000001',
        shift_id: 'shift-00000000-0000-0000-0000-00000000001',
        projected_vacancies: 30,
        status: true,
      },
      {
        id: offeringBId,
        semester_id: semesterId,
        study_plan_id: 'sp-00000000-0000-0000-0000-00000000002',
        academic_program_id: 'prog-0000-0000-0000-0000000000002',
        course_id: 'course-000-0000-0000-000000000000002',
        course_section_id: 'sec-00000000-0000-0000-0000-00000000002',
        campus_id: 'camp-00000000-0000-0000-0000-00000000001',
        delivery_modality_id: 'moda-00000000-0000-0000-0000-00000000002',
        shift_id: 'shift-00000000-0000-0000-0000-00000000001',
        projected_vacancies: 25,
        status: true,
      },
    ],
    ['id'],
  );

  await groupsRepo.upsert(
    [
      {
        id: '33333333-3333-3333-3333-333333333331',
        class_offering_id: offeringAId,
        group_type: 'THEORY',
        code: 'T',
        capacity: 30,
        note: 'Grupo teoria',
      },
      {
        id: '33333333-3333-3333-3333-333333333332',
        class_offering_id: offeringAId,
        group_type: 'PRACTICE',
        code: 'P1',
        capacity: 20,
        note: 'Grupo practica',
      },
      {
        id: '33333333-3333-3333-3333-333333333333',
        class_offering_id: offeringBId,
        group_type: 'THEORY',
        code: 'T',
        capacity: 25,
        note: 'Grupo teoria oferta B',
      },
    ],
    ['id'],
  );

  await meetingsRepo.upsert(
    [
      {
        id: '44444444-4444-4444-4444-444444444441',
        class_offering_id: offeringAId,
        class_group_id: '33333333-3333-3333-3333-333333333331',
        day_of_week: 'LUNES',
        start_time: '08:00:00',
        end_time: '09:40:00',
        minutes: 100,
        academic_hours: 2,
        classroom_id: classroomId,
      },
      {
        id: '44444444-4444-4444-4444-444444444442',
        class_offering_id: offeringAId,
        class_group_id: '33333333-3333-3333-3333-333333333332',
        day_of_week: 'MARTES',
        start_time: '10:00:00',
        end_time: '11:40:00',
        minutes: 100,
        academic_hours: 2,
        classroom_id: classroomId,
      },
      {
        id: '44444444-4444-4444-4444-444444444443',
        class_offering_id: offeringBId,
        class_group_id: '33333333-3333-3333-3333-333333333333',
        day_of_week: 'LUNES',
        start_time: '09:10:00',
        end_time: '10:00:00',
        minutes: 50,
        academic_hours: 1,
        classroom_id: classroomId,
      },
    ],
    ['id'],
  );

  await classTeachersRepo.upsert(
    [
      {
        id: '55555555-5555-5555-5555-555555555551',
        class_offering_id: offeringAId,
        teacher_id: teacherId,
        role: 'TITULAR',
        is_primary: true,
      },
      {
        id: '55555555-5555-5555-5555-555555555552',
        class_offering_id: offeringBId,
        teacher_id: teacherId,
        role: 'TITULAR',
        is_primary: true,
      },
    ],
    ['id'],
  );

  await groupTeachersRepo.upsert(
    [
      {
        id: '66666666-6666-6666-6666-666666666661',
        class_group_id: '33333333-3333-3333-3333-333333333331',
        teacher_id: teacherId,
        role: 'PRIMARY',
        is_primary: true,
        assigned_from: now,
        assigned_to: null,
        notes: 'Docente principal',
        created_at: now,
        updated_at: now,
      },
      {
        id: '66666666-6666-6666-6666-666666666662',
        class_group_id: '33333333-3333-3333-3333-333333333333',
        teacher_id: teacherId,
        role: 'PRIMARY',
        is_primary: true,
        assigned_from: now,
        assigned_to: null,
        notes: 'Docente principal en oferta B',
        created_at: now,
        updated_at: now,
      },
    ],
    ['id'],
  );

  await hourRequirementsRepo.upsert(
    [
      {
        id: '77777777-7777-7777-7777-777777777771',
        course_section_id: 'sec-00000000-0000-0000-0000-00000000001',
        course_format: 'TP',
        theory_hours_academic: 2,
        practice_hours_academic: 2,
        lab_hours_academic: 0,
        academic_minutes_per_hour: 50,
        notes: 'Carga esperada TP',
        created_at: now,
        updated_at: now,
      },
      {
        id: '77777777-7777-7777-7777-777777777772',
        course_section_id: 'sec-00000000-0000-0000-0000-00000000002',
        course_format: 'T',
        theory_hours_academic: 1,
        practice_hours_academic: 0,
        lab_hours_academic: 0,
        academic_minutes_per_hour: 50,
        notes: 'Carga esperada T',
        created_at: now,
        updated_at: now,
      },
    ],
    ['id'],
  );

  await conflictsRepo.upsert(
    [
      {
        id: '88888888-8888-8888-8888-888888888881',
        semester_id: semesterId,
        conflict_type: 'TEACHER_OVERLAP',
        severity: 'CRITICAL',
        teacher_id: teacherId,
        classroom_id: null,
        class_group_id: null,
        class_offering_id: null,
        meeting_a_id: '44444444-4444-4444-4444-444444444441',
        meeting_b_id: '44444444-4444-4444-4444-444444444443',
        overlap_minutes: 30,
        detail_json: {
          day_of_week: 'LUNES',
          overlap_start: '09:10:00',
          overlap_end: '09:40:00',
        },
        detected_at: now,
        created_at: now,
      },
      {
        id: '88888888-8888-8888-8888-888888888882',
        semester_id: semesterId,
        conflict_type: 'CLASSROOM_OVERLAP',
        severity: 'CRITICAL',
        teacher_id: null,
        classroom_id: classroomId,
        class_group_id: null,
        class_offering_id: null,
        meeting_a_id: '44444444-4444-4444-4444-444444444441',
        meeting_b_id: '44444444-4444-4444-4444-444444444443',
        overlap_minutes: 30,
        detail_json: {
          day_of_week: 'LUNES',
          overlap_start: '09:10:00',
          overlap_end: '09:40:00',
        },
        detected_at: now,
        created_at: now,
      },
    ],
    ['id'],
  );

  await conferencesRepo.upsert(
    [
      {
        id: '99999999-9999-9999-9999-999999999991',
        class_offering_id: offeringAId,
        semester_week_id: 'week-00000000-0000-0000-0000-00000000001',
        zoom_user_id: 'zoom-user-0000-0000-0000-000000000001',
        zoom_meeting_id: '980001',
        topic: 'Algebra I - Sesion 1',
        start_time: new Date('2026-01-12T08:00:00.000Z'),
        end_time: new Date('2026-01-12T09:40:00.000Z'),
        join_url: 'https://zoom.example/980001',
        status: 'PROGRAMADA',
        note: 'Sesion semilla',
        created_at: now,
      },
      {
        id: '99999999-9999-9999-9999-999999999992',
        class_offering_id: offeringBId,
        semester_week_id: 'week-00000000-0000-0000-0000-00000000001',
        zoom_user_id: 'zoom-user-0000-0000-0000-000000000001',
        zoom_meeting_id: '980002',
        topic: 'Calculo I - Sesion 1',
        start_time: new Date('2026-01-12T09:10:00.000Z'),
        end_time: new Date('2026-01-12T10:00:00.000Z'),
        join_url: 'https://zoom.example/980002',
        status: 'PROGRAMADA',
        note: 'Sesion semilla B',
        created_at: now,
      },
    ],
    ['id'],
  );

  await classZoomMeetingsRepo.upsert(
    [
      {
        id: 'abababab-abab-abab-abab-ababababab01',
        class_offering_id: offeringAId,
        zoom_meeting_id: '980001',
        zoom_user_id: 'zoom-user-0000-0000-0000-000000000001',
        is_active: true,
        created_at: now,
        updated_at: now,
      },
      {
        id: 'abababab-abab-abab-abab-ababababab02',
        class_offering_id: offeringBId,
        zoom_meeting_id: '980002',
        zoom_user_id: 'zoom-user-0000-0000-0000-000000000001',
        is_active: true,
        created_at: now,
        updated_at: now,
      },
    ],
    ['id'],
  );

  await instancesRepo.upsert(
    [
      {
        id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        video_conference_id: '99999999-9999-9999-9999-999999999991',
        zoom_meeting_id: '980001',
        zoom_meeting_uuid: 'zoom-instance-uuid-980001',
        scheduled_start: new Date('2026-01-12T08:00:00.000Z'),
        scheduled_end: new Date('2026-01-12T09:40:00.000Z'),
        actual_start: new Date('2026-01-12T08:01:00.000Z'),
        actual_end: new Date('2026-01-12T09:40:00.000Z'),
        duration_minutes: 97,
        status: 'ENDED',
        raw_json: { source: 'seed', host: teacherId },
        created_at: now,
        updated_at: now,
      },
      {
        id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd02',
        video_conference_id: '99999999-9999-9999-9999-999999999992',
        zoom_meeting_id: '980002',
        zoom_meeting_uuid: 'zoom-instance-uuid-980002',
        scheduled_start: new Date('2026-01-12T09:10:00.000Z'),
        scheduled_end: new Date('2026-01-12T10:00:00.000Z'),
        actual_start: new Date('2026-01-12T09:11:00.000Z'),
        actual_end: new Date('2026-01-12T09:59:00.000Z'),
        duration_minutes: 48,
        status: 'ENDED',
        raw_json: { source: 'seed', host: teacherId },
        created_at: now,
        updated_at: now,
      },
    ],
    ['id'],
  );

  await participantsRepo.upsert(
    [
      {
        id: 'edededed-eded-eded-eded-ededededed01',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        zoom_participant_id: 'host-980001',
        zoom_user_id: 'zoom-user-host-001',
        display_name: 'Docente Principal',
        email: 'docente@uai.edu',
        role: 'HOST',
        teacher_id: teacherId,
        created_at: now,
      },
      {
        id: 'edededed-eded-eded-eded-ededededed02',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        zoom_participant_id: 'student-980001',
        zoom_user_id: 'zoom-user-student-001',
        display_name: 'Estudiante Uno',
        email: 'estudiante1@uai.edu',
        role: 'ATTENDEE',
        teacher_id: null,
        created_at: now,
      },
      {
        id: 'edededed-eded-eded-eded-ededededed03',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd02',
        zoom_participant_id: 'host-980002',
        zoom_user_id: 'zoom-user-host-001',
        display_name: 'Docente Principal',
        email: 'docente@uai.edu',
        role: 'HOST',
        teacher_id: teacherId,
        created_at: now,
      },
    ],
    ['id'],
  );

  await segmentsRepo.upsert(
    [
      {
        id: 'f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f001',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        participant_id: 'edededed-eded-eded-eded-ededededed01',
        join_time: new Date('2026-01-12T08:01:00.000Z'),
        leave_time: new Date('2026-01-12T08:45:00.000Z'),
        minutes: 44,
        connection_type: 'desktop',
        ip_address: '10.0.0.1',
        raw_json: { reconnect: false },
        created_at: now,
      },
      {
        id: 'f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f002',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        participant_id: 'edededed-eded-eded-eded-ededededed01',
        join_time: new Date('2026-01-12T08:47:00.000Z'),
        leave_time: new Date('2026-01-12T09:40:00.000Z'),
        minutes: 53,
        connection_type: 'desktop',
        ip_address: '10.0.0.1',
        raw_json: { reconnect: true },
        created_at: now,
      },
      {
        id: 'f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f003',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd02',
        participant_id: 'edededed-eded-eded-eded-ededededed03',
        join_time: new Date('2026-01-12T09:11:00.000Z'),
        leave_time: new Date('2026-01-12T09:59:00.000Z'),
        minutes: 48,
        connection_type: 'desktop',
        ip_address: '10.0.0.1',
        raw_json: { reconnect: false },
        created_at: now,
      },
    ],
    ['id'],
  );

  await teacherMetricsRepo.upsert(
    [
      {
        id: '11112222-3333-4444-5555-666677778881',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        teacher_id: teacherId,
        join_count: 2,
        total_minutes: 97,
        first_join: new Date('2026-01-12T08:01:00.000Z'),
        last_leave: new Date('2026-01-12T09:40:00.000Z'),
        is_host: true,
        created_at: now,
        updated_at: now,
      },
      {
        id: '11112222-3333-4444-5555-666677778882',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd02',
        teacher_id: teacherId,
        join_count: 1,
        total_minutes: 48,
        first_join: new Date('2026-01-12T09:11:00.000Z'),
        last_leave: new Date('2026-01-12T09:59:00.000Z'),
        is_host: true,
        created_at: now,
        updated_at: now,
      },
    ],
    ['id'],
  );

  await recordingsRepo.upsert(
    [
      {
        id: '12121212-1212-1212-1212-121212121211',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        zoom_recording_id: 'zoom-recording-980001',
        recording_type: 'MP4',
        file_extension: 'mp4',
        file_size_bytes: '123456',
        download_url: 'https://zoom.example/recording/980001',
        play_url: 'https://zoom.example/play/980001',
        start_time: new Date('2026-01-12T08:01:00.000Z'),
        end_time: new Date('2026-01-12T09:40:00.000Z'),
        status: 'AVAILABLE',
        raw_json: { source: 'seed' },
        created_at: now,
      },
    ],
    ['id'],
  );

  await transcriptsRepo.upsert(
    [
      {
        id: '13131313-1313-1313-1313-131313131311',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        recording_id: '12121212-1212-1212-1212-121212121211',
        format: 'TEXT',
        language: 'es',
        transcript_text:
          'En esta sesion revisamos algebra basica, ecuaciones lineales y funciones. Tambien se explico la resolucion de sistemas y ejercicios de practica con funciones.',
        confidence_avg: '92.50',
        created_at: now,
      },
    ],
    ['id'],
  );

  await syllabusSessionsRepo.upsert(
    [
      {
        id: '14141414-1414-1414-1414-141414141411',
        class_offering_id: offeringAId,
        semester_week_id: 'week-00000000-0000-0000-0000-00000000001',
        session_title: 'Introduccion a algebra y funciones',
        expected_content: 'Algebra basica, ecuaciones lineales, funciones y sistemas de ecuaciones.',
        bibliography: 'Texto institucional de algebra',
        created_at: now,
        updated_at: now,
      },
    ],
    ['id'],
  );

  await syllabusKeywordsRepo.upsert(
    [
      {
        id: '15151515-1515-1515-1515-151515151511',
        syllabus_session_id: '14141414-1414-1414-1414-141414141411',
        keyword: 'algebra',
        weight: '5.00',
      },
      {
        id: '15151515-1515-1515-1515-151515151512',
        syllabus_session_id: '14141414-1414-1414-1414-141414141411',
        keyword: 'ecuaciones',
        weight: '3.00',
      },
      {
        id: '15151515-1515-1515-1515-151515151513',
        syllabus_session_id: '14141414-1414-1414-1414-141414141411',
        keyword: 'funciones',
        weight: '2.00',
      },
    ],
    ['id'],
  );

  await matchesRepo.upsert(
    [
      {
        id: '16161616-1616-1616-1616-161616161611',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        syllabus_session_id: '14141414-1414-1414-1414-141414141411',
        method: 'HYBRID',
        score: '78.50',
        matched_keywords_json: {
          matched_keywords: ['algebra', 'ecuaciones', 'funciones'],
          reason: 'seed baseline',
        },
        notes: 'Registro base de concordancia para pruebas.',
        status: 'OK',
        created_at: now,
      },
    ],
    ['id'],
  );

  await summariesRepo.upsert(
    [
      {
        id: '17171717-1717-1717-1717-171717171711',
        meeting_instance_id: 'cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcd01',
        summary_type: 'EXTRACTIVE',
        summary_text:
          'Se cubrio algebra basica, ecuaciones lineales y funciones con ejercicios de practica y resolucion de sistemas.',
        keywords_json: {
          top_keywords: ['algebra', 'ecuaciones', 'funciones', 'sistemas'],
        },
        created_at: now,
      },
    ],
    ['id'],
  );

  console.log('Seed completed successfully');
}

async function waitForTables(tableNames: string[]) {
  const dbName = process.env.DB_NAME ?? 'uai_planning';
  const maxRetries = 45;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    let readyCount = 0;

    for (const tableName of tableNames) {
      const rows = await dataSource.query(
        `SELECT COUNT(*) AS qty FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
        [dbName, tableName],
      );
      const qty = Number(rows?.[0]?.qty ?? 0);
      if (qty > 0) {
        readyCount += 1;
      }
    }

    if (readyCount === tableNames.length) {
      return;
    }

    await sleep(2000);
  }

  throw new Error('Timeout waiting for tables required by seed');
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function loadDotEnv() {
  if (existsSync('.env')) {
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
}

bootstrap()
  .catch((error) => {
    console.error('Seed failed', error);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (dataSource.isInitialized) {
      await dataSource.destroy();
    }
  });
