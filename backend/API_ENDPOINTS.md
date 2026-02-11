# API Endpoints

## Planning

- `GET /planning/class-offerings`
- `GET /planning/class-offerings/:id`
- `POST /planning/class-offerings`
- `PATCH /planning/class-offerings/:id`
- `DELETE /planning/class-offerings/:id`

- `GET /planning/class-groups?class_offering_id=`
- `POST /planning/class-groups`
- `PATCH /planning/class-groups/:id`
- `DELETE /planning/class-groups/:id`

- `GET /planning/class-meetings?class_offering_id=`
- `POST /planning/class-meetings`
- `PATCH /planning/class-meetings/:id`
- `DELETE /planning/class-meetings/:id`

- `GET /planning/class-teachers?class_offering_id=`
- `POST /planning/class-teachers`
- `PATCH /planning/class-teachers/:id`
- `DELETE /planning/class-teachers/:id`

- `GET /planning/class-group-teachers?class_group_id=`
- `POST /planning/class-group-teachers`
- `PATCH /planning/class-group-teachers/:id`
- `DELETE /planning/class-group-teachers/:id`

- `GET /planning/course-section-hour-requirements?course_section_id=`
- `POST /planning/course-section-hour-requirements`
- `PATCH /planning/course-section-hour-requirements/:id`
- `DELETE /planning/course-section-hour-requirements/:id`

- `POST /planning/hours-validation/:classOfferingId`
- `GET /planning/schedule-conflicts?semester_id=`
- `POST /planning/schedule-conflicts/detect/:semesterId`

## Audit

- `GET /audit/video-conferences?class_offering_id=`
- `POST /audit/video-conferences`

- `GET /audit/class-zoom-meetings?class_offering_id=`
- `POST /audit/class-zoom-meetings`

- `GET /audit/meeting-instances?video_conference_id=`
- `GET /audit/meeting-instances/:id`
- `POST /audit/meeting-instances`
- `PATCH /audit/meeting-instances/:id`
- `DELETE /audit/meeting-instances/:id`

- `GET /audit/meeting-participants?meeting_instance_id=`
- `POST /audit/meeting-participants`

- `GET /audit/meeting-attendance-segments?meeting_instance_id=`
- `POST /audit/meeting-attendance-segments`

- `GET /audit/meeting-teacher-metrics?meeting_instance_id=`
- `POST /audit/meeting-teacher-metrics/recompute/:meetingInstanceId`

- `GET /audit/meeting-recordings?meeting_instance_id=`
- `POST /audit/meeting-recordings`

- `GET /audit/meeting-transcripts?meeting_instance_id=`
- `POST /audit/meeting-transcripts`

## Syllabus

- `GET /syllabus/sessions?class_offering_id=`
- `POST /syllabus/sessions`
- `PATCH /syllabus/sessions/:id`
- `DELETE /syllabus/sessions/:id`

- `GET /syllabus/keywords?syllabus_session_id=`
- `POST /syllabus/keywords`
- `DELETE /syllabus/keywords/:id`

- `GET /syllabus/summaries?meeting_instance_id=`
- `POST /syllabus/summaries/generate/:meetingInstanceId`

- `GET /syllabus/matches?meeting_instance_id=`
- `POST /syllabus/matches/run/:meetingInstanceId/:syllabusSessionId`

## Settings

- `GET /settings/catalog/classroom-section-schedules`
