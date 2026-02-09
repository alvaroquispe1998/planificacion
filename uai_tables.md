    **SINC TOTAL**

semesters -> https://matricula.autonomadeica.edu.pe/admin/periodos/get?start=0\&length=50

campuses-> https://matricula.autonomadeica.edu.pe/admin/campus/get

academic programs-> https://matricula.autonomadeica.edu.pe/admin/carreras/get?

sections-> https://matricula.autonomadeica.edu.pe/admin/codigos-de-seccion/get?length=100

courses\_> https://docente.autonomadeica.edu.pe/admin/cursos/get?length=50\&tid=08ddeb0e-d0ba-41d5-8610-57110e6b2baa(filtrado por semesters_id pero siempre devuelve lo mismo)

classroom_types-> https://matricula.autonomadeica.edu.pe/admin/aulas/categorias/get?\&length=50

zoom_users->https://aulavirtual2.autonomadeica.edu.pe/web/conference/aulas/listar?length=300

**SINC PARCIAL**

faculties: -> https://matricula.autonomadeica.edu.pe/admin/facultades/get?

id

code

name

abbreviation

institucional_email

is_active

is_valid

study_plans-> https://intranet.autonomadeica.edu.pe/admin/planes-de-estudios/get?length=50

id

name

curriculum

curriculum code

is_active

teachers-> https://intranet.autonomadeica.edu.pe/admin/docentes/get?\&length=50

id

dni

paternalSurName

maternalSurName

name

fullname

phoneNumber

picture

username

buildings\_>https://matricula.autonomadeica.edu.pe/admin/campus/pabellones/get?length=50\&campus=08dd5b66-d93e-4ae6-8ebf-6a31892c7743(campuses\_id)

id

name

classrooms-> https://matricula.autonomadeica.edu.pe/admin/aulas/get?length=50

id

name

building_id

campus_id

type_id

faculty_id

capacity

status

ip_address

code

floor

number

academic_program_campuses\_>https://matricula.autonomadeica.edu.pe/admin/campus/carreras/get?\&length=50\&campus=08dd5b66-d93e-4ae6-8ebf-6a31892c7743(campus\_id)

id

name

course_sections-> https://aulavirtual2.autonomadeica.edu.pe/secciones/get?courseId=08dd5b61-21e3-4e76-8a03-317683cdc2d7(course\_id)

&nbsp; id

&nbsp; course_id(enviado arriba)

&nbsp; text

&nbsp; semested_id(semestre actual)

**TABLAS PROPIAS**

semester_weeks

delivery_modalities

shifts

class_offerings

class_groups

class_meetings

class_teachers

video_conferences

course_section_teachers

external_sources

external_sessions

sync_jobs

sync_logs

sync_job_items

external_day_map

zoom_credentials

class_offering_conference_config

generar videoconferencias->https://aulavirtual2.autonomadeica.edu.pe/web/conference/videoconferencias/agregar
form-data:
courseCode:10A04
courseName:Taller de Comunicación Oral
section:AP - CH
dni:22222222
teacher:Doe Perez Doe
day:JUEVES
startTime:19:39
endTime:19:59
termId:08ddf608-236c-45ad-841b-cfb064770e33
facultyId:08dd8495-6996-49b1-87a8-c2d4b91aaf4d
careerId:08dd8495-697b-4d7e-83c2-a5ed72328088
courseId:08dd7252-0fe9-49d8-80fc-f22c40846e2d
name:10A04|Taller de Comunicación Oral|AP - CH|22222222|Doe Perez Doe|JUEVES 19:39-19:40
sectionId:08ddf60e-e2cc-4813-8040-de5708fae5fb
start:10/01/2026 19:39
end:10/01/2026 19:59
minutes:20
daysOfWeek\[0]:6
credentialId:08de0b87-681a-4f7f-817e-f88094da7eee

EJEMPLO DEVOLUCIÓN PETICIONES

{ "draw": 0, "data": [ { "id": "08dd8531-1590-4a48-8633-cea6c82f05ef", "name": "0", "email": "doris.cordova@autonomadeica.edu.pe" }, { "id": "08dd8540-c2bf-461c-8e0a-e0dd29464ce5", "name": "0", "email": "fatima.cuba@autonomadeica.edu.pe" } ], "recordsTotal": 210, "recordsFiltered": 210 } { "data": [ { "id": "08dd60b3-3f89-498e-80f1-14668fcda964", "name": "Aula 201", "building": { "id": "08dd60b3-2d86-4ec2-8e47-ed7204f4608c", "name": "Pabellon1" }, "campus": { "id": "08dd5b66-d93e-4ae6-8ebf-6a31892c7743", "name": "SEDE CENTRAL" }, "type": { "id": "08dd60b2-c1b3-4e3f-8ca4-b25ba8055dd6", "name": "Aula General" }, "facultyId": "08dd5b60-278d-4c3c-8793-9098ed29e3ec", "facultyName": "CIENCIAS DE LA SALUD", "capacity": 9, "status": true, "iPAddress": "8.8.8.8", "code": "1", "floor": "1", "number": 1 }, { "id": "08dd60b3-567e-441e-83c5-09faa48d3ecd", "name": "Sin Asignar", "building": { "id": "08dd60b3-2d86-4ec2-8e47-ed7204f4608c", "name": "Pabellon1" }, "campus": { "id": "08dd5b66-d93e-4ae6-8ebf-6a31892c7743", "name": "SEDE CENTRAL" }, "type": { "id": "08dd60b2-c1b3-4e3f-8ca4-b25ba8055dd6", "name": "Aula General" }, "facultyId": null, "facultyName": "", "capacity": 30, "status": true, "iPAddress": "1.101.1.1", "code": "Sin Asignar", "floor": "1", "number": 1 }, { "id": "08dd6706-fbf0-4e89-8141-64d08ff47f01", "name": "aula 201 - F", "building": { "id": "08dd6706-98cb-489d-8648-800c6c9dc87b", "name": "Pab 1" }, "campus": { "id": "08dd5b66-ff93-410d-8107-96eeb879b4c9", "name": "FILIAL - ICA" }, "type": { "id": "08dd60b2-c1b3-4e3f-8ca4-b25ba8055dd6", "name": "Aula General" }, "facultyId": "08dd5b60-2797-4976-8f8b-b5377bbd21ee", "facultyName": "INGENIERÍA, CIENCIAS Y ADMINISTRACIÓN", "capacity": 30, "status": true, "iPAddress": "8.8.8.8", "code": "1", "floor": "1", "number": 1 } ], "drawCounter": 0, "error": null, "recordsFiltered": 523, "recordsTotal": 3 } te paso ejemplos de algunas peticiones
