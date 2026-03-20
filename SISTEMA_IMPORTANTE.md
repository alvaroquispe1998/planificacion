# Sistema UAI - Resumen Importante

Este archivo resume lo mas importante del sistema tal como existe hoy en el codigo.
Sirve como guia rapida para onboarding, soporte, mantenimiento y desarrollo.

## 1. Que es este sistema

Sistema web para:

- planificacion academica
- configuracion de ofertas, secciones, subsecciones y horarios
- deteccion de cruces
- auditoria de videoconferencias y clases
- sincronizacion de catalogos desde fuentes externas
- seguridad por usuarios, roles, permisos y alcance

El proyecto reemplaza una operacion que antes dependia mucho de Excel y deja trazabilidad de cambios.

## 2. Estructura general

Repositorio:

- `backend/`: NestJS + TypeORM + MySQL
- `frontend/`: Angular standalone

Documentacion base ya existente:

- `README.md`: instalacion, arranque y sincronizacion
- `01_USE_CASES.md`: casos de uso funcionales
- `02_ARCHITECTURE.md`: arquitectura resumida
- `Project UAI Academic Scheduling.txt`: modelo funcional/tablas de referencia

## 3. Modulos reales del backend

Segun `backend/src/app.module.ts`, hoy el backend carga estos modulos:

- `auth`
- `planning`
- `settings`
- `sync`
- `audit`
- `syllabus`
- `videoconference`

Controladores principales:

- `GET /health`
- `POST /auth/login`
- `POST /auth/refresh`
- `POST /auth/logout`
- `GET /auth/me`
- `GET /auth/admin/users`
- `GET /auth/admin/roles`
- `GET /auth/admin/permissions`
- `GET /planning/...`
- `GET /settings/catalog/...`
- `GET|POST /settings/sync/...`
- `GET|POST /audit/...`
- `GET|POST /syllabus/...`
- `GET|POST /videoconference/...`

## 4. Pantallas reales del frontend

Rutas principales actuales:

- `/login`
- `/planning`
- `/planning/cycle-detail`
- `/planning/cycle-editor`
- `/planning/change-log`
- `/planning/offers/:offerId/sections`
- `/planning/conflicts`
- `/videoconferences/audit`
- `/videoconferences`
- `/integrations/sync`
- `/admin/security/users`
- `/admin/security/roles`
- `/class-detail/:id`

Paginas implementadas en `frontend/src/app/pages`:

- `planning`
- `planning-cycle-detail`
- `planning-cycle-editor`
- `planning-offer-sections`
- `planning-change-log`
- `conflicts`
- `audit`
- `videoconferences`
- `settings`
- `security`
- `class-detail`
- `login`

## 5. Seguridad y acceso

Ventanas protegidas:

- `window.planning`
- `window.conflicts`
- `window.audit`
- `window.videoconferences`
- `window.settings`

Permisos de accion importantes:

- `action.users.manage`
- `action.roles.manage`
- `action.permissions.manage`
- `action.settings.manage`
- `action.planning.change_log.view`
- `action.planning.plan.submit_review`
- `action.planning.plan.review_decide`

Roles seed:

- `ADMIN`
- `ADMINISTRATIVE`
- `IT_SUPPORT`

Usuario bootstrap por defecto:

- usuario: `admin`
- password: `admin123`

Importante:

- el backend filtra datos por alcance institucional cuando el usuario no es global
- planning y conflictos estan protegidos por permisos de ventana
- revision/aprobacion de planes tiene permisos de accion separados

## 6. Flujo funcional principal actual

Flujo recomendado de operacion:

1. Configurar o validar sesiones de fuentes externas en `/integrations/sync`
2. Ejecutar sincronizacion de catalogos
3. Crear reglas de planificacion por ciclo
4. Generar o administrar ofertas
5. Crear secciones
6. Crear y configurar subsecciones
7. Asignar docente, turno, modalidad, aula y horario
8. Revisar cruces
9. Enviar el plan a revision
10. Aprobar o devolver a correccion

## 7. Jerarquia principal de planificacion

La planificacion manual gira alrededor de esta jerarquia:

- plan rule
- offer
- section
- subsection
- subsection schedule

Lectura rapida:

- una `plan rule` representa la configuracion de planificacion de un ciclo
- una `offer` representa la apertura concreta de un curso en un periodo/programa/campus
- una `section` agrupa subsecciones bajo una oferta
- una `subsection` es la unidad concreta que recibe responsable, modalidad, aula y horario
- un `subsection schedule` guarda el bloque horario

## 8. Estados y workflow importantes

### 8.1 Estado de oferta de planificacion

Valores actuales:

- `DRAFT`
- `ACTIVE`
- `OBSERVED`
- `CLOSED`

Regla importante:

- si una oferta tiene conflictos, queda en `OBSERVED`
- si no tiene conflictos y esta lista, queda en `ACTIVE`
- si aun no esta lista, queda en `DRAFT`

### 8.2 Workflow de revision del plan

Valores actuales:

- `DRAFT`
- `IN_REVIEW`
- `APPROVED`
- `IN_CORRECTION`

Transiciones importantes:

- `DRAFT` o `IN_CORRECTION` -> `IN_REVIEW`
- `IN_REVIEW` -> `APPROVED`
- `IN_REVIEW` -> `IN_CORRECTION`

## 9. Validaciones de negocio importantes en planning manual

### 9.1 Para que una subseccion este "lista"

Una subseccion se considera configurada cuando tiene:

- `responsible_teacher_id`
- `shift`
- `course_modality_id`
- al menos un horario

Si falta algo de eso, no esta lista para revision.

### 9.2 Restricciones de horario

Las validaciones actuales del horario son:

- solo un horario por subseccion
- rango permitido entre `07:40` y `23:30`
- horas alineadas a la malla academica
- bloques de `50` minutos
- la hora fin debe ser mayor que la hora inicio

Mensajes funcionales visibles en codigo:

- "Solo se permite un horario por subseccion."
- "El horario debe avanzar en bloques de 50 minutos."
- "Las horas deben alinearse a la malla academica de 50 minutos."

### 9.3 Restricciones de vacantes

- la vacante de una subseccion no puede ser mayor que la vacante de su seccion

### 9.4 Restricciones del tipo de subseccion

Valores actuales:

- `THEORY`
- `PRACTICE`
- `MIXED`

Regla importante:

- en cursos teorico practico debe existir al menos una subseccion teorica y una practica

## 10. Cruces: como funcionan hoy

### 10.1 Logica actual usada por la pantalla de cruces

La pantalla `/planning/conflicts` consume:

- `GET /planning/conflicts`

Ese endpoint usa la logica nueva de `PlanningManualService`.

No usa por defecto la logica legacy de:

- `GET /planning/schedule-conflicts`

### 10.2 Regla base de deteccion

El sistema compara horarios de subsecciones de a pares y genera conflicto cuando:

- ambas reuniones son del mismo dia
- hay superposicion real de tiempo
- pertenecen al mismo semestre

### 10.3 Tipos de conflicto actuales en planning manual

- `TEACHER_OVERLAP`
- `CLASSROOM_OVERLAP`
- `SUBSECTION_OVERLAP`
- `SECTION_OVERLAP`

### 10.4 Cruce por docente

Se genera cuando:

- dos subsecciones tienen el mismo `responsible_teacher_id`
- el dia coincide
- las horas se superponen

### 10.5 Cruce por seccion

Se genera cuando:

- `sectionA.code === sectionB.code`
- las ofertas son distintas
- el `campus_id` coincide
- el `academic_program_id` coincide
- el `cycle` coincide
- el horario se superpone

Importante:

- "cruce por seccion" no significa simplemente "misma oferta"
- la logica actual lo interpreta como misma seccion logica en contexto academico equivalente

### 10.6 Severidad de conflictos

Segun minutos de cruce:

- `INFO`: menor a 10 min
- `WARNING`: desde 10 min
- `CRITICAL`: desde 30 min

### 10.7 Efecto funcional de un cruce

- el cruce se guarda en base de datos
- la UI lo muestra en la oferta, seccion, subseccion y pantalla de cruces
- no bloquea guardar el horario
- si existen conflictos, la oferta pasa a estado `OBSERVED`

### 10.8 Recalculo de cruces

Los cruces se reconstruyen cuando cambian elementos relevantes como:

- secciones
- subsecciones
- horarios de subseccion

El recalculo se hace a nivel semestre para mantener consistencia del conjunto.

## 11. Planning nuevo vs planning legacy

Actualmente conviven dos lineas:

### 11.1 Planning manual nuevo

Se usa para el flujo principal visible hoy:

- plan rules
- offers
- sections
- subsections
- subsection schedules
- conflicts
- change log

Endpoints ejemplo:

- `GET /planning/plan-rules`
- `POST /planning/offers`
- `POST /planning/offers/:id/sections`
- `POST /planning/sections/:id/subsections`
- `POST /planning/subsections/:id/schedules`
- `GET /planning/conflicts`
- `GET /planning/change-log`

### 11.2 Planning legacy

Sigue existiendo para entidades antiguas tipo workspace:

- `class_offerings`
- `class_groups`
- `class_meetings`
- `class_teachers`
- `class_group_teachers`
- `course_section_hour_requirements`
- `workspace`
- `schedule-conflicts`

Importante:

- no conviene mezclar la interpretacion de cruces del flujo nuevo con la del flujo legacy
- la UI actual de cruces usa la version nueva

## 12. Sincronizacion e integraciones

Fuentes externas importantes mencionadas en el proyecto:

- `MATRICULA`
- `DOCENTE`
- `INTRANET`
- `AULAVIRTUAL`

Capacidades importantes del modulo de sync:

- guardar cookies de sesion por fuente
- validar sesion
- validar todas las sesiones
- ejecutar sincronizacion por recursos
- consultar jobs de sincronizacion

Importante:

- para `AULAVIRTUAL` existe soporte de auto login por variables de entorno
- el backend clasifica la sesion como `ACTIVE`, `EXPIRED` o `ERROR`

## 13. Auditoria, videoconferencias y syllabus

Ademas de planning, el sistema ya tiene base para:

### 13.1 Auditoria

Controlador `audit` con recursos como:

- video conferences
- class zoom meetings
- meeting instances
- meeting participants
- meeting attendance segments
- meeting teacher metrics
- meeting recordings
- meeting transcripts

### 13.2 Videoconferencias

Controlador `videoconference` con operaciones de:

- consulta de facultades/programas/cursos/secciones
- preview
- generate

### 13.3 Syllabus

Controlador `syllabus` con operaciones de:

- sesiones
- keywords
- summaries
- matches

## 14. Rutas y endpoints mas utiles para soporte

### Frontend

- `/integrations/sync`
- `/planning`
- `/planning/cycle-editor`
- `/planning/offers/:offerId/sections`
- `/planning/conflicts`
- `/planning/change-log`
- `/videoconferences/audit`
- `/admin/security/users`

### Backend

- `GET /health`
- `GET /settings/sync/resources`
- `GET /settings/sync/sources`
- `POST /settings/sync/validate-all-sessions`
- `POST /settings/sync/run`
- `GET /planning/catalog/filters`
- `GET /planning/plan-rules`
- `GET /planning/offers`
- `GET /planning/conflicts`
- `GET /planning/change-log`
- `POST /auth/login`
- `GET /auth/me`

## 15. Puntos tecnicos que no hay que olvidar

- backend y frontend son proyectos separados y se comunican por REST
- la persistencia principal es MySQL con TypeORM
- el backend siembra permisos, roles y usuario admin bootstrap
- el frontend esta protegido por guards de login, ventana y permisos
- el historial de cambios existe y es parte importante del flujo de planning manual
- la pantalla de cruces filtra principalmente `TEACHER_OVERLAP` y `SECTION_OVERLAP`
- la logica de cruces actual no bloquea el guardado del horario
- el recalculo de cruces actualiza tambien el estado de las ofertas

## 16. Archivos clave para entender el sistema

Backend:

- `backend/src/app.module.ts`
- `backend/src/auth/auth.constants.ts`
- `backend/src/planning/planning.controller.ts`
- `backend/src/planning/planning-manual.service.ts`
- `backend/src/planning/planning.service.ts`
- `backend/src/settings/settings-sync.controller.ts`
- `backend/src/settings/settings-sync.service.ts`
- `backend/src/entities/planning.entities.ts`

Frontend:

- `frontend/src/app/app.routes.ts`
- `frontend/src/app/core/api.service.ts`
- `frontend/src/app/core/auth.guard.ts`
- `frontend/src/app/pages/planning/planning.page.ts`
- `frontend/src/app/pages/planning-cycle-editor/planning-cycle-editor.page.ts`
- `frontend/src/app/pages/planning-offer-sections/planning-offer-sections.page.ts`
- `frontend/src/app/pages/conflicts/conflicts.page.ts`
- `frontend/src/app/pages/settings/settings.page.ts`

## 17. Recomendacion de uso de este archivo

Usar este documento como resumen operativo.
Para detalle tecnico profundo, revisar junto con:

- `README.md`
- `01_USE_CASES.md`
- `02_ARCHITECTURE.md`
- el codigo fuente indicado en la seccion 16

