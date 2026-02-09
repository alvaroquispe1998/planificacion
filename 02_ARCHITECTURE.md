# UAIClassAudit – Arquitectura del Sistema

Backend y Frontend son **proyectos independientes**.
Se comunican SOLO vía API REST.
Docker solo orquesta.

---

## Backend

**Stack:**
- Node.js
- NestJS
- TypeORM
- MySQL

### Módulos principales
- catalog
- planning
- conferencing
- audit
- integrations

### Jobs (procesos automáticos)
1. ZoomIngestJob
2. ZoomRecordingsJob
3. TranscriptSummaryJob
4. SyllabusMatchJob
5. ScheduleConflictsJob
6. HoursValidationJob

### Principios
- Idempotencia (re-ejecutar no duplica)
- Auditabilidad total
- Datos reales > datos planificados

---

## Frontend

**Stack:**
- Angular (standalone)
- TailwindCSS

### Features
- planning
- audit
- integrations
- catalog

### Vistas clave
- Gestión de ofertas
- Horarios y grupos
- Cruces
- Auditoría de sesiones
- Detalle de clase (Zoom)

---

## Flujo principal

1. Admin planifica clase
2. Admin asigna Zoom
3. Clase se dicta
4. Job ingesta datos reales
5. Sistema calcula métricas
6. Sistema compara con syllabus
7. Auditor revisa resultados
