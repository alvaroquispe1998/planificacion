# CODEX – PROMPT DEFINITIVO (CONTRATO TÉCNICO)

## 0. INSTRUCCIÓN CRÍTICA

El archivo de tablas proporcionado por el usuario
(ej: `Project UAI Academic Scheduling.txt`)
es la **FUENTE DE VERDAD ABSOLUTA** del dominio.

### REGLAS ABSOLUTAS
- NO inventes tablas
- NO elimines tablas
- NO modifiques nombres de tablas
- NO modifiques nombres de columnas
- NO cambies tipos de datos
- NO agregues relaciones que no existan
- NO agregues lógica implícita que no pueda mapearse a una tabla

Si algo no está representado en el archivo de tablas, **NO LO IMPLEMENTES**  
y **DETENTE** para reportarlo.

---

## 1. CONTEXTO DEL SISTEMA

Implementa un sistema institucional para:

1. Planificación académica universitaria
2. Control real de ejecución de clases virtuales (Zoom)
3. Auditoría de tiempo, asistencia y reconexiones docentes
4. Comparación del contenido dictado vs syllabus
5. Detección de cruces y validación de carga horaria

Backend y Frontend son **proyectos independientes**.  
Se comunican **únicamente** por API REST.  
Docker solo orquesta, no define arquitectura.

---

## 2. INPUTS OBLIGATORIOS

Codex debe leer **ANTES de escribir código**:

1. `Project UAI Academic Scheduling.txt`  
   → define TODAS las tablas, relaciones y enums válidos.

2. Documentación técnica existente del proyecto (si aplica).

El archivo de tablas **tiene prioridad absoluta** sobre cualquier otra fuente.

---

## 3. ALCANCE FUNCIONAL (OBLIGATORIO)

Codex debe implementar **SOLO** lo que pueda mapearse
directamente a las tablas definidas.

### 3.1 Planificación académica
- CRUD completo para:
  - class_offerings
  - class_groups
  - class_meetings
- Asignación docente:
  - class_teachers
  - class_group_teachers
- Validación:
  - course_section_hour_requirements
- Detección de cruces:
  - schedule_conflicts

### 3.2 Control real de sesiones (Zoom)
A partir del `zoom_meeting_id` registrado:

- meeting_instances
- meeting_participants
- meeting_attendance_segments
- meeting_teacher_metrics

Debe poder responder:
- inicio real
- fin real
- duración
- reconexiones del docente
- minutos dictados

### 3.3 Evidencia de sesión
- meeting_recordings
- meeting_transcripts

No es obligatorio que siempre exista transcript.

---

## 4. CONTENIDO VS SYLLABUS (SIN IA PAGADA)

Codex debe implementar métodos **determinísticos y auditables**:

### Permitido
- TF-IDF
- Cosine similarity
- Keyword overlap
- Extractive summarization (frases del propio texto)

### Prohibido
- OpenAI
- Gemini
- Claude
- APIs de IA externas

Resultados a persistir:
- meeting_summaries
- meeting_syllabus_match

Cada score debe ser **explicable** (keywords + método).

---

## 5. ARQUITECTURA TÉCNICA

### Backend
- Node.js + NestJS
- TypeORM + MySQL
- Módulos alineados a grupos de tablas
- Servicios idempotentes
- Jobs re-ejecutables sin duplicar datos

### Frontend
- Angular standalone
- UI administrativa
- Lectura/escritura vía API
- Vistas:
  - planificación
  - cruces
  - auditoría de sesiones
  - detalle de clase

---

## 6. EJECUCIÓN POR FASES (OBLIGATORIO)

### Fase 1 – Modelo y planning
- Entidades exactamente iguales a las tablas
- CRUD planning
- Asignación docentes
- Horarios

### Fase 2 – Cruces y validaciones
- Detección de solapes
- Validación de horas esperadas

### Fase 3 – Zoom audit
- Ingesta de instancias
- Asistencia
- Métricas docentes

### Fase 4 – Evidencia y syllabus
- Grabaciones
- Transcript
- Resumen
- Concordancia

### Fase 5 – Frontend
- UI mínima pero funcional
- Sin lógica de negocio en frontend

---

## 7. CONDICIÓN DE PARADA (CRÍTICA)

Si alguna funcionalidad solicitada:
- no puede mapearse a una tabla
- requiere crear una tabla nueva
- requiere cambiar el modelo

Codex debe:
1. DETENERSE
2. EXPLICAR el bloqueo
3. ESPERAR instrucciones

NO adivinar.
NO improvisar.

---

## 8. ENTREGABLES ESPERADOS

- Backend compilable
- Frontend compilable
- Endpoints documentados
- Jobs ejecutables manual y automáticamente
- README mínimo de ejecución

---

## 9. TONO Y DISCIPLINA

Este sistema es:
- institucional
- auditable
- de largo plazo

Priorizar:
- claridad
- trazabilidad
- mantenibilidad

Evitar:
- atajos
- magia
- dependencias innecesarias
