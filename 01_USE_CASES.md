# UAIClassAudit – Casos de Uso Oficiales

Este documento define los **casos de uso funcionales** del sistema de
Planificación Académica + Auditoría de Clases Zoom.

NO describe implementación técnica.
NO define arquitectura.
SOLO comportamiento esperado del sistema.

---

## UC-01 – Gestionar catálogos académicos
**Actor:** Administrador  
**Descripción:** Mantener semestres, campus, carreras, planes, cursos, secciones, aulas.  
**Resultado:** Catálogos listos para planificación.

---

## UC-02 – Crear oferta académica (Class Offering)
**Actor:** Administrador  
**Descripción:** Crear una oferta real de un curso para un semestre/campus/turno.  
**Resultado:** `class_offerings` creada.

---

## UC-03 – Asignar docentes a la oferta
**Actor:** Administrador  
**Descripción:** Asignar docente titular o co-docentes a la oferta.  
**Tablas:** `class_teachers`  
**Resultado:** Docentes globales definidos.

---

## UC-04 – Definir grupos (T / P1 / P2 / LAB)
**Actor:** Administrador  
**Descripción:** Crear subgrupos por tipo (teoría, práctica, laboratorio).  
**Tablas:** `class_groups`  
**Resultado:** Grupos operativos.

---

## UC-05 – Asignar docentes por grupo
**Actor:** Administrador  
**Descripción:** Asignar docentes específicos a cada grupo.  
**Tablas:** `class_group_teachers`  
**Resultado:** Docentes correctos por grupo.

---

## UC-06 – Programar horarios
**Actor:** Administrador  
**Descripción:** Programar día, hora y aula para cada grupo.  
**Tablas:** `class_meetings`  
**Resultado:** Horario académico.

---

## UC-07 – Detectar cruces
**Actor:** Administrador / Auditor  
**Descripción:** Detectar solapes de:
- docente
- aula
- grupo
- sección  
**Tablas:** `schedule_conflicts`  
**Resultado:** Conflictos visibles y auditables.

---

## UC-08 – Validar horas esperadas (T/P/LAB)
**Actor:** Auditor  
**Descripción:** Comparar horas programadas vs horas requeridas por curso.  
**Tablas:** `course_section_hour_requirements`  
**Resultado:** Cumple / No cumple.

---

## UC-09 – Asociar Zoom a la clase
**Actor:** Administrador  
**Descripción:** Relacionar el `zoom_meeting_id` con la clase planificada.  
**Resultado:** Clase auditable.

---

## UC-10 – Ingestar sesión real de Zoom
**Actor:** Job automático  
**Descripción:** Registrar:
- instancias reales
- inicio/fin real
- reconexiones
- minutos dictados  
**Tablas:** `meeting_instances`, `meeting_attendance_segments`, `meeting_teacher_metrics`

---

## UC-11 – Registrar grabaciones y transcript
**Actor:** Job automático  
**Descripción:** Registrar grabaciones y transcript si Zoom lo provee.  
**Tablas:** `meeting_recordings`, `meeting_transcripts`

---

## UC-12 – Generar resumen sin IA pagada
**Actor:** Job automático  
**Descripción:** Generar resumen extractivo y keywords.  
**Tablas:** `meeting_summaries`

---

## UC-13 – Comparar con syllabus
**Actor:** Job automático  
**Descripción:** Comparar lo hablado vs lo esperado en la semana.  
**Tablas:** `meeting_syllabus_match`  
**Resultado:** OK / REVIEW / MISMATCH.

---

## UC-14 – Auditoría integral
**Actor:** Auditor  
**Descripción:** Ver por clase:
- programado vs real
- reconexiones
- evidencia
- concordancia con syllabus
- cruces  
**Resultado:** Control académico institucional.
