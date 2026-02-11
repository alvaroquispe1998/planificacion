# UAI Academic Scheduling

Implementacion inicial en dos proyectos independientes:

- `backend/` NestJS + TypeORM + MySQL
- `frontend/` Angular standalone

## Requisitos

- Node.js 24+
- npm 11+
- MySQL 8+
- Docker + Docker Compose (opcional)

## Backend

```bash
cd backend
npm install
# copiar .env.example a .env
npm run start:dev
```

API base: `http://localhost:3000`

Health check:

```bash
GET /health
```

Sincronizacion Akademic (carga masiva a BD):

```bash
GET /settings/sync/resources
GET /settings/sync/sources
PUT /settings/sync/sources/:code/session-cookie
POST /settings/sync/sources/:code/validate-session
POST /settings/sync/run
GET /settings/sync/jobs
```

Payload para cookie/sesion:

```json
{
  "cookie_text": "Cookie: nombre1=valor1; nombre2=valor2",
  "expires_at": "2026-02-10T22:00:00.000Z"
}
```

La sesion se valida contra cada fuente y se marca como `ACTIVE`, `EXPIRED` o `ERROR`.
Si el backend detecta redireccion a login/401/403, se considera cookie vencida y se pide renovacion.

Payload para ejecutar sincronizacion:

```json
{
  "mode": "FULL",
  "resources": ["semesters", "campuses", "courses", "teachers", "classrooms"]
}
```

Los recursos de sincronizacion se exponen agrupados por modulo en `GET /settings/sync/resources`.
Para agregar nuevos modulos/recursos, se amplia el catalogo central en backend (`settings-sync.service.ts`).

Seed local (idempotente):

```bash
cd backend
npm run seed
```

## Frontend

```bash
cd frontend
npm install
npm run start
```

App base: `http://localhost:4200`

Flujo recomendado:

- 1) `http://localhost:4200/settings` -> Configuracion -> Sincronizacion con Akademic
- 2) Validar/renovar cookies si estan vencidas
- 3) Ejecutar sincronizacion de catalogos y secciones
- 4) Ir a `http://localhost:4200/planning`

## Docker Compose (MySQL + Backend + Seed + Frontend)

```bash
docker compose up --build
```

Servicios:

- Frontend: `http://localhost:4200`
- Backend API: `http://localhost:3000`
- MySQL: `localhost:3307` (por defecto)

Si ya tienes libre el `3306` y prefieres ese puerto:

```bash
MYSQL_HOST_PORT=3306 docker compose up --build
```

En PowerShell:

```powershell
$env:MYSQL_HOST_PORT=3306; docker compose up --build
```

El contenedor `seed` corre una vez y carga datos de ejemplo para flujo:
planificacion -> auditoria Zoom -> syllabus.

## Build y pruebas ejecutadas

Backend:

```bash
cd backend
npm run build
npm test -- --runInBand
```

Frontend:

```bash
cd frontend
npm run build
npm test -- --watch=false
```

## Nota de alcance

Se implemento solo funcionalidad mapeable a tablas del archivo `Project UAI Academic Scheduling.txt`.
No se agregaron tablas nuevas ni logica con IA pagada.

docker compose up -d --build mysql
docker compose up -d mysql