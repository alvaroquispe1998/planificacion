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

Si MySQL corre en `docker compose` pero el backend lo levantas fuera de Docker, usa `DB_HOST=localhost` y `DB_PORT=3307`.
Aunque por defecto ahora el backend intenta corregir automaticamente el caso comun donde llega `DB_HOST=mysql` fuera de la red de Compose.

API base: `http://localhost:3000`

Acceso de prueba:

- Usuario: `admin`
- Password: `admin123`

Este usuario administrador bootstrap se crea automaticamente solo si la tabla `auth_users` esta vacia al arrancar el backend.
Si ya existen usuarios, no se vuelve a recrear.

Puedes cambiar estas credenciales iniciales agregando en `backend/.env`:

```env
AUTH_BOOTSTRAP_ADMIN_USERNAME=admin
AUTH_BOOTSTRAP_ADMIN_PASSWORD=admin123
```

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

Para `AULAVIRTUAL` tambien puedes activar renovacion automatica por login desde `backend/.env`:

```env
AULA_VIRTUAL_AUTO_LOGIN_ENABLED=true
AULA_VIRTUAL_LOGIN_URL=https://aulavirtual2.autonomadeica.edu.pe/account/login?ReturnUrl=%2F
AULA_VIRTUAL_USERNAME=tu_usuario
AULA_VIRTUAL_PASSWORD=tu_password
AULA_VIRTUAL_LOGIN_USERNAME_FIELD=username
AULA_VIRTUAL_LOGIN_PASSWORD_FIELD=password
AULA_VIRTUAL_LOGIN_RETURN_URL_FIELD=ReturnUrl
AULA_VIRTUAL_LOGIN_RETURN_URL_VALUE=/
```

Cuando la validacion detecta sesion vencida o faltante para `AULAVIRTUAL`, el backend intenta:

- cargar la pagina de login
- recoger cookies e inputs ocultos
- ejecutar el login
- capturar `LAUXAUTH`
- guardar la nueva sesion automaticamente

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

El frontend de desarrollo usa proxy local a `/api`, por lo que no deberias apuntarlo manualmente a la API publicada.

## Como correr

### Local sin Docker

1. Copia `backend/.env.dev.example` a `backend/.env`
2. Levanta MySQL local o con Docker en el puerto `3307`
3. Inicia backend:

```bash
cd backend
npm install
npm run start:dev
```

4. Inicia frontend:

```bash
cd frontend
npm install
npm run start
```

URLs:

- Frontend: `http://localhost:4200`
- Backend: `http://localhost:3000`

### Local con Docker

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

URLs:

- Frontend: `http://localhost:4200`
- Backend: `http://localhost:3000`
- MySQL: `127.0.0.1:3307`

### Produccion / publicada

Hay dos formas validas de trabajar:

- con Docker Compose
- sin Docker, levantando backend y frontend por separado

El archivo `backend/.env.prod.example` no debe ir comentado completo.
Debe quedar como plantilla real de produccion, pero con valores neutrales.
Si publicas sin Docker, cambia `DB_HOST` al host o IP real de tu base.
Si publicas con Docker Compose y usas MySQL dentro del stack, ahi si puedes usar `DB_HOST=mysql`.

Flujo recomendado:

- 1) `http://localhost:4200/settings` -> Configuracion -> Sincronizacion con Akademic
- 2) Validar/renovar cookies si estan vencidas
- 3) Ejecutar sincronizacion de catalogos y secciones
- 4) Ir a `http://localhost:4200/planning`

## Docker Compose

### Desarrollo local

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

Servicios:

- Frontend: `http://localhost:4200`
- Backend API: `http://localhost:3000`
- MySQL: `localhost:3307`

El frontend local habla con el backend por `/api` y el contenedor de frontend lo proxyea hacia `backend:3000`.
Asi se evita mezclar la API publicada con la local y no deberias volver a tener problemas de CORS por ese motivo.

### Produccion / publicada con Docker

```bash
copy backend/.env.prod.example backend/.env.prod
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

En esta modalidad:

- frontend queda publicado por el puerto configurado en `FRONTEND_HOST_PORT` (por defecto `8080`)
- backend ya no se expone al host
- mysql ya no se expone al host
- el frontend sigue hablando al backend por `/api`

### Produccion / publicada sin Docker

Backend:

1. Copia `backend/.env.prod.example` a `backend/.env`
2. Completa secretos, credenciales, dominio y conexion real a base de datos
3. Inicia backend con tu proceso preferido (`node`, `pm2`, servicio del servidor, etc.)

```bash
cd backend
npm install
npm run build
npm run start:prod
```

Frontend:

1. Compila Angular
2. Publica el contenido de `frontend/dist/frontend/browser` en tu Nginx, Apache o IIS
3. Mantiene `runtime-config.js` apuntando a `/api` si frontend y backend quedaran bajo el mismo dominio

```bash
cd frontend
npm install
npm run build
```

Recomendacion para publicada sin Docker:

- publica frontend y backend bajo el mismo dominio si puedes
- haz que el servidor web envie `/api` al backend
- usa `DB_SYNCHRONIZE=false`
- no uses `SYNC_TLS_ALLOW_INSECURE=true` en produccion

Recomendacion:

- usa `backend/.env` solo para desarrollo local
- usa `backend/.env.prod` solo para publicada
- no reutilices el mismo archivo entre ambos entornos

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
