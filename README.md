# CSDI RAG Engine

Motor de búsqueda híbrida y RAG para documentación técnica. El proyecto combina:

- indexado BM25
- búsqueda vectorial con FAISS
- fusión híbrida BM25 + vectores
- re-ranker opcional
- generación de respuestas con un LLM externo
- ingestión desde fuentes configuradas o carga manual de archivos

## Requisitos

- Python 3.11
- Docker y Docker Compose
- una base PostgreSQL con la extensión `vector` de pgvector
- una API key para el proveedor LLM que vayas a usar

## Levantar el proyecto con Docker

1. Clona el repositorio.
2. Levanta los contenedores:

```bash
docker compose up --build
```

3. Abre la API en:

```text
http://localhost:8888
```

Con Docker Compose se levantan dos servicios:

- `app`, la API FastAPI
- `postgres`, PostgreSQL con `pgvector`

La base usa estos valores por defecto dentro del contenedor:

- usuario: `raguser`
- contraseña: `ragpassword`
- base de datos: `ragengine`

## Base de datos

El proyecto usa Alembic para las migraciones. Antes de arrancar la app por primera vez, asegúrate de tener el esquema aplicado:

```bash
alembic upgrade head
```

Si usas Docker Compose, la inicialización de PostgreSQL activa la extensión `vector` mediante:

- [`scripts/init_db.sql`](./scripts/init_db.sql)

## Configuración

La configuración del proyecto se centraliza en [`.env.template`](./.env.template).

1. Copia el archivo a `.env`.
2. Ajusta allí los valores que quieras cambiar.

Ese archivo incluye las variables de entorno principales con sus valores por defecto.

## Endpoints principales

Healthcheck:

- `GET /health`

Búsqueda:

- `POST /api/v1/search`
- `POST /api/v1/search/bm25`
- `POST /api/v1/vector/search`
- `POST /api/v1/rag/query`

Indexado:

- `POST /api/v1/indexing/index`
- `POST /api/v1/indexing/merge`
- `POST /api/v1/vector/index`

Ingesta:

- `POST /api/v1/ingest`
- `GET /api/v1/ingest/sources`

Configuración:

- `GET /api/v1/config`
- `POST /api/v1/config`

Métricas:

- `GET /api/v1/metrics`

Carga manual de archivos:

- `POST /api/v1/upload`

## Fuentes configuradas

El repositorio ya incluye fuentes de ejemplo en:

- [`src/sources_config/data/sources_data.json`](./src/sources_config/data/sources_data.json)

Actualmente aparecen, entre otras:

- `python_docs`
- `mdn_js`

Puedes listar las fuentes disponibles con:

```bash
curl http://localhost:8888/api/v1/ingest/sources
```

## Ejemplos rápidos

Buscar contenido:

```bash
curl -X POST http://localhost:8888/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{"query":"How do I use decorators in Python?","top_k":5}'
```

Consultar RAG:

```bash
curl -X POST http://localhost:8888/api/v1/rag/query \
  -H "Content-Type: application/json" \
  -d '{"query":"Explain the difference between list and tuple"}'
```

Ingerir una fuente configurada:

```bash
curl -X POST http://localhost:8888/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"source_id":"python_docs"}'
```

## Tests

Ejecuta la suite con:

```bash
pytest
```

## Notas de implementación

- La app arranca desde [`main.py`](./main.py), que expone `src.server:app`.
- El servidor FastAPI se define en [`src/server.py`](./src/server.py).
- El comando de despliegue rápido está en [`deploy.sh`](./deploy.sh).
