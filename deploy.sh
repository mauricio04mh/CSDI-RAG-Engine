#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
APP_PORT="${APP_PORT:-8888}"
DATABASE_URL="${DATABASE_URL:-postgresql://raguser:ragpassword@localhost:5444/ragengine}"
PYTHONPATH="${PYTHONPATH:-.}"
VENV=".venv/bin"

# ---------------------------------------------------------------------------
# 1. Start PostgreSQL
# ---------------------------------------------------------------------------
echo "==> Starting PostgreSQL..."
docker compose up -d postgres

echo "==> Waiting for PostgreSQL to be healthy..."
until docker compose exec -T postgres pg_isready -U raguser -d ragengine -q; do
  sleep 1
done
echo "    PostgreSQL is ready."

# ---------------------------------------------------------------------------
# 2. Run migrations
# ---------------------------------------------------------------------------
echo "==> Running Alembic migrations..."
DATABASE_URL="$DATABASE_URL" PYTHONPATH="$PYTHONPATH" "$VENV/alembic" upgrade head
echo "    Migrations applied."

# ---------------------------------------------------------------------------
# 3. Free the port if something is already listening
# ---------------------------------------------------------------------------
if fuser "$APP_PORT/tcp" > /dev/null 2>&1; then
  echo "==> Port $APP_PORT in use — stopping existing process..."
  fuser -k "$APP_PORT/tcp" || true
  sleep 1
fi

# ---------------------------------------------------------------------------
# 4. Start the API server
# ---------------------------------------------------------------------------
echo "==> Starting API server on port $APP_PORT..."
exec env DATABASE_URL="$DATABASE_URL" PYTHONPATH="$PYTHONPATH" \
  "$VENV/uvicorn" main:app --host 0.0.0.0 --port "$APP_PORT" --reload
