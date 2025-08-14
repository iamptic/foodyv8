# Foody Full-Stack Patch (backend + Docker for web/BOT)

## Structure
- `backend/` — FastAPI app (`main:app`), auto-migrations.
- `Dockerfile.backend` — builds backend container.
- `Dockerfile.web` — generic Node.js runner for your existing `web/` (uses `npm start`).
- `Dockerfile.bot` — generic Python runner for your existing `BOT/` (expects `python -m app`).
- `docker-compose.yml` — local stack: Postgres + backend + web + bot.
- `.env.example` — placeholders for local secrets.

## Local Run (one command)
```bash
cp .env.example .env   # edit BOT_TOKEN/WEBHOOK_SECRET
docker compose up --build
# backend   -> http://localhost:8080/health
# web       -> http://localhost:3000
# bot (api) -> http://localhost:8000
```

> `backend` seeds demo data on first run (RID_TEST/KEY_TEST).

## Railway (per-service)
- **backend**
  - Root directory: `backend`
  - Start command:
    ```
    uvicorn main:app --host 0.0.0.0 --port $PORT
    ```
  - ENV (min):
    - `DATABASE_URL=postgresql://...@postgres.railway.internal:5432/...`
    - `RUN_MIGRATIONS=1`
    - `CORS_ORIGINS=https://foodyweb-production.up.railway.app,https://foodybot-production.up.railway.app`
    - `RECOVERY_SECRET=foodyDevRecover123`

- **web**
  - Root directory: `web`
  - Start command: depends on your app, typically `npm start` (package.json).
  - Ensure `FOODY_API=https://<your-backend-host>` is wired into web app config.

- **BOT**
  - Root directory: `BOT`
  - Start command: depends on your bot entrypoint.
  - ENV (min): `BOT_TOKEN`, `WEBAPP_PUBLIC`, `WEBHOOK_SECRET`, `FOODY_API=https://<your-backend-host>`

## Notes
- Do **not** commit real secrets. Use Railway env vars or local `.env` (never push to Git).
- If web uses a different port or build step, adjust `Dockerfile.web` and compose accordingly.
- If bot's entrypoint differs from `python -m app`, change CMD in `Dockerfile.bot`.
