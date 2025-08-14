Foody Backend — MVP

Run (local):
  export DATABASE_URL=postgresql://user:pass@localhost:5432/foody
  export CORS_ORIGINS=https://web-production-5431c.up.railway.app,https://bot-production-0297.up.railway.app
  export RUN_MIGRATIONS=1
  uvicorn backend.main:app --host 0.0.0.0 --port 8080

Railway ENV:
  DATABASE_URL=... (your Postgres DSN)
  CORS_ORIGINS=https://web-production-5431c.up.railway.app,https://bot-production-0297.up.railway.app
  RUN_MIGRATIONS=1

Smoke tests:
  curl -sS https://<backend>/health

  curl -sS -X POST https://<backend>/api/v1/merchant/register_public     -H "Content-Type: application/json"     -d '{"title":"Пекарня №1","phone":"+7 900 000-00-00"}'

  # Take RID/KEY from response or use TEST:
  # RID_TEST / KEY_TEST (seeded if tables were empty or cleaned)

  curl -sS "https://<backend>/api/v1/merchant/profile?restaurant_id=RID_TEST"     -H "X-Foody-Key: KEY_TEST"

  curl -sS -X POST https://<backend>/api/v1/merchant/offers     -H "Content-Type: application/json" -H "X-Foody-Key: KEY_TEST"     -d '{"restaurant_id":"RID_TEST","title":"Набор эклеров","price_cents":19900,"original_price_cents":34900,"qty_total":5,"qty_left":5,"expires_at":"2025-08-13T20:00:00Z"}'

  curl -sS https://<backend>/api/v1/offers

  curl -I "https://<backend>/api/v1/merchant/offers/csv?restaurant_id=RID_TEST"
