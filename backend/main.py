import os
import mimetypes
from uuid import uuid4
from typing import Dict, Any
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

# ====== ENV ======
DATABASE_URL = os.environ.get("DATABASE_URL")
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "")
RUN_MIGRATIONS = os.environ.get("RUN_MIGRATIONS", "0") == "1"

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")  # https://<account>.r2.cloudflarestorage.com
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")

# Плейсхолдер, если фото не загрузили
NO_PHOTO_URL = "https://foodyweb-production.up.railway.app/img/no-photo.png"

# ====== APP / CORS ======
app = FastAPI()
_pool: asyncpg.pool.Pool | None = None

origins = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====== DB bootstrap ======
async def _initialize(conn: asyncpg.Connection):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS merchants (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          address TEXT,
          phone TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS offers (
          id SERIAL PRIMARY KEY,
          merchant_id INT NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
          title TEXT NOT NULL,
          description TEXT,
          category TEXT,
          price NUMERIC(12,2) NOT NULL,
          stock INT NOT NULL DEFAULT 1,
          image_url TEXT NOT NULL,
          expires_at TIMESTAMPTZ NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_offers_expires ON offers(expires_at);
        CREATE INDEX IF NOT EXISTS idx_offers_status ON offers(status);
        """
    )
    # Гарантируем базового мерчанта с id=1 (для пилота)
    await conn.execute(
        """
        INSERT INTO merchants (id, name, address, phone)
        VALUES (1, 'Demo Restaurant', 'Demo address', '+000000000')
        ON CONFLICT (id) DO NOTHING;
        """
    )

async def _ensure(conn: asyncpg.Connection):
    if RUN_MIGRATIONS:
        await _initialize(conn)

@app.on_event("startup")
async def pool():
    global _pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL missing")
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _pool.acquire() as conn:
        await _ensure(conn)

@app.get("/health")
async def health():
    return {"ok": True}

# ====== R2 client / URL helpers ======
def _r2_client():
    if not all([R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        raise RuntimeError("R2 env not configured")
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="auto",
    )

def _public_r2_url(key: str) -> str:
    """
    Account endpoint: https://<account>.r2.cloudflarestorage.com
    Public URL:       https://pub-<account>.r2.dev/<bucket>/<key>
    Фоллбэк (если pub-домен не настроен): <endpoint>/<bucket>/<key>
    """
    try:
        host = R2_ENDPOINT.split("//", 1)[-1]
        account = host.split(".", 1)[0]
        return f"https://pub-{account}.r2.dev/{R2_BUCKET}/{key}"
    except Exception:
        return f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"

# ====== Upload image to R2 ======
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    try:
        ext = os.path.splitext(file.filename or "")[1].lower() or ".jpg"
        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            raise HTTPException(status_code=400, detail="Unsupported image type")

        content = await file.read()  # читаем в память (ASGI-safe)
        key = f"offers/{uuid4().hex}{ext}"

        s3 = _r2_client()
        content_type = file.content_type or mimetypes.guess_type(file.filename or "")[0] or "application/octet-stream"

        # У R2 ACL часто игнорируются — не передаем ACL вовсе.
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=content,
            ContentType=content_type,
        )

        public_url = _public_r2_url(key)
        return {"url": public_url, "key": key}
    except (BotoCoreError, ClientError) as e:
        print("UPLOAD_ERROR_S3:", repr(e))
        raise HTTPException(status_code=500, detail=f"Upload failed (S3): {e}")
    except HTTPException:
        raise
    except Exception as e:
        print("UPLOAD_ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

# ====== Create offer (image optional) ======
def _parse_expires_at(value: str) -> datetime:
    """
    Ожидаем 'YYYY-MM-DD HH:MM' (flatpickr).
    Делаем timezone-aware (UTC).
    """
    if not value:
        raise ValueError("expires_at is empty")
    value = value.strip()
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        # запасной вариант: ISO / c «Z»
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

@app.post("/merchant/offers")
async def create_offer(payload: Dict[str, Any] = Body(...)):
    try:
        required = ["title", "price", "stock", "expires_at"]
        for r in required:
            if r not in payload or (str(payload[r]).strip() == ""):
                raise HTTPException(status_code=400, detail=f"Field {r} is required")

        merchant_id = int(payload.get("merchant_id") or 1)
        image_url = (payload.get("image_url") or "").strip() or NO_PHOTO_URL
        expires_at_dt = _parse_expires_at(payload.get("expires_at"))

        async with _pool.acquire() as conn:
            # На случай, если кто-то дропнул merchants — подчиним мерчанта 1 перед вставкой
            await conn.execute(
                """
                INSERT INTO merchants (id, name)
                VALUES (1, 'Demo Restaurant')
                ON CONFLICT (id) DO NOTHING;
                """
            )

            row = await conn.fetchrow(
                """
                INSERT INTO offers
                  (merchant_id, title, description, price, stock, category, image_url, expires_at, status, created_at)
                VALUES
                  ($1, $2, $3, $4, $5, COALESCE($6,'other'), $7, $8, 'active', NOW())
                RETURNING id
                """,
                merchant_id,
                payload.get("title"),
                payload.get("description"),
                payload.get("price"),
                int(payload.get("stock")),
                payload.get("category"),
                image_url,
                expires_at_dt,
            )
            return {"id": row["id"]}
    except HTTPException:
        raise
    except Exception as e:
        print("CREATE_OFFER_ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"Create offer failed: {e}")

# ====== Public offers ======
@app.get("/public/offers")
async def public_offers():
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT o.id, o.title, o.description, o.price, o.stock, o.category,
                   o.image_url, o.expires_at, o.status,
                   m.id AS merchant_id, m.name AS merchant_name, m.address
            FROM offers o
            JOIN merchants m ON m.id = o.merchant_id
            WHERE o.status = 'active'
              AND o.expires_at > NOW()
              AND o.stock > 0
            ORDER BY o.expires_at ASC
            LIMIT 200
            """
        )
        return [dict(r) for r in rows]
