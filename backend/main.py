import os
import mimetypes
from uuid import uuid4
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta

import asyncpg
from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import bcrypt
import jwt  # PyJWT
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

# используем твой секрет
JWT_SECRET = os.environ.get("RECOVERY_SECRET", "devsecret")
JWT_ALG = "HS256"
SESSION_COOKIE = "foody_session"

# плейсхолдер на случай отсутствия фото
NO_PHOTO_URL = "https://foodyweb-production.up.railway.app/img/no-photo.png"

# ====== APP / CORS ======
app = FastAPI()
_pool: asyncpg.pool.Pool | None = None

origins = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,   # важно для cookie
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====== HELPERS ======
def _hash_pw(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def _check_pw(pw: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False

def _issue_jwt(user_id: int) -> str:
    payload = {"sub": user_id, "iat": int(datetime.utcnow().timestamp())}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)

def _decode_jwt(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except Exception:
        return None

async def get_current_user(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = _decode_jwt(token)
    if not data or "sub" not in data:
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = int(data["sub"])
    async with _pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id, phone, name FROM users WHERE id=$1", user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return dict(user)

def _cookie_response(payload: dict, token: str) -> JSONResponse:
    resp = JSONResponse(payload)
    # HttpOnly cookie для безопасности
    resp.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,  # 30 дней
        path="/",
    )
    return resp

# ====== DB bootstrap ======
async def _initialize(conn: asyncpg.Connection):
    # базовые таблицы пользователей/организаций/локаций
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY,
          phone TEXT UNIQUE NOT NULL,
          password_hash TEXT NOT NULL,
          name TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS organizations (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS organization_users (
          org_id INT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
          user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          role TEXT NOT NULL DEFAULT 'owner',
          PRIMARY KEY (org_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS locations (
          id SERIAL PRIMARY KEY,
          org_id INT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
          name TEXT NOT NULL,
          city TEXT,
          address_line TEXT,
          closing_time TEXT,       -- HH:MM, простой формат
          timezone TEXT,           -- IANA tz, напр. Asia/Dubai
          logo_url TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        -- старая схема merchants/merchant_id сохраняется для back-compat,
        -- но офферы привязываем к locations
        CREATE TABLE IF NOT EXISTS merchants (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          address TEXT,
          phone TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS offers (
          id SERIAL PRIMARY KEY,
          merchant_id INT REFERENCES merchants(id) ON DELETE SET NULL,
          location_id INT REFERENCES locations(id) ON DELETE CASCADE,
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
        CREATE INDEX IF NOT EXISTS idx_offers_location ON offers(location_id);
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

# ====== R2 client / URL helpers (upload) ======
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

def _pub_url_or_none(key: str) -> Optional[str]:
    try:
        host = R2_ENDPOINT.split("//", 1)[-1]
        account = host.split(".", 1)[0]
        return f"https://pub-{account}.r2.dev/{R2_BUCKET}/{key}"
    except Exception:
        return None

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    try:
        ext = os.path.splitext(file.filename or "")[1].lower() or ".jpg"
        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            raise HTTPException(status_code=400, detail="Unsupported image type")

        content = await file.read()
        key = f"offers/{uuid4().hex}{ext}"

        s3 = _r2_client()
        ctype = file.content_type or mimetypes.guess_type(file.filename or "")[0] or "application/octet-stream"
        s3.put_object(Bucket=R2_BUCKET, Key=key, Body=content, ContentType=ctype)

        public_url = _pub_url_or_none(key)
        display_url = public_url or s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": R2_BUCKET, "Key": key},
            ExpiresIn=60 * 60 * 24 * 365,
        )
        return {"url": public_url, "display_url": display_url, "key": key}
    except (BotoCoreError, ClientError) as e:
        print("UPLOAD_ERROR_S3:", repr(e))
        raise HTTPException(status_code=500, detail=f"Upload failed (S3): {e}")
    except Exception as e:
        print("UPLOAD_ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

# ====== AUTH ======
@app.post("/auth/register")
async def register(payload: Dict[str, Any] = Body(...)):
    """
    payload: name, phone, password, city, address_line, closing_time, timezone, (опц.) org_name
    Создаём: user -> organization -> location, маппим пользователя как owner,
    возвращаем профиль + ставим cookie.
    """
    required = ["name", "phone", "password"]
    for r in required:
        if r not in payload or not str(payload[r]).strip():
            raise HTTPException(status_code=400, detail=f"Field {r} is required")

    name = payload["name"].strip()
    phone = payload["phone"].strip()
    password_hash = _hash_pw(payload["password"])
    city = (payload.get("city") or "").strip()
    address_line = (payload.get("address_line") or "").strip()
    closing_time = (payload.get("closing_time") or "").strip()
    timezone_str = (payload.get("timezone") or "").strip()
    org_name = (payload.get("org_name") or name).strip()

    async with _pool.acquire() as conn:
        async with conn.transaction():
            # user
            u = await conn.fetchrow("SELECT id FROM users WHERE phone=$1", phone)
            if u:
                raise HTTPException(status_code=409, detail="Phone already registered")

            user_id = await conn.fetchval(
                "INSERT INTO users (phone, password_hash, name) VALUES ($1,$2,$3) RETURNING id",
                phone, password_hash, name
            )

            # organization
            org_id = await conn.fetchval(
                "INSERT INTO organizations (name) VALUES ($1) RETURNING id",
                org_name
            )
            await conn.execute(
                "INSERT INTO organization_users (org_id, user_id, role) VALUES ($1,$2,'owner')",
                org_id, user_id
            )

            # first location
            loc_id = await conn.fetchval(
                """
                INSERT INTO locations (org_id, name, city, address_line, closing_time, timezone)
                VALUES ($1,$2,$3,$4,$5,$6)
                RETURNING id
                """,
                org_id, name, city, address_line, closing_time, timezone_str
            )

    token = _issue_jwt(user_id)
    return _cookie_response({"user_id": user_id, "org_id": org_id, "location_id": loc_id}, token)

@app.post("/auth/login")
async def login(payload: Dict[str, Any] = Body(...)):
    """
    payload: phone, password
    """
    phone = (payload.get("phone") or "").strip()
    password = payload.get("password") or ""
    if not phone or not password:
        raise HTTPException(status_code=400, detail="Phone and password are required")

    async with _pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id, password_hash FROM users WHERE phone=$1", phone)
        if not user or not _check_pw(password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _issue_jwt(user["id"])
    return _cookie_response({"user_id": user["id"]}, token)

@app.post("/auth/logout")
async def logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp

@app.get("/auth/me")
async def me(user = Depends(get_current_user)):
    # отдадим базовый профиль + организации/локации
    async with _pool.acquire() as conn:
        orgs = await conn.fetch("""
            SELECT o.id, o.name, ou.role
            FROM organization_users ou
            JOIN organizations o ON o.id = ou.org_id
            WHERE ou.user_id=$1
        """, user["id"])
        locs = await conn.fetch("""
            SELECT l.id, l.org_id, l.name, l.city, l.address_line, l.closing_time, l.timezone
            FROM locations l
            WHERE l.org_id IN (SELECT org_id FROM organization_users WHERE user_id=$1)
            ORDER BY l.id
        """, user["id"])
    return {"user": user, "organizations": [dict(x) for x in orgs], "locations": [dict(x) for x in locs]}

# ====== Locations (для сетей) ======
@app.get("/locations")
async def list_locations(user = Depends(get_current_user)):
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT l.id, l.org_id, l.name, l.city, l.address_line, l.closing_time, l.timezone
            FROM locations l
            WHERE l.org_id IN (SELECT org_id FROM organization_users WHERE user_id=$1)
            ORDER BY l.id
        """, user["id"])
    return [dict(r) for r in rows]

@app.post("/locations")
async def create_location(payload: Dict[str, Any] = Body(...), user = Depends(get_current_user)):
    required = ["name"]
    for r in required:
        if r not in payload or not str(payload[r]).strip():
            raise HTTPException(status_code=400, detail=f"Field {r} is required")

    # берём первую организацию пользователя (для простоты пилота)
    async with _pool.acquire() as conn:
        org_row = await conn.fetchrow("""
            SELECT o.id FROM organizations o
            JOIN organization_users ou ON ou.org_id = o.id
            WHERE ou.user_id=$1
            ORDER BY o.id LIMIT 1
        """, user["id"])
        if not org_row:
            raise HTTPException(status_code=400, detail="Organization not found for user")
        org_id = org_row["id"]

        loc_id = await conn.fetchval("""
            INSERT INTO locations (org_id, name, city, address_line, closing_time, timezone, logo_url)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id
        """,
        org_id,
        payload.get("name").strip(),
        (payload.get("city") or "").strip(),
        (payload.get("address_line") or "").strip(),
        (payload.get("closing_time") or "").strip(),
        (payload.get("timezone") or "").strip(),
        (payload.get("logo_url") or "").strip(),
        )
    return {"id": loc_id}

# ====== Offers (привязка к location) ======
def _parse_expires_at(value: str) -> datetime:
    if not value:
        raise ValueError("expires_at is empty")
    value = value.strip()
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

@app.post("/merchant/offers")
async def create_offer(payload: Dict[str, Any] = Body(...), user = Depends(get_current_user)):
    required = ["title", "price", "stock", "expires_at"]
    for r in required:
        if r not in payload or (str(payload[r]).strip() == ""):
            raise HTTPException(status_code=400, detail=f"Field {r} is required")

    # если явно не указали location_id — берём первую доступную
    async with _pool.acquire() as conn:
        loc_id = payload.get("location_id")
        if loc_id:
            own = await conn.fetchval("""
                SELECT 1 FROM locations l
                WHERE l.id=$1 AND l.org_id IN (SELECT org_id FROM organization_users WHERE user_id=$2)
            """, int(loc_id), user["id"])
            if not own:
                raise HTTPException(status_code=403, detail="No access to location")
        else:
            row = await conn.fetchrow("""
                SELECT l.id FROM locations l
                WHERE l.org_id IN (SELECT org_id FROM organization_users WHERE user_id=$1)
                ORDER BY l.id LIMIT 1
            """, user["id"])
            if not row:
                raise HTTPException(status_code=400, detail="No locations found")
            loc_id = row["id"]

        image_url = (payload.get("image_url") or "").strip() or NO_PHOTO_URL
        expires_at_dt = _parse_expires_at(payload.get("expires_at"))

        row = await conn.fetchrow(
            """
            INSERT INTO offers (location_id, title, description, price, stock, category, image_url, expires_at, status, created_at)
            VALUES ($1, $2, $3, $4, $5, COALESCE($6,'other'), $7, $8, 'active', NOW())
            RETURNING id
            """,
            int(loc_id),
            payload.get("title"),
            payload.get("description"),
            payload.get("price"),
            int(payload.get("stock")),
            payload.get("category"),
            image_url,
            expires_at_dt,
        )
    return {"id": row["id"]}

@app.get("/public/offers")
async def public_offers():
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT o.id, o.title, o.description, o.price, o.stock, o.category,
                   o.image_url, o.expires_at, o.status,
                   l.id AS location_id, l.name AS merchant_name, l.address_line AS address, l.city
            FROM offers o
            JOIN locations l ON l.id = o.location_id
            WHERE o.status = 'active'
              AND o.expires_at > NOW()
              AND o.stock > 0
            ORDER BY o.expires_at ASC
            LIMIT 200
            """
        )
        return [dict(r) for r in rows]
