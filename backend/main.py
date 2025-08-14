import os, secrets, csv, io, datetime as dt
from typing import Optional, Dict, Any
import asyncpg
from fastapi import FastAPI, Header, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import bootstrap_sql

import boto3
from botocore.client import Config as BotoConfig

DB_URL = os.getenv("DATABASE_URL")
CORS = [s.strip() for s in (os.getenv("CORS_ORIGINS","").split(",") if os.getenv("CORS_ORIGINS") else [])]
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_BUCKET   = os.getenv("R2_BUCKET")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

app = FastAPI(title="Foody Backend — MVP API")
if CORS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

pool: asyncpg.Pool = None

def auth_key(x_key: Optional[str]):
    if not x_key:
        raise HTTPException(status_code=401, detail="X-Foody-Key required")
    return x_key

async def get_conn():
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    return pool

@app.on_event("startup")
async def on_startup():
    bootstrap_sql.ensure()
    await get_conn()
    async with pool.acquire() as conn:
        try:
            await conn.fetchval("SELECT COUNT(*) FROM foody_restaurants")
        except Exception as e:
            print("DB not ready yet:", e)

@app.get("/health")
async def health():
    return {"ok": True, "db": DB_URL is not None}

@app.post("/api/v1/merchant/register_public")
async def register_public(data: Dict[str, Any] = Body(...)):
    name = (data.get("name") or "").strip() or "Foody Merchant"
    phone = (data.get("phone") or "").strip()
    rid = "RID_" + secrets.token_hex(6).upper()
    key = "KEY_" + secrets.token_hex(12).upper()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO foody_restaurants (restaurant_id, api_key, name, phone)
            VALUES ($1,$2,$3,$4)
        ''', rid, key, name, phone)
    return {"restaurant_id": rid, "api_key": key}

@app.get("/api/v1/merchant/profile")
async def merchant_profile(restaurant_id: str = Query(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", restaurant_id, x_key)
        if not row:
            raise HTTPException(status_code=403, detail="Invalid credentials")
        return dict(row)

@app.post("/api/v1/merchant/profile")
async def save_profile(data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    rid = data.get("restaurant_id")
    if not rid:
        raise HTTPException(400, "restaurant_id required")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", rid, x_key)
        if not row:
            raise HTTPException(status_code=403, detail="Invalid credentials")
        await conn.execute('''
            UPDATE foody_restaurants SET
                name=COALESCE($2,name),
                phone=COALESCE($3,phone),
                address=COALESCE($4,address),
                lat=$5, lng=$6,
                close_time=COALESCE($7, close_time)
            WHERE restaurant_id=$1
        ''', rid, data.get("name"), data.get("phone"), data.get("address"),
           data.get("lat"), data.get("lng"), data.get("close_time"))
        row = await conn.fetchrow("SELECT * FROM foody_restaurants WHERE restaurant_id=$1", rid)
        return dict(row)

@app.post("/api/v1/merchant/offers")
async def create_offer(data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    rid = data.get("restaurant_id")
    if not rid:
        raise HTTPException(400, "restaurant_id required")
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", rid, x_key)
        if not ok:
            raise HTTPException(403, "Invalid credentials")
        row = await conn.fetchrow('''
            INSERT INTO foody_offers
                (restaurant_id,title,price_cents,original_price_cents,qty_total,qty_left,expires_at,image_url,description,status)
            VALUES
                ($1,$2,$3,$4,$5,$6,$7,$8,$9,'active')
            RETURNING *
        ''', rid, data.get("title"), data.get("price_cents"), data.get("original_price_cents"),
           data.get("qty_total"), data.get("qty_left"), data.get("expires_at"),
           data.get("image_url"), data.get("description"))
        return dict(row)

@app.get("/api/v1/merchant/offers")
async def list_offers(restaurant_id: str = Query(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", restaurant_id, x_key)
        if not ok:
            raise HTTPException(403, "Invalid credentials")
        rows = await conn.fetch("SELECT * FROM foody_offers WHERE restaurant_id=$1 ORDER BY created_at DESC", restaurant_id)
        return [dict(r) for r in rows]

@app.put("/api/v1/merchant/offers/{offer_id}")
async def update_offer_put(offer_id: int, data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    rid = data.get("restaurant_id")
    if not rid:
        raise HTTPException(400, "restaurant_id required")
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", rid, x_key)
        if not ok:
            raise HTTPException(403, "Invalid credentials")
        row = await conn.fetchrow("SELECT * FROM foody_offers WHERE id=$1 AND restaurant_id=$2", offer_id, rid)
        if not row:
            raise HTTPException(404, "Offer not found")
        await conn.execute('''
            UPDATE foody_offers SET
                title=COALESCE($3,title),
                price_cents=COALESCE($4,price_cents),
                original_price_cents=$5,
                qty_total=COALESCE($6,qty_total),
                expires_at=COALESCE($7,expires_at),
                image_url=COALESCE($8,image_url),
                description=COALESCE($9,description)
            WHERE id=$1 AND restaurant_id=$2
        ''', offer_id, rid, data.get("title"), data.get("price_cents"),
           data.get("original_price_cents"), data.get("qty_total"),
           data.get("expires_at"), data.get("image_url"), data.get("description"))
        row2 = await conn.fetchrow("SELECT * FROM foody_offers WHERE id=$1", offer_id)
        return dict(row2)

@app.post("/api/v1/merchant/offers/update")
async def update_offer_post(data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    offer_id = int(data.get("offer_id") or 0)
    if offer_id <= 0:
        raise HTTPException(400, "offer_id required")
    return await update_offer_put(offer_id, data, x_foody_key=x_foody_key)

@app.post("/api/v1/merchant/offers/status")
async def update_offer_status(data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    rid = data.get("restaurant_id"); offer_id = int(data.get("offer_id") or 0)
    action = (data.get("action") or "").lower()
    if not rid or not offer_id or action not in ("archive","activate"):
        raise HTTPException(400, "restaurant_id, offer_id, action required")
    new_status = "archived" if action == "archive" else "active"
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", rid, x_key)
        if not ok: raise HTTPException(403, "Invalid credentials")
        await conn.execute("UPDATE foody_offers SET status=$3 WHERE id=$1 AND restaurant_id=$2", offer_id, rid, new_status)
        row = await conn.fetchrow("SELECT * FROM foody_offers WHERE id=$1", offer_id)
        return dict(row)

@app.post("/api/v1/merchant/offers/delete")
async def delete_offer(data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    rid = data.get("restaurant_id"); offer_id = int(data.get("offer_id") or 0)
    if not rid or not offer_id:
        raise HTTPException(400, "restaurant_id and offer_id required")
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", rid, x_key)
        if not ok: raise HTTPException(403, "Invalid credentials")
        await conn.execute("DELETE FROM foody_offers WHERE id=$1 AND restaurant_id=$2", offer_id, rid)
        return {"deleted": offer_id}

@app.get("/api/v1/offers")
async def public_offers():
    now = dt.datetime.utcnow()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT * FROM foody_offers
            WHERE status='active' AND (expires_at IS NULL OR expires_at > $1) AND qty_left > 0
            ORDER BY expires_at ASC NULLS LAST
        ''', now)
        return [dict(r) for r in rows]

@app.get("/api/v1/merchant/offers/csv")
async def export_csv(restaurant_id: str = Query(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", restaurant_id, x_key)
        if not ok: raise HTTPException(403, "Invalid credentials")
        rows = await conn.fetch("SELECT * FROM foody_offers WHERE restaurant_id=$1 ORDER BY id DESC", restaurant_id)
    def gen():
        buf = io.StringIO(); import csv as _csv; w = _csv.writer(buf)
        w.writerow(["id","title","price_cents","original_price_cents","qty_total","qty_left","expires_at","status"])
        for r in rows:
            w.writerow([r["id"],r["title"],r["price_cents"],r["original_price_cents"],r["qty_total"],r["qty_left"],r["expires_at"],r["status"]])
        yield buf.getvalue()
    headers = {"Content-Disposition": f'attachment; filename="offers_{restaurant_id}.csv"'}
    return StreamingResponse(gen(), media_type="text/csv", headers=headers)

@app.post("/api/v1/merchant/redeem")
async def redeem(data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    rid = data.get("restaurant_id")
    code = (data.get("code") or "").strip()
    offer_id = data.get("offer_id")
    if not rid or not code:
        raise HTTPException(400, "restaurant_id and code required")
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", rid, x_key)
        if not ok: raise HTTPException(403, "Invalid credentials")
        amount = 0
        if offer_id:
            off = await conn.fetchrow("SELECT price_cents FROM foody_offers WHERE id=$1 AND restaurant_id=$2", int(offer_id), rid)
            if off: amount = off["price_cents"] or 0
        existing = await conn.fetchrow("SELECT * FROM foody_redeems WHERE code=$1", code)
        if existing:
            return {"ok": True, "already": True, "redeem": dict(existing)}
        await conn.execute('''
            INSERT INTO foody_redeems (restaurant_id, offer_id, code, amount_cents)
            VALUES ($1,$2,$3,$4)
        ''', rid, int(offer_id) if offer_id else None, code, amount)
        if offer_id:
            await conn.execute("UPDATE foody_offers SET qty_left = GREATEST(qty_left-1,0) WHERE id=$1 AND restaurant_id=$2", int(offer_id), rid)
        rec = await conn.fetchrow("SELECT * FROM foody_redeems WHERE code=$1", code)
        return {"ok": True, "redeem": dict(rec)}

@app.get("/api/v1/merchant/stats")
async def stats(restaurant_id: str = Query(...), metric: str = Query("redeems"),
                date_from: Optional[str] = None, date_to: Optional[str] = None,
                x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    async with pool.acquire() as conn:
        ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", restaurant_id, x_key)
        if not ok: raise HTTPException(403, "Invalid credentials")

        def parse_date(s):
            try: return dt.datetime.fromisoformat(s.replace("Z",""))
            except: return None

        start = parse_date(date_from) or (dt.datetime.utcnow() - dt.timedelta(days=14))
        end   = parse_date(date_to)   or dt.datetime.utcnow()

        if metric == "revenue":
            rows = await conn.fetch('''
                SELECT date_trunc('day', redeemed_at) AS d, SUM(amount_cents) AS v
                FROM foody_redeems
                WHERE restaurant_id=$1 AND redeemed_at BETWEEN $2 AND $3
                GROUP BY d ORDER BY d
            ''', restaurant_id, start, end)
        elif metric == "offers_created":
            rows = await conn.fetch('''
                SELECT date_trunc('day', created_at) AS d, COUNT(*) AS v
                FROM foody_offers
                WHERE restaurant_id=$1 AND created_at BETWEEN $2 AND $3
                GROUP BY d ORDER BY d
            ''', restaurant_id, start, end)
        else:
            rows = await conn.fetch('''
                SELECT date_trunc('day', redeemed_at) AS d, COUNT(*) AS v
                FROM foody_redeems
                WHERE restaurant_id=$1 AND redeemed_at BETWEEN $2 AND $3
                GROUP BY d ORDER BY d
            ''', restaurant_id, start, end)

        points = [{"x": r["d"].isoformat(), "y": int(r["v"] or 0)} for r in rows]
        return {"points": points}

def s3_client():
    if not (R2_ENDPOINT and R2_BUCKET and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY):
        raise HTTPException(400, "R2 not configured")
    return boto3.client("s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4"))

@app.post("/api/v1/merchant/uploads/presign")
async def presign_upload(data: Dict[str, Any] = Body(...), x_foody_key: str = Header(None)):
    x_key = auth_key(x_foody_key)
    # --- HOTFIX: safe filename parsing (escaped backslash) ---
    name_raw = (data.get("filename") or "file.bin")
    # Windows-style paths may include backslashes; keep only the last segment
    filename = name_raw.replace("\\","/").split("/")[-1]
    content_type = data.get("content_type") or "application/octet-stream"
    rid = data.get("restaurant_id") or "misc"
    if data.get("restaurant_id"):
        async with pool.acquire() as conn:
            ok = await conn.fetchrow("SELECT 1 FROM foody_restaurants WHERE restaurant_id=$1 AND api_key=$2", rid, x_key)
            if not ok: raise HTTPException(403, "Invalid credentials")
    key = f"restaurants/{rid}/{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}_{filename}"
    s3 = s3_client()
    post = s3.generate_presigned_post(
        Bucket=R2_BUCKET, Key=key,
        Fields={"Content-Type": content_type},
        Conditions=[{"Content-Type": content_type}],
        ExpiresIn=3600
    )
    public_url = f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"
    return {"upload_url": post["url"], "fields": post["fields"], "method": "POST",
            "public_url": public_url, "key": key}
