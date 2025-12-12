import os
import hmac
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from dotenv import load_dotenv
from typing import Optional, List, Dict, Any

load_dotenv()

# ==================== CẤU HÌNH ====================
DATABASE_URL = os.getenv("DATABASE_URL")  # Dùng Session Pooler port 6543
API_KEY = os.getenv("INTERNAL_API_KEY")

if not DATABASE_URL:
    raise RuntimeError("Thiếu DATABASE_URL trong .env (dùng port 6543)")
if not API_KEY:
    raise RuntimeError("Thiếu INTERNAL_API_KEY")

# ==================== DANH SÁCH 39 BẢNG ====================
TABLES = [
    "account", "account_route", "address", "bank_account",
    "customer", "customer_voucher", "destination", "domestic",
    "domestic_packing", "feedback", "order_links", "order_process_log",
    "orders", "otp", "packing", "packing_domestic",
    "partial_shipment", "payment", "payment_orders", "product_type",
    "purchases", "route", "shipment_tracking", "staff",
    "voucher", "voucher_route", "warehouse", "warehouse_location",
    "websites", "order_payment_link", "notification"
]

# ==================== KẾT NỐI POOL (TỰ ĐỘNG REUSE) ====================
def get_db():
    conn = psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        connect_timeout=10
    )
    try:
        yield conn
    finally:
        conn.close()

# ==================== FASTAPI APP ====================
app = FastAPI(
    title="Tiximax Supabase API – Raw PostgreSQL (psycopg2 + Pooler)",
    version="3.0.0",
    description="Dùng psycopg2 + Session Pooler → TỐC ĐỘ SIÊU NHANH\n"
                "Bypass hoàn toàn supabase-py → JOIN, CTE, raw SQL thoải mái",
    swagger_ui_parameters={"persistAuthorization": True},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== BẢO MẬT X-API-Key ====================
def check_api_key(request: Request):
    header = request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(header, API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized – Sai X-API-Key")

# ==================== OPENAPI + AUTH ====================
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, description=app.description, routes=app.routes)
    schema["components"]["securitySchemes"] = {
        "XApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "INTERNAL_API_KEY từ .env"
        }
    }
    schema["security"] = [{"XApiKeyAuth": []}]
    app.openapi_schema = schema
    return schema

app.openapi = custom_openapi

# ==================== ROUTES ====================
@app.get("/health")
def health():
    return {
        "status": "OK",
        "mode": "psycopg2 + Session Pooler (port 6543)",
        "tables": len(TABLES),
        "message": "Siêu nhanh, siêu mạnh!"
    }

@app.get("/api/meta/tables")
def list_tables(request: Request, db=Depends(get_db)):
    check_api_key(request)
    return {"allowed_tables": TABLES, "total": len(TABLES)}

# ==================== ENDPOINT CHÍNH: LẤY BẤT KỲ BẢNG NÀO ====================
@app.get("/api/{table}")
async def get_table(
    table: str,
    request: Request,
    db=Depends(get_db),
    select: str = Query("*", description="VD: order_id,status,created_at"),
    where: Optional[str] = Query(None, description="Raw WHERE clause, VD: status='DA_GIAO' AND created_at > '2025-01-01'"),
    order: Optional[str] = Query(None, description="Cột để ORDER BY"),
    desc: bool = Query(False),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    check_api_key(request)
    if table not in TABLES:
        raise HTTPException(status_code=404, detail=f"Bảng '{table}' không được phép truy cập")

    cursor = db.cursor()

    try:
        # Xây dựng query an toàn
        query = f"SELECT {select} FROM public.\"{table}\""
        params = []
        if where:
            query += f" WHERE {where}"
        if order:
            query += f" ORDER BY \"{order}\" {'DESC' if desc else 'ASC'}"
        query += f" LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cursor.execute(query, params)
        rows = cursor.fetchall()

        # Đếm tổng
        cursor.execute(f"SELECT COUNT(*) AS total FROM public.\"{table}\"")
        if where:
            cursor.execute(f"SELECT COUNT(*) AS total FROM public.\"{table}\" WHERE {where}")
        total = cursor.fetchone()["total"]

        return {
            "table": table,
            "count": total,
            "returned": len(rows),
            "data": [dict(row) for row in rows],
            "limit": limit,
            "offset": offset,
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Lỗi query bảng {table}: {str(e)}")

# ==================== LỖI TOÀN CỤC ====================
@app.exception_handler(Exception)
async def global_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": str(exc)}
    )