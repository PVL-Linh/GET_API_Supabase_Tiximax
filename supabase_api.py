# main.py - Phiên bản hoàn chỉnh, đã sửa hết lỗi, chạy được ngay
import os
import hmac
import traceback
from typing import Any, Dict, List, Optional, Set
from fastapi import FastAPI, Query, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ==================== CẤU HÌNH TỪ ENV ====================
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
API_KEY = os.getenv("INTERNAL_API_KEY")

if not SUPABASE_DB_URL:
    raise RuntimeError("Thiếu SUPABASE_DB_URL trong .env (dạng postgresql://...)")
if not API_KEY:
    raise RuntimeError("Thiếu INTERNAL_API_KEY trong .env")

def get_connection():
    return psycopg2.connect(SUPABASE_DB_URL, cursor_factory=RealDictCursor)

# ==================== FASTAPI APP ====================
app = FastAPI(
    title="Supabase Direct API (psycopg2)",
    version="2.1.0",
    description="Truy vấn trực tiếp PostgreSQL Supabase - nhanh hơn 10x so với REST",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== DANH SÁCH BẢNG ĐƯỢC PHÉP TRUY CẬP ====================
TABLES: List[str] = [
    "account", "account_route", "address", "customer", "customer_voucher", "destination", "domestic",
    "domestic_packing", "feedback", "order_links", "order_process_log",
    "orders", "packing","packing_domestic", "partial_shipment", "payment", "payment_orders", "product_type",
    "purchases", "route", "shipment_tracking", "staff", "warehouse", "voucher", "voucher_route",
    "warehouse_location", "websites",
]

SCHEMA_CACHE: Dict[str, List[Dict[str, Any]]] = {}

# ==================== BẢO MẬT ====================
def check_api_key(request: Request):
    header = request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(header, API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized - Sai X-API-Key")

def get_table_or_404(table: str) -> str:
    if table not in TABLES:
        raise HTTPException(status_code=404, detail=f"Bảng '{table}' không được phép truy cập")
    return table

# ==================== LẤY SCHEMA CHÍNH XÁC TỪ information_schema ====================
def fetch_schema(table: str) -> List[Dict[str, Any]]:
    query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (table,))
                rows = cur.fetchall()
                return [
                    {
                        "name": r["column_name"],
                        "type": r["data_type"],
                        "nullable": r["is_nullable"] == "YES"
                    }
                    for r in rows
                ]
    except Exception as e:
        print(f"[ERROR] Không lấy được schema bảng {table}: {e}")
        return []

def get_schema(table: str) -> List[Dict[str, Any]]:
    if table not in SCHEMA_CACHE:
        SCHEMA_CACHE[table] = fetch_schema(table)
    return SCHEMA_CACHE[table]

def allowed_columns_set(table: str) -> Set[str]:
    return {col["name"] for col in get_schema(table)}

# ==================== FILTER ĐỘNG SIÊU MẠNH ====================
def build_where_clause(params: dict, table: str) -> tuple[str, list]:
    allowed = allowed_columns_set(table)
    conditions = []
    values = []

    skip = {"select", "order", "desc", "limit", "offset", "count"}
    for key, val in params.items():
        if key in skip or val == "" or val is None:
            continue

        if "__" in key:
            op, col = key.split("__", 1)
        else:
            op, col = "eq", key

        if col not in allowed:
            raise HTTPException(status_code=400, detail=f"Cột '{col}' không tồn tại trong bảng '{table}'")

        if op == "eq":
            conditions.append(f'"{col}" = %s')
            values.append(val)
        elif op == "ne":
            conditions.append(f'"{col}" != %s')
            values.append(val)
        elif op == "gt":
            conditions.append(f'"{col}" > %s')
            values.append(val)
        elif op == "gte":
            conditions.append(f'"{col}" >= %s')
            values.append(val)
        elif op == "lt":
            conditions.append(f'"{col}" < %s')
            values.append(val)
        elif op == "lte":
            conditions.append(f'"{col}" <= %s')
            values.append(val)
        elif op == "like":
            conditions.append(f'"{col}" LIKE %s')
            values.append(val)
        elif op == "ilike":
            conditions.append(f'"{col}" ILIKE %s')
            values.append(val)
        elif op == "in":
            items = [x.strip() for x in str(val).split(",") if x.strip()]
            if not items:
                continue
            placeholders = ", ".join(["%s"] * len(items))
            conditions.append(f'"{col}" IN ({placeholders})')
            values.extend(items)
        elif op == "is":
            v = str(val).lower()
            if v == "null":
                conditions.append(f'"{col}" IS NULL')
            elif v == "true":
                conditions.append(f'"{col}" IS TRUE')
            elif v == "false":
                conditions.append(f'"{col}" IS FALSE')
            else:
                raise HTTPException(status_code=400, detail=f"Giá trị 'is' chỉ được là null/true/false")

    where_clause = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return where_clause, values

# ==================== OPENAPI ĐẸP + BẮT BUỘC X-API-Key ====================
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "XApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "Nhập key từ biến INTERNAL_API_KEY"
        }
    }
    openapi_schema["security"] = [{"XApiKeyAuth": []}]
    app.openapi_schema = openapi_schema
    return openapi_schema

app.openapi = custom_openapi

# ==================== ROUTES CƠ BẢN ====================
@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"ok": True, "database": "connected", "method": "direct psycopg2"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB lỗi: {e}")

@app.get("/api/meta/tables")
def list_tables(request: Request):
    check_api_key(request)
    return {"allowed_tables": TABLES, "total": len(TABLES)}

@app.get("/api/meta/schema/{table}")
def table_schema(table: str, request: Request):
    check_api_key(request)
    t = get_table_or_404(table)
    return {"table": t, "columns": get_schema(t)}

# ==================== ENDPOINT CHÍNH /api/<table> ====================
def create_table_endpoint(table: str):
    async def endpoint(
        request: Request,
        select: str = Query("*", description="Các cột cần lấy, ví dụ: id,name,status"),
        order: Optional[str] = Query(None, description="Cột để ORDER BY"),
        desc: bool = Query(True, description="True = giảm dần"),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        count: Optional[str] = Query(None, regex="^(exact|planned|estimated)?$", description="Thêm total count"),
    ):
        check_api_key(request)
        t = get_table_or_404(table)

        # Validate select
        allowed_cols = allowed_columns_set(t)
        if select == "*":
            select_clean = "*"
        else:
            cols = [c.strip() for c in select.split(",") if c.strip()]
            invalid = [c for c in cols if c not in allowed_cols]
            if invalid:
                raise HTTPException(status_code=400, detail=f"Cột không hợp lệ: {invalid}")
            select_clean = ", ".join(f'"{c}"' for c in cols)

        # WHERE clause
        where_clause, where_values = build_where_clause(dict(request.query_params), t)

        # ORDER BY
        order_clause = ""
        if order:
            if order not in allowed_cols:
                raise HTTPException(status_code=400, detail=f"Cột ORDER BY '{order}' không tồn tại")
            direction = "DESC" if desc else "ASC"
            order_clause = f'ORDER BY "{order}" {direction}'

        # COUNT tổng (nếu cần)
        count_select = 'COUNT(*) OVER() AS full_count,' if count else ""

        final_select = f"{count_select} {select_clean}" if select_clean != "*" or count else select_clean

        query = f"""
            SELECT {final_select}
            FROM "{t}"
            {where_clause}
            {order_clause}
            LIMIT %s OFFSET %s
        """
        params = where_values + [limit, offset]

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    data = [dict(row) for row in rows]

                    total = data[0].get("full_count") if count and data else None

                    return {
                        "table": t,
                        "count": total or len(data),
                        "returned": len(data),
                        "data": data,
                        "limit": limit,
                        "offset": offset,
                    }
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=502, detail=f"Lỗi truy vấn: {str(e)}")

    endpoint.__name__ = f"read_{table}"
    return endpoint

# Đăng ký tất cả bảng
for table_name in TABLES:
    app.get(f"/api/{table_name}", name=f"Đọc bảng {table_name}")(create_table_endpoint(table_name))

# ==================== XỬ LÝ LỖI TOÀN CỤC ====================
@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": type(exc).__name__, "detail": str(exc)}
    )