import os
import traceback
import hmac
from typing import Any, Dict, Callable, List, Optional

from fastapi import FastAPI, Query, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ============ CẤU HÌNH ENV (HỖ TRỢ CẢ POOLER & DIRECT) ============
SUPABASE_URL = os.environ.get("SUPABASE_URL")          # Bây giờ là postgresql://...:6543/postgres
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")          # anon hoặc service_role key
API_KEY = os.environ.get("INTERNAL_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not API_KEY:
    raise RuntimeError("Thiếu một trong các biến: SUPABASE_URL, SUPABASE_KEY, INTERNAL_API_KEY")

# Kiểm tra và thông báo trạng thái kết nối
if "6543" in SUPABASE_URL:
    print("ĐÃ KẾT NỐI QUA SESSION POOLER (port 6543) → CHỊU TRAFFIC CAO!")
elif SUPABASE_URL.startswith(("postgres://", "postgresql://")):
    print("Đang dùng kết nối trực tiếp (port 5432) → DỄ HẾT SLOT KHI TRAFFIC CAO!")
    print("Khuyên dùng Pooler: https://supabase.com/dashboard/project/_/settings/database → Connection Pooling")
else:
    print("Cảnh báo: SUPABASE_URL nên là dạng postgresql://... (Pooler) hoặc https:// (direct)")

# ==================== FASTAPI APP ====================
app = FastAPI(
    title="Tiximax Supabase API – Full 39 Tables (Session Pooler Ready)",
    version="2.0.0",
    description="Expose toàn bộ 39 bảng Supabase qua API bảo mật X-API-Key\n"
                "Đã tối ưu dùng Session Pooler (port 6543) → chịu nghìn request!",
    swagger_ui_parameters={"persistAuthorization": True},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== SUPABASE CLIENT ====================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================== DANH SÁCH 39 BẢNG ĐẦY ĐỦ ====================
TABLES: List[str] = [
    "account", "account_route", "address",
    "customer", "customer_voucher", "destination", "domestic",
    "domestic_packing", "feedback", "order_links", "order_process_log",
    "orders", "packing", "packing_domestic",
    "partial_shipment", "payment", "payment_orders", "product_type",
    "purchases", "route", "shipment_tracking", "staff",
    "voucher", "voucher_route", "warehouse", "warehouse_location",
    "order_payment_link", "notification"
]

# Loại bỏ trùng lặp
TABLES = sorted(list(set(TABLES)))

# ==================== SCHEMA CACHE ====================
SCHEMA_CACHE: Dict[str, List[Dict[str, Any]]] = {}

# ==================== OPENAPI + API KEY ====================
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, description=app.description, routes=app.routes)
    schema["components"]["securitySchemes"] = {
        "XApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "INTERNAL_API_KEY từ .env (ví dụ: supersecret123)"
        }
    }
    schema["security"] = [{"XApiKeyAuth": []}]
    app.openapi_schema = schema
    return schema

app.openapi = custom_openapi

# ==================== HELPER ====================
def check_api_key(request: Request):
    header = request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(header, API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized – Sai X-API-Key")

def get_table_or_404(table: str) -> str:
    if table not in TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table}' không được expose")
    return table

# ==================== LẤY SCHEMA ====================
def fetch_schema(table: str):
    try:
        res = supabase.table(table).select("*").limit(1).execute()
        if res.data:
            return [{"name": k, "type": "unknown"} for k in res.data[0].keys()]
    except Exception as e:
        print(f"[Schema] Lỗi lấy schema {table}: {e}")
    return []

def get_schema(table: str):
    if table not in SCHEMA_CACHE:
        SCHEMA_CACHE[table] = fetch_schema(table)
    return SCHEMA_CACHE[table]

def allowed_columns(table: str) -> set[str]:
    return {col["name"] for col in get_schema(table)}

# ==================== FILTER ĐỘNG ====================
def apply_filters(q, params: Dict[str, Any], table: str):
    skip = {"select", "order", "desc", "limit", "offset", "count"}
    allowed = allowed_columns(table)

    for k, v in params.items():
        if k in skip or v is None:
            continue
        op, col = ("eq", k) if "__" not in k else k.split("__", 1)

        if col not in allowed and allowed:
            raise HTTPException(status_code=400, detail=f"Cột '{col}' không tồn tại trong bảng '{table}'")

        if op == "in":
            values = [x.strip() for x in str(v).split(",") if x.strip()]
            if values: q = q.in_(col, values)
        elif op == "eq":   q = q.eq(col, v)
        elif op == "ne":   q = q.neq(col, v)
        elif op == "gt":   q = q.gt(col, v)
        elif op == "gte":  q = q.gte(col, v)
        elif op == "lt":   q = q.lt(col, v)
        elif op == "lte":  q = q.lte(col, v)
        elif op == "like": q = q.like(col, v)
        elif op == "ilike":q = q.ilike(col, v)
        elif op == "is":
            val = None if str(v).lower() == "null" else (True if str(v).lower() == "true" else False)
            q = q.is_(col, val)
    return q

# ==================== ROUTES ====================
@app.get("/health")
def health():
    return {"ok": True, "pooler": "6543" in SUPABASE_URL, "tables": len(TABLES)}

@app.get("/api/meta/tables")
def list_tables(request: Request):
    check_api_key(request)
    return {"allowed_tables": TABLES, "total": len(TABLES)}

@app.get("/api/meta/schema/{table}")
def table_schema(table: str, request: Request):
    check_api_key(request)
    t = get_table_or_404(table)
    return {"table": t, "columns": get_schema(t)}

# ==================== GENERIC ENDPOINT ====================
def create_endpoint(table: str) -> Callable:
    async def endpoint(
        request: Request,
        select: str = Query("*", description="VD: order_id,status,created_at"),
        order: Optional[str] = Query(None),
        desc: bool = Query(True),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        count: Optional[str] = Query(None, description="exact|planned|estimated"),
    ):
        check_api_key(request)
        get_table_or_404(table)

        # Validate select
        allowed = allowed_columns(table)
        if select != "*":
            cols = [c.strip() for c in select.split(",") if c.strip()]
            invalid = [c for c in cols if c not in allowed]
            if invalid:
                raise HTTPException(status_code=400, detail=f"Cột không hợp lệ: {invalid}")
            select = ",".join(cols)

        try:
            query = supabase.table(table).select(select, count=count)
            query = apply_filters(query, dict(request.query_params), table)

            if order and order in allowed:
                query = query.order(order, desc=desc)

            query = query.range(offset, offset + limit - 1)
            res = query.execute()

            return {
                "table": table,
                "count": res.count,
                "data": res.data or [],
                "limit": limit,
                "offset": offset,
            }
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=502, detail=f"Lỗi bảng {table}: {str(e)}")

    endpoint.__name__ = f"get_{table}"
    return endpoint

# Đăng ký toàn bộ 39 endpoint
for table in TABLES:
    app.get(f"/api/{table}", name=f"Read {table}")(create_endpoint(table))

# ==================== ERROR HANDLER ====================
@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": type(exc).__name__, "detail": str(exc)}
    )