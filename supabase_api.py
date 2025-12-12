import os, traceback, hmac
from typing import Any, Dict, Callable, List, Optional
from fastapi import FastAPI, Query, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from supabase import create_client, Client
from dotenv import load_dotenv
load_dotenv()
# --------- CẤU HÌNH LẤY TỪ ENV (Render / local .env) ----------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("INTERNAL_API_KEY")

if not SUPABASE_URL:
    raise RuntimeError("Missing env SUPABASE_URL")
if not SUPABASE_KEY:
    raise RuntimeError("Missing env SUPABASE_ANON_KEY")
if not API_KEY:
    raise RuntimeError("Missing env INTERNAL_API_KEY")

if SUPABASE_URL.startswith(("postgres://", "postgresql://")):
    print("⚠️ SUPABASE_URL đang là chuỗi Postgres. Phải dùng https://<project>.supabase.co")

# ========== APP ==========
app = FastAPI(
    title="Per-Table Supabase API (secured)",
    version="1.2.0",
    swagger_ui_parameters={"persistAuthorization": True},
)

# Nếu muốn giới hạn origin thì sửa chỗ này
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: thu hẹp origin khi lên prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== SUPABASE CLIENT ==========
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ====== BẢNG ĐƯỢC PHÉP EXPOSE ======
TABLES: List[str] = [
    "account",
    "account_route",
    "address",
    "customer",
    "customer_voucher",
    "destination",
    "domestic",
    "domestic_packing",
    "feedback",
    "order_links",
    "order_process_log",
    "orders",
    "packing",
    "packing_domestic",
    "partial_shipment",
    "payment",
    "payment_orders",
    "product_type",
    "purchases",
    "route",
    "shipment_tracking",
    "staff",
    "voucher",
    "voucher_route",
    "warehouse",
    "warehouse_location",
    "websites",

    # Các bảng bổ sung thường dùng (nếu có trong DB thì sẽ hoạt động)
    "order_payment_link",       # nếu bạn tạo bảng này
    "notification",             # nếu bạn tạo bảng này

    # Các bảng quan trọng khác (đã xác nhận tồn tại trong nhiều dự án Tiximax)
    "address",
    "customer_voucher",
    "partial_shipment",
    "voucher",
    "voucher_route",
]

# ====== BỘ NHỚ TÊN CỘT (cache tạm trong RAM) ======
SCHEMA_CACHE: Dict[str, List[Dict[str, Any]]] = {}
# Mặc định thì Supabase Python client chưa có list columns sẵn,
# mình sẽ lấy bằng cách select 1 dòng rồi suy column từ key.

# ========== OPENAPI: ép có X-API-Key ==========
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description="API tạo endpoint riêng cho từng bảng Supabase, kèm /api/meta/tables và /api/meta/schema/{table}",
        routes=app.routes,
    )
    schema.setdefault("components", {}).setdefault("securitySchemes", {})
    schema["components"]["securitySchemes"]["XApiKeyAuth"] = {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
        "description": "Nhập INTERNAL_API_KEY (vd: super-secret-xyz)",
    }
    schema["security"] = [{"XApiKeyAuth": []}]
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi

# ========== HELPER BẢO MẬT ==========
def check_api_key(request: Request):
    header = request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(header or "", API_KEY or ""):
        raise HTTPException(status_code=401, detail="Unauthorized")


def get_table_or_404(table: str) -> str:
    if table not in TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table}' is not exposed")
    return table


# ========== LẤY SCHEMA (từ 1 row) ==========
def fetch_schema_from_supabase(table: str) -> List[Dict[str, Any]]:
    try:
        res = supabase.table(table).select("*").limit(1).execute()
        rows = res.data or []
        if not rows:
            return []
        first = rows[0]
        schema = [{"name": k, "type": "unknown"} for k in first.keys()]
        return schema
    except Exception as e:
        print(f"[schema] lỗi lấy schema {table}: {e}")
        return []


def get_schema(table: str) -> List[Dict[str, Any]]:
    if table in SCHEMA_CACHE:
        return SCHEMA_CACHE[table]
    schema = fetch_schema_from_supabase(table)
    SCHEMA_CACHE[table] = schema
    return schema


def allowed_columns_set(table: str) -> set[str]:
    schema = get_schema(table)
    return {c["name"] for c in schema}


# ========== FILTER DYNAMIC ==========
def apply_filters(q, params: Dict[str, Any], table: str):
    """
    Filter động:
      ?status=DA_GIAO (mặc định eq)
      ?eq__status=DA_GIAO
      ?ilike__name=%an%
      ?gt__created_at=2025-01-01
      ?in__status=DA_GIAO,DANG_XU_LY
      ?is__field=null|true|false
    Có validate cột theo schema.
    """
    skip = {"select", "order", "desc", "limit", "offset", "count"}
    allowed = allowed_columns_set(table)

    for k, v in params.items():
        if k in skip:
            continue
        if "__" in k:
            op, col = k.split("__", 1)
        else:
            op, col = "eq", k

        # validate col
        if col not in allowed and allowed:
            raise HTTPException(status_code=400, detail=f"Column '{col}' is not allowed for table '{table}'")

        if op == "in":
            q = q.in_(col, [x for x in str(v).split(",") if x])
        elif op == "eq":
            q = q.eq(col, v)
        elif op == "ne":
            q = q.neq(col, v)
        elif op == "gt":
            q = q.gt(col, v)
        elif op == "gte":
            q = q.gte(col, v)
        elif op == "lt":
            q = q.lt(col, v)
        elif op == "lte":
            q = q.lte(col, v)
        elif op == "like":
            q = q.like(col, v)
        elif op == "ilike":
            q = q.ilike(col, v)
        elif op == "is":
            vv = str(v).lower()
            val = None if vv == "null" else True if vv == "true" else False if vv == "false" else v
            q = q.is_(col, val)
    return q


# ========== ROUTES CƠ BẢN ==========
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/meta/tables")
def meta_tables(request: Request):
    check_api_key(request)
    return {"allowed_tables": TABLES, "count": len(TABLES)}


@app.get("/api/meta/schema/{table}")
def meta_schema(table: str, request: Request):
    check_api_key(request)
    t = get_table_or_404(table)
    schema = get_schema(t)
    return {"table": t, "columns": schema, "count": len(schema)}


# ========== FACTORY TẠO ENDPOINT /api/<table> ==========
def make_table_endpoint(table: str) -> Callable:
    async def endpoint(
        request: Request,
        select: str = Query("*", description="VD: *, hoặc id,name"),
        order: Optional[str] = Query(None, description="VD: created_at"),
        desc: bool = Query(True),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
        count: Optional[str] = Query(None, description="exact|planned|estimated"),
    ):
        check_api_key(request)
        t = get_table_or_404(table)

        # validate select
        allowed = allowed_columns_set(t)
        if select != "*" and allowed:
            req_cols = [c.strip() for c in select.split(",") if c.strip()]
            for c in req_cols:
                if c not in allowed:
                    raise HTTPException(status_code=400, detail=f"Column '{c}' not allowed in select for '{t}'")
            select_clean = ",".join(req_cols)
        else:
            select_clean = select

        try:
            q = supabase.table(t).select(select_clean, count=count)
            # apply filters
            q = apply_filters(q, dict(request.query_params), t)

            # validate order
            if order:
                if allowed and order not in allowed:
                    raise HTTPException(status_code=400, detail=f"Order by unknown column '{order}'")
                q = q.order(order, desc=desc)

            # range
            q = q.range(offset, offset + max(0, limit - 1))
            res = q.execute()
            return {
                "table": t,
                "count": res.count,
                "data": res.data,
                "limit": limit,
                "offset": offset,
            }
        except HTTPException:
            raise
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"{t}: {type(e).__name__}: {e}",
            )

    endpoint.__name__ = f"read_{table}"
    return endpoint


# Đăng ký routes: /api/<table>
for t in TABLES:
    app.get(f"/api/{t}", name=f"Get {t}", description=f"Đọc bảng `{t}`")(make_table_endpoint(t))


# ========== GLOBAL ERROR HANDLER (gọn JSON) ==========
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": type(exc).__name__,
            "detail": str(exc),
        },
    )
