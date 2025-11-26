# api/main.py  ← Chỉ giữ lại đúng 1 file này thôi
import os
import json
from urllib.parse import parse_qs
from fastapi import FastAPI, Request, HTTPException

from supabase import create_client

app = FastAPI()

# === ENV ===
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
API_KEY = os.environ.get("INTERNAL_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, API_KEY]):
    raise Exception("Missing required environment variables")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLES = [
    "account", "account_route", "customer", "destination", "domestic",
    "domestic_packing", "feedback", "order_links", "order_process_log",
    "orders", "packing", "payment", "payment_orders", "product_type",
    "purchases", "route", "shipment_tracking", "staff", "warehouse",
    "warehouse_location", "websites"
]

SCHEMA_CACHE = {}

def get_schema(table):
    if table in SCHEMA_CACHE:
        return SCHEMA_CACHE[table]
    try:
        res = supabase.table(table).select("*").limit(1).execute()
        row = res.data[0] if res.data else {}
        schema = list(row.keys())
        SCHEMA_CACHE[table] = schema
        return schema
    except:
        return []

def apply_filters(query, params, table):
    schema = set(get_schema(table))
    for key, value in params.items():
        if key in ("select", "order", "desc", "limit", "offset", "count"):
            continue
        if "__" in key:
            op, col = key.split("__", 1)
        else:
            op, col = "eq", key
        if col not in schema:
            continue
        v = value[0]
        if op == "eq": query = query.eq(col, v)
        elif op == "ne": query = query.neq(col, v)
        elif op == "gt": query = query.gt(col, v)
        elif op == "gte": query = query.gte(col, v)
        elif op == "lt": query = query.lt(col, v)
        elif op == "lte": query = query.lte(col, v)
        elif op == "like": query = query.like(col, v)
        elif op == "ilike": query = query.ilike(col, v)
        elif op == "in": query = query.in_(col, v.split(","))
    return query

@app.get("/")
def root():
    return {"message": "API Supabase chạy trên Vercel thành công!"}

@app.options("/api/{table}")
async def options():
    return {
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
        }
    }

@app.get("/api/{table}")
async def proxy(table: str, request: Request):
    # CORS
    response_headers = {"Access-Control-Allow-Origin": "*"}

    # API Key check
    if request.headers.get("X-API-Key") != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if table not in TABLES:
        raise HTTPException(status_code=404, detail="Invalid table")

    qs = parse_qs(request.url.query)
    select = qs.get("select", ["*"])[0]
    order = qs.get("order", [None])[0]
    desc = qs.get("desc", ["true"])[0].lower() == "true"
    limit = int(qs.get("limit", ["100"])[0])
    offset = int(qs.get("offset", ["0"])[0])
    count = qs.get("count", [None])[0]

    q = supabase.table(table).select(select, count=count)
    q = apply_filters(q, qs, table)
    if order:
        q = q.order(order, desc=desc)
    q = q.range(offset, offset + limit - 1)

    try:
        res = q.execute()
        return {
            "table": table,
            "data": res.data,
            "count": res.count,
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))