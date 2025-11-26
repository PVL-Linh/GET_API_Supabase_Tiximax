from supabase import create_client
import os
import json
from urllib.parse import parse_qs

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
API_KEY = os.environ.get("INTERNAL_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLES = [
    "account", "account_route", "customer", "destination",
    "domestic", "domestic_packing", "feedback", "order_links",
    "order_process_log", "orders", "packing", "payment",
    "payment_orders", "product_type", "purchases", "route",
    "shipment_tracking", "staff", "warehouse",
    "warehouse_location", "websites",
]

SCHEMA_CACHE = {}

def get_schema(table):
    if table in SCHEMA_CACHE:
        return SCHEMA_CACHE[table]
    res = supabase.table(table).select("*").limit(1).execute()
    row = res.data[0] if res.data else {}
    schema = list(row.keys())
    SCHEMA_CACHE[table] = schema
    return schema

def apply_filters(q, params, table):
    schema = set(get_schema(table))

    for key, value in params.items():
        if key in ("select","order","desc","limit","offset","count"):
            continue

        if "__" in key:
            op, col = key.split("__",1)
        else:
            op, col = "eq", key

        if col not in schema:
            continue

        v = value[0]

        if op == "eq": q = q.eq(col, v)
        elif op == "ne": q = q.neq(col, v)
        elif op == "gt": q = q.gt(col, v)
        elif op == "gte": q = q.gte(col, v)
        elif op == "lt": q = q.lt(col, v)
        elif op == "lte": q = q.lte(col, v)
        elif op == "like": q = q.like(col, v)
        elif op == "ilike": q = q.ilike(col, v)
        elif op == "in": q = q.in_(col, v.split(","))

    return q


def handler(request):
    # CORS
    if request.method == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "GET",
            },
            "body": ""
        }

    # Check API Key
    headers = request.headers
    if headers.get("X-API-Key") != API_KEY:
        return {"statusCode": 401, "body": json.dumps({"error": "Unauthorized"})}

    # Parse query
    path = request.path
    qs = parse_qs(request.query_string or "")

    if not path.startswith("/api/"):
        return {"statusCode": 404, "body": "Not Found"}

    table = path.replace("/api/", "")

    if table not in TABLES:
        return {"statusCode": 404, "body": json.dumps({"error": "Invalid table"})}

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
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "table": table,
                "data": res.data,
                "count": res.count,
                "limit": limit,
                "offset": offset,
            }, ensure_ascii=False),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }

