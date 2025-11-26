# Tiximax Supabase Per-Table API

API FastAPI đọc từng bảng Supabase qua Supabase Python client, siết bảo mật bằng `X-API-Key`, dùng để phục vụ chatbot / internal tools.

## Cấu trúc

- `supabase_api.py` — FastAPI app, expose:
  - `GET /health`
  - `GET /api/meta/tables`
  - `GET /api/meta/schema/{table}`
  - `GET /api/<table>` (orders, payment, staff, v.v.)
- `requirements.txt` — thư viện Python
- `render.yaml` — cấu hình deploy Render
- `runtime.txt` — version Python

## Chạy local

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

export SUPABASE_URL="https://<project>.supabase.co"
export SUPABASE_ANON_KEY="<anon-key>"
export INTERNAL_API_KEY="super-secret-xyz"

uvicorn supabase_api:app --host 0.0.0.0 --port 8000
```
