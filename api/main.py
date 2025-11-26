from fastapi import FastAPI, Request
from api.supabase import handler

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API Supabase chạy trên Vercel thành công!"}

@app.api_route("/api/{table}", methods=["GET", "OPTIONS"])
async def proxy(table: str, request: Request):
    """
    Chuyển Request của FastAPI → dạng dict để supabase_handler dùng được.
    """
    req = {
        "method": request.method,
        "headers": dict(request.headers),
        "path": f"/api/{table}",
        "query_string": request.url.query
    }
    return handler(req)
