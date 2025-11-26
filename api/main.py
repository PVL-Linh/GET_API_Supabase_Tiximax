from fastapi import FastAPI
from api.supabase import handler  # Import từ api/supabase.py

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API Supabase chạy trên Vercel thành công!"}

@app.get("/test")
def test():
    return {"status": "ok"}

# Convert handler (Vercel style) → FastAPI route
@app.get("/api/{table}")
def supabase_proxy(table: str, request: dict = None):
    return handler(request)
