from fastapi import FastAPI
from api.supabase import handler

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API Supabase chạy trên Vercel thành công!"}

@app.get("/api/{table}")
def supabase_proxy(table: str, request: dict = None):
    return handler(request)
