from fastapi import FastAPI
from supabase_api import get_user_data   # nếu anh dùng supabase_api.py
# hoặc anh import từ api/supabase.py nếu cần

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API Supabase chạy trên Vercel thành công!"}

@app.get("/test")
def test():
    return {"status": "ok"}
