from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.database.database import engine, DATABASE_URL
from app.api.endpoints import router
from app.models import Group, Post, Comment # noqa


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with engine.connect():
            print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
    yield
    engine.dispose()
    print("🔴 Database connections closed")

app = FastAPI(title="VK Parser API Service", lifespan=lifespan)

app.include_router(router)


@app.get("/")
async def root():
    return {"message": "VK Parser API"}


@app.get("/debug/db")
async def debug_db():
    db_url = DATABASE_URL
    return {"DATABASE_URL": db_url}
