from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.clickhouse import check_connection
from app.api.endpoints.dashboards import router as dashboards_router
from app.api.endpoints.filters import router as filters_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting up...")
    # Проверка ClickHouse
    try:
        if await check_connection():
            print("✅ ClickHouse connection successful")
        else:
            print("❌ ClickHouse connection failed")
    except Exception as e:
        print(f"❌ ClickHouse connection error: {e}")
    yield
    print("🛑 Shutting down...")

app = FastAPI(title="Analitic Service", lifespan=lifespan)

app.include_router(dashboards_router)
app.include_router(filters_router)


@app.get("/")
async def root():
    return {"message": "Analitic Service API"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/debug/db")
async def debug_db():
    try:
        is_ok = await check_connection()
        return {
            "status": "ok" if is_ok else "error",
            "clickhouse": "connected"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
