from fastapi import FastAPI
from app.user_service.user import router as user_router
from app.FastAPIparser.fast_api_service import router as comment_router


app = FastAPI()

app.include_router(user_router)
app.include_router(comment_router)


@app.get("/")
def reed_root():
    return {"message": "Gateway service"}
