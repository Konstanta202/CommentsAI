import httpx
import os
from dotenv import load_dotenv
from fastapi import APIRouter
from app.user_service.schemas import User as schemas


router = APIRouter(prefix="/users", tags=["Users"])


load_dotenv()
USER_SERVICE = os.getenv("USER_SERVICE")


@router.post("/register")
async def register(
    user: schemas.UserRegister
):
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.post(
            f"{USER_SERVICE}/users/register/",
            json={
                "email": user.email,
                "username": user.username,
                "password": user.password,
                "active": True
            }
        )
    return response.json()


@router.post("/login")
async def login(
    user: schemas.UserLogin,
):
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.post(
            f"{USER_SERVICE}/users/login/",
            json={
                "email": user.email,
                "username": user.username,
                "password": user.password
            }
        )
    return response.json()


@router.get("/me")
async def get_info(
    token: str
):
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{USER_SERVICE}/users/me?token={token}")
    return response.json()
