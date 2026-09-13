from pydantic import BaseModel, EmailStr


class UserRegister(BaseModel):
    email: EmailStr
    username: str
    password: str
    city: str | None = None  # Необязательное поле


class UserLogin(BaseModel):
    email: EmailStr | None = None
    username: str | None = None
    password: str
