from pydantic import BaseModel, EmailStr, field_validator
from app.db.models import UserRoleChoice


class UserRegister(BaseModel):
    email: EmailStr
    username: str
    password: str
    active: bool
    # role: UserRoleChoice = UserRoleChoice.USER
    city: str | None = None  # Необязательное поле

    @field_validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit")
        if v.islower():
            raise ValueError("Password must contain one uppercase sym")
        if not set('!@#$%&*').intersection(v):
            raise ValueError(
                "Password must contain at least one symbol like(! @ # $ % & *)"
            )
        return v

    # @field_validator('role')
    # def validate_role(cls, v):
    #     if isinstance(v, UserRoleChoice):
    #         return v.value
    #     return v

    class Config:
        use_enum_values = True 


class UserLogin(BaseModel):
    email: EmailStr | None = None
    username: str | None = None
    password: str


class UserResponse(BaseModel):
    email: EmailStr
    username: str
    active: bool
    role: UserRoleChoice = UserRoleChoice.USER  # Значение по умолчанию
    city: str | None = None  # Необязательное поле


class PasswordUpdate(BaseModel):
    email: EmailStr
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
