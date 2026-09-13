from sqlalchemy.orm import Session
from app.schemas import User as schemas
from app.core.security import verify_password, create_access_token
from app.core.security import get_password_hash, get_email_from_token
from jose import JWTError, ExpiredSignatureError
from app.crud import users as crud


class UserService:
    def __init__(self, db: Session):
        self.db = db

    def register(self, user: schemas.UserRegister):
        if crud.get_by_email(self.db, user.email):
            raise ValueError("Email already registered")

        if crud.get_by_username(self.db, user.username):
            raise ValueError("username already registered")

        hashed_password = get_password_hash(user.password)
        user.password = hashed_password
        return crud.create_user(self.db, user)

    # def change_password(
    #         self,
    #         email: str,
    #         old_password: str,
    #         new_password: str
    # ):
    #     user = crud.get_by_email(self.db, email)
    #     if not user or not verify_password(old_password, user.password):
    #         raise ValueError("Old password is wrong")
    #     hashed_new_password = get_password_hash(new_password)
    #     user.password = hashed_new_password
    #     return crud.update_password(self.db, user)

    def authenticate(self, email: str, password: str):
        user = crud.get_by_email(self.db, email)
        if not user or not verify_password(password, user.password):
            return None
        return create_access_token({"sub": email,
                                    "username": user.username,
                                    "role": user.role,
                                    })

    def authenticate_username(self, username: str, password: str):
        user = crud.get_by_username(self.db, username)
        if not user or not verify_password(password, user.password):
            return None
        return create_access_token({"sub": user.email,
                                    "username": username,
                                    "role": user.role,
                                    })

    def refresh_token(self, token: str):
        email = get_email_from_token(token)
        user = crud.get_by_email(self.db, email)
        if not user:
            return None

        return create_access_token({"sub": user.email,
                                    "username": user.username,
                                    "role": user.role,
                                    })

    def info(self, token: str):
        try:
            email = get_email_from_token(token)
            user = crud.get_by_email(self.db, email)
            return user
        except ExpiredSignatureError:
            raise ValueError("Token has expired")
        except JWTError:
            raise ValueError("Invalid token")
