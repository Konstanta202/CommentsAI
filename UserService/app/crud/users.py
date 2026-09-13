from sqlalchemy.orm import Session
from app.schemas import User as schemas
from app.db import models


def create_user(db: Session, user: schemas.UserRegister):
    # print(90*f"ROLE VALUE: {user.role}")  # ← что здесь выводится?
    # print(90*f"ROLE TYPE: {type(user.role)}")
    db_item = models.User(**user.model_dump())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item


def get_by_email(db: Session, email: str):
    return db.query(
        models.User
        ).filter(
        models.User.email == email
        ).first()


def get_by_username(db: Session, username: str):
    return db.query(
        models.User
        ).filter(
        models.User.username == username
        ).first()


def get_by_id(db: Session, id: int):
    return db.query(
        models.User
        ).filter(
        models.User.id == id
        ).first()


# def update_password(
#         db: Session,
#         user: schemas.PasswordUpdate,
# ):
#     db_item = get_by_email(db, user.email)
#     if db_item:
#         db_item.password = user.password
#         db.commit()
#         db.refresh(db_item)
#     return db_item
