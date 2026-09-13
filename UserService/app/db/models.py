# import asyncio
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Enum
# from app.db.database import sync_engine
import enum

Base = declarative_base()


class UserRoleChoice(str, enum.Enum):
    USER = "USER"
    ADMIN = "ADMIN"
    MODERATOR = "MODERATOR"

    def __str__(self):
        return self.value


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True,
                index=True,
                autoincrement=True)
    username = Column(String)
    email = Column(String)
    password = Column(String)
    active = Column(Boolean)
    role = Column(Enum(UserRoleChoice),
                  nullable=False,
                  default=UserRoleChoice.USER)
    city = Column(String)


# Base.metadata.create_all(bind=sync_engine)

# async def create_tables():
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)

# asyncio.run(create_tables())
