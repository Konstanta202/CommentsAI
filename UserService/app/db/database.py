# from dataclasses import Field
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
# from base import metadata_obj

import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str
    DB_NAME: str

    # JWT settings (добавьте эти поля)
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    @property
    def DATABASE_URL_asyncpg(self):
        base_url = f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@"
        host_port = f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        return base_url + host_port

    @property
    def DATABASE_URL_psycopg(self):
        # DSN
        base_url = f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASS}@"
        host_port = f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        return base_url + host_port

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()

sync_engine = create_engine(
    url=settings.DATABASE_URL_psycopg,
    echo=True
)


async_engine = create_async_engine(
    url=settings.DATABASE_URL_asyncpg,
    echo=True
)
# with sync_engine.connect() as conn:
#     print("Successfully connected to database")
session_factory = sessionmaker(sync_engine)
