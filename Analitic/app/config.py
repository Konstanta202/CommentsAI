from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    CLICKHOUSE_HOST: str = "clickhouse"  # имя сервиса в docker-compose
    CLICKHOUSE_PORT: int = 9000
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"

    class Config:
        env_file = ".env"


settings = Settings()
