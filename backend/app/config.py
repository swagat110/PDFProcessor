from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://redis:6379/0"
    upload_dir: str = "/data/uploads"

    class Config:
        env_file = ".env"


settings = Settings()
