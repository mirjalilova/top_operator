import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str 
    POSTGRES_PASSWORD: str
    SECRET_KEY: str  
    ALGORITHM: str
    REDIS_HOST: str

    class Config:
        env_file = ".env"

settings = Settings()
