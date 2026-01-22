from fastapi.middleware.cors import CORSMiddleware
from app.database import Base, engine
from app.routers import dashboard_router
from fastapi import FastAPI
import asyncio

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Top Operators API", version="1.0.0")

app.include_router(dashboard_router.router)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://top-operators.netlify.app/",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
