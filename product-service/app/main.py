from fastapi import FastAPI, Depends
from sqlmodel import Session
from contextlib import asynccontextmanager
from app.config.db import create_tables, get_session
from app.router.product import product_router

# from app.kafka.producer_consumer import kafka_consumer

# from app.models.user_model import User, Register_User
# from app.utils.get_user import get_user_from_db
# from app.utils.security import hash_password
# from typing import Annotated


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    yield


app: FastAPI = FastAPI(
    lifespan=lifespan,
    title="Product Microservice",
    version="1.0.0",
    root_path="/product-service",
    root_path_in_servers=True,
    servers=[
        {
            "url": "http://127.0.0.1:8000",
            "description": "Product Service's Development Server",
        }
    ],
)

app.include_router(router=product_router)

@app.get("/")
async def product_service():
    return {"message": "Welcome to product service."}
