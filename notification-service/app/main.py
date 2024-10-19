from fastapi import FastAPI, Depends
from sqlmodel import Session
from contextlib import asynccontextmanager

# from app.config.db import create_tables, get_session
# from app.router.order import order_router
from app.config.setting import (
    BOOTSTRAP_SERVER,
    KAFKA_SEND_NOTIFICATION_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_SEND_NOTIFICATION,
)

from app.kafka.producer_consumer import kafka_consumer
import asyncio


@asynccontextmanager
async def lifespan(app: FastAPI):
    # create_tables()
    
    # ? Send Notification Consumer:
    create_order = asyncio.create_task(
        kafka_consumer(
            KAFKA_SEND_NOTIFICATION_TOPIC,
            BOOTSTRAP_SERVER,
            KAFKA_CONSUMER_GROUP_FOR_SEND_NOTIFICATION,
        )
    )
    yield


app: FastAPI = FastAPI(
    lifespan=lifespan,
    title="Notification Microservice",
    version="1.0.0",
    root_path="/notification-service",
    root_path_in_servers=True,
    servers=[
        {
            "url": "http://127.0.0.1:8104",
            "description": "Notification Service's Development Server",
        },
        {
            "url": "http://host.docker.internal:8104",
            "description": "Notification Service's Development Server",
        },
    ],
)

# app.include_router(router=order_router)


@app.get("/")
async def notification_service():
    return {"message": "Welcome to notification service."}
