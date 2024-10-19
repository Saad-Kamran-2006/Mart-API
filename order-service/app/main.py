from fastapi import FastAPI, Depends
from sqlmodel import Session
from contextlib import asynccontextmanager
from app.config.db import create_tables, get_session
from app.router.order import order_router
from app.config.setting import (
    BOOTSTRAP_SERVER,
    KAFKA_CREATE_ORDER_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_CREATE_ORDER,
)
from app.kafka.producer_consumer import kafka_consumer
import asyncio


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    # # ? Create Inventory Consumer:
    # create_order = asyncio.create_task(
    #     kafka_consumer(
    #         KAFKA_CREATE_ORDER_TOPIC,
    #         BOOTSTRAP_SERVER,
    #         KAFKA_CONSUMER_GROUP_FOR_CREATE_ORDER,
    #     )
    # )

    # # ? Delete Inventory Consumer:
    # delete_inventory = asyncio.create_task(
    #     kafka_consumer(
    #         KAFKA_DELETE_INVENTORY_TOPIC,
    #         BOOTSTRAP_SERVER,
    #         KAFKA_CONSUMER_GROUP_FOR_DELETE_INVENTORY,
    #     )
    # )
    yield


app: FastAPI = FastAPI(
    lifespan=lifespan,
    title="Order Microservice",
    version="1.0.0",
    root_path="/order-service",
    root_path_in_servers=True,
    servers=[
        {
            "url": "http://127.0.0.1:8103",
            "description": "Order Service's Development Server",
        },
        {
            "url": "http://host.docker.internal:8103",
            "description": "Order Service's Development Server",
        },
    ],
)

app.include_router(router=order_router)


@app.get("/")
async def order_service():
    return {"message": "Welcome to order service."}
