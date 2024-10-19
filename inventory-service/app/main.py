from fastapi import FastAPI, Depends
from sqlmodel import Session
from contextlib import asynccontextmanager
from app.config.db import create_tables, get_session
from app.router.inventory import inventory_router

# from app.router.auth import auth_router
from app.config.setting import (
    BOOTSTRAP_SERVER,
    KAFKA_DELETE_INVENTORY_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_DELETE_INVENTORY,
    KAFKA_CREATE_INVENTORY_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_CREATE_INVENTORY,
    KAFKA_GET_PRODUCT_INVENTORY_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_GET_PRODUCT_INVENTORY,
    KAFKA_UPDATE_ORDER_INVENTORY_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_UPDATE_ORDER_INVENTORY,
    KAFKA_DELETE_ORDER_INVENTORY_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_DELETE_ORDER_INVENTORY,
)


from app.kafka.producer_consumer import kafka_consumer
from app.utils.order_consumer import order_consumer
from app.utils.update_order_consumer import update_order_consumer
from app.utils.delete_order_consumer import delete_order_consumer
import asyncio


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    # ? Create Inventory Consumer:
    new_inventory = asyncio.create_task(
        kafka_consumer(
            KAFKA_CREATE_INVENTORY_TOPIC,
            BOOTSTRAP_SERVER,
            KAFKA_CONSUMER_GROUP_FOR_CREATE_INVENTORY,
        )
    )

    # ? Delete Inventory Consumer:
    delete_inventory = asyncio.create_task(
        kafka_consumer(
            KAFKA_DELETE_INVENTORY_TOPIC,
            BOOTSTRAP_SERVER,
            KAFKA_CONSUMER_GROUP_FOR_DELETE_INVENTORY,
        )
    )

    # ? Get Product Inventory Consumer:
    product_inventory = asyncio.create_task(
        order_consumer(
            KAFKA_GET_PRODUCT_INVENTORY_TOPIC,
            BOOTSTRAP_SERVER,
            KAFKA_CONSUMER_GROUP_FOR_GET_PRODUCT_INVENTORY,
        )
    )

    # ? Update Order Inventory Consumer:
    update_order_inventory = asyncio.create_task(
        update_order_consumer(
            KAFKA_UPDATE_ORDER_INVENTORY_TOPIC,
            BOOTSTRAP_SERVER,
            KAFKA_CONSUMER_GROUP_FOR_UPDATE_ORDER_INVENTORY,
        )
    )

    # ? Delete Order Inventory Consumer:
    delete_order_inventory = asyncio.create_task(
        delete_order_consumer(
            KAFKA_DELETE_ORDER_INVENTORY_TOPIC,
            BOOTSTRAP_SERVER,
            KAFKA_CONSUMER_GROUP_FOR_DELETE_ORDER_INVENTORY,
        )
    )

    yield


app: FastAPI = FastAPI(
    lifespan=lifespan,
    title="Inventory Microservice",
    version="1.0.0",
    root_path="/inventory-service",
    root_path_in_servers=True,
    servers=[
        {
            "url": "http://127.0.0.1:8102",
            "description": "Inventory Service's Development Server",
        },
        {
            "url": "http://host.docker.internal:8102",
            "description": "Inventory Service's Development Server",
        },
    ],
)

app.include_router(router=inventory_router)


@app.get("/")
async def inventory_service():
    return {"message": "Welcome to inventory service."}
