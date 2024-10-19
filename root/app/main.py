from fastapi import FastAPI, Depends
from fastapi.responses import RedirectResponse

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # create_tables()
    # new_user = asyncio.create_task(
    #     kafka_consumer(
    #         KAFKA_USER_REGISTER_TOPIC,
    #         BOOTSTRAP_SERVER,
    #         KAFKA_CONSUMER_GROUP_FOR_REGISTER_USER,
    #     )
    # )
    yield


app: FastAPI = FastAPI(
    lifespan=lifespan,
    title="Mart-API Service",
    version="1.0.0",
    # root_path="/user-service",
    # root_path_in_servers=True,
    servers=[
        {
            "url": "http://127.0.0.1:8110",
            "description": "Mart-API Service's Development Server",
        },
        {
            "url": "http://host.docker.internal:8110",
            "description": "Mart-API Service's Development Server",
        },
    ],
)


@app.get("/")
async def martapi_service():
    return {"message": "Welcome to Mart-API service."}


@app.get("/user-service")
async def user_service():
    return RedirectResponse(url="http://127.0.0.1:8000/user-service")


@app.get("/product-service")
async def product_service():
    return RedirectResponse(url="http://127.0.0.1:8000/product-service")


@app.get("/inventory-service")
async def inventory_service():
    return RedirectResponse(url="http://127.0.0.1:8000/inventory-service")


@app.get("/order-service")
async def order_service():
    return RedirectResponse(url="http://127.0.0.1:8000/order-service")


@app.get("/notification-service")
async def notification_service():
    return RedirectResponse(url="http://127.0.0.1:8000/notification-service")
