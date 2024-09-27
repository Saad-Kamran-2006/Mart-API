from fastapi import APIRouter, Depends, Form, HTTPException
from sqlmodel import Session, select
from app.config.db import get_session
from typing import Annotated, Optional, Any
from app.models.product_model import Create_Product, Product, Get_Product
from app.models.user_model import User
from app.models.roles import UserRole
from app.utils.verify_token import current_user
from fastapi.security import OAuth2PasswordBearer
from app.config.setting import KAFKA_CREATE_PRODUCT_TOPIC, KAFKA_CREATE_INVENTORY_TOPIC,KAFKA_DELETE_INVENTORY_TOPIC
from app.kafka.producer_consumer import kafka_producer
from app.protobuf import product_pb2, inventory_pb2
from aiokafka import AIOKafkaProducer
from app.utils.generate_product_id import generate_product_id
import requests
import json

product_router = APIRouter(
    prefix="/product", tags=["product"], responses={404: {"description": "Not Found"}}
)


@product_router.post("/create-product", response_model=Product)
async def create_product(
    current_user: Annotated[User, Depends(current_user)],
    new_product: Annotated[Create_Product, Depends()],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid token, please login again")
    if current_user["role"] != UserRole.super_user.value:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["role"] == UserRole.super_user.value:

        generated_id = generate_product_id()
        
        newProduct: Product = Product(product_id=generated_id, title=new_product.title, price=new_product.price)

        # ? Product Protobuf Data:
        product = product_pb2.Products(
           product_id=generated_id, title=new_product.title, price=new_product.price
        )
        print("\nProduct Data:\n", product)

        # ? Product Serialized Data:
        product_data = product.SerializeToString()
        print("Product Serialized Data:\n", product_data)

        # ? Send To Produce Kafka Topic:
        await producer.send_and_wait(KAFKA_CREATE_PRODUCT_TOPIC, product_data)

# ? <-------------------------------------- Product Topic Ended -------------------------------------->

        # ? Inventory Protobuf Data:
        inventory = inventory_pb2.Inventories(
           product_id=generated_id, quantity=new_product.quantity, is_available=new_product.quantity >= 1 if True else False
        )
        print("\nInventory Data:\n", inventory)

        # ? Inventory Serialized Data:
        inventory_data = inventory.SerializeToString()
        print("Inventory Serialized Data:\n", inventory_data)

        # ? Send To Inventory Kafka Topic:
        await producer.send_and_wait(KAFKA_CREATE_INVENTORY_TOPIC, inventory_data)

# ? <-------------------------------------- Inventory Topic Ended -------------------------------------->
        # ? Return the created product:
        return newProduct


@product_router.get("/products", response_model=list[Get_Product])
async def get_all_products(session: Annotated[Session, Depends(get_session)]):
    products: Get_Product = session.exec(select(Product)).all()
    if products:
        return products
    else:
        raise HTTPException(status_code=404, detail="No product found")


@product_router.get("/{product_id}", response_model=Get_Product)
async def get_single_product(product_id: str, session: Annotated[Session, Depends(get_session)]):
    product: Get_Product = session.exec(select(Product).where(Product.product_id == product_id)).first()
    if product:
        return product
    else:
        raise HTTPException(status_code=404, detail="No product found")

@product_router.put("/{product_id}", response_model=Get_Product)
async def edit_product(
    product_id: str,
    new_product: Annotated[Create_Product, Depends()],
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid token, please login again")
    if current_user["role"] != UserRole.super_user.value:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["role"] == UserRole.super_user.value:
        existing_product = session.exec(select(Product).where(Product.product_id == product_id)).first()

        if existing_product:
            existing_product.title = new_product.title
            existing_product.price = new_product.price
            
            session.add(existing_product)
            session.commit()
            session.refresh(existing_product)
            return existing_product
        else:
            raise HTTPException(status_code=404, detail="No product found")


@product_router.delete("/{product_id}")
async def delete_product(
    product_id: str,
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid token, please login again")
    if current_user["role"] != UserRole.super_user.value:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["role"] == UserRole.super_user.value:
        product = session.exec(select(Product).where(Product.product_id == product_id)).first()

        if product.product_id:
            # ? Inventory Protobuf Data:
            inventory = product_pb2.Products(product_id=product.product_id)
            print("\nInventory Data:\n", inventory)

            # ? Inventory Serialized Data:
            inventory_data = inventory.SerializeToString()
            print("Inventory Serialized Data:\n", inventory_data)

            # ? Send To Inventory Kafka Topic:
            await producer.send_and_wait(KAFKA_DELETE_INVENTORY_TOPIC, inventory_data)

# ? <-------------------------------------- Inventory Topic Ended -------------------------------------->

        if product:
            session.delete(product)
            session.commit()
            return {"message": "Product successfully deleted"}
        else:
            raise HTTPException(status_code=404, detail="No product found")