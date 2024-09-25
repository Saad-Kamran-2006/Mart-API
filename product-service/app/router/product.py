from fastapi import APIRouter, Depends, Form, HTTPException
from sqlmodel import Session, select
from app.config.db import get_session
from typing import Annotated, Optional, Any
from app.models.product_model import Create_Product, Product, Get_Product
from app.models.user_model import User
from app.models.roles import UserRole
from app.utils.verify_token import current_user
from fastapi.security import OAuth2PasswordBearer
from app.config.setting import KAFKA_CREATE_PRODUCT_TOPIC
from app.kafka.producer_consumer import kafka_producer
from app.protobuf import product_pb2
from aiokafka import AIOKafkaProducer
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
        
        newProduct: Product = Product(title=new_product.title, quantity=new_product.quantity, price=new_product.price)

        product: Create_Product = product_pb2.Products(
            title=new_product.title, quantity=new_product.quantity, price=new_product.price
        )
        print("\nProduct's Data: ", product)

        product_data = product.SerializeToString()
        print("Product's Serialized Data: ", product_data)

        # ? Produce the message with headers
        await producer.send_and_wait(KAFKA_CREATE_PRODUCT_TOPIC, product_data)
        # print(product_data)
        # product: Product = Product(title=product_data.title, quantity=product_data.quantity, price=product_data.price)
        # session.add(product)
        # session.commit()
        # session.refresh(product)

        return newProduct

@product_router.get("/products", response_model=list[Get_Product])
async def get_all_products(session: Annotated[Session, Depends(get_session)]):
    products: Get_Product = session.exec(select(Product)).all()
    if products:
        return products
    else:
        raise HTTPException(status_code=404, detail="No product found")


@product_router.get("/{id}", response_model=Get_Product)
async def get_single_product(id: int, session: Annotated[Session, Depends(get_session)]):
    product: Get_Product = session.exec(select(Product).where(Product.id == id)).first()
    if product:
        return product
    else:
        raise HTTPException(status_code=404, detail="No product found")

@product_router.put("/{id}", response_model=Get_Product)
async def edit_product(
    id: int,
    new_product: Annotated[Create_Product, Depends()],
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid token, please login again")
    if current_user["role"] != UserRole.super_user.value:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["role"] == UserRole.super_user.value:
        existing_product = session.exec(select(Product).where(Product.id == id)).first()

        if existing_product:
            existing_product.title = new_product.title
            existing_product.quantity = new_product.quantity
            existing_product.price = new_product.price
            if new_product.quantity >= 1:
                existing_product.is_available = True
            elif new_product.quantity < 1:
                existing_product.is_available = False
            session.add(existing_product)
            session.commit()
            session.refresh(existing_product)
            return existing_product
        else:
            raise HTTPException(status_code=404, detail="No product found")


@product_router.delete("/{id}")
async def delete_product(
    id: int,
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid token, please login again")
    if current_user["role"] != UserRole.super_user.value:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if current_user["role"] == UserRole.super_user.value:
        product = session.exec(select(Product).where(Product.id == id)).first()
        if product:
            session.delete(product)
            session.commit()
            return {"message": "Product successfully deleted"}
        else:
            raise HTTPException(status_code=404, detail="No product found")