from fastapi import APIRouter, Depends
from sqlmodel import Session
from app.config.db import get_session
from typing import Annotated, Optional
from app.models.product_model import Create_Product
# from app.utils.verify_token import current_user
import requests

product_router = APIRouter(
    prefix="/product", tags=["product"], responses={404: {"description": "Not Found"}}
)


@product_router.post("/create-product")
async def create_product(
    product_data: Annotated[Create_Product, Depends()],
    # current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
):
    return product_data
