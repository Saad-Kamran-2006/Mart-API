from fastapi import APIRouter, Depends
from sqlmodel import Session
from app.config.db import get_session
from typing import Annotated, Optional

product_router = APIRouter(
    prefix="/product", tags=["product"], responses={404: {"description": "Not Found"}}
)


@product_router.post("/create-product")
async def create_product(session: Annotated[Session, Depends(get_session)]):
    return "This is a product"
