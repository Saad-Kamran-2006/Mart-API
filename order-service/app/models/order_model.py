from sqlmodel import SQLModel, Field
from typing import Optional, Annotated, Union
from pydantic import BaseModel
from fastapi import Form


class Order(SQLModel, table=True):
    order_id: Optional[int] = Field(primary_key=True, default=None, index=True)
    product_id: str = Field(default=None, index=True)
    # title: str = Field(index=True, min_length=3, max_length=20)
    # price: float = Field(default=None)
    quantity: int = Field(default=None)


class Create_Order(BaseModel):
    product_id: Annotated[str, Form()]
    quantity: Annotated[int, Form()]


class Get_Order(BaseModel):
    order_id: int
    product_id: str
    # title: str
    # price: float
    quantity: int


class Edit_Order(BaseModel):
    quantity: Annotated[int, Form()]
