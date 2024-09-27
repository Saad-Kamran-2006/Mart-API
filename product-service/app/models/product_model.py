from sqlmodel import SQLModel, Field
from typing import Optional, Annotated, Union
from pydantic import BaseModel
from fastapi import Form



class Product(SQLModel, table=True):
    product_id: str = Field(primary_key=True, default=None, index=True)
    title: str = Field(index=True, min_length=3, max_length=20)
    # description: str = Field(default=None)
    # quantity: int = Field(default=None)
    price: float = Field(default=None)
    # is_available: bool = Field(default=None)


class Create_Product(BaseModel):
    title: Annotated[str, Form()]
    quantity: Annotated[int, Form()]
    price: Annotated[float, Form()]

class Edit_Product(BaseModel):
    title: Annotated[str, Form()]
    price: Annotated[float, Form()]

class Get_Product(BaseModel):
    product_id: str
    title: str
    # quantity: int
    price: float
    # is_available: bool




