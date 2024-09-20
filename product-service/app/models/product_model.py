from sqlmodel import SQLModel, Field
from typing import Optional, Annotated
from pydantic import BaseModel
from fastapi import Form


class Product(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None, index=True)
    title: str = Field(index=True, min_length=3, max_length=20)
    description: str = Field(default=None)
    quantity: str = Field(default=None)
    price: str = Field(default=None)
    is_available: bool = Field(default={True if quantity >= 1 else False})

class Create_Product(BaseModel):
    title: Annotated[str, Form()]
    description: Annotated[str, Form()]
    quantity: Annotated[int, Form()]
    price: Annotated[int, Form()]
