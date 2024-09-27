from sqlmodel import SQLModel, Field
from typing import Optional, Annotated, Union
from pydantic import BaseModel
from fastapi import Form
# import random
# import string


class Inventory(SQLModel, table=True):
    product_id: str = Field(primary_key=True, default=None, index=True)
    quantity: int = Field(default=None)
    is_available: bool = Field(default=None)

    def __init__(self, **data):
        # Call the parent constructor with the updated data
        super().__init__(**data)

        # Automatically set is_available based on quantity
        self.is_available = self.quantity >= 1


class Edit_Inventory(BaseModel):
    quantity: Annotated[int, Form()]

class Get_Inventory(BaseModel):
    product_id: str
    quantity: int
    is_available: bool