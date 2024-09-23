from typing import Annotated
from fastapi import Depends, HTTPException
from sqlmodel import Session
from app.models.roles import UserRole
from app.models.user_model import User


def is_super_user(user_data: User):
    if user_data:
        if user_data.role == UserRole.super_user:
            return True
        return False
