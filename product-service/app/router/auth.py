from fastapi import APIRouter, Depends, Form, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from sqlmodel import Session
from app.config.db import get_session
from typing import Annotated, Optional, Any
from app.models.product_model import Create_Product
from app.utils.verify_token import current_user
from fastapi.security import OAuth2PasswordBearer
import requests
import json
import httpx

auth_router = APIRouter(
    prefix="/auth", tags=["auth"], responses={404: {"description": "Not Found"}}
)


@auth_router.post("/token")
async def token(
    user_data: Annotated[OAuth2PasswordRequestForm, Depends()],
):
    try:
        url = 'http://host.docker.internal:8000/user-service/auth/login'
        data = {
            'username': user_data.username,
            'password': user_data.password
        }
        # async with httpx.AsyncClient() as client:
        response = requests.post(url, data=data)
        token = response.json()
    except:
        raise HTTPException(status_code=401, detail={"Something went wrong"})
    return token
