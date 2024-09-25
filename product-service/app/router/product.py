from fastapi import APIRouter, Depends, Form
from sqlmodel import Session
from app.config.db import get_session
from typing import Annotated, Optional, Any
from app.models.product_model import Create_Product, User_Data
from app.models.user_model import User
from app.utils.verify_token import current_user
from fastapi.security import OAuth2PasswordBearer
import requests
import json

product_router = APIRouter(
    prefix="/product", tags=["product"], responses={404: {"description": "Not Found"}}
)



# oauth_scheme = OAuth2PasswordBearer(tokenUrl="/auth/get-user")


@product_router.post("/create-product")
async def create_product(
    current_user: Annotated[User, Depends(current_user)],
    # username: str = Form(...),  # Accepting form data from the client
    # password: str = Form(...)
):
    # print()
    # # Define the target URL
    # url = 'http://host.docker.internal:8000/user-service/auth/admin'
    
    # # Form data to send in the POST request
    # form_data = {
    #     'username': username,
    #     'password': password
    # }

    # # Send POST request with form-encoded data
    # response = requests.post(url, data=form_data)
    
    # # Return the response from the external service
    # return {
    #     "status_code": response.status_code,
    #     "response_content": response.json()
    # }
    print(current_user)
    return current_user

