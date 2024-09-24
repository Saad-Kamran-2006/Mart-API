from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlmodel import Session
from app.models.user_model import User, Register_User
from app.models.token_model import Token
from app.utils.get_user import get_user_from_db
from app.utils.security import hash_password
from app.utils.verify_user import authenticate_user
from app.utils.create_token import create_access_token
from app.utils.verify_token import validate_refresh_token
from app.config.db import get_session
from app.config.setting import (
    EXPIRY_TIME,
    ALGORITHYM,
    SECRET_KEY,
    BOOTSTRAP_SERVER,
    BOOTSTRAP_SERVER1,
    BOOTSTRAP_SERVER2,
    BOOTSTRAP_SERVER3,
    KAFKA_USER_REGISTER_TOPIC,
)

from typing import Annotated
from datetime import timedelta
from aiokafka import AIOKafkaProducer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from app.kafka.producer_consumer import kafka_producer
from confluent_kafka.serialization import SerializationContext, MessageField
from app.utils.get_schema import get_schema
from app.protobuf import user_pb2
from app.utils.verify_token import current_user
from app.utils.super_user import is_super_user
import asyncio


bootstrap_servers = [BOOTSTRAP_SERVER1, BOOTSTRAP_SERVER2, BOOTSTRAP_SERVER3]
bootstrap_server = BOOTSTRAP_SERVER

auth_router = APIRouter(
    prefix="/auth", tags=["auth"], responses={404: {"description": "Not Found"}}
)


@auth_router.post("/register")
async def register_user(
    new_user: Annotated[Register_User, Depends()],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    db_user = get_user_from_db(session, new_user.username, new_user.email)
    if db_user:
        raise HTTPException(
            status_code=409, detail="User with these credientials already exist"
        )
    if not db_user:
        user: Register_User = user_pb2.Users(
            username=new_user.username,
            email=new_user.email,
            password=hash_password(new_user.password),
        )
        print("\nUser's Data: ", user)

        user_data = user.SerializeToString()
        print("User's Serialized Data: ", user_data)

        # ? Produce the message with headers
        await producer.send_and_wait(KAFKA_USER_REGISTER_TOPIC, user_data)
        return {"message": f"User with {user.username} successfully registered"}


@auth_router.post("/login")
async def login_user(
    user_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    session: Annotated[Session, Depends(get_session)],
):
    user: User = authenticate_user(user_data.username, user_data.password, session)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    expire_time = timedelta(minutes=EXPIRY_TIME)
    access_token = create_access_token({"sub": user_data.username}, expire_time)
    refresh_expire_time = timedelta(days=7)
    refresh_token = create_access_token({"sub": user.email}, refresh_expire_time)
    # print("user data from login: ", user)
    is_admin = is_super_user(user)
    # print("is-admin: ", is_admin)
    return {
        "token": Token(
            access_token=access_token,
            token_type="bearer",
            refresh_token=refresh_token,
        ),
        "is_admin": is_admin,
    }


@auth_router.post("/token", response_model=Token)
def refresh_token(
    old_refresh_token: str,
    session: Annotated[Session, Depends(get_session)],
):
    credential_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid token, please login again",
        headers={"www-Authenticate": "Bearer"},
    )
    user = validate_refresh_token(old_refresh_token, session)
    if not user:
        raise credential_exception

    expire_time = timedelta(minutes=EXPIRY_TIME)
    access_token = create_access_token({"sub": user.username}, expire_time)
    refresh_expire_time = timedelta(days=7)
    refresh_token = create_access_token({"sub": user.email}, refresh_expire_time)
    return Token(
        access_token=access_token, token_type="bearer", refresh_token=refresh_token
    )


@auth_router.post("/admin")
async def is_admin(
    current_user: Annotated[User, Depends(current_user)],
    session: Annotated[Session, Depends(get_session)],
):
    db_user = get_user_from_db(session, current_user.username, current_user.email)
    if not db_user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if db_user:
        super_user = is_super_user(db_user)
        if super_user:
            return True
        return {"message": "Unauthorized"}
