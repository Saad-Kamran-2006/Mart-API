from fastapi import APIRouter, Depends, Form, HTTPException
from sqlmodel import Session, select
from app.config.db import get_session
from app.models.order_model import Get_Order, Order, Create_Order, Edit_Order
from app.config.setting import (
    KAFKA_GET_PRODUCT_INVENTORY_TOPIC,
    BOOTSTRAP_SERVER,
    KAFKA_CREATE_ORDER_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_CREATE_ORDER,
    KAFKA_UPDATE_ORDER_INVENTORY_TOPIC,
    KAFKA_UPDATE_ORDER_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_UPDATE_ORDER,
    KAFKA_DELETE_ORDER_INVENTORY_TOPIC,
    KAFKA_DELETE_ORDER_TOPIC,
    KAFKA_CONSUMER_GROUP_FOR_DELETE_ORDER,
)
from app.kafka.producer_consumer import kafka_producer, kafka_consumer
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.protobuf import order_pb2, order_message_pb2, update_order_pb2
from app.utils.produce_notification import produce_notification
from typing import Annotated


order_router = APIRouter(
    prefix="/order", tags=["order"], responses={404: {"description": "Not Found"}}
)


@order_router.post("/create-order")
async def create_order(
    new_order: Annotated[Create_Order, Depends()],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    # consumer: Annotated[AIOKafkaConsumer, Depends(order_consumer)],
):
    # ? Order Protobuf Data:
    order = order_pb2.Orders(
        product_id=new_order.product_id, quantity=new_order.quantity
    )
    print("\nOrder Data:\n", order)

    # ? Order Serialized Data:
    order_data = order.SerializeToString()
    print("Order Serialized Data:\n", order_data)

    try:
        # ? Send To Produce Kafka Topic:
        order_producer = await producer.send_and_wait(
            KAFKA_GET_PRODUCT_INVENTORY_TOPIC, order_data
        )
    except Exception as e:
        raise e

    order_consumer = await kafka_consumer(
        KAFKA_CREATE_ORDER_TOPIC,
        BOOTSTRAP_SERVER,
        KAFKA_CONSUMER_GROUP_FOR_CREATE_ORDER,
    )

    order_message = order_consumer()
    if order_message.status == "success":
        existing_order = session.exec(
            select(Order).where(Order.product_id == new_order.product_id)
        ).first()
        if existing_order:
            existing_order.quantity += new_order.quantity

            session.add(existing_order)
            session.commit()
            session.refresh(existing_order)

        elif not existing_order:
            order: Order = Order(
                product_id=new_order.product_id, quantity=new_order.quantity
            )
            session.add(order)
            session.commit()
            session.refresh(order)

        await produce_notification(message=order_message.message, producer=producer)

        return {"message": order_message.message}

    elif order_message.status == "error":
        return {"message": order_message.message}


@order_router.get("/orders", response_model=list[Get_Order])
async def get_all_orders(session: Annotated[Session, Depends(get_session)]):
    orders: Get_Order = session.exec(select(Order)).all()
    if orders:
        return orders
    else:
        raise HTTPException(status_code=404, detail="No order found")


@order_router.get("/{order_id}", response_model=Get_Order)
async def get_single_order(
    order_id: int, session: Annotated[Session, Depends(get_session)]
):
    order: Get_Order = session.exec(
        select(Order).where(Order.order_id == order_id)
    ).first()
    if order:
        return order
    else:
        raise HTTPException(status_code=404, detail="No order found")


@order_router.put("/edit-order")
async def edit_order(
    product_id: str,
    order_data: Annotated[Edit_Order, Depends()],
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    existing_order: Get_Order = session.exec(
        select(Order).where(Order.product_id == product_id)
    ).first()
    if existing_order:
        # ? Update Order Protobuf Data:
        update_order = update_order_pb2.Update_Order(
            product_id=existing_order.product_id,
            quantity=existing_order.quantity,
            new_quantity=order_data.quantity,
        )
        print("\nUpdated order Data:\n", update_order)

        # ? Order Serialized Data:
        updated_order_data = update_order.SerializeToString()
        print("Updated order Serialized Data:\n", updated_order_data)

        try:
            # ? Send To Produce Kafka Topic:
            order_producer = await producer.send_and_wait(
                KAFKA_UPDATE_ORDER_INVENTORY_TOPIC, updated_order_data
            )
        except Exception as e:
            raise e

        order_consumer = await kafka_consumer(
            KAFKA_UPDATE_ORDER_TOPIC,
            BOOTSTRAP_SERVER,
            KAFKA_CONSUMER_GROUP_FOR_UPDATE_ORDER,
        )

        updated_order_message = order_consumer()
        if updated_order_message.status == "success":
            existing_order.quantity = order_data.quantity

            session.add(existing_order)
            session.commit()
            session.refresh(existing_order)

            await produce_notification(
                message=updated_order_message.message, producer=producer
            )

            return {"message": updated_order_message.message}

        elif updated_order_message.status == "error":
            return {"message": updated_order_message.message}

    else:
        raise HTTPException(status_code=404, detail="No order found")


@order_router.delete("/cancel-order")
async def cancel_order(
    order_id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
):
    if order_id:
        existing_order: Get_Order = session.exec(
            select(Order).where(Order.order_id == order_id)
        ).first()

        if existing_order:
            delete_order = update_order_pb2.Update_Order(
                product_id=existing_order.product_id,
                quantity=existing_order.quantity,
            )
            print("\nUpdated order Data:\n", delete_order)

            # ? Order Serialized Data:
            delete_order_data = delete_order.SerializeToString()
            print("Updated order Serialized Data:\n", delete_order_data)

            try:
                # ? Send To Produce Kafka Topic:
                order_producer = await producer.send_and_wait(
                    KAFKA_DELETE_ORDER_INVENTORY_TOPIC, delete_order_data
                )

            except Exception as e:
                raise e

            order_consumer = await kafka_consumer(
                KAFKA_DELETE_ORDER_TOPIC,
                BOOTSTRAP_SERVER,
                KAFKA_CONSUMER_GROUP_FOR_DELETE_ORDER,
            )

            deleted_order_message = order_consumer()
            if deleted_order_message.status == "success":
                session.delete(existing_order)
                session.commit()

                await produce_notification(
                    message=deleted_order_message.message, producer=producer
                )

                return {"message": deleted_order_message.message}

            elif deleted_order_message.status == "error":
                return {"message": deleted_order_message.message}

        else:
            raise HTTPException(status_code=404, detail="No order found")
