from aiokafka import AIOKafkaConsumer
from app.protobuf import inventory_pb2
from app.config.db import engine
from sqlmodel import Session, select
from app.models.inventory_model import Inventory
from app.utils.order_producer import order_producer
from fastapi import HTTPException
from app.config.setting import BOOTSTRAP_SERVER, KAFKA_CREATE_ORDER_TOPIC


# ? Kafka consumer:
async def order_consumer(topic, bootstrap_server, group_id):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        group_id=group_id,
        auto_offset_reset="earliest",
    )

    # Start the consumer.
    await consumer.start()

    try:
        # Continuously listen for messages.
        async for message in consumer:
            print(f"\nConsumer Serialized Data:\n{message.value}")

            order = inventory_pb2.Inventories()
            order.ParseFromString(message.value)
            print(f"\nConsumer Deserialized Data:\n{order}")

            with Session(engine) as session:
                if order.product_id:
                    inventory = session.exec(
                        select(Inventory).where(
                            Inventory.product_id == order.product_id
                        )
                    ).first()
                    if inventory:
                        if inventory.is_available:
                            if inventory.quantity >= order.quantity:
                                if inventory.quantity > order.quantity:
                                    inventory.quantity -= order.quantity

                                    session.add(inventory)
                                    session.commit()
                                    session.refresh(inventory)

                                elif inventory.quantity == order.quantity:
                                    inventory.quantity -= order.quantity
                                    inventory.is_available = False

                                    session.add(inventory)
                                    session.commit()
                                    session.refresh(inventory)

                                message = {
                                    "status": "success",
                                    "message": f"Your order with product id {order.product_id} has been created successfully.",
                                }

                                await order_producer(
                                    BOOTSTRAP_SERVER,
                                    KAFKA_CREATE_ORDER_TOPIC,
                                    message,
                                )

                            else:
                                message = {
                                    "status": "error",
                                    "message": f"Product with id {order.product_id} has insufficient stock level.",
                                }

                                await order_producer(
                                    BOOTSTRAP_SERVER,
                                    KAFKA_CREATE_ORDER_TOPIC,
                                    message,
                                )

                        elif not inventory.is_available:
                            message = {
                                "status": "error",
                                "message": f"Product with id {order.product_id} is out of stock.",
                            }

                            await order_producer(
                                BOOTSTRAP_SERVER,
                                KAFKA_CREATE_ORDER_TOPIC,
                                message,
                            )

                    elif not inventory:
                        message = {
                            "status": "error",
                            "message": f"Product not found with id {order.product_id}."
                        }

                        await order_producer(
                            BOOTSTRAP_SERVER,
                            KAFKA_CREATE_ORDER_TOPIC,
                            message,
                        )

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
