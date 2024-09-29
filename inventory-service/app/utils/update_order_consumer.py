from aiokafka import AIOKafkaConsumer
from app.protobuf import update_order_pb2
from app.config.db import engine
from sqlmodel import Session, select
from app.models.inventory_model import Inventory
from app.utils.order_producer import order_producer
from fastapi import HTTPException
from app.config.setting import BOOTSTRAP_SERVER, KAFKA_UPDATE_ORDER_TOPIC


# ? Kafka consumer:
async def update_order_consumer(topic, bootstrap_server, group_id):
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

            update_order = update_order_pb2.Update_Order()
            update_order.ParseFromString(message.value)
            print(f"\nConsumer Deserialized Data:\n{update_order}")

            with Session(engine) as session:
                if update_order.product_id:
                    inventory = session.exec(
                        select(Inventory).where(
                            Inventory.product_id == update_order.product_id
                        )
                    ).first()
                    if inventory:
                        inventory.quantity += update_order.quantity
                        if inventory.quantity >= update_order.new_quantity:
                            if inventory.quantity > update_order.new_quantity:
                                inventory.quantity -= update_order.new_quantity

                                session.add(inventory)
                                session.commit()
                                session.refresh(inventory)

                            elif inventory.quantity == update_order.new_quantity:
                                inventory.quantity -= update_order.new_quantity
                                inventory.is_available = False

                                session.add(inventory)
                                session.commit()
                                session.refresh(inventory)

                            message = {
                                "status": "success",
                                "message": f"Your order with product id {update_order.product_id} has been updated successfully.",
                            }

                            await order_producer(
                                BOOTSTRAP_SERVER,
                                KAFKA_UPDATE_ORDER_TOPIC,
                                message,
                            )

                        else:
                            message = {
                                "status": "error",
                                "message": f"Product with id {update_order.product_id} has insufficient stock level.",
                            }

                            await order_producer(
                                BOOTSTRAP_SERVER,
                                KAFKA_UPDATE_ORDER_TOPIC,
                                message,
                            )

                    elif not inventory:
                        message = {
                            "status": "error",
                            "message": f"Product not found with id {update_order.product_id}.",
                        }

                        await order_producer(
                            BOOTSTRAP_SERVER,
                            KAFKA_UPDATE_ORDER_TOPIC,
                            message,
                        )

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
