from aiokafka import AIOKafkaConsumer
from app.protobuf import update_order_pb2
from app.config.db import engine
from sqlmodel import Session, select
from app.models.inventory_model import Inventory
from app.utils.order_producer import order_producer
from fastapi import HTTPException
from app.config.setting import BOOTSTRAP_SERVER, KAFKA_DELETE_ORDER_TOPIC



# ? Kafka consumer:
async def delete_order_consumer(topic, bootstrap_server, group_id):
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

            delete_order = update_order_pb2.Update_Order()
            delete_order.ParseFromString(message.value)
            print(f"\nConsumer Deserialized Data:\n{delete_order}")

            with Session(engine) as session:
                if delete_order.product_id:
                    inventory = session.exec(
                        select(Inventory).where(
                            Inventory.product_id == delete_order.product_id
                        )
                    ).first()

                    if inventory:
                        inventory.quantity += delete_order.quantity
                        inventory.is_available = True

                        session.add(inventory)
                        session.commit()
                        session.refresh(inventory)

                        message = {
                            "status": "success",
                            "message": f"Your order with product id {delete_order.product_id} has been canceled successfully.",
                        }

                        await order_producer(
                            BOOTSTRAP_SERVER,
                            KAFKA_DELETE_ORDER_TOPIC,
                            message,
                        )

                    elif not inventory:
                        message = {
                            "status": "error",
                            "message": f"Product not found with id {delete_order.product_id}.",
                        }

                        await order_producer(
                            BOOTSTRAP_SERVER,
                            KAFKA_DELETE_ORDER_TOPIC,
                            message,
                        )

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
