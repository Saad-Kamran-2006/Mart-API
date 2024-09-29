from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.config.setting import BOOTSTRAP_SERVERS
from app.protobuf import order_message_pb2
from typing import Annotated, Optional
from app.config.db import engine
from sqlmodel import Session
# from app.router.order import consume_order_message
import asyncio

# bootstrap_servers = ["broker1:19092", "broker2:19092", "broker3:19092"]
bootstrap_server = "broker:19092"

# ? Kafka Producer as a dependency:
async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


# ? Kafka consumer:
async def kafka_consumer(
    topic, bootstrap_server, group_id
):
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

            order_message = order_message_pb2.Order_Message()
            order_message.ParseFromString(message.value)
            print(f"\nConsumer Deserialized Data:\n{order_message}")

            def order_consumer():
                message_data = order_message
                return message_data

            return order_consumer

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
