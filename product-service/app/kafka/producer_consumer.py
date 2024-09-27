from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.config.setting import BOOTSTRAP_SERVERS
from app.protobuf import product_pb2
from app.models.product_model import Product
from typing import Annotated
from app.config.db import engine
from sqlmodel import Session
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

    # await consumer.subscribe(topics=[topic])

    try:
        # Continuously listen for messages.
        async for message in consumer:
            print(f"\nConsumer Serialized Data:\n{message.value}")

            product = product_pb2.Products()
            product.ParseFromString(message.value)
            print(f"\nConsumer Deserialized Data:\n{product}")

            # ? Session & Database:
            with Session(engine) as session:
                if product.title and product.price:
                    new_product: Product = Product(
                        product_id=product.product_id, title=product.title, price=product.price
                    )
                    session.add(new_product)
                    session.commit()
                    session.refresh(new_product)

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
