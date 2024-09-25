from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.config.setting import BOOTSTRAP_SERVERS
from app.protobuf import product_pb2
from app.models.product_model import Product
from typing import Annotated
from app.config.db import engine
from sqlmodel import Session

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
            print(f"\n Consumer Serialized Data: {message.value}")

            product = product_pb2.Products()
            product.ParseFromString(message.value)
            print(f"\n Consumer Deserialized Data: {product}")

            # ? Session & Database:
            with Session(engine) as session:
                if product.title and product.quantity and product.price:
                    new_product: Product = Product(
                        title=product.title, quantity=product.quantity, price=product.price
                    )
                    session.add(new_product)
                    session.commit()
                    session.refresh(new_product)

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
