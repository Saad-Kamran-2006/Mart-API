from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.config.setting import BOOTSTRAP_SERVERS
from app.protobuf import inventory_pb2
from app.models.inventory_model import Inventory
from typing import Annotated
from app.config.db import engine
from sqlmodel import Session, select
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

            inventory = inventory_pb2.Inventories()
            inventory.ParseFromString(message.value)
            print(f"\nConsumer Deserialized Data:\n{inventory}")

            # ? Session & Database:
            with Session(engine) as session:
                if inventory.product_id and inventory.quantity and inventory.is_available:
                    new_inventory: Inventory = Inventory(
                        product_id=inventory.product_id, quantity=inventory.quantity, is_available=inventory.is_available
                    )
                    print("\nnew_inventory:\n",new_inventory)
                    session.add(new_inventory)
                    session.commit()
                    session.refresh(new_inventory)

                elif inventory.product_id:
                    print("\ninventory.product_id:\n",inventory.product_id)
                    existing_inventory = session.exec(select(Inventory).where(Inventory.product_id == inventory.product_id)).first()
                    print("\nexisting_inventory:\n",existing_inventory)

                    if not existing_inventory:
                        new_inventory: Inventory = Inventory(
                        product_id=inventory.product_id, quantity=0, is_available=False
                    )
                        print("\nLast new_inventory:\n",new_inventory)
                        session.add(new_inventory)
                        session.commit()
                        session.refresh(new_inventory)

                    elif existing_inventory:
                        session.delete(existing_inventory)
                        session.commit()
                    
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
