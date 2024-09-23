from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.config.setting import BOOTSTRAP_SERVERS
from app.protobuf import user_pb2
from app.models.user_model import User
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

            user = user_pb2.Users()
            user.ParseFromString(message.value)
            print(f"\n Consumer Deserialized Data: {user}")

            # ? Session & Database:
            with Session(engine) as session:
                if user.username and user.email and user.password:
                    new_user: User = User(
                        username=user.username, email=user.email, password=user.password
                    )
                    session.add(new_user)
                    session.commit()
                    session.refresh(new_user)

    finally:
        # Ensure to close the consumer when done.

        await consumer.stop()

# async def db_session(user_data):
#     session = next(get_session())  # Use next to get the session from the generator
#     try:
#         # new_user: Register_User = Register_User(
#         #     user_data=user.username, user_data=user.email, user_data=user.password
#         # )
#         session.add(user_data)
#         session.commit()  # Commit the transaction
#         session.refresh(user_data)
#     except Exception as e:
#         session.rollback()  # Rollback in case of error
#         print(f"Error occurred: {e}")
#     finally:
#         session.close()
