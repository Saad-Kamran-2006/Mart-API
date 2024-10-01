from typing import Annotated, Optional
from fastapi import Depends
from aiokafka import AIOKafkaProducer
from app.kafka.producer_consumer import kafka_producer
from app.config.setting import KAFKA_SEND_NOTIFICATION_TOPIC
from app.protobuf import notification_pb2


async def produce_notification(
    message: str, producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    # ? Notification Protobuf Data:
    notification = notification_pb2.Notification(message=message)
    print("\nNotification Data:\n", notification)

    # ? Notification Serialized Data:
    notification_data = notification.SerializeToString()
    print("Notification Serialized Data:\n", notification_data)

    try:
        # ? Send To Produce Kafka Topic:
        await producer.send_and_wait(KAFKA_SEND_NOTIFICATION_TOPIC, notification_data)
    except Exception as e:
        raise e
