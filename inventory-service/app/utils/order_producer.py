from aiokafka import AIOKafkaProducer
from app.protobuf import order_pb2

async def order_producer(bootstrap_server, topic, value):
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)

    await producer.start()

    # ? Order Protobuf Data:
    order = order_pb2.Order_Message(status=value["status"], message=value["message"])
    print("\nOrder Data:\n", order)

    # ? Order Serialized Data:
    order_data = order.SerializeToString()
    print("Order Serialized Data:\n", order_data)

    try:
        await producer.send_and_wait(topic, order_data)
    finally:
        await producer.stop()
