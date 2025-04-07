import json
import aio_pika
from my_venv.src.utils.logger import logger


class QueueService:
    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url

    async def publish(self, event: dict):
        connection = await aio_pika.connect(self.rabbitmq_url)
        channel = await connection.channel()
        await channel.declare_queue("events", durable=True)

        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(event).encode()),
            routing_key="events"
        )
        logger.info(f"Событие отправлено в очередь: {event['event_name']}")

        # !!! Закрываем канал и подключение
        await channel.close()
        await connection.close()


###
if __name__ == "__main__":
    import asyncio

    async def main():
        queue_service = QueueService("amqp://guest:guest@localhost/")
        sample_event = {"event_name": "Test Event", "data": "Sample data"}
        await queue_service.publish(sample_event)

    asyncio.run(main())

