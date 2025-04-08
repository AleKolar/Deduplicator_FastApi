""" worker подписывается на очередь RabbitMQ, извлекает события,
проверяет их на дубликаты и сохраняет """
import asyncio
import json
from aio_pika import connect, ExchangeType
from src.services.deduplicator import Deduplicator
from src.utils.logger import logger

async def process_event(event: dict):
    deduplicator = Deduplicator()
    await deduplicator.initialize()

    if not await deduplicator.is_duplicate(event):
        await deduplicator.mark_as_processed(event)
        logger.info(f"Событие обработано: {event['event_name']}")
    else:
        logger.info(f"Событие дубликат: {event['event_name']}")

    await deduplicator.close()

async def on_message(message):
    async with message.process():
        event_data = json.loads(message.body)
        await process_event(event_data)

async def main():
    # Устанавливаем соединение с RabbitMQ
    connection = await connect("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    # Создаем очередь, если она еще не существует
    queue = await channel.declare_queue("events", durable=True)

    # Подписываемся на очередь
    await queue.consume(on_message)

    logger.info("Worker запущен и ожидает события...")

    # Бесконечный цикл, чтобы worker продолжал работать
    try:
        await asyncio.Future()  # Ожидаем бесконечного выполнения
    finally:
        await connection.close()

if __name__ == "__main__":
    asyncio.run(main())