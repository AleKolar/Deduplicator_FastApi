import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import aio_pika
import json
from redis.asyncio import Redis

from fastApiProject_Deduplicator.src.database.models import Event
from fastApiProject_Deduplicator.src.services.deduplicator import DeduplicationService
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация соединений
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise ValueError("REDIS_URL environment variable is not set.")

    app.redis = Redis.from_url(redis_url)
    app.deduplicator = DeduplicationService(app.redis)

    rabbitmq_url = os.getenv("RABBITMQ_URL")
    if not rabbitmq_url:
        raise ValueError("RABBITMQ_URL environment variable is not set.")

    app.rabbit_conn = await aio_pika.connect_robust(rabbitmq_url)
    app.rabbit_channel = await app.rabbit_conn.channel()
    await app.rabbit_channel.declare_queue("events", durable=True)
    yield
    await app.redis.close()
    await app.rabbit_channel.close()
    await app.rabbit_conn.close()

app = FastAPI(lifespan=lifespan)

@app.post("/v1/events")
async def handle_event(event: Event):
    try:
        if await app.deduplicator.process_event(event.data):
            await app.rabbit_channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(event.data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key="events"
            )
            return {"status": "accepted"}
        return {"status": "duplicate"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# if __name__ == "__main__":
#     import uvicorn
#
#     uvicorn.run(app, host="0.0.0.0", port=8000)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)