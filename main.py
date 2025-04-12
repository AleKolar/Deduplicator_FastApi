from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import aio_pika
import json
from redis.asyncio import Redis
from fastApiProject_Deduplicator.src.services.deduplicator import DeduplicationService

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация подключений
    app.redis = Redis.from_url("redis://localhost:6379")
    app.deduplicator = DeduplicationService(app.redis)
    # Подключение к RabbitMQ
    app.rabbit_conn = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    app.rabbit_channel = await app.rabbit_conn.channel()
    await app.rabbit_channel.declare_queue("events", durable=True)
    yield
    # Корректное закрытие подключений
    await app.redis.close()
    await app.rabbit_channel.close()
    await app.rabbit_conn.close()

app = FastAPI(lifespan=lifespan)

class Event(BaseModel):
    data: dict

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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
