import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import aio_pika
import os
import logging

from fastApiProject_Deduplicator.src.database.models import Event

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    connection = None

    try:
        logger.info(f"Connecting to RabbitMQ at {rabbitmq_url}")
        connection = await aio_pika.connect_robust(rabbitmq_url)
        app.state.rabbit_conn = connection
        logger.info("RabbitMQ connected successfully")
        yield
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise
    finally:
        if connection:
            await connection.close()
            logger.info("RabbitMQ connection closed")


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

    uvicorn.run(app, host="127.0.0.1", port=8001)