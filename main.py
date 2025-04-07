from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from my_venv.src.schemas import EventBatch, DedupeResponse
from my_venv.src.repositories.clickhouse_repo import ClickHouseRepository
from typing import List

from my_venv.src.utils.logger import logger


# Lifespan management
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация при старте
    logger.info("Starting application...")
    app.state.clickhouse_repo = ClickHouseRepository()
    yield
    # Очистка при завершении
    logger.info("Shutting down application...")
    del app.state.clickhouse_repo


app = FastAPI(lifespan=lifespan)


@app.post("/deduplicate", response_model=DedupeResponse)
async def deduplicate_events(events: List[EventBatch]):
    """
    Эндпоинт для дедупликации событий.
    Принимает список событий, возвращает результат обработки.
    """
    try:
        # Преобразуем запрос в EventBatch
        event_batch = EventBatch(events=events)

        # Получаем репозиторий из состояния приложения
        repo = app.state.clickhouse_repo

        # Выполняем дедупликацию
        result = repo.deduplicate_events(event_batch)

        logger.info(f"Processed {len(events)} events, duplicates: {result.duplicates}")
        return result

    except Exception as e:
        logger.error(f"Error processing events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {
        "status": "OK",
        "database": "ClickHouse",
        "ready": True
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
