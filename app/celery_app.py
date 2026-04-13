import os
from celery import Celery

db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/app")

celery = Celery(
    "klines-service",
    broker=f"sqla+{db_url}",
    backend=f"db+{db_url}",
)

celery.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
)