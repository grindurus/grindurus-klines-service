from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


def _database_url() -> str:
    return os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/app")


engine = create_engine(_database_url())

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)

def init_db() -> None:
    from app.database.models import Base, OHLCV
    Base.metadata.create_all(bind=engine)

    init_timescale_db(engine)

def init_timescale_db(engine: Engine) -> None:
    with engine.begin() as connection:
        # Enable TimescaleDB extension if not already enabled
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"))

        try:
            connection.execute(
                text("SELECT create_hypertable('ohlcv', 'timestamp', if_not_exists => TRUE)")
            )
        except Exception as e:
            # If it's already a hypertable or table doesn't exist, continue
            print(f"Hypertable creation note: {e}")

        # Create composite index for common queries
        try:
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_ohlcv_exchange_symbol_timeframe_timestamp "
                    "ON ohlcv (exchange, symbol, timeframe, timestamp DESC)"
                )
            )
        except Exception as e:
            print(f"Index creation note: {e}")

def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def db_session() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()