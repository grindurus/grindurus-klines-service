from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from pathlib import Path
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import Session, sessionmaker


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

        migrate_legacy_ohlcv_timestamp_split(connection)

        init_hypertables(connection)

        init_indexes(connection)

        init_functions(connection)


def migrate_legacy_ohlcv_timestamp_split(connection) -> None:
    """Upgrade pre-split schema: single timestamptz ``timestamp`` -> ``timestamp_human`` + bigint ``timestamp``."""
    ohlcv_exists = connection.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'ohlcv'
            )
            """
        )
    ).scalar()
    if not ohlcv_exists:
        return

    has_human = connection.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'ohlcv'
                  AND column_name = 'timestamp_human'
            )
            """
        )
    ).scalar()
    if has_human:
        return

    row = connection.execute(
        text(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'ohlcv'
              AND column_name = 'timestamp'
            """
        )
    ).fetchone()
    if not row:
        return

    if row[0] not in ("timestamp with time zone", "timestamp without time zone"):
        return

    connection.execute(text("ALTER TABLE ohlcv ADD COLUMN IF NOT EXISTS _migrate_open_ms BIGINT"))
    connection.execute(
        text(
            """
            UPDATE ohlcv
            SET _migrate_open_ms = (EXTRACT(EPOCH FROM "timestamp") * 1000)::bigint
            WHERE _migrate_open_ms IS NULL
            """
        )
    )

    # RENAME preserves the primary key on the time column (no DROP needed — safer on Timescale hypertables).
    connection.execute(
        text('ALTER TABLE ohlcv RENAME COLUMN "timestamp" TO timestamp_human')
    )
    connection.execute(
        text('ALTER TABLE ohlcv RENAME COLUMN _migrate_open_ms TO "timestamp"')
    )
    connection.execute(text('ALTER TABLE ohlcv ALTER COLUMN "timestamp" SET NOT NULL'))

    uq_exists = connection.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE n.nspname = 'public' AND t.relname = 'ohlcv'
                  AND c.conname = 'uq_ohlcv_timestamp_candle'
            )
            """
        )
    ).scalar()
    if not uq_exists:
        connection.execute(
            text(
                """
                ALTER TABLE ohlcv
                ADD CONSTRAINT uq_ohlcv_timestamp_candle
                UNIQUE ("timestamp", exchange, symbol, timeframe)
                """
            )
        )


def init_hypertables(connection):
    try:
        connection.execute(
            text(
                "SELECT create_hypertable('ohlcv', 'timestamp_human', if_not_exists => TRUE)"
            )
        )
    except Exception as e:
        # If it's already a hypertable or table doesn't exist, continue
        print(f"Hypertable creation note: {e}")

def init_indexes(connection):
    # Create composite index for common queries
    try:
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_exchange_symbol_timeframe_timestamp_human "
                "ON ohlcv (exchange, symbol, timeframe, timestamp_human DESC)"
            )
        )
    except Exception as e:
        print(f"Index creation note: {e}")

def init_functions(connection):
    sql_path = Path(__file__).parent / "sql_scripts" / "find_gaps.sql"
    try:
        sql = sql_path.read_text()
        connection.exec_driver_sql(sql)
    except Exception as e:
        print(f"SQL function or type creation error: {e}")


def find_gaps(
    conn,
    start_date: datetime,
    end_date: datetime,
    timeframe: str,
    exchange: str,
    symbol: str,
    check_right: bool = True,
) -> list[tuple[datetime, datetime]]:
    """Call the find_ohlcv_gaps SQL function and return list of (gap_start, gap_end)."""
    # Explicit casts: bound string params are typed "unknown" in PG and won't match TEXT args.
    query = text("""
        SELECT gap_start, gap_end
        FROM find_ohlcv_gaps(
            CAST(:start_date AS TIMESTAMPTZ),
            CAST(:end_date AS TIMESTAMPTZ),
            CAST(:timeframe AS TEXT),
            CAST(:exchange AS TEXT),
            CAST(:symbol AS TEXT),
            CAST(:check_right AS BOOLEAN)
        )
    """)

    rows = conn.execute(query, {
            "start_date": start_date,
            "end_date": end_date,
            "timeframe": timeframe,
            "exchange": exchange,
            "symbol": symbol,
            "check_right": check_right,
        }).fetchall()

    return [(row[0], row[1]) for row in rows]

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