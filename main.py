from fastapi import FastAPI, Query

from app.service import data_service
from app.tasks.backfill import backfill_ohlcv_task
from app.database.database import init_db
from app.forms import OHLCVResponse, BackfillResponse, BackfillRequest
from datetime import datetime
from celery.result import AsyncResult
from app.celery_app import celery

init_db()

app = FastAPI()

@app.get("/health")
async def root():
    return {"health": "OK"}

@app.get("/ohlcv", response_model=OHLCVResponse)
async def get_ohlcv(
        start_time: str = Query(..., description="Start timestamp (ISO 8601 format, e.g., 2024-01-01T00:00:00Z)"),
        end_time: str = Query(..., description="End timestamp (ISO 8601 format, e.g., 2024-01-02T00:00:00Z)"),
        exchange: str = Query(..., description="Exchange name (e.g., binance, kraken)"),
        symbol: str = Query(..., description="Trading symbol (e.g., BTC/USDT)"),
        timeframe: str = Query(..., description="Timeframe (e.g., 1m, 5m, 15m, etc.)"),
) -> OHLCVResponse:
    """
    Retrieve OHLCV data for a given symbol and time range.

    Query Parameters:
    - start_timestamp: ISO 8601 datetime string for data start (e.g., 2024-01-01T00:00:00Z)
    - end_timestamp: ISO 8601 datetime string for data end (e.g., 2024-01-02T00:00:00Z)
    - exchange: Exchange identifier (binance, kraken, coinbase, etc.)
    - symbol: Trading pair symbol (BTC/USDT, ETH/USDC, etc.)

    Returns OHLCV candle data for the requested period.
    """

    # Parse ISO 8601 timestamps
    start_dt = datetime.fromisoformat(start_time.strip().replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_time.strip().replace("Z", "+00:00"))

    return data_service.get_data_between_dates(
        start_date=start_dt,
        end_date=end_dt,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe)


@app.post("/ohlcv", response_model=BackfillResponse)
async def backfill_ohlcv(request: BackfillRequest) -> BackfillResponse:
    """
    Trigger a backfill job to fetch and store OHLCV data.

    Request Body:
    - start_timestamp: Unix timestamp (seconds) for data start
    - end_timestamp: Unix timestamp (seconds) for data end
    - exchange: Exchange identifier (binance, kraken, coinbase, etc.)
    - symbol: Trading pair symbol (BTC/USDT, ETH/USDC, etc.)

    Returns a job ID for tracking the backfill progress.
    In production, this would queue a background task (Celery, RQ, etc.)
    """
    start_timestamp = datetime.fromisoformat(request.start_time.strip().replace("Z", "+00:00"))
    end_timestamp = datetime.fromisoformat(request.end_time.strip().replace("Z", "+00:00"))

    task = backfill_ohlcv_task.delay(
        exchange=request.exchange,
        symbol=request.symbol,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        timeframe=request.timeframe,
    )

    return BackfillResponse(
        status="backfill_queued",
        job_id=task.id,
        exchange=request.exchange,
        symbol=request.symbol,
        start_timestamp=(start_timestamp),
        end_timestamp=(end_timestamp),
        message="Backfill job has been queued for processing",
    )

@app.get("/ohlcv/jobs/{job_id}")
async def get_job_status(job_id: str):
    result = AsyncResult(job_id, app=celery)
    return {
        "job_id": job_id,
        "state": result.state,
        "meta": result.info if result.state != "PENDING" else None,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)