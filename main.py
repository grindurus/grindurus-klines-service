import csv
import io
from urllib.parse import urlencode

from fastapi import FastAPI, Query, HTTPException

from app.service import data_service, symbols_service
from app.service.background_execution_service import background_execution
from app.tasks.backfill import backfill_ohlcv_task
from app.database.database import init_db
from app.forms import BackfillResponse
from datetime import datetime, date, timedelta, timezone
from fastapi.responses import StreamingResponse

init_db()

app = FastAPI()

@app.get("/health")
async def root():
    return {"health": "OK"}

def results_to_csv(results) -> str:
    output = io.StringIO()
    fields = ["timestamp", "exchange", "symbol", "timeframe", "open", "high", "low", "close", "volume"]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(results)
    return output.getvalue()


@app.get("/klines", response_model=list[str])
async def get_backtest_klines_links(
        start_date: date = Query(..., description="Start timestamp (ISO 8601 format, e.g., 2024-01-01T00:00:00Z)"),
        end_date: date = Query(..., description="End timestamp (ISO 8601 format, e.g., 2024-01-02T00:00:00Z)"),
        symbol: str = Query(..., description="Trading symbol"),
        exchange: str = Query("binance", description="Exchange name"),
        timeframe: str = Query("1m", description="Timeframe (1m, 1h, etc.)"),
        domain: str = Query("grindurus.xyz", description="Base domain without protocol"),
):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be less than or equal to end_date")

    links = []
    current_day = start_date

    while current_day < end_date:
        day_start = datetime.combine(current_day, datetime.min.time()).replace(tzinfo=timezone.utc)
        next_day = current_day + timedelta(days=1)
        day_end = datetime.combine(next_day, datetime.min.time()).replace(tzinfo=timezone.utc)

        if day_end > datetime.combine(end_date + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc):
            day_end = datetime.combine(end_date + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc)

        params = urlencode(
            {
                "start_time": day_start.isoformat().replace("+00:00", "Z"),
                "end_time": day_end.isoformat().replace("+00:00", "Z"),
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "response_format": "csv",
            }
        )
        links.append(f"https://klines.{domain}/klines.csv?{params}")
        current_day = next_day

    return links

@app.get("/klines.csv")
async def get_ohlcv(
        start_time: str = Query(..., description="Start timestamp (ISO 8601 format, e.g., 2024-01-01T00:00:00Z)"),
        end_time: str = Query(..., description="End timestamp (ISO 8601 format, e.g., 2024-01-02T00:00:00Z)"),
        symbol: str = Query(..., description="Trading symbol"),
        exchange: str = Query("binance", description="Exchange name"),
        timeframe: str = Query("1m", description="Timeframe (1m, 1h, etc.)"),
):
    start_dt = datetime.fromisoformat(start_time.strip().replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_time.strip().replace("Z", "+00:00"))

    results, gaps = data_service.get_data_between_dates(
        start_date=start_dt,
        end_date=end_dt,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )

    status = "complete" if not gaps else "partial"

    if status == "complete":
        csv_data = results_to_csv(results)
        return StreamingResponse(
            io.StringIO(csv_data),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=ohlcv.csv"},
        )

    raise HTTPException(
        status_code=404,
        detail="Requested data not found in database. Backfill job queued, try again in few minutes.",
    )


@app.post("/ohlcv", response_model=BackfillResponse)
async def backfill_ohlcv(start_time: str = Query(..., description="Start timestamp (ISO 8601 format, e.g., 2024-01-01T00:00:00Z)"),
    end_time: str = Query(..., description="End timestamp (ISO 8601 format, e.g., 2024-01-02T00:00:00Z)"),
    timeframe: str = Query("1m", description="Timeframe (1m, 1h, etc.)"),
    exchange: str = Query("binance", description="Exchange name"),
    symbol: str = Query(..., description="Trading symbol"),
) -> BackfillResponse:
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
    start_dt = datetime.fromisoformat(start_time.strip().replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_time.strip().replace("Z", "+00:00"))
    background_execution.submit(
        backfill_ohlcv_task,
            exchange=exchange,
            symbol=symbol,
            start_timestamp=start_dt,
            end_timestamp=end_dt,
            timeframe=timeframe,
        )

    return BackfillResponse(
        status="backfill_queued",
        exchange=exchange,
        symbol=symbol,
        start_timestamp=start_dt,
        end_timestamp=end_dt,
        message="Backfill job has been queued for processing"
    )

@app.get("/symbols")
async def get_symbols(exchange: str = Query("binance", description="Binance, kraken, coinbase, etc.")):
    return symbols_service.get_symbols(exchange)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)