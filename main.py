import csv
import io

from fastapi import FastAPI, Query, HTTPException

from app.service import data_service
from app.service.background_execution_service import background_execution
from app.tasks.backfill import backfill_ohlcv_task
from app.database.database import init_db
from app.forms import OHLCVResponse, BackfillResponse
from datetime import datetime
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

@app.get("/klines.csv")
async def get_ohlcv(
        start_time: str = Query(..., description="Start timestamp (ISO 8601 format, e.g., 2024-01-01T00:00:00Z)"),
        end_time: str = Query(..., description="End timestamp (ISO 8601 format, e.g., 2024-01-02T00:00:00Z)"),
        exchange: str = Query("binance", description="Exchange name"),
        symbol: str = Query(..., description="Trading symbol"),
        timeframe: str = Query("1m", description="Timeframe (1m, 1h, etc.)"),
        response_format: str = Query("csv", description="Response format: json or csv"),
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

    if response_format == "csv":
        if status == "complete":
            csv_data = results_to_csv(results)
            return StreamingResponse(
                io.StringIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=ohlcv.csv"},
            )
        else:
            raise HTTPException(status_code=404,
                                detail="Requested data not found in database. Backfill job queued, try again in few minutes.")
    else :
        return OHLCVResponse(
            status=status,
            exchange=exchange,
            symbol=symbol,
            start_timestamp=start_dt,
            end_timestamp=end_dt,
            data=results,
            count=len(results),
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

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)