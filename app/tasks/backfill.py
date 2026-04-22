from datetime import datetime

from app.service.backfill_service import BackfillService


def backfill_ohlcv_task(exchange: str, symbol: str, start_timestamp: datetime, end_timestamp: datetime, timeframe: str):
    service = BackfillService(exchange)
    service.run(start_timestamp, end_timestamp, symbol, timeframe)

    return {
        "exchange": exchange,
        "symbol": symbol,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "status": "completed",
    }