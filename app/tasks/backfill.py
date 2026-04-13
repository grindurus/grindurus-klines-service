from datetime import datetime

from app.celery_app import celery
from app.service.backfill_service import BackfillService


@celery.task(bind=True, name="backfill_ohlcv")
def backfill_ohlcv_task(self, exchange: str, symbol: str, start_timestamp: datetime, end_timestamp: datetime, timeframe: str):
    self.update_state(state="RUNNING", meta={"exchange": exchange, "symbol": symbol})

    service = BackfillService(exchange)
    service.run(symbol, start_timestamp, end_timestamp, timeframe)

    return {
        "exchange": exchange,
        "symbol": symbol,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "status": "completed",
    }