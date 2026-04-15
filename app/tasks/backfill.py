from datetime import datetime

from app.celery_app import celery
from app.service.backfill_service import BackfillService


@celery.task(bind=True, name="backfill_ohlcv")
def backfill_ohlcv_task(self, exchange: str, symbol: str, start_timestamp: datetime, end_timestamp: datetime, timeframe: str):
    start_dt = datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_timestamp.replace("Z", "+00:00"))

    self.update_state(state="RUNNING", meta={"exchange": exchange, "symbol": symbol})

    service = BackfillService(exchange)
    service.run(start_dt, end_dt, symbol, timeframe)

    return {
        "exchange": exchange,
        "symbol": symbol,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "status": "completed",
    }