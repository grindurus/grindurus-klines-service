from datetime import datetime

from app.adapters.adapter_registry import get_adapter


class BackfillService:
    def __init__(self, exchange: str):
        self.exchange = exchange
        self.adapter = get_adapter(exchange)

    def run(self, start_timestamp: datetime, end_timestamp: datetime,symbol: str, timeframe: str):
        self.adapter.backfill_ohlcv(start_timestamp, end_timestamp, symbol, timeframe)
