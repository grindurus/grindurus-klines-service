

from datetime import datetime


class Adapter:
    def backfill_ohlcv(self, start_date : datetime, end_date : datetime, symbol : str, timeframe : str):
        """Take OHLCV data from exchange for timeframe from start_date to end_date and write it into the database"""
        raise NotImplementedError("Backfill OHLCV data is not implemented.")

    def get_available_symbols(self):
        raise NotImplementedError("Fetching available symbols data is not implemented.")