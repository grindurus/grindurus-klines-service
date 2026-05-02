from dotenv import load_dotenv
import ccxt
import os
import datetime
import time

from app.adapters.adapter import Adapter
from app.database.database import db_session
from app.database.models import OHLCV

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

exchange = ccxt.binance({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,  # helps prevent rate-limit issues
})


def get_candles_left(since_time, end_time, timeframe):
    if isinstance(since_time, datetime.datetime) and isinstance(end_time, datetime.datetime):
        duration_seconds = (end_time - since_time).total_seconds()
    else:
        # 2. Fallback if they are already millisecond timestamps (integers)
        duration_seconds = (end_time - since_time) / 1000

    # Map timeframes to seconds for easy division
    seconds_map = {
        "1s": 1,
        "15s": 15,
        "30s": 30,
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400
    }

    divider = seconds_map.get(timeframe, 60)  # Default to 1m if not found
    return int(duration_seconds / divider)


class BinanceAdapter(Adapter):
    def backfill_ohlcv(self, start_date : datetime.datetime, end_date : datetime.datetime, symbol, timeframe):
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        since = start_time
        candles_left = get_candles_left(since, end_time, timeframe)

        while candles_left > 0:
            limit = 1000
            if candles_left < 1000:
                limit = candles_left

            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
            if not ohlcv:
                break

            rows = [
                OHLCV(
                    timestamp=int(candle[0]),
                    timestamp_human=datetime.datetime.fromtimestamp(
                        candle[0] / 1000, tz=datetime.timezone.utc
                    ),
                    exchange="binance",
                    symbol=symbol,
                    timeframe=timeframe,
                    open=candle[1],
                    high=candle[2],
                    low=candle[3],
                    close=candle[4],
                    volume=candle[5],
                )
                for candle in ohlcv
            ]

            with db_session() as session:
                session.bulk_save_objects(rows)

            since = ohlcv[-1][0] + 1
            candles_left = get_candles_left(since, end_time, timeframe)
            time.sleep(exchange.rateLimit / 1000)
            print(f"Loaded {len(rows)} candles up to {since}")


    def get_available_symbols(self):
        markets = exchange.fetch_markets()
        symbols = {}
        for market in markets:
            if not symbols.get(market["base"]):
                symbols[market["base"]] = set()

            symbols[market["base"]].add(market["quote"])

        return symbols