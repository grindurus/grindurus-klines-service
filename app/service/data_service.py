from datetime import datetime

from app.database.database import get_db, db_session, find_gaps
from app.database.models import OHLCV
from app.forms import OHLCVResponse
from app.tasks.backfill import backfill_ohlcv_task

def get_data_between_dates(start_date: datetime, end_date: datetime, exchange: str, symbol: str, timeframe: str):
    with db_session() as session:
        gaps = find_gaps(session.connection(), start_date, end_date,  timeframe, exchange, symbol)
        if len(gaps) > 0:
            for gap in gaps:
                backfill_ohlcv_task.delay(
                    exchange=exchange,
                    symbol=symbol,
                    start_timestamp=gap[0],
                    end_timestamp=gap[1],
                    timeframe=timeframe,
                )

        results = session.query(OHLCV).filter(
            OHLCV.timestamp >= start_date,
            OHLCV.timestamp <= end_date,
            OHLCV.exchange == exchange,
            OHLCV.symbol == symbol,
            OHLCV.timeframe == timeframe,
        ).order_by(OHLCV.timestamp).all()

        return results

