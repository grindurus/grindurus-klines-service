from datetime import datetime

from app.database.database import get_db, db_session, find_gaps
from app.database.models import OHLCV
from app.forms import OHLCVResponse
from app.tasks.backfill import backfill_ohlcv_task

def get_data_between_dates(start_date: datetime, end_date: datetime, exchange:str, symbol:str, timeframe : str) -> OHLCVResponse:
    with (db_session() as session):
        gaps = find_gaps(session.connection(), start_date, end_date, exchange, symbol, timeframe)
        if len(gaps) > 0:
            for gap in gaps:
                backfill_ohlcv_task.delay(
                    exchange=exchange,
                    symbol=symbol,
                    start_timestamp=gap[0],
                    end_timestamp=gap[1],
                    timeframe=timeframe,
                )

        get_results = session.query(OHLCV).filter(OHLCV.timestamp >= start_date, OHLCV.timestamp <= end_date, exchange == exchange, symbol == symbol, timeframe == timeframe).all()
        if get_results:
            return OHLCVResponse(
                status="success",
                exchange=exchange,
                symbol=symbol,
                start_timestamp=start_date,
                end_timestamp=end_date,
                data=get_results,
                count=len(get_results)
            )
        #TODO add partial success
        return OHLCVResponse(
            status="failure",
            exchange=exchange,
            symbol=symbol,
            start_timestamp=start_date,
            end_timestamp=end_date,
            data=get_results,
            count=0
        )

