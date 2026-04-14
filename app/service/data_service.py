from datetime import datetime

from app.database.database import get_db, db_session
from app.database.models import OHLCV
from app.forms import OHLCVResponse


def get_data_between_dates(start_date: datetime, end_date: datetime, exchange:str, symbol:str, timeframe : str) -> OHLCVResponse:
    with (db_session() as session):
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

