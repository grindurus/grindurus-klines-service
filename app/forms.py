import datetime

from pydantic import BaseModel, ConfigDict
from typing import List



class OHLCVCandle(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    timestamp: int
    timestamp_human: datetime.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

class OHLCVResponse(BaseModel):
    status: str
    exchange: str
    symbol: str
    start_timestamp: datetime.datetime
    end_timestamp: datetime.datetime
    data: List[OHLCVCandle]
    count: int


class BackfillRequest(BaseModel):
    start_time: str
    end_time: str
    timeframe: str
    exchange: str
    symbol: str


class BackfillResponse(BaseModel):
    status: str
    exchange: str
    symbol: str
    start_timestamp: datetime.datetime
    end_timestamp: datetime.datetime
    message: str
