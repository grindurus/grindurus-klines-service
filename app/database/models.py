from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OHLCV(Base):
    __tablename__ = "ohlcv"

    timestamp = Column(BigInteger, nullable=False)
    timestamp_human = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    exchange = Column(String, nullable=False, primary_key=True)
    symbol = Column(String, nullable=False, primary_key=True)
    timeframe = Column(String, nullable=False, primary_key=True)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("timestamp_human", "exchange", "symbol", "timeframe"),
        UniqueConstraint(
            "timestamp", "exchange", "symbol", "timeframe", name="uq_ohlcv_timestamp_candle"
        ),
    )

    def __repr__(self):
        return (
            f"OHLCV(timestamp={self.timestamp}, timestamp_human={self.timestamp_human}, "
            f"exchange={self.exchange}, "
            f"symbol={self.symbol}, timeframe={self.timeframe}, "
            f"open={self.open}, high={self.high}, low={self.low}, "
            f"close={self.close}, volume={self.volume})"
        )