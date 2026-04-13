from sqlalchemy import Column, String, Numeric, DateTime, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OHLCV(Base):
    __tablename__ = "ohlcv"

    timestamp = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    exchange = Column(String, nullable=False, primary_key=True)
    symbol = Column(String, nullable=False, primary_key=True)
    timeframe = Column(String, nullable=False, primary_key=True)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("timestamp", "exchange", "symbol", "timeframe"),
    )

    def __repr__(self):
        return (
            f"OHLCV(timestamp={self.timestamp}, exchange={self.exchange}, "
            f"symbol={self.symbol}, timeframe={self.timeframe}, "
            f"open={self.open}, high={self.high}, low={self.low}, "
            f"close={self.close}, volume={self.volume})"
        )