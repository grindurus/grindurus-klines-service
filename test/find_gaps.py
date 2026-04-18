from datetime import datetime, timezone
from sqlalchemy import text
import app.database.database as db


def find_gaps(
    conn,
    start_date: datetime,
    end_date: datetime,
    timeframe: str,
    exchange: str,
    symbol: str,
    check_right: bool = True,
) -> list[tuple[datetime, datetime]]:
    """Call the find_ohlcv_gaps SQL function and return list of (gap_start, gap_end)."""
    query = text("""
        SELECT gap_start, gap_end
        FROM find_ohlcv_gaps(
            :start_date,
            :end_date,
            :timeframe,
            :exchange,
            :symbol,
            :check_right
        )
    """)

    rows = conn.execute(query, {
            "start_date": start_date,
            "end_date": end_date,
            "timeframe": timeframe,
            "exchange": exchange,
            "symbol": symbol,
            "check_right": check_right,
        }).fetchall()

    return [(row[0], row[1]) for row in rows]


# When using SQLAlchemy Sessions, execute returns a Result object
    result = session.execute(query, {
            "start_date": start_date,
            "end_date": end_date,
            "timeframe": timeframe,
            "exchange": exchange,
            "symbol": symbol,
            "check_right": check_right,
        })

    return [(row[0], row[1]) for row in result.fetchall()]


def install_sql_functions(engine):
    """Install the SQL functions from find_gaps.sql into the database."""
    # Ensure the path to find_gaps.sql is correct relative to this script
    if not os.path.exists("F:/hackathon/solana startup terminal/klines-service/app/database/sql_scripts/find_gaps.sql"):
        print("Warning: find_gaps.sql not found.")
        return

    with open("F:/hackathon/solana startup terminal/klines-service/app/database/sql_scripts/find_gaps.sql", "r") as f:
        sql = f.read()

    with engine.begin() as conn:  # .begin() handles the commit automatically
        conn.execute(text(sql))


if __name__ == "__main__":
    import os

    # 1. Initialize the tables and TimescaleDB settings
    db.init_db()

    # 2. (Optional) Install the custom SQL gap-finding function
    # Use the engine exported from your database.py
    install_sql_functions(db.engine)

    # 3. Use the 'with' statement to get the actual session out of the context manager
    with db.db_session() as session:
        gaps = find_gaps(
            session, # Now passing the actual Session object
            start_date=datetime(2024, 12, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 2, 1, tzinfo=timezone.utc),
            timeframe="1h",
            exchange="binance",
            symbol="BTC/USDT",
            check_right=True,
        )

        if not gaps:
            print("No gaps found — data is complete.")
        else:
            print(f"Found {len(gaps)} gap(s):")
            for gap_start, gap_end in gaps:
                print(f"  {gap_start} → {gap_end}")