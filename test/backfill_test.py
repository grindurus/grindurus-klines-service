from datetime import datetime
from app.service.backfill_service import BackfillService

start_timestamp = datetime.fromisoformat("2023-01-01T00:00:00Z".strip().replace("Z", "+00:00"))
end_timestamp = datetime.fromisoformat("2024-04-01T00:00:00Z".replace("Z", "+00:00"))
service = BackfillService("binance")
service.run(start_timestamp, end_timestamp, "BTC/USDT", "1m")