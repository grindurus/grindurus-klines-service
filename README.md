# klines-service

REST API for storing and querying cryptocurrency **OHLCV** (candlestick) data. Historical candles can be **backfilled** from an exchange into **PostgreSQL** with the **TimescaleDB** extension; reads are served from the same database.

## Features

- **FastAPI** HTTP API with OpenAPI docs (`/docs`)
- **TimescaleDB** hypertable on `timestamp` for time-series storage
- **Celery** for asynchronous backfill (broker and result backend use **SQLAlchemy** on the same Postgres URL as the app)
- **Binance** integration via **ccxt** (other exchanges can be added behind the adapter interface)

## API overview

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Health check |
| `GET` | `/ohlcv` | Query stored candles (query params: ISO 8601 `start_time`, `end_time`, `exchange`, `symbol`, `timeframe`) |
| `POST` | `/ohlcv` | Enqueue a backfill job (JSON body: `start_time`, `end_time`, `exchange`, `symbol`, `timeframe`) |
| `GET` | `/ohlcv/jobs/{job_id}` | Celery task state for a backfill job |

## Configuration

| Variable | Purpose | Default (local) |
|----------|---------|-------------------|
| `DATABASE_URL` | SQLAlchemy + Celery broker/backend | `postgresql://postgres:postgres@localhost:5432/app` |

For Binance backfill, optional API credentials (read from process env or from `app/adapters/implementations/.env` if that file exists):

| Variable | Purpose |
|----------|---------|
| `BINANCE_API_KEY` | Binance API key |
| `BINANCE_API_SECRET` | Binance API secret |

## Prerequisites

- **Docker** and **Docker Compose** (recommended stack), or
- **Python 3.12+**, **PostgreSQL** with TimescaleDB (or use Compose for DB only), and dependencies from `requirements.txt` or `pyproject.toml`.

## Run with Docker Compose

Builds the API image, starts TimescaleDB, waits until the database passes its health check, then starts the API.

```bash
docker compose up --build -d
```

- API: [http://localhost:8000](http://localhost:8000) — interactive docs: [http://localhost:8000/docs](http://localhost:8000/docs)
- Database (from the host): `localhost:5432`, database `app`, user/password `postgres` / `postgres`

The API container receives `DATABASE_URL=postgresql://postgres:postgres@db:5432/app` (hostname `db` is the Compose service name).

Stop:

```bash
docker compose down
```

### Celery worker (backfill)

`docker-compose.yml` starts **db** and **api** only. `POST /ohlcv` queues a Celery task; a **worker** must consume it.

Run a worker on the host (same `DATABASE_URL` as the API, pointing at `localhost` if DB is exposed on 5432):

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/app"
celery -A app.celery_app:celery worker --loglevel=info
```

## Project layout (high level)

- `main.py` — FastAPI app and routes
- `app/database/` — SQLAlchemy engine, session, models, Timescale init
- `app/service/` — read path and backfill orchestration
- `app/adapters/` — exchange adapters (Binance implementation under `implementations/`)
- `app/tasks/` — Celery tasks
- `app/celery_app.py` — Celery application instance

## License

Proprietary (see `pyproject.toml`).
