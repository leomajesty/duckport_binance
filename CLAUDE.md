# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DuckPort Binance is a cryptocurrency K-line (candlestick) data service built on Apache Arrow Flight protocol. It downloads historical data from Binance and serves it via gRPC with support for real-time updates via WebSocket or REST API polling.

## Commands

```bash
# Install dependencies (uses uv package manager)
uv sync
source .venv/bin/activate

# Load historical data (required before first server start)
uv run loadhist.py

# Start the Flight server
uv run start_server.py

# Lint code
uv run ruff check .
uv run ruff check --fix .
```

## Architecture

### Data Flow

1. **Historical Data Loading** (`loadhist.py`): Downloads Binance K-line data (ZIP files), extracts and converts to Parquet files, then loads recent data into DuckDB
2. **Flight Server** (`start_server.py`): Serves data via Arrow Flight protocol on port 8815 (configurable)
3. **Real-time Updates**: Either WebSocket listeners (`ENABLE_WS=true`) or REST API polling fetch new K-lines and insert into DuckDB
4. **Query Strategy**: Automatically routes queries to Parquet (historical) or DuckDB (recent) based on time range

### Key Components

- **`flight/flight_server.py`**: Arrow Flight server implementation, handles `do_get`, `list_flights`, `get_schema`, `do_action`
- **`core/flight_func/flight_api.py`**:
  - `FlightActions`: Server actions (ping, ready)
  - `FlightGets`: Data query logic with Parquet/DuckDB/hybrid strategies
- **`core/flight_func/flight_data_jobs.py`**:
  - `DataJobs`: Base class for historical data init and K-line updates
  - `RestfulDataJobs`: Periodic REST API polling for updates
  - `WebsocketsDataJobs`: WebSocket-based real-time updates
- **`utils/db_manager.py`**: Thread-safe DuckDB wrapper with `KlineDBManager` for K-line tables
- **`hist/`**: Historical data download and processing (symbol management, file downloads, Parquet conversion)

### Storage

- **Parquet files** (`data/pqt/`): Historical K-line data partitioned by market and interval (e.g., `usdt_perp_5m/`)
- **DuckDB** (`data/duckdb.db`): Recent K-line data with automatic retention policy (exports to Parquet periodically)

### Supported Markets

- `usdt_perp`: USDT perpetual futures
- `usdt_spot`: USDT spot trading pairs

## Configuration

All configuration is in `config.env`:

- `KLINE_INTERVAL`: K-line period (e.g., `5m`, `15m`) - must be consistent with database
- `FLIGHT_PORT`: Server port (default: 8815)
- `ENABLE_WS`: Use WebSocket mode for real-time data (default: false)
- `RETENTION_DAYS`: Days to keep in DuckDB before exporting to Parquet
- `START_DATE`: Historical data start date filter

## Code Conventions

- All timestamps are in UTC
- K-line interval must be a divisor of 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60 minutes)
- Table names follow pattern: `{market}_{interval}` (e.g., `usdt_perp_5m`)
- Ruff is used for linting with line length 100
