"""
Configuration module for data center
Contains all configuration parameters and constants
"""

import os
import platform
import asyncio
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

from utils.log_kit import logger

# Load environment variables
load_dotenv('config.env')
KLINE_INTERVAL = os.getenv("KLINE_INTERVAL", "5m")  # Default interval for kline data
KLINE_INTERVAL_MINUTES = int(KLINE_INTERVAL.replace('m', ''))
SUFFIX = f"_{KLINE_INTERVAL_MINUTES}m"

# Storage configuration
ENABLE_PQT = os.getenv("ENABLE_PQT", "true").lower() == "true"  # Whether to enable parquet files
if not ENABLE_PQT:
    logger.warning('ENABLE_PQT:false is Deprecated.')

# Server configuration
DUCKDB_DIR = os.getenv("DUCKDB_DIR", "data/duckdb.db")
if not os.path.isabs(DUCKDB_DIR):
    DUCKDB_DIR = os.path.join(os.getcwd(), DUCKDB_DIR)
PARQUET_DIR = os.getenv("PARQUET_DIR", "data/pqt")
if not os.path.isabs(PARQUET_DIR):
    PARQUET_DIR = os.path.join(os.getcwd(), PARQUET_DIR)
RESOURCE_PATH = os.getenv("RESOURCE_PATH", "data/hist")

# Set environment variables
os.environ['NUMEXPR_MAX_THREADS'] = "256"

# Platform specific settings
if platform.system() == 'Windows':
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Concurrency settings
CONCURRENCY = int(os.getenv('CONCURRENCY', 2))
semaphore = asyncio.Semaphore(value=min(CONCURRENCY, 8))
api_semaphore = asyncio.Semaphore(value=min(CONCURRENCY, 3))
FETCH_CONCURRENCY = min(CONCURRENCY, 10)

# Base URLs
BASE_URL = 'https://data.binance.vision/'
root_center_url = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'

# Settled symbols configuration
SETTLED_USDT_PERP_SYMBOLS = {
    'ICPUSDT': ['2022-06-10 09:00:00', '2022-09-27 02:30:00'],
    'BNXUSDT': ['2023-02-11 04:00:00', '2023-02-22 22:45:00'],
    'TLMUSDT': ['2022-06-09 23:59:00', '2023-03-30 12:30:00'],
    'AERGOUSDT': ['2025-03-27 23:59:00', '2023-04-16 18:30:00'],
}

SETTLED_USDT_SPOT_SYMBOLS = {
    "BNXUSDT": ["2023-02-16 03:00:00", "2023-02-22 08:00:00"],
    "BTCSTUSDT": ["2021-03-15 07:00:00", "2021-03-19 07:00:00"],
    "COCOSUSDT": ["2021-01-19 02:00:00", "2021-01-23 02:00:00"],
    "CVCUSDT": ["2022-12-09 03:00:00", "2023-05-12 08:00:00"],
    "DREPUSDT": ["2021-03-29 04:00:00", "2021-04-02 04:00:00"],
    "FTTUSDT": ["2022-11-15 05:00:00", "2023-09-22 08:00:00"],
    "KEYUSDT": ["2023-02-10 03:00:00", "2023-03-10 08:00:00"],
    "LUNAUSDT": ["2022-05-13 01:00:00", "2022-05-31 06:00:00"],
    "QUICKUSDT": ["2023-07-17 03:00:00", "2023-07-21 08:00:00"],
    "STRAXUSDT": ["2024-03-20 03:00:00", "2024-03-28 08:00:00"],
    "SUNUSDT": ["2021-06-14 04:00:00", "2021-06-18 04:00:00"],
    "VIDTUSDT": ["2022-10-31 03:00:00", "2022-11-09 08:00:00"],
    "STX-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"],
    "TIA-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"],
    "VEN-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"],
    "POND-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"]
}

# Delisted symbols
usdt_perp_delist_symbol_set = {
    '1000BTTCUSDT', 'CVCUSDT', 'DODOUSDT', 'RAYUSDT', 'SCUSDT', 'SRMUSDT', 
    'LENDUSDT', 'NUUSDT', 'LUNAUSDT', 'YFIIUSDT', 'BTCSTUSDT'
}

usdt_spot_blacklist = []

# Set settled symbols based on trading type
# SETTLED_SYMBOLS will be set dynamically based on current trade_type
prefix = None  # Will be set dynamically
metrics_prefix = None  # Will be set dynamically
SETTLED_SYMBOLS = None  # Will be set dynamically

# Proxy configuration
proxy = os.getenv('PROXY_URL', '')
use_proxy_download_file = False
file_proxy = proxy if use_proxy_download_file else None

# Market root path
if not os.path.exists(RESOURCE_PATH):
    os.makedirs(RESOURCE_PATH)

# Download settings
retry_times = 10
thunder = True
blind = False

# Convert START_MONTH to START_DATE if specified
GENESIS_TIME = pd.to_datetime('2009-01-03 00:00:00').tz_localize(tz=timezone.utc)
START_DATE = os.getenv("START_DATE", None)  # Format: "YYYY-MM-DD"
START_MONTH = None
if START_DATE:
    START_MONTH = START_DATE[:7]
    START_DATE = datetime.strptime(START_DATE, "%Y-%m-%d").date()
else:
    START_DATE = datetime(2009, 1, 3).date()  # Default start date

# Global sets for tracking
need_analyse_set = set()
daily_updated_set = set()

# Flight server configuration
FLIGHT_PORT = os.getenv("FLIGHT_PORT", "8815")
REDUNDANCY_HOURS = int(os.getenv('REDUNDANCY_HOURS', 1))
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS', 0))
ENABLE_WS = os.getenv("ENABLE_WS", "false").lower() == "true"

# Parquet file configuration
PARQUET_FILE_PERIOD = int(os.getenv('PARQUET_FILE_PERIOD', 1))  # Days per parquet file