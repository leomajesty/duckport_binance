"""
Simplified Data Center for Binance Data
Focuses only on data fetching and downloading functionality
Downloads usdt_perp data first, then usdt_spot data
"""

import asyncio
from datetime import datetime, timedelta
import os.path
import random
from glob import glob
import aiohttp

import duckdb
import pandas as pd
from tqdm import tqdm

from utils.config import KLINE_INTERVAL, DUCKDB_DIR, PARQUET_DIR
from utils.date_partition import get_parquet_cutoff_date
from utils.db_manager import KlineDBManager
from utils.log_kit import logger, divider

from hist import (
    # Config
    proxy, RESOURCE_PATH, need_analyse_set, START_MONTH,

    # Symbol management
    async_get_usdt_symbols, spot_symbols_filter,

    # Data listing
    async_get_daily_list, async_get_monthly_list,
    get_download_prefix,

    # Downloading
    async_download_file,

    # File management
    get_local_path, clean_old_daily_zip,

    # Data processing
    batch_process_data, get_latest_parquet
)
from utils.timer import timer


async def ping():
    """
    对 fapi.binance.com 进行联通测试
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url='https://fapi.binance.com/fapi/v1/ping', proxy=proxy, timeout=5) as response:
                t = await response.text()
                if t == '{}':
                    logger.info('币安接口已连通')
                else:
                    logger.info('币安接口fapi.binance.com连接异常，请检查网络配置后重新运行')
                    exit(0)
        except Exception as e:
            logger.info('币安接口fapi.binance.com无法连接，请检查网络配置后重新运行', e)
            exit(0)


def download_trade_type_data(trade_type, start_time, interval='5m'):
    """Download data for a specific trade type (usdt_perp or usdt_spot)"""
    logger.info(f'开始下载 {trade_type} 数据...')

    # Get symbols
    params = {
        'delimiter': '/',
        'prefix': get_download_prefix(trade_type, 'klines', 'daily', None, interval)
    }
    symbols = async_get_usdt_symbols(params)
    if trade_type == 'usdt_spot':
        symbols = spot_symbols_filter(symbols)
    logger.info(f'{trade_type} usdt交易对数量: {len(symbols)}')

    # Network check for usdt_perp
    if trade_type == 'usdt_perp':
        asyncio.run(ping())

    # Get data lists
    logger.info(f'开始获取 {trade_type} 数据目录')
    daily_list = async_get_daily_list(RESOURCE_PATH, symbols, trade_type, 'klines', interval)
    logger.info(f'{trade_type} daily zip num in latest 2 months ={len(daily_list)}')

    monthly_list = async_get_monthly_list(RESOURCE_PATH, symbols, trade_type, 'klines', interval)
    logger.info(f'{trade_type} monthly zip num ={len(monthly_list)}')

    # Combine all lists
    all_list = monthly_list + daily_list
    random.shuffle(all_list)  # 打乱monthly和daily的顺序，合理利用网络带宽

    get_time = datetime.now()
    logger.info(f'{trade_type} 所有数据包个数为 {len(all_list)}，获取目录耗费 {(get_time - start_time).seconds} s')

    # Clean old daily data
    logger.info(f'开始清理 {trade_type} daily旧数据...')
    clean_old_daily_zip(get_local_path(RESOURCE_PATH, trade_type, 'klines', 'daily', None, interval), symbols, interval)
    logger.info(f'{trade_type} 清理完成')

    # Download files
    logger.info(f'开始下载 {trade_type} 文件:')
    error_info_list = set()
    async_download_file(all_list, error_info_list)
    logger.info(f'{trade_type} need analyse: {need_analyse_set}')

    if len(error_info_list) > 0:
        logger.info(f'{trade_type} 下载过程发生错误，已完成重试下载，请核实')
        logger.info(error_info_list)

    end_time = datetime.now()
    logger.info(f'{trade_type} download end cost {(end_time - get_time).seconds} s = {(end_time - get_time).seconds / 60} min')

    history_to_storage(trade_type, interval)

    return end_time

def history_to_storage(trade_type, interval='5m'):
    # 根据配置处理数据：parquet转换或duckdb写入
    logger.info(f'开始处理 {trade_type} 数据...')
    try:
        success_count, error_count = batch_process_data(trade_type, interval)
        if error_count > 0:
            logger.warning(f'{trade_type} 数据初始化完成，但有 {error_count} 个文件转换失败')
        else:
            logger.info(f'{trade_type} 数据初始化完成，所有数据写入成功')
    except Exception as e:
        logger.error(f'{trade_type} 数据初始化过程中发生错误: {e}')


def run(interval='5m'):
    """Main execution function"""
    # Display START_MONTH configuration
    if START_MONTH:
        logger.info(f'数据过滤配置: 仅下载 {START_MONTH} 之后的数据')
    else:
        logger.info('数据过滤配置: 下载所有历史数据')
    
    start_time = datetime.now()
    
    # Download usdt_perp data first
    logger.info('=== 第一阶段：下载期货数据 ===')
    usdt_perp_end_time = download_trade_type_data('usdt_perp', start_time, interval)
    
    # Download usdt_spot data second
    logger.info('=== 第二阶段：下载现货数据 ===')
    usdt_spot_end_time = download_trade_type_data('usdt_spot', start_time, interval)
    
    # Final summary
    total_end_time = datetime.now()
    logger.info(f'期货数据下载耗时: {(usdt_perp_end_time - start_time).seconds / 60:.2f} 分钟')
    logger.info(f'现货数据下载耗时: {(usdt_spot_end_time - usdt_perp_end_time).seconds / 60:.2f} 分钟')
    logger.info(f'总耗时: {(total_end_time - start_time).seconds / 60:.2f} 分钟')

def get_all_trading_range(market: str, conn: duckdb.DuckDBPyConnection):
    res = conn.execute(f"""
        SELECT symbol, min(open_time) as first_candle, max(open_time) as last_candle FROM read_parquet('data/pqt/{market}_{KLINE_INTERVAL}/*.parquet') where volume > 0 group by symbol 
    """)
    return res.df()

def find_useless_symbols(market: str, conn: duckdb.DuckDBPyConnection):
    res = conn.execute(f"""
        SELECT symbol, sum(volume) FROM read_parquet('data/pqt/{market}_{KLINE_INTERVAL}/*.parquet') group by symbol having sum(volume) = 0 
    """)
    return res.df()

# 移除所有在交易时间以外的数据
def remove_out_of_trading_time(market: str, conn: duckdb.DuckDBPyConnection):
    trading_range = get_all_trading_range(market, conn)
    useless_symbols = find_useless_symbols(market, conn)
    files = glob(f'data/pqt/{market}_{KLINE_INTERVAL}/*.parquet')
    for file in tqdm(files, desc=f'处理{market}数据'):
        df = pd.read_parquet(file)
        df = df.merge(trading_range, on='symbol', how='left')
        df = df[df['open_time'] >= df['first_candle']]
        df = df[df['open_time'] <= df['last_candle']]
        if len(useless_symbols) > 0:
            df = df[~df['symbol'].isin(useless_symbols['symbol'])]
        df = df.drop(columns=['first_candle', 'last_candle'])
        df.to_parquet(file, index=False)

def cleaning():
    con = duckdb.connect(DUCKDB_DIR)
    for market in ['usdt_perp', 'usdt_spot']:
        remove_out_of_trading_time(market, conn=con)
    con.close()

def save_latest_parquet_in_duckdb():
    db_manager = KlineDBManager(database_path=DUCKDB_DIR, new=True)
    for market in ['usdt_perp', 'usdt_spot']:
        try:
            file_name = get_latest_parquet(market, KLINE_INTERVAL)
            file_path = os.path.join(PARQUET_DIR, f'{market}_{KLINE_INTERVAL}', file_name)
            pqt_glob = f"{PARQUET_DIR}/{market}_{KLINE_INTERVAL}/{market}_*.parquet"
            cutoff = get_parquet_cutoff_date(current_date=datetime.now(), days_before=30)
            cutoff = cutoff - timedelta(days=1)

            with timer(f"写入 {market}_{KLINE_INTERVAL} 数据"):
                max_time = db_manager.fetch_one(f"""select max(open_time) from read_parquet('{file_path}')""")[0]
                db_manager.execute_write("""INSERT OR REPLACE INTO config_dict (key, value) VALUES (?, ?)""",
                                         (f'{market}_duck_time', max_time.strftime("%Y-%m-%d %H:%M:%S")))
                db_manager.execute_write(f"""
                        insert into {market}_{KLINE_INTERVAL} 
                        select 
                            open_time, symbol, open, high, low, close,
                            volume, quote_volume, trade_num,
                            taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
                            avg_price from read_parquet('{pqt_glob}') where open_time >= '{cutoff:%Y-%m-%d %H:%M:%S}' on conflict do nothing""")

            logger.info(f"清除 {file_name} 文件")
            os.remove(file_path)
        except Exception as e:
            logger.error(f"{market} 转换失败, 未知异常: {e}")
    db_manager.close()

if __name__ == "__main__":
    divider('loading history data')

    run(KLINE_INTERVAL)

    cleaning()

    save_latest_parquet_in_duckdb()