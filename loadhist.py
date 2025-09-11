"""
Simplified Data Center for Binance Data
Focuses only on data fetching and downloading functionality
Downloads usdt_perp data first, then usdt_spot data
"""

import asyncio
import datetime
import random

import duckdb

from data_cleaning import remove_out_of_trading_time, cleaning
from utils.config import KLINE_INTERVAL, ENABLE_PQT, DUCKDB_DIR
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
    get_local_path, clean_old_daily_zip, transfer_daily_to_monthly,

    # Data processing
    batch_process_data
)


async def ping():
    """
    对 fapi.binance.com 进行联通测试
    """
    import aiohttp
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
        asyncio.get_event_loop().run_until_complete(ping())

    # Get data lists
    logger.info(f'开始获取 {trade_type} 数据目录')
    daily_list = async_get_daily_list(RESOURCE_PATH, symbols, trade_type, 'klines', interval)
    logger.info(f'{trade_type} daily zip num in latest 2 months ={len(daily_list)}')

    monthly_list = async_get_monthly_list(RESOURCE_PATH, symbols, trade_type, 'klines', interval)
    logger.info(f'{trade_type} monthly zip num ={len(monthly_list)}')

    # Combine all lists
    all_list = monthly_list + daily_list
    random.shuffle(all_list)  # 打乱monthly和daily的顺序，合理利用网络带宽

    get_time = datetime.datetime.now()
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

    end_time = datetime.datetime.now()
    logger.info(f'{trade_type} download end cost {(end_time - get_time).seconds} s = {(end_time - get_time).seconds / 60} min')

    tasks = transfer_daily_to_monthly(daily_list, need_analyse_set)
    if len(tasks) > 0:
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))

    history_to_storage(trade_type, interval)

    return end_time

def history_to_storage(trade_type, interval='5m'):
    # 根据配置处理数据：parquet转换或duckdb写入
    logger.info(f'开始处理 {trade_type} 数据...')
    try:
        success_count, error_count = batch_process_data(trade_type, interval)
        if error_count > 0:
            if ENABLE_PQT:
                logger.warning(f'{trade_type} parquet转换完成，但有 {error_count} 个年份转换失败')
            else:
                logger.warning(f'{trade_type} duckdb写入完成，但有 {error_count} 个年份写入失败')
        else:
            if ENABLE_PQT:
                logger.info(f'{trade_type} parquet转换完成，所有年份转换成功')
            else:
                logger.info(f'{trade_type} duckdb写入完成，所有年份写入成功')
    except Exception as e:
        if ENABLE_PQT:
            logger.error(f'{trade_type} parquet转换过程中发生错误: {e}')
        else:
            logger.error(f'{trade_type} duckdb写入过程中发生错误: {e}')


def run(interval='5m'):
    """Main execution function"""
    # Display START_MONTH configuration
    if START_MONTH:
        logger.info(f'数据过滤配置: 仅下载 {START_MONTH} 之后的数据')
    else:
        logger.info('数据过滤配置: 下载所有历史数据')
    
    start_time = datetime.datetime.now()
    
    # Download usdt_perp data first
    logger.info('=== 第一阶段：下载期货数据 ===')
    usdt_perp_end_time = download_trade_type_data('usdt_perp', start_time, interval)
    
    # Download usdt_spot data second
    logger.info('=== 第二阶段：下载现货数据 ===')
    usdt_spot_end_time = download_trade_type_data('usdt_spot', start_time, interval)
    
    # Final summary
    total_end_time = datetime.datetime.now()
    logger.info(f'期货数据下载耗时: {(usdt_perp_end_time - start_time).seconds / 60:.2f} 分钟')
    logger.info(f'现货数据下载耗时: {(usdt_spot_end_time - usdt_perp_end_time).seconds / 60:.2f} 分钟')
    logger.info(f'总耗时: {(total_end_time - start_time).seconds / 60:.2f} 分钟')


if __name__ == "__main__":
    divider('loading history data')

    run(KLINE_INTERVAL)

    cleaning()