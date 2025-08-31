"""
Data listing module for data center
Handles getting lists of data files to download
"""

import asyncio
import datetime
import time
import aiohttp
from dateutil.relativedelta import relativedelta
from lxml import objectify
from utils.log_kit import logger
from utils.config import metrics_prefix, START_DATE
from .symbol_manager import request_session, request_session_4_list
from .file_manager import get_local_path


def get_download_prefix(trading_type, market_data_type, time_period, symbol, interval):
    """Build download prefix for different data types"""
    trading_type_path = 'data/spot'
    if trading_type == 'usdt_perp':
        trading_type_path = 'data/futures/um'
    
    # Handle case where symbol is None (for getting general prefix)
    if symbol is None:
        return f'{trading_type_path}/{time_period}/{market_data_type}/'
    
    return f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/'


def is_after_start_date(file_date_str):
    """Check if file date is after START_DATE"""
    if START_DATE is None:
        return True  # No filtering, download all data
    
    try:
        # Extract date from filename (e.g., "BTCUSDT-5m-2024-01-01.zip")
        # Look for date pattern YYYY-MM-DD in filename
        import re
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', file_date_str)
        if date_match:
            file_date = datetime.datetime.strptime(date_match.group(1), "%Y-%m-%d").date()
            return file_date >= START_DATE
        return True  # If can't parse date, download anyway
    except Exception:
        return True  # If error parsing, download anyway


def async_get_daily_list(download_folder, symbols, trading_type, data_type, interval):
    """Get daily data file list synchronously"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(download_daily_list(download_folder, symbols, trading_type, data_type, interval))


async def download_daily_list(download_folder, symbols, trading_type, data_type, interval):
    """Get daily data file list asynchronously"""
    today = datetime.date.today()
    this_month_first_day = datetime.date(today.year, today.month, 1)
    daily_end = max((this_month_first_day - relativedelta(months=1)), START_DATE)
    logger.info(f"Daily end date for filtering: {daily_end}")

    result = []
    param_list = []
    for symbol in symbols:
        daily_prefix = get_download_prefix(trading_type, data_type, 'daily', symbol, interval)
        checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, daily_end - relativedelta(days=1))
        first_checksum_file_uri = '{}{}'.format(daily_prefix, checksum_file_name)
        param = {
            'delimiter': '/',
            'prefix': daily_prefix,
            'marker': first_checksum_file_uri
        }
        param_list.append(param)
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        tasks = [asyncio.create_task(request_session(session, p)) for p in param_list]
        await asyncio.wait(tasks)

    for task in tasks:
        data = task.result()
        root = objectify.fromstring(data.encode('ascii'))
        if getattr(root, 'Contents', None) is None:
            continue
        symbol = root.Prefix.text.split('/')[-3]
        local_path = get_local_path(download_folder, trading_type, data_type, 'daily', symbol, interval)
        for item in root.Contents:
            key = item.Key.text
            if key.endswith('CHECKSUM'):
                # Apply START_DATE filtering
                if is_after_start_date(key):
                    struct_time = time.strptime(item.LastModified.text, '%Y-%m-%dT%H:%M:%S.%fZ')
                    _tmp = {
                        'key': key,
                        'last_modified': time.mktime(struct_time),
                        'local_path': local_path,
                        'interval': interval
                    }
                    result.append(_tmp)
                else:
                    logger.debug(f"Skipping {key} (before START_DATE)")
    return result


def async_get_monthly_list(download_folder, symbols, trading_type, data_type, interval):
    """Get monthly data file list synchronously"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        build_download_monthly_list(download_folder, symbols, trading_type, data_type, interval))


async def build_download_monthly_list(download_folder, symbols, trading_type, data_type, interval):
    """Get monthly data file list asynchronously"""
    today = datetime.date.today()
    this_month_first_day = datetime.date(today.year, today.month, 1)
    daily_end = this_month_first_day - relativedelta(months=2)
    end_month = str(daily_end)[0:-3]
    start_month = str(START_DATE)[0:-3]

    param_list = []
    for symbol in symbols:
        monthly_prefix = get_download_prefix(trading_type, data_type, 'monthly', symbol, interval)
        param = {
            'delimiter': '/',
            'prefix': monthly_prefix
        }
        param_list.append(param)

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        tasks = [asyncio.create_task(request_session(session, p)) for p in param_list]
        await asyncio.wait(tasks)

    result = []
    for task in tasks:
        data = task.result()
        root = objectify.fromstring(data.encode('ascii'))
        if getattr(root, 'Contents', None) is None:
            continue
        symbol = root.Prefix.text.split('/')[-3]
        local_path = get_local_path(download_folder, trading_type, data_type, 'monthly', symbol, interval)
        for item in root.Contents:
            key = item.Key.text
            if key.endswith('CHECKSUM') and (key[-20:-13] <= end_month) and (key[-20:-13] >= start_month):
                # Apply START_DATE filtering
                if is_after_start_date(key):
                    struct_time = time.strptime(item.LastModified.text, '%Y-%m-%dT%H:%M:%S.%fZ')
                    _tmp = {
                        'key': key,
                        'last_modified': time.mktime(struct_time),
                        'local_path': local_path
                    }
                    result.append(_tmp)
                else:
                    logger.debug(f"Skipping {key} (before START_DATE)")
    return result


def async_get_metrics_list(download_folder, symbols, trading_type, data_type, interval):
    """Get metrics data file list synchronously"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        build_download_metrics_list(download_folder, symbols, trading_type, data_type, interval))


async def build_download_metrics_list(download_folder, symbols, trading_type, data_type, interval):
    """Get metrics data file list asynchronously"""
    param_list = []
    for symbol in symbols:
        symbol_prefix = f'{metrics_prefix}{symbol}/'
        param = {
            'delimiter': '/',
            'prefix': symbol_prefix
        }
        param_list.append(param)

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        tasks = [asyncio.create_task(request_session_4_list(session, p)) for p in param_list]
        await asyncio.wait(tasks)

    result = []
    for task in tasks:
        datas = task.result()
        for root in datas:
            if getattr(root, 'Contents', None) is None:
                continue
            symbol = root.Prefix.text.split('/')[-2]
            local_path = get_local_path(download_folder, trading_type, data_type, 'daily', symbol, interval)
            for item in root.Contents:
                key = item.Key.text
                if key.endswith('CHECKSUM'):
                    # Apply START_DATE filtering
                    if is_after_start_date(key):
                        struct_time = time.strptime(item.LastModified.text, '%Y-%m-%dT%H:%M:%S.%fZ')
                        _tmp = {
                            'key': key,
                            'last_modified': time.mktime(struct_time),
                            'local_path': local_path
                        }
                        result.append(_tmp)
                    else:
                        logger.debug(f"Skipping {key} (before START_DATE)")
    return result
