"""
File management module for data center
Handles file paths, directory creation and cleanup
"""

import os
from datetime import *
from glob import glob

from dateutil.relativedelta import relativedelta
from numpy import int64
from joblib import Parallel, delayed
from itertools import groupby
from operator import itemgetter

from tqdm import tqdm

from utils.log_kit import logger
import pandas as pd
from utils.config import (daily_updated_set, RESOURCE_PATH, PARQUET_DIR, START_DATE, ENABLE_PQT, DUCKDB_DIR)
from utils.db_manager import DatabaseManager


def get_local_path(root_path, trading_type, market_data_type, time_period, symbol, interval='5m'):
    """Build local file path for different data types"""
    trade_type_folder = trading_type + '_' + interval
    path = os.path.join(root_path, trade_type_folder, f'{time_period}_{market_data_type}')

    if symbol:
        path = os.path.join(path, symbol.upper())
    return path


def clean_old_daily_zip(local_daily_path, symbols, interval):
    """Clean old daily zip files"""
    today = date.today()
    this_month_first_day = date(today.year, today.month, 1)
    daily_end = this_month_first_day - relativedelta(months=1)

    for symbol in symbols:
        local_daily_symbol_path = os.path.join(local_daily_path, symbol)
        if os.path.exists(local_daily_symbol_path):
            zip_file_path = os.path.join(local_daily_symbol_path, "{}-{}-{}.zip".format(symbol.upper(), interval, daily_end))
            for item in os.listdir(local_daily_symbol_path):
                item_path = os.path.join(local_daily_symbol_path, item)
                if item_path < zip_file_path:
                    os.remove(item_path)
            if not os.listdir(local_daily_symbol_path):
                # 删除空文件夹，即已下架的币种
                os.rmdir(local_daily_symbol_path)


def ensure_directory_exists(path):
    """Ensure directory exists, create if not"""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def get_file_size(file_path):
    """Get file size in bytes"""
    if os.path.exists(file_path):
        return os.path.getsize(file_path)
    return 0


def is_file_complete(file_path, expected_size=None):
    """Check if file download is complete"""
    if not os.path.exists(file_path):
        return False
    
    if expected_size is not None:
        actual_size = get_file_size(file_path)
        return actual_size == expected_size
    
    # Basic check: file exists and has some content
    return get_file_size(file_path) > 0

def transfer_daily_to_monthly(daily_list, need_analyse_set):
    sorted_list = sorted(daily_list, key=lambda x: x['local_path'], reverse=False)

    tasks = []

    num = 0
    logger.info(f'开始合并，数量为{len(sorted_list)}')
    for local_path, items in groupby(sorted_list, key=itemgetter('local_path')):
        zip_files = []
        day_set = set()
        interval = ''
        symbol = os.path.basename(local_path)
        monthly_path = local_path.replace('daily_', 'monthly_')
        for i in items:
            interval = i['interval']
            zip_files.append(os.path.join(i['local_path'], os.path.basename(i['key'])[0:-9]))
            _day = datetime.strptime(i['key'][-23:-13], "%Y-%m-%d")
            day_set.add(_day.toordinal())

        days = pd.DataFrame(sorted(list(day_set)))
        if len(days.diff().value_counts()) > 1:
            # daily zip缺失某天或某几天的zip
            need_analyse_set.add(monthly_path)
        df_latest = pd.concat(Parallel(4)(
            delayed(pd.read_csv)(path_, header=None, encoding="utf-8", compression='zip') for path_ in zip_files),
            ignore_index=True)
        df_latest = df_latest[df_latest[0] != 'open_time']
        df_latest = df_latest.astype(dtype={0: int64})
        df_latest.sort_values(by=0)
        latest_monthly_zip = os.path.join(monthly_path, f'{symbol}-{interval}-latest.zip')
        if local_path in daily_updated_set or not os.path.exists(latest_monthly_zip) or max((os.path.getmtime(file) for file in zip_files)) > os.path.getmtime(latest_monthly_zip):
            if not os.path.exists(monthly_path):
                os.makedirs(monthly_path)
            compression_options = dict(method='zip', archive_name=f'{symbol}-{interval}-latest.csv')
            df_latest.to_csv(latest_monthly_zip, header=None, index=None, compression=compression_options)

        num += 1
    logger.info('合并结束')
    return tasks

def read_symbol_csv(symbol, zip_path, interval='5m', year=None, month=None):
    reg = '-'.join([part for part in [symbol, interval, year, month] if isinstance(part, str) and part])
    zip_list = glob(os.path.join(zip_path, f'{symbol}/{reg}*.zip'))
    if int(year) == datetime.now().year:
        recent_data = [os.path.join(zip_path, f'{symbol}/{symbol}-{interval}-latest.zip')]
        recent_data = [path for path in recent_data if os.path.exists(path)]
        if recent_data:
            zip_list.extend(recent_data)

    if not zip_list:
        return pd.DataFrame()

    # 合并monthly daily 数据
    df = pd.concat(
        Parallel(4)(delayed(pd.read_csv)(path_, header=None, encoding="utf-8", compression='zip',
                                         names=['open_time', 'open', 'high', 'low', 'close', 'volume',
                                                'close_time', 'quote_volume', 'trade_num',
                                                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                'ignore']
                                         ) for path_ in zip_list), ignore_index=True)
    # 过滤表头行
    df = df[df['open_time'] != 'open_time']
    # 规范数据类型，防止计算avg_price报错
    df = df.astype(
        dtype={'open_time': int64, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float,
               'quote_volume': float,
               'trade_num': int, 'taker_buy_base_asset_volume': float, 'taker_buy_quote_asset_volume': float})
    df['avg_price'] = df['quote_volume'] / df['volume']  # 增加 均价
    # 如果open_time大于13位，则截断为13位
    df['open_time'] = df['open_time'].apply(lambda x: int(str(x)[0:13]))
    df.drop(columns=['close_time', 'ignore'], inplace=True)
    df.sort_values(by='open_time', inplace=True)  # 排序
    df.drop_duplicates(subset=['open_time'], inplace=True, keep='last')  # 去除重复值
    df.reset_index(drop=True, inplace=True)  # 重置index
    return df

def to_pqt(year: int, interval: str = '5m', market: str = 'usdt_perp'):
    data_path = os.path.join(RESOURCE_PATH, f'{market}_{interval}/monthly_klines')
    symbols = os.listdir(data_path)
    dfs = []
    for syb in tqdm(symbols):
        df = read_symbol_csv(syb, data_path, interval, str(year))
        if df.empty:
            logger.warning(f'No data for {syb} in {year}')
            continue
        df['symbol'] = syb
        df['candle_begin_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)
        dfs.append(df)

    dfs = pd.concat(dfs, ignore_index=True)
    dfs = dfs[dfs['candle_begin_time'] <= dfs['candle_begin_time'].max() - timedelta(days=1)]  # 确保数据完整性
    dfs.rename(columns={"candle_begin_time": "open_time"}, inplace=True)
    
    # 确保输出目录存在
    ensure_parquet_directories(market, interval)
    dfs.to_parquet(f'{PARQUET_DIR}/{market}_{interval}/{market}_{year}.parquet', index=False)


def get_available_years(market: str, interval: str = '5m'):
    """获取从START_DATE至今的所有年份列表"""
    current_year = datetime.now().year
    start_year = START_DATE.year
    
    years = list(range(start_year, current_year + 1))
    logger.info(f'{market}_{interval} 配置年份范围: {START_DATE} 至今，共 {len(years)} 年: {years}')
    return years

def ensure_parquet_directories(market: str, interval: str = '5m'):
    """确保parquet输出目录存在"""
    parquet_dir = os.path.join(PARQUET_DIR, f'{market}_{interval}')
    if not os.path.exists(parquet_dir):
        os.makedirs(parquet_dir, exist_ok=True)
        logger.info(f'创建parquet目录: {parquet_dir}')
    return parquet_dir

def get_duckdb_path():
    """获取duckdb数据库文件路径"""
    return DUCKDB_DIR

def ensure_duckdb_tables(market: str, interval: str = '5m'):
    """确保duckdb表结构存在"""
    db_path = get_duckdb_path()
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.info(f'创建duckdb目录: {db_dir}')
    
    with DatabaseManager(db_path) as db:
        # 创建表结构
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {market}_{interval} (
            open_time TIMESTAMP,
            symbol VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trade_num BIGINT,
            taker_buy_base_asset_volume DOUBLE,
            taker_buy_quote_asset_volume DOUBLE,
            avg_price DOUBLE,
            PRIMARY KEY (open_time, symbol)
        )
        """
        db.execute_query(create_table_sql)
        
        logger.info(f'duckdb表 {market}_{interval} 结构已确保存在')

def to_duckdb(year: int, interval: str = '5m', market: str = 'usdt_perp'):
    """将指定年份的数据写入duckdb数据库"""
    data_path = os.path.join(RESOURCE_PATH, f'{market}_{interval}/monthly_klines')
    symbols = os.listdir(data_path)
    
    # 确保数据库表结构存在
    ensure_duckdb_tables(market, interval)
    
    db_path = get_duckdb_path()
    with DatabaseManager(db_path) as db:
        # 删除该年份的旧数据（如果存在）
        delete_sql = f"DELETE FROM {market}_{interval} WHERE EXTRACT(YEAR FROM open_time) = ?"
        db.execute_query(delete_sql, (year,))
        logger.info(f'已清理 {market}_{interval} {year}年的旧数据')
        
        total_rows = 0
        for syb in tqdm(symbols, desc=f'写入 {market}_{interval} {year}年数据'):
            df = read_symbol_csv(syb, data_path, interval, str(year))
            if df.empty:
                logger.warning(f'No data for {syb} in {year}')
                continue
            
            # 准备数据
            df['symbol'] = syb
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            
            # 按时间排序
            df = df.sort_values('open_time')
            df = df[['open_time', 'symbol', 'open', 'high', 'low', 'close', 'volume',
                     'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                     'taker_buy_quote_asset_volume', 'avg_price']]
            
            # 写入数据库
            insert_sql = f"""INSERT INTO {market}_{interval} SELECT * FROM df"""
            db.execute_write(insert_sql, df=df)

            total_rows += len(df)
        
        logger.info(f'{market}_{interval} {year}年数据写入完成，共 {total_rows} 行')

def batch_write_to_duckdb(market: str, interval: str = '5m'):
    """批量将指定市场和间隔的所有可用年份数据写入duckdb数据库"""
    logger.info(f'开始批量写入 {market}_{interval} 数据到duckdb数据库...')
    
    # 确保数据库表结构存在
    ensure_duckdb_tables(market, interval)
    
    # 获取可用年份
    available_years = get_available_years(market, interval)
    if not available_years:
        logger.warning(f'{market}_{interval} 没有检测到可用年份数据')
        return
    
    # 批量写入每个年份
    success_count = 0
    error_count = 0
    
    for year in available_years:
        try:
            logger.info(f'开始写入 {market}_{interval} {year}年数据到duckdb...')
            to_duckdb(year, interval, market)
            success_count += 1
            logger.info(f'{market}_{interval} {year}年数据写入duckdb完成')
        except Exception as e:
            error_count += 1
            logger.error(f'{market}_{interval} {year}年数据写入duckdb失败: {e}')
    
    logger.info(f'{market}_{interval} 批量写入duckdb完成: 成功 {success_count} 个年份, 失败 {error_count} 个年份')
    return success_count, error_count

def batch_convert_to_parquet(market: str, interval: str = '5m'):
    """批量转换指定市场和间隔的所有可用年份数据为parquet格式"""
    logger.info(f'开始批量转换 {market}_{interval} 数据为parquet格式...')
    
    # 确保输出目录存在
    ensure_parquet_directories(market, interval)
    
    # 获取可用年份
    available_years = get_available_years(market, interval)
    if not available_years:
        logger.warning(f'{market}_{interval} 没有检测到可用年份数据')
        return
    
    # 批量转换每个年份
    success_count = 0
    error_count = 0
    
    for year in available_years:
        try:
            logger.info(f'开始转换 {market}_{interval} {year}年数据...')
            to_pqt(year, interval, market)
            success_count += 1
            logger.info(f'{market}_{interval} {year}年数据转换完成')
        except Exception as e:
            error_count += 1
            logger.error(f'{market}_{interval} {year}年数据转换失败: {e}')
    
    logger.info(f'{market}_{interval} 批量转换完成: 成功 {success_count} 个年份, 失败 {error_count} 个年份')
    return success_count, error_count

def batch_process_data(market: str, interval: str = '5m'):
    """根据配置批量处理数据：parquet转换或duckdb写入"""
    if ENABLE_PQT:
        logger.info(f'配置启用parquet文件，开始转换 {market}_{interval} 数据...')
        return batch_convert_to_parquet(market, interval)
    else:
        logger.info(f'配置禁用parquet文件，开始写入 {market}_{interval} 数据到duckdb...')
        return batch_write_to_duckdb(market, interval)