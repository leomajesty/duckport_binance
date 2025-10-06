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
from utils.config import (daily_updated_set, RESOURCE_PATH, PARQUET_DIR, START_DATE, ENABLE_PQT, DUCKDB_DIR,
                          PARQUET_FILE_PERIOD, CONCURRENCY)
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

def read_symbol_csv(symbol, zip_path, interval='5m', ydashm=None):
    reg = '-'.join([part for part in [symbol, interval, ydashm] if isinstance(part, str) and part])
    zip_list = glob(os.path.join(zip_path, 'monthly_klines', f'{symbol}/{reg}*.zip'))
    daily_files = glob(os.path.join(zip_path, 'daily_klines', f'{symbol}/{reg}*.zip'))
    if daily_files:
        zip_list.extend(daily_files)

    if not zip_list:
        return pd.DataFrame()

    # 合并monthly daily 数据
    df = pd.concat([pd.read_csv(path_, header=None, encoding="utf-8", compression='zip',
                                         names=['open_time', 'open', 'high', 'low', 'close', 'volume',
                                                'close_time', 'quote_volume', 'trade_num',
                                                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                'ignore']
                                         ) for path_ in zip_list], ignore_index=True)
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

def to_pqt(yms: list[str], interval: str = '5m', market: str = 'usdt_perp'):
    filename = f'{market}_{yms[0]}_{len(yms)}M.parquet'
    latest_pqt = get_latest_parquet(market, interval)
    if os.path.exists(os.path.join(PARQUET_DIR, f'{market}_{interval}', filename)) and filename != latest_pqt:
        logger.info(f'跳过 {filename}')
        return
    logger.info(f'开始转换 {market}_{interval} {yms[0]}数据...')
    data_path = os.path.join(RESOURCE_PATH, f'{market}_{interval}')
    symbols = os.listdir(os.path.join(data_path, 'monthly_klines'))
    dfs = []

    def process_symbol_month(syb, ydashm):
        df = read_symbol_csv(syb, data_path, interval, ydashm)
        if df.empty:
            return None
        df['symbol'] = syb
        df['candle_begin_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)
        return df

    results = Parallel(n_jobs=CONCURRENCY)(
        delayed(process_symbol_month)(syb, ydashm)
        for syb in tqdm(symbols)
        for ydashm in yms
    )
    dfs.extend([df for df in results if df is not None])

    dfs = pd.concat(dfs, ignore_index=True)
    dfs.rename(columns={"candle_begin_time": "open_time"}, inplace=True)
    
    # 确保输出目录存在
    ensure_parquet_directories(market, interval)
    dfs.to_parquet(f'{PARQUET_DIR}/{market}_{interval}/{filename}', index=False)
    logger.info(f'{market}_{interval} {yms[0]}数据转换完成')

def get_available_years_months(period_month: int = 6):
    """
    获取从START_DATE至今的所有年份列表
    return:
    {
        "2021-01": ["2021-01", "2021-02", "2021-03", "2021-04", "2021-05", "2021-06"],
        "2021-07": ["2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12"],
    }
    """
    assert 12 % period_month == 0, "period_month error"
    yms = []
    current_date = datetime.now()
    init_date = pd.to_datetime('2019-01-01')

    current = init_date.replace(day=1)  # 从月初开始

    while current <= current_date:
        yms.append(f'{current.year}-{current.month:02d}')
        # 移动到下个月
        if current.month + period_month > 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + period_month)

    first = f'{START_DATE.year}-{START_DATE.month:02d}'
    yms = [[ym for ym in yms if ym <= first][-1]] + [ym for ym in yms if ym > first]
    yms_dict = {}
    for ym in yms:
        month = int(ym.split('-')[1])
        yms_values = []
        for range_ym in range(month, month + period_month):
            yms_value = f'{ym.split('-')[0]}-{range_ym:02d}'
            if yms_value >= first:
                yms_values.append(yms_value)
        yms_dict[ym] = yms_values

    return yms_dict

def get_latest_parquet(market: str, interval: str = '5m'):
    """根据文件名获取现有parquet文件的最新时间"""
    parquet_dir = os.path.join(PARQUET_DIR, f'{market}_{interval}')
    if not os.path.exists(parquet_dir):
        return None
    
    # 获取所有parquet文件
    parquet_files = glob(os.path.join(parquet_dir, '*.parquet'))
    if not parquet_files:
        return None

    files = [os.path.basename(parquet_file) for parquet_file in parquet_files]

    return max(files)

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

def to_duckdb(yms: list[str], interval: str = '5m', market: str = 'usdt_perp'):
    """将指定年份/月份的数据写入duckdb数据库"""
    data_path = os.path.join(RESOURCE_PATH, f'{market}_{interval}/monthly_klines')
    symbols = os.listdir(data_path)
    
    # 确保数据库表结构存在
    ensure_duckdb_tables(market, interval)
    
    db_path = get_duckdb_path()
    with DatabaseManager(db_path) as db:
        for ydashm in yms:
            year = ydashm.split('-')[0]
            month = ydashm.split('-')[1]
            # 删除该年份/月份的旧数据（如果存在）
            delete_sql = f"DELETE FROM {market}_{interval} WHERE EXTRACT(YEAR FROM open_time) = ? AND EXTRACT(MONTH FROM open_time) = ?"
            db.execute_query(delete_sql, (year, month))
            logger.info(f'已清理 {market}_{interval} {year}年{month}月的旧数据')

            total_rows = 0
            for syb in tqdm(symbols, desc=f'写入 {market}_{interval} {year}年{month}月数据'):
                df = read_symbol_csv(syb, data_path, interval, ydashm)
                if df.empty:
                    logger.warning(f'No data for {syb} in {ydashm}')
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

            logger.info(f'{market}_{interval} {year}年{month}月数据写入完成，共 {total_rows} 行 {ydashm}数据写入完成，共 {total_rows} 行')

def batch_write_to_duckdb(market: str, interval: str = '5m'):
    """批量将指定市场和间隔的所有可用数据写入duckdb数据库"""
    logger.info(f'开始批量写入 {market}_{interval} 数据到duckdb数据库...')
    
    # 确保数据库表结构存在
    ensure_duckdb_tables(market, interval)
    
    # 获取可用年份
    available_yms = get_available_years_months(1)
    if not available_yms:
        logger.warning(f'{market}_{interval} 没有检测到可用数据')
        return
    
    success_count = 0
    error_count = 0
    
    for k, v in available_yms.items():
        try:
            logger.info(f'开始写入 {market}_{interval} {k}数据到duckdb...')
            to_duckdb(v, interval=interval, market=market)
            success_count += 1
            logger.info(f'{market}_{interval} {k}数据写入duckdb完成')
        except Exception as e:
            error_count += 1
            logger.error(f'{market}_{interval} {k}数据写入duckdb失败: {e}')
    
    logger.info(f'{market}_{interval} 批量写入duckdb完成: 成功 {success_count} 个, 失败 {error_count} 个')
    return success_count, error_count

def batch_convert_to_parquet(market: str, interval: str = '5m'):
    """批量转换指定市场和间隔的所有可用数据为parquet格式"""
    logger.info(f'开始批量转换 {market}_{interval} 数据为parquet格式...')
    
    # 确保输出目录存在
    ensure_parquet_directories(market, interval)
    
    available_yms = get_available_years_months(PARQUET_FILE_PERIOD)
    if not available_yms:
        logger.warning(f'{market}_{interval} 没有检测到可用数据')
        return
    
    success_count = 0
    error_count = 0

    for k, v in available_yms.items():
        try:
            to_pqt(v, interval=interval, market=market)
            success_count += 1
        except Exception as e:
            error_count += 1
            logger.error(f'{market}_{interval} {k}数据转换失败: {e}')

    logger.info(f'{market}_{interval} 批量转换完成: 成功 {success_count} 个, 失败 {error_count} 个')
    return success_count, error_count

def batch_process_data(market: str, interval: str = '5m'):
    """根据配置批量处理数据：parquet转换或duckdb写入"""
    if ENABLE_PQT:
        logger.info(f'配置启用parquet文件，开始转换 {market}_{interval} 数据...')
        return batch_convert_to_parquet(market, interval)
    else:
        logger.info(f'配置禁用parquet文件，开始写入 {market}_{interval} 数据到duckdb...')
        return batch_write_to_duckdb(market, interval)
