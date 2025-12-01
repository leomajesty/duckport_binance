#!/usr/bin/env python3
"""
Flight服务器演示脚本
展示如何使用Arrow Flight协议进行数据查询和传输
"""

from datetime import datetime, timedelta, timezone
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import pandas as pd
from flight.flight_client import FlightClient
from utils.log_kit import logger
from utils.timer import timer
from utils.config import KLINE_INTERVAL, KLINE_INTERVAL_MINUTES


def demo_basic_operations():
    """演示基本操作"""
    logger.info("=" * 60)
    logger.info("🚀 Arrow Flight 服务器演示")
    logger.info("=" * 60)

    client = FlightClient(addr=ADDR)

    # 1. 健康检查
    logger.info("\n1. 健康检查")
    logger.info("-" * 30)
    response = client.ping()
    logger.info(f"服务器响应: {response}")

    # 2. 列出可用数据源
    logger.info("\n2. 可用数据源")
    logger.info("-" * 30)
    flights = client.list_available_data()
    for flight_info in flights:
        descriptor = flight_info.descriptor
        path = descriptor.path[0] if descriptor.path else "未知"
        total_records = flight_info.total_records
        logger.info(f"  📊 {path}: {total_records} 条记录")


def demo_schema_information():
    """演示Schema信息"""
    logger.info("\n3. Schema信息演示")
    logger.info("-" * 30)

    client = FlightClient(addr=ADDR)

    tables = [f"usdt_perp_{KLINE_INTERVAL}", f"usdt_spot_{KLINE_INTERVAL}"]

    for table in tables:
        logger.info(f"\n📋 {table}表结构:")
        schema = client.get_schema(table)
        # if schema:
        #     for field in schema:
        #         logger.info(f"  - {field.name}: {field.type}")


def demo_actions():
    """演示自定义操作"""
    logger.info("\n4. Action演示")
    logger.info("-" * 30)

    client = FlightClient(addr=ADDR)

    # Ping操作
    logger.info("\n🏓 Ping操作:")
    response = client.ping()
    logger.info(f"响应: {response}")

    # Ready操作
    logger.info("\n📅 Ready操作:")
    market = "usdt_perp"
    client.ready(market)


def demo_get_data(symbol: str = 'BTC'):
    """演示市场数据查询"""
    logger.info("\n5. Get演示")
    logger.info("-" * 30)

    client = FlightClient(addr=ADDR)
    begin = datetime.now(timezone.utc) - timedelta(days=1)
    end = datetime.now(timezone.utc)

    with timer("获取USDT永续合约市场数据", logger.info):
        res = client.get_market('usdt_perp', 5, 0, begin=begin)
    with timer("转换为Pandas DataFrame", logger.info):
        df = res.to_pandas()
    print(df.tail(10))

    with timer(f"获取{symbol}永续合约数据"):
        res = client.get_symbol('usdt_perp', f'{symbol}USDT', interval=5, offset=0, begin=begin, end=end)
    with timer("转换为Pandas DataFrame"):
        df = res.to_pandas()
    print(df.tail(10))


def check_data_integrity():
    client = FlightClient(addr=ADDR)
    with timer("获取BTC永续合约数据"):
        res = client.get_symbol('usdt_perp', 'BTCUSDT', interval=KLINE_INTERVAL_MINUTES, offset=0)
    with timer("转换为Pandas DataFrame"):
        df = res.to_pandas()
    print(df.tail(5))
    df['diff'] = df['resample_time'] - df['resample_time'].shift(1)
    df = df[df['diff'] > pd.Timedelta(minutes=KLINE_INTERVAL_MINUTES)]
    print(df.tail(24))


def fetch_symbol_data(symbol, market='usdt_perp', interval=5, offset=0, begin=None, end=None):
    """
    单个线程查询单个币种的数据

    Args:
        symbol: 币种名称（如'BTC', 'ETH'等，会自动添加USDT后缀）
        market: 市场类型
        interval: K线间隔（分钟）
        offset: 时间偏移
        begin: 开始时间
        end: 结束时间

    Returns:
        dict: 包含symbol和DataFrame的字典
    """
    thread_name = threading.current_thread().name
    symbol_pair = f"{symbol}USDT"

    try:
        logger.info(f"[{thread_name}] 开始查询 {symbol_pair}")
        start_time = time.time()

        # 每个线程创建自己的客户端连接
        client = FlightClient(addr=ADDR)

        # 执行查询
        res = client.get_symbol(market, symbol_pair, interval=interval, offset=offset, begin=begin, end=end)
        df = res.to_pandas()

        elapsed = time.time() - start_time
        logger.ok(f"[{thread_name}] {symbol_pair} 查询完成，耗时: {elapsed:.2f}秒，数据行数: {len(df)}")

        return {
            'symbol': symbol_pair,
            'data': df,
            'rows': len(df),
            'elapsed': elapsed,
            'thread': thread_name
        }

    except Exception as e:
        logger.error(f"[{thread_name}] {symbol_pair} 查询失败: {e}")
        return {
            'symbol': symbol_pair,
            'data': None,
            'error': str(e),
            'thread': thread_name
        }


def demo_multithread_query(symbols=None, max_workers=5):
    """
    演示多线程并发查询多个币种

    Args:
        symbols: 币种列表，默认查询主流币种
        max_workers: 最大线程数
    """
    logger.info("\n" + "=" * 60)
    logger.info("🚀 多线程并发查询演示")
    logger.info("=" * 60)

    # 默认查询的币种列表
    if symbols is None:
        symbols = ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOGE', 'AVAX', 'DOT', 'MATIC']

    # 设置时间范围（最近1天，使用 UTC 时间）
    begin = datetime.now(timezone.utc) - timedelta(days=1)
    end = datetime.now(timezone.utc)

    logger.info(f"查询币种: {symbols}")
    logger.info(f"时间范围: {begin} 到 {end}")
    logger.info(f"最大线程数: {max_workers}")
    logger.info("-" * 60)

    # 记录总开始时间
    total_start = time.time()

    # 使用线程池并发查询
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_symbol = {
            executor.submit(fetch_symbol_data, symbol, 'usdt_perp', 5, 0, begin, end): symbol
            for symbol in symbols
        }

        # 收集结果（按完成顺序）
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"{symbol} 任务执行异常: {e}")

    # 统计结果
    total_elapsed = time.time() - total_start

    logger.info("\n" + "=" * 60)
    logger.info("📊 查询结果汇总")
    logger.info("=" * 60)

    successful = [r for r in results if r.get('data') is not None]
    failed = [r for r in results if r.get('data') is None]

    logger.info(f"总耗时: {total_elapsed:.2f}秒")
    logger.info(f"成功: {len(successful)} 个币种")
    logger.info(f"失败: {len(failed)} 个币种")

    if successful:
        logger.info("\n✅ 成功查询的币种:")
        for r in successful:
            logger.info(f"  - {r['symbol']}: {r['rows']} 行数据, 耗时 {r['elapsed']:.2f}秒 [{r['thread']}]")

    if failed:
        logger.warning("\n❌ 查询失败的币种:")
        for r in failed:
            logger.warning(f"  - {r['symbol']}: {r.get('error', '未知错误')} [{r['thread']}]")

    # 显示所有成功查询的币种数据
    if successful:
        logger.info("\n" + "-" * 60)
        logger.info("📈 所有查询成功的币种数据 (显示最新10条):")
        logger.info("-" * 60)
        for result in successful:
            print(f"\n{'=' * 60}")
            print(f"{result['symbol']} - 最新10条数据 (共{result['rows']}行, 线程:{result['thread']}):")
            print('=' * 60)
            print(result['data'].tail(10))
            print()

    # 性能对比
    if len(successful) > 0:
        avg_single_time = sum(r['elapsed'] for r in successful) / len(successful)
        estimated_serial_time = avg_single_time * len(symbols)
        speedup = estimated_serial_time / total_elapsed

        logger.info("\n" + "=" * 60)
        logger.info("⚡ 性能对比")
        logger.info("=" * 60)
        logger.info(f"单个查询平均耗时: {avg_single_time:.2f}秒")
        logger.info(f"串行执行预计耗时: {estimated_serial_time:.2f}秒")
        logger.info(f"并发执行实际耗时: {total_elapsed:.2f}秒")
        logger.info(f"性能提升: {speedup:.2f}x")

    return results


def main():
    """运行所有演示"""
    try:
        # 检查服务器连接
        client = FlightClient(addr=ADDR)
        client.ping()
        client.list_actions()

        pd.set_option('display.max_columns', None)  # 显示所有列
        pd.set_option('display.max_rows', 1000)  # 设置宽度，避免换行

        # 运行演示
        # demo_basic_operations()
        # demo_schema_information()
        # demo_actions()
        # demo_get_data('LSK')
        # check_data_integrity()

        # 多线程并发查询演示
        demo_multithread_query(
            symbols=['BTC', 'ETH', 'BNB', 'SOL', 'XRP'],  # 可以自定义币种列表
            max_workers=5  # 可以调整线程数
        )

        logger.info("\n" + "=" * 60)
        logger.info("🎉 演示完成！")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"演示失败: {e}")
        logger.info("\n❌ 演示失败，请确保Flight服务器正在运行")
        logger.info("启动服务器: python start_server.py")


if __name__ == "__main__":
    ADDR = 'localhost:8815'
    main() 