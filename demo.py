#!/usr/bin/env python3
"""
Flight服务器演示脚本
展示如何使用Arrow Flight协议进行数据查询和传输
"""

import datetime

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
        if schema:
            for field in schema:
                logger.info(f"  - {field.name}: {field.type}")

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

def demo_get_data():
    """演示市场数据查询"""
    logger.info("\n5. Get演示")
    logger.info("-" * 30)

    client = FlightClient(addr=ADDR)
    begin = datetime.datetime.now() - datetime.timedelta(days=5)
    end = datetime.datetime.now() - datetime.timedelta(days=1)

    with timer("获取USDT永续合约市场数据", logger.info):
        res = client.get_market('usdt_perp', 60, 15, begin=begin)
    with timer("转换为Pandas DataFrame", logger.info):
        df = res.to_pandas()
    print(df.tail(10))

    with timer("获取BTC永续合约数据"):
        res = client.get_symbol('usdt_perp', 'BTCUSDT', interval=30, offset=15)
    with timer("转换为Pandas DataFrame"):
        df = res.to_pandas()
    print(df.tail(10))

    # with timer("获取USDT spot exginfo数据"):
    #     res = client.get_exginfo('usdt_spot')
    # with timer("转换为Pandas DataFrame"):
    #     df = res.to_pandas()
    # print(df.tail(10))
    # res = client.get_funding('usdt_perp')
    # df = res.to_pandas()
    # print(df.tail(10))

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
        demo_basic_operations()
        demo_schema_information()
        demo_actions()
        demo_get_data()
        check_data_integrity()
        
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