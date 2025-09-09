#!/usr/bin/env python3
"""
Flight服务器启动脚本
"""
import sys
import os
import signal

from flight.flight_server import FlightServer
from utils.config import FLIGHT_PORT, DUCKDB_DIR, PARQUET_DIR
from utils.db_manager import KlineDBManager
from core.component.candle_fetcher import *

# 设置时区为UTC
os.environ['TZ'] = 'UTC'

def signal_handler(signum, frame):
    """处理退出信号"""
    logger.critical("Received exit signal, shutting down...")
    server.db_manager.close()
    sys.exit(0)
    

if __name__ == "__main__":
    """启动Flight服务器"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 读取端口
    port = FLIGHT_PORT
    location = f"grpc://0.0.0.0:{port}"
    
    try:
        # 创建并启动服务器
        # 全局数据库连接
        duckdb_dir = DUCKDB_DIR
        if not os.path.isabs(duckdb_dir):
            duckdb_dir = os.path.join(os.getcwd(), duckdb_dir)
        # absolute path
        pqt_dir = PARQUET_DIR
        if not os.path.isabs(pqt_dir):
            pqt_dir = os.path.join(os.getcwd(), pqt_dir)
        logger.info(f"Using Parquet directory: {pqt_dir}")
        
        # 创建数据库管理器
        global db_manager
        if duckdb_dir:
            logger.info(f"Using DuckDB directory: {duckdb_dir}")
            db_manager = KlineDBManager(database_path=duckdb_dir)
        else:
            logger.info("Using in-memory DuckDB")
            db_manager = KlineDBManager(database_path=None)

        server = FlightServer(location=location, db_manager=db_manager, pqt_path=pqt_dir)
        server.serve()
        
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected, shutting down server...")
    except Exception as e:
        logger.error(f"Failed to start Flight server: {e}")
        sys.exit(1)