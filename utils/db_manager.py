import os
import threading
from typing import Any, List, Dict

import duckdb
import pandas as pd
import pyarrow as pa

from utils.config import SUFFIX, KLINE_INTERVAL, DUCKDB_THREAD, DUCKDB_MEMORY
from utils.log_kit import logger, divider

dtypes_dict = {
    'trade_num': pa.int32(),
}


class DatabaseManager:
    """
    数据库连接管理器，确保线程安全的数据库操作
    使用锁机制保证同一时间只有一个查询或写入操作
    """
    
    def __init__(self, database_path: str = None, new: bool = False):
        """
        初始化数据库管理器
        
        Args:
            database_path: 数据库文件路径，None表示内存数据库
            read_only: 是否只读模式
        """
        self.database_path = database_path
        self._lock = threading.Lock()
        self._connection = None
        if new:
            self.destroy_all()
        self._init_connection()

    def destroy_all(self):
        if self.database_path and os.path.exists(self.database_path):
            try:
                os.remove(self.database_path)
                logger.info(f"已删除数据库文件: {self.database_path}")
            except Exception as e:
                logger.error(f"删除数据库文件失败: {e}")
                raise

    def _init_connection(self):
        """初始化数据库连接"""
        try:
            if self.database_path:
                self._connection = duckdb.connect(database=self.database_path, read_only=False, config={'timezone': 'UTC'})
                logger.info(f"Using DuckDB directory: {self.database_path}")
            else:
                self._connection = duckdb.connect(database=':memory:', read_only=False, config={'timezone': 'UTC'})
                logger.info("Using in-memory DuckDB")
            self._connection.execute(f"SET threads = {DUCKDB_THREAD};")
            self._connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY}';")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> Any:
        """
        执行查询操作（线程安全）
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果
        """
        with self._lock:
            try:
                if params:
                    result = self._connection.execute(query, params)
                else:
                    result = self._connection.execute(query)
                return result
            except Exception as e:
                logger.error(f"查询执行失败: {e}")
                logger.error(f"问题查询: {query}")
                if params:
                    logger.error(f"查询参数: {params}")
                raise
    
    def execute_write(self, query: str, params: tuple = None, df: pd.DataFrame = pd.DataFrame()) -> Any:
        """
        执行写入操作（线程安全）
        
        Args:
            query: SQL写入语句
            params: 写入参数
            df: DataFrame对象
        Returns:
            写入结果
        """
        with self._lock:
            try:
                if params:
                    result = self._connection.execute(query, params)
                else:
                    result = self._connection.execute(query)
                return result
            except Exception as e:
                logger.error(f"写入执行失败: {e}")
                logger.error(f"问题写入: {query}")
                if params:
                    logger.error(f"写入参数: {params}")
                if not df.empty:
                    logger.error(f"写入DataFrame: {df.head()}")
                raise
    
    def execute_transaction(self, queries: List[Dict[str, Any]]) -> bool:
        """
        执行事务操作（线程安全）
        
        Args:
            queries: 查询列表，每个查询包含 'query'、'params' 和可选的 'df' 键
            
        Returns:
            事务是否成功
        """
        with self._lock:
            try:
                # 开始事务
                self._connection.execute("BEGIN TRANSACTION")
                
                for query_info in queries:
                    query = query_info['query']
                    params = query_info.get('params')
                    df = query_info.get('df')
                    
                    if params:
                        self._connection.execute(query, params)
                    else:
                        # 都没有的情况
                        self._connection.execute(query)
                
                # 提交事务
                self._connection.execute("COMMIT")
                logger.info("事务执行成功")
                return True
                
            except Exception as e:
                # 回滚事务
                try:
                    self._connection.execute("ROLLBACK")
                    logger.info("事务已回滚")
                except Exception as rollback_error:
                    logger.error(f"事务回滚失败: {rollback_error}")
                
                logger.error(f"事务执行失败: {e}")
                return False
    
    def fetch_arrow_table(self, query: str, params: tuple = None):
        """
        执行查询并返回Arrow表（线程安全）
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            Arrow表对象
        """
        with self._lock:
            try:
                if params:
                    result = self._connection.execute(query, params)
                else:
                    result = self._connection.execute(query)
                table = result.fetch_arrow_table()
                for col, dtype in dtypes_dict.items():
                    if col in table.column_names and table.schema.field(col).type != dtype:
                        table = table.set_column(
                            table.schema.get_field_index(col),
                            col,
                            table[col].cast(dtype)
                        )
                return table
            except Exception as e:
                logger.error(f"Arrow表查询失败: {e}")
                logger.error(f"问题查询: {query}")
                if params:
                    logger.error(f"查询参数: {params}")
                raise
    
    def fetch_df(self, query: str, params: tuple = None):
        """
        执行查询并返回DataFrame（线程安全）
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            DataFrame对象
        """
        with self._lock:
            try:
                if params:
                    result = self._connection.execute(query, params)
                else:
                    result = self._connection.execute(query)
                return result.fetchdf()
            except Exception as e:
                logger.error(f"DataFrame查询失败: {e}")
                logger.error(f"问题查询: {query}")
                if params:
                    logger.error(f"查询参数: {params}")
                raise
    
    def fetch_one(self, query: str, params: tuple = None):
        """
        执行查询并返回单行结果（线程安全）
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            单行结果
        """
        with self._lock:
            try:
                if params:
                    result = self._connection.execute(query, params)
                else:
                    result = self._connection.execute(query)
                return result.fetchone()
            except Exception as e:
                logger.error(f"单行查询失败: {e}")
                logger.error(f"问题查询: {query}")
                if params:
                    logger.error(f"查询参数: {params}")
                raise
    
    def fetch_all(self, query: str, params: tuple = None):
        """
        执行查询并返回所有结果（线程安全）
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            所有结果
        """
        with self._lock:
            try:
                if params:
                    result = self._connection.execute(query, params)
                else:
                    result = self._connection.execute(query)
                return result.fetchall()
            except Exception as e:
                logger.error(f"全量查询失败: {e}")
                logger.error(f"问题查询: {query}")
                if params:
                    logger.error(f"查询参数: {params}")
                raise
    
    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            logger.info("数据库连接已关闭")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()

class KlineDBManager(DatabaseManager):

    def __init__(self, database_path: str = None, new: bool = False):
        super().__init__(database_path, new)
        self._init_database()
        divider("数据库已启动")

    def _init_database(self):
        self.execute_write("""CREATE TABLE IF NOT EXISTS config_dict (
            key VARCHAR,
            value VARCHAR,
            PRIMARY KEY (key)
        );""")
        self._verify_kline_interval_consistency()
        self.execute_write(f"""CREATE TABLE IF NOT EXISTS usdt_perp{SUFFIX} (
            open_time TIMESTAMP,
            symbol VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trade_num INT,
            taker_buy_base_asset_volume DOUBLE,
            taker_buy_quote_asset_volume DOUBLE,
            avg_price DOUBLE,
            PRIMARY KEY (open_time, symbol)
        );""")

        self.execute_write(f"""CREATE TABLE IF NOT EXISTS usdt_spot{SUFFIX} (
            open_time TIMESTAMP,
            symbol VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trade_num INT,
            taker_buy_base_asset_volume DOUBLE,
            taker_buy_quote_asset_volume DOUBLE,
            avg_price DOUBLE,
            PRIMARY KEY (open_time, symbol)
        );""")

        self.execute_write("""CREATE TABLE IF NOT EXISTS exginfo (
            market VARCHAR,
            symbol VARCHAR,
            status VARCHAR,
            base_asset VARCHAR,
            quote_asset VARCHAR,
            price_tick VARCHAR,
            lot_size VARCHAR,
            min_notional_value VARCHAR,
            contract_type VARCHAR,
            margin_asset VARCHAR,
            pre_market BOOLEAN,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (market, symbol)
        );""")

    def _verify_kline_interval_consistency(self):
        """验证k线周期配置一致性"""
        try:
            # 向 config_dict 表写入当前配置
            self.execute_write(
                "INSERT INTO config_dict (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                ('kline_interval', KLINE_INTERVAL)
            )
            # 从 config_dict 表读取已存储的配置
            stored_value = self.fetch_one(
                "SELECT value FROM config_dict WHERE key = 'kline_interval'"
            )

            if not stored_value:
                logger.error("无法从数据库读取k线周期配置")
                raise ValueError("数据库配置读取失败")

            stored_interval = stored_value[0]

            # 比较配置一致性
            if stored_interval != KLINE_INTERVAL:
                error_msg = f"k线周期配置不一致！配置文件: {KLINE_INTERVAL}, 数据库: {stored_interval}"
                logger.error(error_msg)
                logger.error("请检查配置文件或清理数据库后重新启动系统")
                raise ValueError(error_msg)

            logger.info("k线周期配置一致性验证通过")

        except Exception as e:
            logger.error(f"k线周期配置一致性验证失败: {e}")
            logger.error("系统将退出，请检查配置后重新启动")
            import sys
            sys.exit(1)