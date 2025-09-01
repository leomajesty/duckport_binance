import threading
import duckdb
from typing import Optional, Any, List, Dict

import pandas as pd
import pyarrow as pa

from utils.log_kit import logger

dtypes_dict = {
    'trade_num': pa.int32(),
}


class DatabaseManager:
    """
    数据库连接管理器，确保线程安全的数据库操作
    使用锁机制保证同一时间只有一个查询或写入操作
    """
    
    def __init__(self, database_path: str = None, read_only: bool = False):
        """
        初始化数据库管理器
        
        Args:
            database_path: 数据库文件路径，None表示内存数据库
            read_only: 是否只读模式
        """
        self.database_path = database_path
        self.read_only = read_only
        self._lock = threading.Lock()
        self._connection = None
        self._init_connection()
    
    def _init_connection(self):
        """初始化数据库连接"""
        try:
            if self.database_path:
                self._connection = duckdb.connect(database=self.database_path, read_only=self.read_only)
                logger.info(f"数据库连接已建立: {self.database_path}")
            else:
                self._connection = duckdb.connect(database=':memory:', read_only=self.read_only)
                logger.info("内存数据库连接已建立")
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
            queries: 查询列表，每个查询包含 'query' 和 'params' 键
            
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
                    
                    if params:
                        self._connection.execute(query, params)
                    else:
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
                print(f'{table.nbytes /1024} kb')
                for col, dtype in dtypes_dict.items():
                    if col in table.column_names and table.schema.field(col).type != dtype:
                        table = table.set_column(
                            table.schema.get_field_index(col),
                            col,
                            table[col].cast(dtype)
                        )
                print(f'{table.nbytes / 1024} kb')
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
