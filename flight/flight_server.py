import json
import os

import pyarrow as pa
import pyarrow.flight as flight

from core.flight_func.flight_api import FlightActions, FlightGets
from core.flight_func.flight_data_jobs import WebsocketsDataJobs, RestfulDataJobs
from utils.config import REDUNDANCY_HOURS
from utils.db_manager import KlineDBManager
from utils.log_kit import logger
from utils.timer import func_timer

# 设置时区为UTC
os.environ['TZ'] = 'UTC'

class FlightServer(flight.FlightServerBase):
    """
    基于Airport文档的Arrow Flight服务器
    支持数据查询、传输和元数据管理
    """
    
    def __init__(self, db_manager: KlineDBManager, location: str = "grpc://0.0.0.0:8815", pqt_path: str = "../data/pqt", ws_mode: bool = True):
        super().__init__(location)
        self.db_manager = db_manager
        self._location = location
        
        # 加载环境配置
        self.redundancy_hours = REDUNDANCY_HOURS

        self.flight_gets = FlightGets(self.db_manager, self.redundancy_hours, pqt_path)
        self.flight_actions = FlightActions(self.db_manager)

        if ws_mode:
            self.data_jobs = WebsocketsDataJobs(self.db_manager, self.flight_actions, self.flight_gets)
        else:
            self.data_jobs = RestfulDataJobs(self.db_manager, self.flight_actions, self.flight_gets)

        logger.info(f"Flight server initialized at {self._location}")
        logger.info(f"Redundancy hours: {self.redundancy_hours} hours")

    @func_timer
    def do_get(self, context, ticket):
        """大数据查询操作"""
        try:
            ticket_data = ticket.ticket.decode('utf-8')
            logger.query(f"{ticket_data}")
            # str 转 dict
            ticket_data = json.loads(ticket_data)

            # 通过反射调用FlightActions中的方法
            func = getattr(self.flight_gets, f"get_{ticket_data['type']}")
            return func(**ticket_data)
                
        except Exception as e:
            logger.error(f"数据获取错误: {e}")
            raise

    @func_timer
    def list_flights(self, context, criteria):
        """列出可用的数据源"""
        logger.info("列出可用数据源")
        
        # 获取所有表名
        tables_result = self.db_manager.fetch_all("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        tables = [row[0] for row in tables_result]
        logger.info(f"找到 {len(tables)} 个表: {tables}")
        flights = []
        for table in tables:
            try:
                # 获取表的行数
                count_result = self.db_manager.fetch_one(f"SELECT COUNT(*) FROM {table}")
                count = count_result[0]
                
                descriptor = flight.FlightDescriptor.for_path(table)
                schema = self._get_table_schema(table)
                
                flight_info = flight.FlightInfo(
                    schema=schema,
                    descriptor=descriptor,
                    endpoints=[
                        flight.FlightEndpoint(
                            ticket=flight.Ticket(f"table:{table}".encode()),
                            locations=[self._location]
                        )
                    ],
                    total_records=count,
                    total_bytes=0
                )
                flights.append(flight_info)
            except Exception as e:
                logger.error(f"处理表 {table} 时出错: {e}")
                continue
        
        return flights
    
    def _get_table_schema(self, table_name: str):
        """获取表的schema"""
        columns = self.db_manager.fetch_all(f"PRAGMA table_info({table_name})")
        
        fields = []
        for col in columns:
            col_name = col[1]
            col_type = col[2].upper()
            
            # 映射SQLite类型到Arrow类型
            if col_type in ('INTEGER', 'INT'):
                arrow_type = pa.int64()
            elif col_type in ('REAL', 'FLOAT', 'DOUBLE'):
                arrow_type = pa.float64()
            elif col_type in ('TEXT', 'VARCHAR', 'STRING'):
                arrow_type = pa.string()
            elif col_type in ('BLOB'):
                arrow_type = pa.binary()
            elif col_type in ('BOOLEAN', 'BOOL'):
                arrow_type = pa.bool_()
            else:
                arrow_type = pa.string()
            
            fields.append(pa.field(col_name, arrow_type))
        
        return pa.schema(fields)
    
    @func_timer
    def get_schema(self, context, descriptor):
        """获取指定数据源的schema"""
        try:
            path = descriptor.path[0] if descriptor.path else None
            logger.info(f"获取schema，路径: {path}, 类型: {type(path)}")
            
            if path:
                # 检查路径是否包含额外的字符
                if isinstance(path, bytes):
                    path = path.decode('utf-8')
                
                # 移除可能的额外字符
                path = path.strip()
                
                logger.info(f"处理后的路径: '{path}'")
                
                # 检查表是否存在
                table_exists = self.db_manager.fetch_one("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name = ?
                """, [path])
                
                if not table_exists:
                    available_tables = self.db_manager.fetch_all("""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    """)
                    available_table_names = [row[0] for row in available_tables]
                    logger.error(f"表 '{path}' 不存在。可用表: {available_table_names}")
                    raise ValueError(f"表 '{path}' 不存在。可用表: {available_table_names}")
                
                schema = self._get_table_schema(path)
                return flight.SchemaResult(schema)
            else:
                raise ValueError("未指定数据源路径")
        except Exception as e:
            logger.error(f"获取schema错误: {e}")
            raise

    def do_action(self, context, action):
        """处理自定义操作"""
        action_type = action.type
        action_body = action.body.to_pybytes().decode('utf-8') if action.body else ""
        
        logger.info(f"Act: {action_type}, context: {action_body}")

        # 通过反射调用FlightActions中的方法
        func = getattr(self.flight_actions, f"action_{action_type}")
        param = json.loads(action_body) if action_body else {}
        return func(**param)

    def list_actions(self, context):
        """列出支持的操作"""
        return [flight.ActionType(action, action) for action in dir(FlightActions) if action.startswith('action_')]

def main():
    """启动Flight服务器"""
    db_manager = KlineDBManager(database_path=None)
    server = FlightServer(db_manager)
    
    # 启动服务器
    server.serve()
    
    logger.info("Daas is running...")

if __name__ == "__main__":
    interval = 1
    li = [i for i in range(interval, 61, interval) if (60 % i == 0)]
    print(li)
    # main()