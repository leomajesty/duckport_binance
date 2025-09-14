import json
import os

import pyarrow as pa
import pyarrow.flight as flight

from core.flight_func.flight_api import FlightActions, FlightGets
from core.flight_func.flight_data_jobs import DataJobs, WebsocketsDataJobs, RestfulDataJobs
from utils.config import REDUNDANCY_HOURS
from utils.db_manager import DatabaseManager, KlineDBManager
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

    # def async_job(self):
    #     # 启动定时任务线程
    #     fetch_job = threading.Thread(target=self.start_periodic_fetch_job, daemon=True)
    #     fetch_job.start()
    #     retention_job = threading.Thread(target=self.start_retention_job, daemon=True)
    #     retention_job.start()

    # @func_timer
    # async def save_exginfo(self, fetcher, market):
    #     exginfo = await fetcher.get_exchange_info()
    #     symbols_trading = TRADE_TYPE_MAP[market][0](exginfo)
    #     infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
    #     symbols_trading_df = pd.DataFrame.from_records(infos_trading)
    #     # 保存symbols_trading数据到数据库
    #     try:
    #         # 更新内存缓存
    #         self.flight_gets.update_exginfo(market, symbols_trading_df)
    #         # 删除旧数据并插入新数据
    #         self.db_manager.execute_write(f"DELETE FROM exginfo WHERE market = '{market}';")
    #         # 明确指定列名，排除created_time
    #         columns = ['market', 'symbol', 'status', 'base_asset', 'quote_asset', 'price_tick', 'lot_size',
    #                    'min_notional_value', 'contract_type', 'margin_asset', 'pre_market']
    #         self.db_manager.execute_write(f"INSERT INTO exginfo ({', '.join(columns)}) SELECT {', '.join(columns)} FROM df;",
    #                                  df=symbols_trading_df)
    #         logger.info(f"symbols_trading数据已保存到数据库: {market}, {len(symbols_trading_df)} 条记录")
    #     except Exception as e:
    #         logger.error(f"保存symbols_trading数据失败: {e}")
    #     return symbols_trading

    # @func_timer
    # async def save_duck_time(self, market, current_time):
    #     """保存当前时间到config_dict表"""
    #     try:
    #         # 更新内存缓存
    #         self.flight_actions.duck_time[market] = str(current_time)
    #         self.db_manager.execute_write("INSERT OR REPLACE INTO config_dict (key, value) VALUES (?, ?)",
    #                                  (f'{market}_duck_time', str(current_time)))
    #         logger.info(f"已保存 {market} 的当前时间: {current_time}")
    #     except Exception as e:
    #         logger.error(f"保存 {market} 当前时间失败: {e}")

    # async def fetch_and_insert_binance_data_async(self, market, current_time, concurrent=FETCH_CONCURRENCY, interval='5m'):
    #     """异步获取K线并写入duckdb表"""
    #     async with create_aiohttp_session(10) as session:
    #         fetcher = BinanceFetcher(market, session)
    #
    #         symbols_trading = await self.save_exginfo(fetcher, market)
    #
    #         optimized_fetcher = OptimizedKlineFetcher(fetcher, max_concurrent=concurrent)
    #         results = await optimized_fetcher.get_all_klines(symbols_trading, interval=interval)
    #         # 过滤掉失败的结果
    #         successful_results = [pd.DataFrame(r['data']) for r in results if r.get('success', False)]
    #         if not successful_results:
    #             logger.info("没有成功的结果，无法保存")
    #             return
    #         df = pd.concat(successful_results)
    #         df = df[df['open_time'] < current_time]
    #         with timer("write to duckdb"):
    #             # 写入数据库忽略重复数据
    #             self.db_manager.execute_write(f"insert into {market}_{interval} select * from df on conflict do nothing;",
    #                                      df=df)
    #
    #         await self.save_duck_time(market, current_time)

    # async def duckdb_retention_async(self):
    #     """定时导出一小时前的数据到parquet，并清理n周前的parquet文件"""
    #     logger.info("[Scheduler] Starting periodic cleanup task")
    #
    #     try:
    #         # 清理过期的duckdb数据
    #         self.db_manager.execute_write(
    #             f"DELETE FROM usdt_perp_{KLINE_INTERVAL} WHERE open_time < now() - interval '{RETENTION_DAYS} days'")
    #         self.db_manager.execute_write(
    #             f"DELETE FROM usdt_spot_{KLINE_INTERVAL} WHERE open_time < now() - interval '{RETENTION_DAYS} days'")
    #         logger.info("[Scheduler] Cleaned up old DuckDB data")
    #     except Exception as e:
    #         logger.error(f"Error during DuckDB cleanup: {e}")

    # def start_periodic_fetch_job(self):
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)
    #
    #     async def periodic():
    #         while True:
    #             next_time = next_run_time(KLINE_INTERVAL)
    #             divider(f"[Scheduler] next fetch runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
    #             await async_sleep_until_run_time(next_time)
    #             try:
    #                 await self.fetch_and_insert_binance_data_async(market='usdt_perp', current_time=next_time,
    #                                                           interval=KLINE_INTERVAL)
    #                 await self.fetch_and_insert_binance_data_async(market='usdt_spot', current_time=next_time,
    #                                                           interval=KLINE_INTERVAL)
    #             except Exception as e:
    #                 logger.error(f"Scheduler Error: {e}")
    #
    #     loop.run_until_complete(periodic())

    # def start_retention_job(self):
    #     """定时导出parquet文件,并清理过期的duckdb数据"""
    #     if RETENTION_DAYS == 0:
    #         logger.info("Retention days is set to 0, skipping retention job.")
    #         return
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)
    #
    #     async def periodic():
    #         while True:
    #             next_time = next_run_time('1h') + timedelta(minutes=3)  # 每小时的5分执行
    #             divider(f"[Scheduler] next retention job runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
    #             await async_sleep_until_run_time(next_time)
    #             try:
    #                 await self.duckdb_retention_async()
    #             except Exception as e:
    #                 logger.error(f"Scheduler Error: {e}")
    #
    #     loop.run_until_complete(periodic())

def main():
    """启动Flight服务器"""
    db_manager = DatabaseManager(database_path=None)
    server = FlightServer(db_manager)
    
    # 启动服务器
    server.serve()
    
    logger.info("Daas is running...")

if __name__ == "__main__":
    interval = 1
    li = [i for i in range(interval, 61, interval) if (60 % i == 0)]
    print(li)
    # main()