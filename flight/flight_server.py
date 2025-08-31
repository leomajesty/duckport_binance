import asyncio
import datetime
import json
import os
import glob
import pandas as pd
from dateutil import parser
import pyarrow as pa
import pyarrow.flight as flight
import threading

from core.bus import *
from core.component.candle_fetcher import BinanceFetcher, OptimizedKlineFetcher
from utils.log_kit import logger
from utils.config import REDUNDANCY_HOURS, SUFFIX, \
    KLINE_INTERVAL_MINUTES, FETCH_CONCURRENCY, KLINE_INTERVAL, RETENTION_DAYS
from utils import create_aiohttp_session, next_run_time, async_sleep_until_run_time
from utils.timer import func_timer, timer
from utils.db_manager import DatabaseManager
from utils.log_kit import divider

# 设置时区为UTC
os.environ['TZ'] = 'UTC'

class FlightServer(flight.FlightServerBase):
    """
    基于Airport文档的Arrow Flight服务器
    支持数据查询、传输和元数据管理
    """
    
    def __init__(self, db_manager: DatabaseManager, location: str = "grpc://0.0.0.0:8815", pqt_path: str = "../data"):
        super().__init__(location)
        self.db_manager = db_manager
        self._location = location
        
        # 加载环境配置
        self.redundancy_hours = REDUNDANCY_HOURS

        self._init_database()
        self.flight_gets = FlightGets(self.db_manager, self.redundancy_hours, pqt_path)
        self.flight_actions = FlightActions(self.db_manager)
        self.async_job()

        logger.info(f"Flight server initialized at {self._location}")
        logger.info(f"Redundancy hours: {self.redundancy_hours} hours")

    def _init_database(self):
        self.db_manager.execute_write("""CREATE TABLE IF NOT EXISTS config_dict (
            key VARCHAR,
            value VARCHAR,
            PRIMARY KEY (key)
        );""")
        self._verify_kline_interval_consistency()
        self.db_manager.execute_write(f"""CREATE TABLE IF NOT EXISTS usdt_perp{SUFFIX} (
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
        );""")

        self.db_manager.execute_write(f"""CREATE TABLE IF NOT EXISTS usdt_spot{SUFFIX} (
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
        );""")

        self.db_manager.execute_write("""CREATE TABLE IF NOT EXISTS exginfo (
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
            self.db_manager.execute_write(
                "INSERT INTO config_dict (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                ('kline_interval', KLINE_INTERVAL)
            )
            # 从 config_dict 表读取已存储的配置
            stored_value = self.db_manager.fetch_one(
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

    def async_job(self):
        # 启动定时任务线程
        fetch_job = threading.Thread(target=self.start_periodic_fetch_job, daemon=True)
        fetch_job.start()
        retention_job = threading.Thread(target=self.start_retention_job, daemon=True)
        retention_job.start()

    @func_timer
    async def save_exginfo(self, exginfo, symbols_trading, market):
        infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
        symbols_trading_df = pd.DataFrame.from_records(infos_trading)
        # 保存symbols_trading数据到数据库
        try:
            # 更新内存缓存
            self.flight_gets._update_exginfo(market, symbols_trading_df)
            # 删除旧数据并插入新数据
            self.db_manager.execute_write(f"DELETE FROM exginfo WHERE market = '{market}';")
            # 明确指定列名，排除created_time
            columns = ['market', 'symbol', 'status', 'base_asset', 'quote_asset', 'price_tick', 'lot_size',
                       'min_notional_value', 'contract_type', 'margin_asset', 'pre_market']
            self.db_manager.execute_write(f"INSERT INTO exginfo ({', '.join(columns)}) SELECT {', '.join(columns)} FROM df;",
                                     df=symbols_trading_df)
            logger.info(f"symbols_trading数据已保存到数据库: {market}, {len(symbols_trading_df)} 条记录")
        except Exception as e:
            logger.error(f"保存symbols_trading数据失败: {e}")

    @func_timer
    async def save_duck_time(self, market, current_time):
        """保存当前时间到config_dict表"""
        try:
            # 更新内存缓存
            self.flight_actions.duck_time[market] = str(current_time)
            self.db_manager.execute_write("INSERT OR REPLACE INTO config_dict (key, value) VALUES (?, ?)",
                                     (f'{market}_duck_time', str(current_time)))
            logger.info(f"已保存 {market} 的当前时间: {current_time}")
        except Exception as e:
            logger.error(f"保存 {market} 当前时间失败: {e}")

    async def fetch_and_insert_binance_data_async(self, market, current_time, concurrent=FETCH_CONCURRENCY, interval='5m'):
        """异步获取K线并写入duckdb表"""
        async with create_aiohttp_session(10) as session:
            fetcher = BinanceFetcher(market, session)
            exginfo = await fetcher.get_exchange_info()
            symbols_trading = TRADE_TYPE_MAP[market][0](exginfo)
            await self.save_exginfo(exginfo, symbols_trading, market)

            optimized_fetcher = OptimizedKlineFetcher(fetcher, max_concurrent=concurrent)
            results = await optimized_fetcher.get_all_klines(symbols_trading, interval=interval)
            # 过滤掉失败的结果
            successful_results = [pd.DataFrame(r['data']) for r in results if r.get('success', False)]
            if not successful_results:
                logger.info("没有成功的结果，无法保存")
                return
            df = pd.concat(successful_results)
            df = df[df['open_time'] < current_time]
            with timer("write to duckdb"):
                # 写入数据库忽略重复数据
                self.db_manager.execute_write(f"insert into {market}_{interval} select * from df on conflict do nothing;",
                                         df=df)

            await self.save_duck_time(market, current_time)

    async def duckdb_retention_async(self):
        """定时导出一小时前的数据到parquet，并清理n周前的parquet文件"""
        logger.info("[Scheduler] Starting periodic cleanup task")

        try:
            # 清理过期的duckdb数据
            self.db_manager.execute_write(
                f"DELETE FROM usdt_perp_{KLINE_INTERVAL} WHERE open_time < now() - interval '{RETENTION_DAYS} days'")
            self.db_manager.execute_write(
                f"DELETE FROM usdt_spot_{KLINE_INTERVAL} WHERE open_time < now() - interval '{RETENTION_DAYS} days'")
            logger.info("[Scheduler] Cleaned up old DuckDB data")
        except Exception as e:
            logger.error(f"Error during DuckDB cleanup: {e}")

    def start_periodic_fetch_job(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def periodic():
            while True:
                next_time = next_run_time(KLINE_INTERVAL)
                divider(f"[Scheduler] next fetch runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
                await async_sleep_until_run_time(next_time)
                try:
                    await self.fetch_and_insert_binance_data_async(market='usdt_perp', current_time=next_time,
                                                              interval=KLINE_INTERVAL)
                    await self.fetch_and_insert_binance_data_async(market='usdt_spot', current_time=next_time,
                                                              interval=KLINE_INTERVAL)
                except Exception as e:
                    logger.error(f"Scheduler Error: {e}")

        loop.run_until_complete(periodic())

    def start_retention_job(self):
        """定时导出parquet文件,并清理过期的duckdb数据"""
        if RETENTION_DAYS == 0:
            logger.info("Retention days is set to 0, skipping retention job.")
            return
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def periodic():
            while True:
                next_time = next_run_time('1h') + datetime.timedelta(minutes=3)  # 每小时的5分执行
                divider(f"[Scheduler] next retention job runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
                await async_sleep_until_run_time(next_time)
                try:
                    await self.duckdb_retention_async()
                except Exception as e:
                    logger.error(f"Scheduler Error: {e}")

        loop.run_until_complete(periodic())

class FlightActions:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.duck_time = {
            'usdt_perp': self._duck_time('usdt_perp'),
            'usdt_spot': self._duck_time('usdt_spot')
        }

    def _duck_time(self, market: str):
        """更新Parquet文件的最新时间"""
        max_time = '2009-01-03 00:00:00'
        try:
            result = self.db_manager.fetch_one(f"SELECT value FROM config_dict WHERE key = '{market}_duck_time'")
            # ducktime自检，与当前时间的差值不能大于BASE_INERTVAL的495倍
            if abs(pd.to_datetime(result[0]) - datetime.datetime.now(tz=datetime.timezone.utc)) > datetime.timedelta(minutes=KLINE_INTERVAL_MINUTES) * 495:
                logger.warning(f"{market} duck_time 自检失败，差值大于{KLINE_INTERVAL_MINUTES * 495}分钟")
                raise ValueError(f"{market} duck_time 自检失败，差值大于{KLINE_INTERVAL_MINUTES * 495}分钟")
            max_time = result[0]
        except Exception as e:
            logger.warning(f"未找到{market} duck_time")
        return max_time

    def action_ping(self, **kwargs):
        """Ping操作"""
        return [flight.Result(pa.scalar("pong").as_buffer())]

    def action_ready(self, market, **kwargs):
        """检查最新一次完成fetch的时间 如果没有 则返回pqt_time"""
        duck_time = self.duck_time[market]
        return [flight.Result(pa.scalar(duck_time).as_buffer())]

class FlightGets:

    def __init__(self, db_manager: DatabaseManager, redundancy_hours: int = 1, pqt_path: str = "../data"):
        self.db_manager = db_manager
        self.redundancy_hours = redundancy_hours
        self.pqt_path = pqt_path
        self.pqt_time = {
            'usdt_perp': self._pqt_time('usdt_perp'),
            'usdt_spot': self._pqt_time('usdt_spot')
        }

        self.exginfo = {}
        self._init_exginfo()

    def _pqt_time(self, market: str):
        """更新Parquet文件的最新时间"""
        max_time = pd.to_datetime('2009-01-03 00:00:00')
        files = glob.glob(os.path.join(self.pqt_path, f"{market}{SUFFIX}", "*.parquet"))
        if len(files) > 0:
            try:
                result = self.db_manager.fetch_one(f"SELECT max(open_time) as max_time from read_parquet('{self.pqt_path}/{market}{SUFFIX}/*.parquet')")
                max_time = pd.to_datetime(result[0])
            except Exception as e:
                logger.warning(f"未找到{market}{SUFFIX} Parquet文件中的最大时间")
            self.db_manager.execute_write("INSERT OR REPLACE INTO config_dict (key, value) VALUES (?, ?)",
                            (f'{market}{SUFFIX}_pqt_time', str(max_time)))
            logger.info(f"{market}{SUFFIX} Parquet时间已更新: {max_time}")
        else:
            logger.warning(f"未找到{market}{SUFFIX} Parquet文件")
        return max_time

    def _get_historical_threshold(self, market):
        """获取历史数据阈值时间"""
        return self.pqt_time[market]

    def _determine_query_strategy(self, begin, end, market):
        """确定查询策略：纯历史、纯近期或混合查询"""
        threshold = self._get_historical_threshold(market)
        
        if begin >= threshold and end >= threshold:
            return "recent_only"  # 纯近期查询
        elif begin < threshold and end < threshold:
            return "historical_only"  # 纯历史查询
        else:
            return "hybrid"  # 混合查询

    def _execute_parquet_query(self, market, interval, offset, begin, end, symbol=None):
        """执行Parquet文件查询"""
        try:
            # 构建Parquet查询SQL
            pqt_glob = f"{self.pqt_path}/{market}{SUFFIX}/{market}_*.parquet"
            
            # 基础查询
            base_query = f"""
            SELECT
                DATE_TRUNC('minute', open_time - (EXTRACT(MINUTE FROM (open_time - INTERVAL '{offset} minute')) % {interval}) * INTERVAL '1 minute') AS resample_time,
                first(open) as open, max(high) as high, min(low) as low, last(close) as close,
                sum(volume) as volume, sum(quote_volume) as quote_volume, sum(trade_num) as trade_num,
                sum(taker_buy_base_asset_volume) as taker_buy_base_asset_volume, sum(taker_buy_quote_asset_volume) as taker_buy_quote_asset_volume,
                first(avg_price) as avg_price, symbol
            FROM read_parquet('{pqt_glob}')
            WHERE open_time >= '{begin:%Y-%m-%d %H:%M:%S}' AND open_time <= '{end:%Y-%m-%d %H:%M:%S}'
            """
            
            if symbol:
                base_query += f" AND symbol = '{symbol}'"
            
            base_query += f"""
            GROUP BY symbol, resample_time
            HAVING count(open_time) = {interval // KLINE_INTERVAL_MINUTES}
            ORDER BY resample_time
            """
            
            logger.info(f"执行Parquet查询: {market}, 时间范围: {begin} - {end}")
            return self._execute_query(base_query)
            
        except Exception as e:
            logger.error(f"Parquet查询失败: {e}")
            raise

    def _execute_duckdb_query(self, market, interval, offset, begin, end, symbol=None):
        """执行DuckDB表查询"""
        try:
            # 构建DuckDB查询SQL
            base_query = f"""
            SELECT
                DATE_TRUNC('minute', open_time - (EXTRACT(MINUTE FROM (open_time - INTERVAL '{offset} minute')) % {interval}) * INTERVAL '1 minute') AS resample_time,
                first(open) as open, max(high) as high, min(low) as low, last(close) as close,
                sum(volume) as volume, sum(quote_volume) as quote_volume, sum(trade_num) as trade_num,
                sum(taker_buy_base_asset_volume) as taker_buy_base_asset_volume, sum(taker_buy_quote_asset_volume) as taker_buy_quote_asset_volume,
                first(avg_price) as avg_price, symbol
            FROM {market}{SUFFIX}
            WHERE open_time >= '{begin:%Y-%m-%d %H:%M:%S}' AND open_time <= '{end:%Y-%m-%d %H:%M:%S}'
            """
            
            if symbol:
                base_query += f" AND symbol = '{symbol}'"
            
            base_query += f"""
            GROUP BY symbol, resample_time
            HAVING count(open_time) = {interval // KLINE_INTERVAL_MINUTES}
            ORDER BY resample_time
            """
            
            logger.info(f"执行DuckDB查询: {market}, 时间范围: {begin} - {end}")
            return self._execute_query(base_query)
            
        except Exception as e:
            logger.error(f"DuckDB查询失败: {e}")
            raise

    def _execute_hybrid_query(self, market, interval, offset, begin, end, symbol=None):
        """执行混合查询：先GROUP BY再UNION，带冗余时间"""
        try:
            threshold = self._get_historical_threshold(market)
            
            # 计算扩展范围（添加冗余时间）
            historical_begin = begin - datetime.timedelta(hours=self.redundancy_hours)
            historical_end = min(end, threshold) + datetime.timedelta(hours=self.redundancy_hours)
            recent_begin = max(begin, threshold) - datetime.timedelta(hours=self.redundancy_hours)
            recent_end = end + datetime.timedelta(hours=self.redundancy_hours)
            
            logger.info(f"扩展查询范围: 历史数据({historical_begin} - {historical_end}), 近期数据({recent_begin} - {recent_end})")
            
            # 检查是否有有效的时间范围
            has_historical = historical_begin < historical_end
            has_recent = recent_begin < recent_end
            
            if not has_historical and not has_recent:
                logger.warning("混合查询：没有有效的时间范围")
                return self._execute_query("SELECT NULL LIMIT 0")
            
            # 构建先GROUP BY再UNION的SQL
            parquet_path = f"{self.pqt_path}/{market}{SUFFIX}/{market}_*.parquet"
            
            base_query = f"""
            SELECT resample_time, last(open) as open, last(high) as high, last(low) as low, last(close) as close,
                   last(volume) as volume, last(quote_volume) as quote_volume, last(trade_num) as trade_num,
                   last(taker_buy_base_asset_volume) as taker_buy_base_asset_volume, last(taker_buy_quote_asset_volume) as taker_buy_quote_asset_volume,
                   last(avg_price) as avg_price, symbol
            FROM (
                """
            
            # 添加历史数据预聚合查询（如果有效）
            if has_historical:
                base_query += f"""
                -- 历史数据预聚合（Parquet）
                SELECT 
                    DATE_TRUNC('minute', open_time - (EXTRACT(MINUTE FROM (open_time - INTERVAL '{offset} minute')) % {interval}) * INTERVAL '1 minute') AS resample_time,
                    first(open) as open, max(high) as high, min(low) as low, last(close) as close,
                    sum(volume) as volume, sum(quote_volume) as quote_volume, sum(trade_num) as trade_num,
                    sum(taker_buy_base_asset_volume) as taker_buy_base_asset_volume, sum(taker_buy_quote_asset_volume) as taker_buy_quote_asset_volume,
                    first(avg_price) as avg_price, symbol
                FROM read_parquet('{parquet_path}')
                WHERE open_time >= '{historical_begin:%Y-%m-%d %H:%M:%S}' AND open_time < '{historical_end:%Y-%m-%d %H:%M:%S}'
                """
                if symbol:
                    base_query += f" AND symbol = '{symbol}'"
                base_query += f"""
                GROUP BY symbol, resample_time
                HAVING count(open_time) = {interval // KLINE_INTERVAL_MINUTES}
                """
            
            # 添加UNION ALL（如果需要合并）
            if has_historical and has_recent:
                base_query += f"""
                UNION ALL
                """
            
            # 添加近期数据预聚合查询（如果有效）
            if has_recent:
                base_query += f"""
                -- 近期数据预聚合（DuckDB）
                SELECT 
                    DATE_TRUNC('minute', open_time - (EXTRACT(MINUTE FROM (open_time - INTERVAL '{offset} minute')) % {interval}) * INTERVAL '1 minute') AS resample_time,
                    first(open) as open, max(high) as high, min(low) as low, last(close) as close,
                    sum(volume) as volume, sum(quote_volume) as quote_volume, sum(trade_num) as trade_num,
                    sum(taker_buy_base_asset_volume) as taker_buy_base_asset_volume, sum(taker_buy_quote_asset_volume) as taker_buy_quote_asset_volume,
                    first(avg_price) as avg_price, symbol
                FROM {market}{SUFFIX}
                WHERE open_time >= '{recent_begin:%Y-%m-%d %H:%M:%S}' AND open_time <= '{recent_end:%Y-%m-%d %H:%M:%S}'
                """
                if symbol:
                    base_query += f" AND symbol = '{symbol}'"
                base_query += f"""
                GROUP BY symbol, resample_time
                HAVING count(open_time) = {interval // KLINE_INTERVAL_MINUTES}
                """
            
            base_query += f"""
            ) pre_aggregated_data
            GROUP BY symbol, resample_time
            ORDER BY resample_time
            """
            
            logger.info(f"执行混合查询: {market}")
            return self._execute_query(base_query)
            
        except Exception as e:
            logger.error(f"混合查询失败: {e}")
            raise

    def get_market(self, market, interval, offset,
                   begin=None, end=None, **kwargs):
        """获取市场信息"""
        if market not in ['usdt_perp', 'usdt_spot']:
            raise ValueError("Unsupported market type. Supported types are: 'usdt_perp', 'usdt_spot'.")
        valid_interval = [i for i in range(interval, 61, interval) if (60 % i == 0)]
        if interval not in valid_interval:
            raise ValueError(f"Unsupported interval. Supported intervals are: {valid_interval} minutes.")
        if offset >= interval or offset % KLINE_INTERVAL_MINUTES != 0:
            raise ValueError(f"Offset must be a multiple of {KLINE_INTERVAL_MINUTES} and less than the interval.")
        if begin is None:
            datetime.datetime.now() - datetime.timedelta(days=90)
        elif isinstance(begin, str):
            begin = parser.parse(begin)
        if end is None:
            end = datetime.datetime.now()
        elif isinstance(end, str):
            end = parser.parse(end)

        # 确定查询策略
        strategy = self._determine_query_strategy(begin, end, market)
        logger.info(f"市场查询策略: {strategy}, 时间范围: {begin} - {end}")

        try:
            if strategy == "historical_only":
                return self._execute_parquet_query(market, interval, offset, begin, end)
            elif strategy == "recent_only":
                return self._execute_duckdb_query(market, interval, offset, begin, end)
            else:  # hybrid
                return self._execute_hybrid_query(market, interval, offset, begin, end)
        except Exception as e:
            logger.error(f"查询失败，尝试降级策略: {e}")
            # 降级策略：如果混合查询失败，尝试纯DuckDB查询
            try:
                return self._execute_duckdb_query(market, interval, offset, begin, end)
            except Exception as e2:
                logger.error(f"降级查询也失败: {e2}")
                raise
        
    def get_funding(self, begin=None, end=None, **kwargs):
        """获取资金费率信息"""
        if begin is None:
            begin = datetime.datetime.now() - datetime.timedelta(days=90)
        elif isinstance(begin, str):
            begin = parser.parse(begin)
        if end is None:
            end = datetime.datetime.now()
        elif isinstance(end, str):
            end = parser.parse(end)
        pass
        
    def get_symbol(self, market, symbol, interval, offset,
                   begin=None, end=None, **kwargs):
        """获取symbol信息"""
        if market not in ['usdt_perp', 'usdt_spot']:
            raise ValueError("Unsupported market type. Supported types are: 'usdt_perp', 'usdt_spot'.")
        valid_interval = [i for i in range(interval, 61, interval) if (60 % i == 0)]
        if interval not in valid_interval:
            raise ValueError(f"Unsupported interval. Supported intervals are: {valid_interval} minutes.")
        if offset >= interval or offset % KLINE_INTERVAL_MINUTES != 0:
            raise ValueError(f"Offset must be a multiple of {KLINE_INTERVAL_MINUTES} and less than the interval.")
        if begin is None:
            begin = datetime.datetime.now() - datetime.timedelta(days=90)
        elif isinstance(begin, str):
            begin = parser.parse(begin)
        if end is None:
            end = datetime.datetime.now()
        elif isinstance(end, str):
            end = parser.parse(end)

        # 确定查询策略
        strategy = self._determine_query_strategy(begin, end, market)
        logger.info(f"Symbol查询策略: {strategy}, 时间范围: {begin} - {end}, Symbol: {symbol}")

        try:
            if strategy == "historical_only":
                return self._execute_parquet_query(market, interval, offset, begin, end, symbol)
            elif strategy == "recent_only":
                return self._execute_duckdb_query(market, interval, offset, begin, end, symbol)
            else:  # hybrid
                return self._execute_hybrid_query(market, interval, offset, begin, end, symbol)
        except Exception as e:
            logger.error(f"查询失败，尝试降级策略: {e}")
            # 降级策略：如果混合查询失败，尝试纯DuckDB查询
            try:
                return self._execute_duckdb_query(market, interval, offset, begin, end, symbol)
            except Exception as e2:
                logger.error(f"降级查询也失败: {e2}")
                raise

    def _init_exginfo(self):
        """exginfo初始化"""
        try:
            # 从数据库加载exginfo数据到内存
            df = self.db_manager.fetch_df("SELECT * FROM exginfo")
            
            if not df.empty:
                # 按market分组存储到内存
                for market in df['market'].unique():
                    market_data = df[df['market'] == market]
                    self.exginfo[market] = market_data
                    logger.info(f"成功加载 {market} 的exginfo数据: {len(market_data)} 条记录")
            else:
                logger.info("数据库中暂无exginfo数据")
                
        except Exception as e:
            logger.error(f"初始化exginfo失败: {e}")
            self.exginfo = {}

    def get_exginfo(self, market, **kwargs):
        """获取exginfo数据"""
        if market not in ['usdt_perp', 'usdt_spot']:
            raise ValueError("Unsupported market type. Supported types are: 'usdt_perp', 'usdt_spot'.")
        
        if market not in self.exginfo:
            raise ValueError(f"No exginfo data found for market: {market}")
        
        # 返回exginfo数据
        table = pa.Table.from_pandas(self.exginfo[market])
        return flight.RecordBatchStream(table)

    def _update_exginfo(self, market, exginfo_df):
        """更新exginfo内存缓存"""
        self.exginfo[market] = exginfo_df
        logger.info(f"已更新 {market} 的exginfo内存缓存: {len(exginfo_df)} 条记录")

    def _execute_query(self, query: str):
        """执行SQL查询并返回Arrow数据，增强错误处理和类型转换"""
        try:
            with timer(f"执行查询:"):
                table = self.db_manager.fetch_arrow_table(query)
            return flight.RecordBatchStream(table)
            
        except Exception as e:
            logger.error(f"查询执行错误: {e}")
            logger.error(f"问题查询: {query}")
            raise

def main():
    """启动Flight服务器"""
    db_manager = DatabaseManager(database_path=None, read_only=False)
    server = FlightServer(db_manager)
    
    # 启动服务器
    server.serve()
    
    logger.info("Daas is running...")

if __name__ == "__main__":
    interval = 1
    li = [i for i in range(interval, 61, interval) if (60 % i == 0)]
    print(li)
    # main()