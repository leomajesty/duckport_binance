import asyncio
from datetime import *
import os
import glob
import pandas as pd
from dateutil import parser
import pyarrow as pa
import pyarrow.flight as flight
import sys

from utils import next_run_time, async_sleep_until_run_time
from utils.log_kit import logger, divider
from utils.config import SUFFIX, KLINE_INTERVAL_MINUTES, RETENTION_DAYS, KLINE_INTERVAL
from utils.db_manager import DatabaseManager
from core.component.candle_fetcher import BinanceFetcher, OptimizedKlineFetcher
from utils import create_aiohttp_session
from utils.config import FETCH_CONCURRENCY
from core.bus import TRADE_TYPE_MAP
from utils.timer import timer


class FlightActions:
    def __init__(self, db_manager: DatabaseManager):
        self._db_manager = db_manager
        self.duck_time = {
            'usdt_perp': self._duck_time('usdt_perp'),
            'usdt_spot': self._duck_time('usdt_spot')
        }

    def _duck_time(self, market: str):
        """更新Parquet文件的最新时间"""
        max_time = '2009-01-03 00:00:00'
        try:
            result = self._db_manager.fetch_one(f"SELECT value FROM config_dict WHERE key = '{market}_duck_time'")
            # ducktime自检，与当前时间的差值不能大于BASE_INERTVAL的495倍
            if abs(pd.to_datetime(result[0]) - datetime.now(tz=timezone.utc)) > timedelta(
                    minutes=KLINE_INTERVAL_MINUTES) * 495:
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

    def __init__(self, db_manager: DatabaseManager, redundancy_hours: int = 1, pqt_path: str = "../data/pqt"):
        self._db_manager = db_manager
        self._redundancy_hours = redundancy_hours
        self._pqt_path = pqt_path
        self.pqt_time = {
            'usdt_perp': self._pqt_time('usdt_perp'),
            'usdt_spot': self._pqt_time('usdt_spot')
        }

        self.exginfo = {}
        self._init_exginfo()

    def _pqt_time(self, market: str):
        """更新Parquet文件的最新时间"""
        max_time = pd.to_datetime('2009-01-03 00:00:00')
        files = glob.glob(os.path.join(self._pqt_path, f"{market}{SUFFIX}", "*.parquet"))
        if len(files) > 0:
            try:
                sql = f"SELECT max(open_time) as max_time from read_parquet('{self._pqt_path}/{market}{SUFFIX}/*.parquet')"
                max_time = self._db_manager.fetch_one(sql)[0]
            except Exception as e:
                logger.warning(f"未找到{market}{SUFFIX} Parquet文件中的最大时间")
            self._db_manager.execute_write("INSERT OR REPLACE INTO config_dict (key, value) VALUES (?, ?)",
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
            pqt_glob = f"{self._pqt_path}/{market}{SUFFIX}/{market}_*.parquet"

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
            historical_begin = begin - timedelta(hours=self._redundancy_hours)
            historical_end = min(end, threshold) + timedelta(hours=self._redundancy_hours)
            recent_begin = max(begin, threshold) - timedelta(hours=self._redundancy_hours)
            recent_end = end + timedelta(hours=self._redundancy_hours)

            logger.info(
                f"扩展查询范围: 历史数据({historical_begin} - {historical_end}), 近期数据({recent_begin} - {recent_end})")

            # 检查是否有有效的时间范围
            has_historical = historical_begin < historical_end
            has_recent = recent_begin < recent_end

            if not has_historical and not has_recent:
                logger.warning("混合查询：没有有效的时间范围")
                return self._execute_query("SELECT NULL LIMIT 0")

            # 构建先GROUP BY再UNION的SQL
            parquet_path = f"{self._pqt_path}/{market}{SUFFIX}/{market}_*.parquet"

            subquery = ""
            # 添加历史数据预聚合查询（如果有效）
            if has_historical:
                subquery += f"""
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
                    subquery += f" AND symbol = '{symbol}'"
                subquery += f"""
                GROUP BY symbol, resample_time
                HAVING count(open_time) = {interval // KLINE_INTERVAL_MINUTES}
                """

            # 添加UNION ALL（如果需要合并）
            if has_historical and has_recent:
                subquery += f"""
                UNION ALL
                """

            # 添加近期数据预聚合查询（如果有效）
            if has_recent:
                subquery += f"""
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
                    subquery += f" AND symbol = '{symbol}'"
                subquery += f"""
                GROUP BY symbol, resample_time
                HAVING count(open_time) = {interval // KLINE_INTERVAL_MINUTES}
                """

            base_query = f"""
                        SELECT resample_time, last(open) as open, last(high) as high, last(low) as low, last(close) as close,
                               last(volume) as volume, last(quote_volume) as quote_volume, last(trade_num) as trade_num,
                               last(taker_buy_base_asset_volume) as taker_buy_base_asset_volume, last(taker_buy_quote_asset_volume) as taker_buy_quote_asset_volume,
                               last(avg_price) as avg_price, symbol
                        FROM ( {subquery} ) pre_aggregated_data
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
            datetime.now() - timedelta(days=90)
        elif isinstance(begin, str):
            begin = parser.parse(begin)
        if end is None:
            end = datetime.now()
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
            begin = datetime.now() - timedelta(days=90)
        elif isinstance(begin, str):
            begin = parser.parse(begin)
        if end is None:
            end = datetime.now()
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
            begin = datetime.now() - timedelta(days=90)
        elif isinstance(begin, str):
            begin = parser.parse(begin)
        if end is None:
            end = datetime.now()
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
            df = self._db_manager.fetch_df("SELECT * FROM exginfo")

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

    def update_exginfo(self, market, exginfo_df):
        """更新exginfo内存缓存"""
        self.exginfo[market] = exginfo_df
        logger.info(f"已更新 {market} 的exginfo内存缓存: {len(exginfo_df)} 条记录")

    def _execute_query(self, query: str):
        """执行SQL查询并返回Arrow数据，增强错误处理和类型转换"""
        try:
            with timer(f"执行查询:"):
                table = self._db_manager.fetch_arrow_table(query)
            return flight.RecordBatchStream(table)

        except Exception as e:
            logger.error(f"查询执行错误: {e}")
            logger.error(f"问题查询: {query}")
            raise

class DataJobs:
    def __init__(self, db_manager: DatabaseManager, flight_actions: FlightActions, flight_gets: FlightGets):
        self._flight_gets = flight_gets
        self._flight_actions = flight_actions
        self._db_manager: DatabaseManager = db_manager

        self.init_history_data()
        self.update_recent_data()

    def init_history_data(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        """初始化历史数据"""
        logger.info("开始初始化历史数据")

        async def history_data():
            # 第一步：获取最新数据时间
            latest_times, latest_symbols = self._get_latest_data_time()
            async with create_aiohttp_session(10) as session:
                fetchers = {}
                for market in ['usdt_perp', 'usdt_spot']:
                    fetchers[market] = BinanceFetcher(market, session)

                    if latest_times[market]:
                        await self._check_symbol_consistency(market, latest_symbols[market], fetchers[market])

                # 第三步：更新历史K线数据
                for market in ['usdt_perp', 'usdt_spot']:
                    if latest_times[market]:
                        await self._update_historical_klines(market, latest_times[market], fetchers[market])

        loop.run_until_complete(history_data())
        logger.info("历史数据初始化完成")

    def _get_latest_data_time(self):
        """获取最新数据时间，优先ducktime，其次pqttime"""
        latest_times = {}
        latest_symbols = {}
        
        for market in ['usdt_perp', 'usdt_spot']:
            try:
                # 尝试获取ducktime
                duck_time = self._flight_actions.duck_time[market]
                if duck_time and duck_time != '2009-01-03 00:00:00':
                    latest_times[market] = pd.to_datetime(duck_time)
                    latest_symbols[market] = self.get_trading_symbols_by_time(duck_time, market, is_duck=True)
                    logger.info(f"{market} 使用ducktime: {latest_times[market]}")
                else:
                    # 如果没有ducktime，使用pqttime
                    pqt_time = self._flight_gets.pqt_time[market]
                    if pqt_time and pqt_time != pd.to_datetime('2009-01-03 00:00:00'):
                        latest_times[market] = pd.to_datetime(pqt_time)
                        latest_symbols[market] = self.get_trading_symbols_by_time(pqt_time, market, is_duck=False)
                        logger.info(f"{market} 使用pqttime: {latest_times[market]}")
                    else:
                        latest_times[market] = None
                        logger.warning(f"{market} 未找到有效的数据时间")
            except Exception as e:
                logger.error(f"获取{market}数据时间失败: {e}")
                latest_times[market] = None
        
        return latest_times, latest_symbols

    def get_trading_symbols_by_time(self, snaptime, market, is_duck=True):
        if is_duck:
            df = self._db_manager.fetch_df(f"select symbol from {market}{SUFFIX} where open_time = '{snaptime}'")
            return df['symbol'].to_list()
        else:
            df = self._db_manager.fetch_df(f"SELECT symbol from read_parquet('{self._flight_gets._pqt_path}/{market}{SUFFIX}/*.parquet') where open_time = '{snaptime}'")
            return df['symbol'].to_list()

    @staticmethod
    async def _check_symbol_consistency(market, historical_symbols: set, fetcher: BinanceFetcher):
        """检查币种一致性，如果有币种下架则退出程序"""
        try:
            exginfo = await fetcher.get_exchange_info()
            current_symbols = TRADE_TYPE_MAP[market][0](exginfo)

            # 检查是否有币种下架
            delisted_symbols = set(historical_symbols) - set(current_symbols)
            if delisted_symbols:
                logger.error(f"{market} 发现下架币种: {delisted_symbols}")
                logger.error("检测到币种下架，需要重新执行loadhist脚本")
                logger.error("程序将退出，请执行: python loadhist.py")
                sys.exit(1)
            else:
                logger.info(f"{market} 币种一致性检查通过")

        except Exception as e:
            logger.error(f"检查{market}币种一致性失败: {e}")

    async def _update_historical_klines(self, market, start_time):
        """更新历史K线数据"""


    async def _fetch_and_insert_binance_data_async(self, market, current_time, interval='5m'):
        """异步获取K线并写入duckdb表（从flight_server.py复制的方法）"""
        
        async with create_aiohttp_session(10) as session:
            fetcher = BinanceFetcher(market, session)
            
            # 获取交易所信息
            exginfo = await fetcher.get_exchange_info()
            symbols_trading = TRADE_TYPE_MAP[market][0](exginfo)
            infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
            symbols_trading_df = pd.DataFrame.from_records(infos_trading)
            
            # 更新exginfo内存缓存
            self._flight_gets.update_exginfo(market, symbols_trading_df)
            
            # 保存到数据库
            try:
                self._db_manager.execute_write(f"DELETE FROM exginfo WHERE market = '{market}';")
                columns = ['market', 'symbol', 'status', 'base_asset', 'quote_asset', 'price_tick', 'lot_size',
                          'min_notional_value', 'contract_type', 'margin_asset', 'pre_market']
                self._db_manager.execute_write(f"INSERT INTO exginfo ({', '.join(columns)}) SELECT {', '.join(columns)} FROM df;",
                                         df=symbols_trading_df)
                logger.info(f"symbols_trading数据已保存到数据库: {market}, {len(symbols_trading_df)} 条记录")
            except Exception as e:
                logger.error(f"保存symbols_trading数据失败: {e}")
            
            # 获取K线数据
            optimized_fetcher = OptimizedKlineFetcher(fetcher, max_concurrent=FETCH_CONCURRENCY)
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
                self._db_manager.execute_write(f"insert into {market}_{interval} select * from df on conflict do nothing;",
                                         df=df)
            
            # 保存当前时间到config_dict表
            try:
                self._flight_actions.duck_time[market] = str(current_time)
                self._db_manager.execute_write("INSERT OR REPLACE INTO config_dict (key, value) VALUES (?, ?)",
                                         (f'{market}_duck_time', str(current_time)))
                logger.info(f"已保存 {market} 的当前时间: {current_time}")
            except Exception as e:
                logger.error(f"保存 {market} 当前时间失败: {e}")

    def update_recent_data(self):
        """更新近期数据"""
        pass

    def duckdb_retention_policy(self):
        """DuckDB数据保留策略"""
        """定时导出parquet文件,并清理过期的duckdb数据"""
        if RETENTION_DAYS == 0:
            logger.info("Retention days is set to 0, skipping retention job.")
            return
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def periodic():
            while True:
                next_time = next_run_time('1h') + timedelta(minutes=3)  # 每小时的5分执行
                divider(f"[Scheduler] next retention job runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
                await async_sleep_until_run_time(next_time)
                try:
                    await self._duckdb_retention_async()
                except Exception as e:
                    logger.error(f"Scheduler Error: {e}")

        loop.run_until_complete(periodic())

    async def _duckdb_retention_async(self):
        """定时导出一小时前的数据到parquet，并清理n周前的parquet文件"""
        logger.info("[Scheduler] Starting periodic cleanup task")

        try:
            # 清理过期的duckdb数据
            self._db_manager.execute_write(
                f"DELETE FROM usdt_perp_{KLINE_INTERVAL} WHERE open_time < now() - interval '{RETENTION_DAYS} days'")
            self._db_manager.execute_write(
                f"DELETE FROM usdt_spot_{KLINE_INTERVAL} WHERE open_time < now() - interval '{RETENTION_DAYS} days'")
            logger.info("[Scheduler] Cleaned up old DuckDB data")
        except Exception as e:
            logger.error(f"Error during DuckDB cleanup: {e}")

