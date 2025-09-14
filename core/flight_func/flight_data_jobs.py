import abc
import asyncio
import threading
from datetime import *
import pandas as pd
import sys

from core.flight_func.flight_api import FlightActions, FlightGets
from utils import next_run_time, async_sleep_until_run_time
from utils.log_kit import logger, divider
from utils.config import SUFFIX, KLINE_INTERVAL_MINUTES, RETENTION_DAYS, KLINE_INTERVAL, START_DATE
from utils.db_manager import DatabaseManager
from core.component.candle_fetcher import BinanceFetcher, OptimizedKlineFetcher
from utils import create_aiohttp_session
from utils.config import FETCH_CONCURRENCY
from core.bus import TRADE_TYPE_MAP
from utils.timer import timer, func_timer

class DataJobs:
    def __init__(self, db_manager: DatabaseManager, flight_actions: FlightActions, flight_gets: FlightGets):
        self._flight_gets = flight_gets
        self._flight_actions = flight_actions
        self._db_manager: DatabaseManager = db_manager

        self.init_history_data()
        self.update_recent_data()

    def init_history_data(self):
        """初始化历史数据"""
        logger.info("开始初始化历史数据")
        current_symbols = {}

        async def history_data():
            # 第一步：获取最新数据时间
            latest_times, latest_symbols = self._get_latest_data_time()
            current_time = next_run_time(KLINE_INTERVAL) - timedelta(minutes=KLINE_INTERVAL_MINUTES)
            async with create_aiohttp_session(10) as session:
                fetchers = {}
                for market in ['usdt_perp', 'usdt_spot']:
                    fetchers[market] = BinanceFetcher(market, session)
                    exginfo = await fetchers[market].get_exchange_info()
                    current_symbols[market] = TRADE_TYPE_MAP[market][0](exginfo)
                    if latest_times[market]:
                        await self._check_symbol_consistency(market, current_symbols[market], latest_symbols[market])

                # 第三步：更新历史K线数据
                for market in ['usdt_perp', 'usdt_spot']:
                    if latest_times[market]:
                        await self._update_historical_klines(fetchers[market], current_symbols[market],
                                                             latest_times[market], market, current_time)

        asyncio.run(history_data())
        logger.info("历史数据初始化完成")

    def _get_latest_data_time(self):
        """获取最新数据时间，优先ducktime，其次pqttime"""
        latest_times = {}
        latest_symbols = {}

        for market in ['usdt_perp', 'usdt_spot']:
            try:
                # 尝试获取ducktime
                duck_time = self._flight_actions.duck_time[market]
                pqt_time = self._flight_gets.pqt_time[market]
                if duck_time and duck_time != pd.to_datetime('2009-01-03 00:00:00').tz_localize(tz=timezone.utc):
                    latest_times[market] = duck_time
                    latest_symbols[market] = self.get_trading_symbols_by_time(duck_time, market, is_duck=True)
                    logger.info(f"{market} 使用ducktime: {latest_times[market]}")
                elif pqt_time and pqt_time != pd.to_datetime('2009-01-03 00:00:00').tz_localize(tz=timezone.utc):
                    latest_times[market] = pqt_time
                    latest_symbols[market] = self.get_trading_symbols_by_time(pqt_time, market, is_duck=False)
                    logger.info(f"{market} 使用pqttime: {latest_times[market]}")
                else:
                    logger.warning(f"{market} 使用config.env中的配置")
                    latest_times[market] = pd.to_datetime(START_DATE).tz_localize(tz=timezone.utc)
                    latest_symbols[market] = None
            except Exception as e:
                logger.error(f"Failed to _get_latest_data_time: {e}")
                sys.exit(1)
        return latest_times, latest_symbols

    def get_trading_symbols_by_time(self, snaptime, market, is_duck=True):
        if is_duck:
            # duck_time是字符串格式
            df = self._db_manager.fetch_df(f"select symbol from {market}{SUFFIX} where open_time = '{snaptime}'")
            return df['symbol'].to_list()
        else:
            # pqt_time是pandas Timestamp格式，需要转换为字符串
            snaptime_str = pd.to_datetime(snaptime).strftime('%Y-%m-%d %H:%M:%S')
            df = self._db_manager.fetch_df(
                f"SELECT symbol from read_parquet('{self._flight_gets._pqt_path}/{market}{SUFFIX}/*.parquet') where open_time = '{snaptime_str}'")
            return df['symbol'].to_list()

    @staticmethod
    async def _check_symbol_consistency(market, current_symbols: set, historical_symbols: set):
        """检查币种一致性，如果有币种下架则退出程序"""
        try:
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

    async def _update_historical_klines(self, fetcher, symbols, start_time, market, current_time):
        """更新历史K线数据"""
        optimized_fetcher = OptimizedKlineFetcher(fetcher, max_concurrent=FETCH_CONCURRENCY)
        res = await optimized_fetcher.get_all_klines(symbols, start_time=start_time, interval='5m',
                                                     limit=499)
        df = pd.concat([i['data'] for i in res])
        df.sort_values(by=['open_time'], inplace=True)
        # 保证k线闭合
        df = df[df['open_time'] < current_time]
        self.write_kline(df, market, current_time)

    @func_timer
    async def save_exginfo(self, fetcher, market):
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
            self._db_manager.execute_write(
                f"INSERT INTO exginfo ({', '.join(columns)}) SELECT {', '.join(columns)} FROM df;",
                df=symbols_trading_df)
            logger.info(f"symbols_trading数据已保存到数据库: {market}, {len(symbols_trading_df)} 条记录")
        except Exception as e:
            logger.error(f"保存symbols_trading数据失败: {e}")
        return symbols_trading

    def write_kline(self, df, market, current_time):
        with timer("write to duckdb"):
            # 写入数据库忽略重复数据
            self._db_manager.execute_write(
                f"insert into {market}_{KLINE_INTERVAL} select * from df on conflict do nothing;",
                df=df)

        # 保存当前时间到config_dict表
        try:
            print(current_time)
            self._flight_actions.duck_time[market] = pd.to_datetime(current_time)
            self._db_manager.execute_write("INSERT OR REPLACE INTO config_dict (key, value) VALUES (?, ?)",
                                           (f'{market}_duck_time', current_time.strftime("%Y-%m-%d %H:%M:%S")))
            logger.info(f"已保存 {market} 的当前时间: {current_time}")
        except Exception as e:
            logger.error(f"保存 {market} 当前时间失败: {e}")

    @abc.abstractmethod
    def update_recent_data(self):
        raise NotImplementedError()

    def duckdb_retention_policy(self):
        """DuckDB数据保留策略"""
        """定时导出parquet文件,并清理过期的duckdb数据"""
        if RETENTION_DAYS == 0:
            logger.info("Retention days is set to 0, skipping retention job.")
            return

        async def periodic():
            while True:
                next_time = next_run_time('1h') + timedelta(minutes=3)  # 每小时的5分执行
                divider(f"[Scheduler] next retention job runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
                await async_sleep_until_run_time(next_time)
                try:
                    await self._duckdb_retention_async()
                except Exception as e:
                    logger.error(f"Scheduler Error: {e}")

        asyncio.run(periodic())

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

class RestfulDataJobs(DataJobs):
    def __init__(self, db_manager: DatabaseManager, flight_actions: FlightActions, flight_gets: FlightGets):
        super().__init__(db_manager, flight_actions, flight_gets)

    def update_recent_data(self):
        """更新近期数据"""
        threading.Thread(target=self.duckdb_retention_policy, daemon=True).start()
        threading.Thread(target=self.duckdb_periodic_fetch_policy, daemon=True).start()

    def duckdb_periodic_fetch_policy(self):
        """DuckDB数据更新"""

        async def periodic():
            while True:
                await self._duckdb_periodic_fetch_async()

        asyncio.run(periodic())

    async def _duckdb_periodic_fetch_async(self):
        next_time = next_run_time(KLINE_INTERVAL)
        divider(f"[Scheduler] next fetch runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
        await async_sleep_until_run_time(next_time)
        try:
            await self._fetch_and_insert_binance_data_async(market='usdt_perp', current_time=next_time,
                                                            interval=KLINE_INTERVAL)
            await self._fetch_and_insert_binance_data_async(market='usdt_spot', current_time=next_time,
                                                            interval=KLINE_INTERVAL)
        except Exception as e:
            logger.error(f"Scheduler Error: {e}")

    async def _fetch_and_insert_binance_data_async(self, market, current_time, interval='5m'):
        """异步获取K线并写入duckdb表（从flight_server.py复制的方法）"""

        async with create_aiohttp_session(10) as session:
            fetcher = BinanceFetcher(market, session)
            symbols_trading = await self.save_exginfo(fetcher, market)
            # 获取K线数据
            optimized_fetcher = OptimizedKlineFetcher(fetcher, max_concurrent=FETCH_CONCURRENCY)
            results = await optimized_fetcher.get_all_klines(symbols_trading, interval=interval, limit=99)

            # 过滤掉失败的结果
            successful_results = [r['data'] for r in results if r.get('success', False)]
            if not successful_results:
                logger.info("没有成功的结果，无法保存")
                return

            df = pd.concat(successful_results)
            df = df[df['open_time'] < current_time]
            self.write_kline(df, market, current_time)

class WebsocketsDataJobs(DataJobs):
    def __init__(self, db_manager: DatabaseManager, flight_actions: FlightActions, flight_gets: FlightGets):
        super().__init__(db_manager, flight_actions, flight_gets)

    def update_recent_data(self):
        threading.Thread(target=self.duckdb_retention_policy, daemon=True).start()