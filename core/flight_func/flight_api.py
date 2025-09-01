from datetime import *
import os
import glob
import pandas as pd
from dateutil import parser
import pyarrow as pa
import pyarrow.flight as flight

from utils.log_kit import logger
from utils.config import SUFFIX, KLINE_INTERVAL_MINUTES
from utils.timer import timer
from utils.db_manager import DatabaseManager


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
                result = self.db_manager.fetch_one(
                    f"SELECT max(open_time) as max_time from read_parquet('{self.pqt_path}/{market}{SUFFIX}/*.parquet')")
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
            historical_begin = begin - timedelta(hours=self.redundancy_hours)
            historical_end = min(end, threshold) + timedelta(hours=self.redundancy_hours)
            recent_begin = max(begin, threshold) - timedelta(hours=self.redundancy_hours)
            recent_end = end + timedelta(hours=self.redundancy_hours)

            logger.info(
                f"扩展查询范围: 历史数据({historical_begin} - {historical_end}), 近期数据({recent_begin} - {recent_end})")

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