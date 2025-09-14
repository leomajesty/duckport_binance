import abc
import asyncio
from datetime import datetime
from typing import Dict, Any

import pandas as pd

from core.api import (get_coin_futures_multi_candlesticks_socket, get_spot_multi_candlesticks_socket,
                         get_usdt_futures_multi_candlesticks_socket)
from core.bus import TRADE_TYPE_MAP
from core.component.candle_fetcher import BinanceFetcher
from utils import convert_interval_to_timedelta, get_logger, create_aiohttp_session
from utils.config import CONCURRENCY, KLINE_INTERVAL, SUFFIX
from utils.db_manager import KlineDBManager
from utils.log_kit import logger, divider
from utils.time import now_time, async_sleep_until_run_time, next_run_time


def convert_to_dataframe(x, interval_delta):
    """
    解析 WS 返回的数据字典，返回 DataFrame
    """
    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
    ]
    candle_data = [
        pd.to_datetime(int(x['t']), unit='ms', utc=True),
        float(x['o']),
        float(x['h']),
        float(x['l']),
        float(x['c']),
        float(x['v']),
        float(x['q']),
        int(x['n']),
        float(x['V']),
        float(x['Q'])
    ]

    # 以 K 线结束时间为时间戳
    return pd.DataFrame(data=[candle_data], columns=columns, index=[candle_data[0] + interval_delta])


class CandleListener:

    # 交易类型到 ws 函数映射
    TRADE_TYPE_MAP = {
        'usdt_perp': get_usdt_futures_multi_candlesticks_socket,
        'coin_perp': get_coin_futures_multi_candlesticks_socket,
        'usdt_spot': get_spot_multi_candlesticks_socket
    }

    def __init__(self, type_, symbols, time_interval, que):
        # 交易类型
        self.trade_type = type_
        # 交易标的
        self.symbols = set(symbols)
        # K 线周期
        self.time_interval = time_interval
        self.interval_delta = convert_interval_to_timedelta(time_interval)
        # 消息队列
        self.que: asyncio.Queue = que
        # 重链接 flag
        self.req_reconnect = False

    async def start_listen(self):
        """
        WS 监听主函数
        """

        if not self.symbols:
            return

        socket_func = self.TRADE_TYPE_MAP[self.trade_type]
        while True:
            # 创建 WS
            socket = socket_func(self.symbols, self.time_interval)
            async with socket as socket_conn:
                # WS 连接成功后，获取并解析数据
                while True:
                    if self.req_reconnect:  # 如果需要重连，则退出重新连接
                        self.req_reconnect = False
                        break
                    try:
                        res = await socket_conn.recv()
                        self.handle_candle_data(res)
                    except asyncio.TimeoutError:  # 如果长时间未收到数据（默认60秒，正常情况K线每1-2秒推送一次），则退出重新连接
                        get_logger().error('Recv candle ws timeout, reconnecting')
                        break

    def handle_candle_data(self, res):
        """
        处理 WS 返回数据
        """

        # 防御性编程，如果币安出现错误未返回 data 字段，则抛弃
        if 'data' not in res:
            return

        # 取出 data 字段
        data = res['data']

        # 防御性编程，如果 data 中不包含 e 字段或 e 字段（数据类型）不为 kline 或 data 中没有 k 字段（K 线数据），则抛弃
        if data.get('e', None) != 'kline' or 'k' not in data:
            return

        # 取出 k 字段，即 K 线数据
        candle = data['k']

        # 判断 K 线是否闭合，如未闭合则抛弃
        is_closed = candle.get('x', False)
        if not is_closed:
            return

        # 将 K 线转换为 DataFrame
        df_candle = convert_to_dataframe(candle, self.interval_delta)

        # 将 K 线 DataFrame 放入通信队列
        self.que.put_nowait({
            'type': 'candle_data',
            'data': df_candle,
            'closed': is_closed,
            'run_time': df_candle.index[0],
            'symbol': data['s'],
            'time_interval': self.time_interval,
            'trade_type': self.trade_type,
            'recv_time': now_time()
        })

    def add_symbols(self, *symbols):
        for symbol in symbols:
            self.symbols.add(symbol)

    def remove_symbols(self, *symbols):
        for symbol in symbols:
            if symbol in self.symbols:
                self.symbols.remove(symbol)

    def reconnect(self):
        self.req_reconnect = True


class MarketListener:

    def __init__(self, market='usdt_perp', db_manager: KlineDBManager = None, data_callback=None):
        self.main_queue = asyncio.Queue()
        self.market = market
        self.listeners = {}
        # 延迟初始化symbols，避免在事件循环中调用asyncio.run
        self.symbols = None
        self._db_manager = db_manager
        # 数据收集相关变量
        self._data_batches = {}  # {run_time: {symbol: kline_data}}
        self._batch_status = {}  # {run_time: {'collected_symbols': set(), 'total_symbols': int}}
        # 数据处理回调函数
        self.data_callback = data_callback

    async def build_and_run(self):
        # 初始化symbols
        if self.symbols is None:
            self.symbols = set(await self.get_exginfo())

        self.listeners = self.create_listeners(market=self.market, symbols=self.symbols, que=self.main_queue)
        listen_tasks = [v.start_listen() for k, v in self.listeners.items()]
        dispatcher_task = self.dispatcher()
        scheduled_task = self.scheduled_run()

        logger.ok(f'{self.market} Market Listener initialized...')

        tasks = listen_tasks + [dispatcher_task, scheduled_task]

        await asyncio.gather(*tasks)

    def _collect_kline_data(self, run_time: datetime, symbol: str, kline_data: Dict[str, Any]) -> None:
        """
        按run_time收集kline数据

        Args:
            run_time: 数据时间戳
            symbol: 交易对符号
            kline_data: kline数据字典
        """
        try:
            # 初始化run_time的数据批次
            if run_time not in self._data_batches:
                self._data_batches[run_time] = {}
                self._batch_status[run_time] = {
                    'collected_symbols': set(),
                }

            # 存储kline数据
            self._data_batches[run_time][symbol] = kline_data
            self._batch_status[run_time]['collected_symbols'].add(symbol)

            collected = len(self._batch_status[run_time]['collected_symbols'])
            if collected % 100 == 0:
                logger.debug(f"[Websocket] {self.market}, process: {collected} / {len(self.symbols)}")

        except Exception as e:
            logger.error(f"收集kline数据失败: {e}, run_time: {run_time}, symbol: {symbol}")

    def _check_batch_completeness(self, run_time: datetime) -> bool:
        """
        检查指定run_time的数据批次是否完整

        Args:
            run_time: 数据时间戳

        Returns:
            bool: 如果数据完整返回True，否则返回False
        """
        try:
            if run_time not in self._batch_status:
                return False

            status = self._batch_status[run_time]
            collected_symbols = status['collected_symbols']

            # 检查是否收集了所有symbol的数据
            is_complete = (len(collected_symbols) == len(self.symbols) and collected_symbols == self.symbols)

            if is_complete:
                logger.ok(f"[Websocket] {self.market} - process done. total: {len(collected_symbols)}")

            return is_complete

        except Exception as e:
            logger.error(f"检查批次完整性失败: {e}, run_time: {run_time}")
            return False

    async def _batch_write_to_db(self, run_time: datetime) -> None:
        if self.data_callback:
            await self.data_callback(run_time, self._data_batches[run_time], self.market)
        else:
            logger.warning('_batch_write_to_db has not callback function.')


    def _cleanup_batch_data(self, run_time: datetime) -> None:
        """
        清理指定run_time的已处理数据批次

        Args:
            run_time: 数据时间戳
        """
        try:
            # 清理数据批次
            if run_time in self._data_batches:
                del self._data_batches[run_time]

            # 清理状态信息
            if run_time in self._batch_status:
                del self._batch_status[run_time]

        except Exception as e:
            logger.error(f"清理数据批次失败: {e}, run_time: {run_time}")

    async def dispatcher(self):
        while True:
            req = await self.main_queue.get()
            run_time = req['run_time']
            req_type = req['type']
            if req_type == 'candle_data':
                # 收集kline数据
                self._collect_kline_data(run_time, req['symbol'], req)

                # 检查批次完整性
                if self._check_batch_completeness(run_time):
                    # 批量写入数据库
                    await self._batch_write_to_db(run_time)
                    # 清理已处理的数据
                    self._cleanup_batch_data(run_time)

    async def scheduled_run(self):
        while True:
            # 计算出 self.interval 周期下次运行时间 run_time, 并 sleep 到 run_time
            next_time = next_run_time(KLINE_INTERVAL)
            divider(f"[{self.market} Listener] next fetch runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
            await async_sleep_until_run_time(next_time)
            await asyncio.sleep(10)  # 避免紧邻时间点，导致多次执行
            await self.update_exginfo()
            await asyncio.sleep(1)  # 避免紧邻时间点，导致多次执行

    async def get_exginfo(self):
        async with create_aiohttp_session(10) as session:
            fetcher = BinanceFetcher(self.market, session)
            exginfo = await fetcher.get_exchange_info()
            symbols_trading = TRADE_TYPE_MAP[self.market][0](exginfo)
            # TODO 更新缓存，更新数据库
        logger.info(f'Fetched {len(symbols_trading)} trading symbols from exchange info')
        return symbols_trading

    async def update_exginfo(self):
        symbols_trading = await self.get_exginfo()
        # 更新ws订阅
        delist = self.symbols - set(symbols_trading)
        onboard = set(symbols_trading) - self.symbols

        changed_groups = set()

        if delist:
            logger.warning(f'Symbols delist: {delist}')
            for symbol in delist:
                group_id = hash(symbol) % CONCURRENCY
                if group_id in self.listeners:
                    listener: CandleListener = self.listeners[group_id]
                    listener.remove_symbols(symbol)
                    changed_groups.add(group_id)

        if onboard:
            logger.warning(f'Symbols onboard: {onboard}')
            for symbol in onboard:
                group_id = hash(symbol) % CONCURRENCY
                if group_id not in self.listeners:
                    # 创建新的监听器组
                    self.listeners[group_id] = CandleListener(self.market, [symbol], KLINE_INTERVAL, self.main_queue)
                    logger.info(f'Created new listener group {group_id} for symbol {symbol}')
                else:
                    listener: CandleListener = self.listeners[group_id]
                    listener.add_symbols(symbol)
                changed_groups.add(group_id)

        for group_id in changed_groups:
            if group_id in self.listeners:
                listener: CandleListener = self.listeners[group_id]
                listener.reconnect()

        self.symbols = set(symbols_trading)
        # TODO 发送消息

    @staticmethod
    def create_listeners(market, symbols, que) -> dict[int, CandleListener]:
        groups = [[] for i in range(CONCURRENCY)]
        for sym in symbols:
            group_id = hash(sym) % CONCURRENCY
            groups[group_id].append(sym)
        listeners = {}
        for idx, grp in enumerate(groups):
            num = len(grp)
            if num > 0:
                logger.debug(f'Create WS listen group {idx}, {num} symbols')
                listeners[idx] = CandleListener(market, grp, KLINE_INTERVAL, que)
        return listeners