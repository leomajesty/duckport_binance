from .ws_basics import ReconnectingWebsocket
from utils.config import proxy

# 现货 WS Base Url
SPOT_STREAM_URL = 'wss://stream.binance.com:9443/'

# U 本位合约 WS Base Url
USDT_FUTURES_FSTREAM_URL = 'wss://fstream.binance.com/'

# 币本位合约 WS Base Url
COIN_FUTURES_DSTREAM_URL = 'wss://dstream.binance.com/'


def get_coin_futures_multi_candlesticks_socket(symbols, time_inteval):
    """
    返回币本位合约单周期多个 symbol K 线 websocket 连接
    """
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=COIN_FUTURES_DSTREAM_URL,
        prefix='stream?streams=',
        proxy=proxy
    )


def get_usdt_futures_multi_candlesticks_socket(symbols, time_inteval):
    """
    返回 U 本位合约单周期多个 symbol K 线 websocket 连接
    """
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=USDT_FUTURES_FSTREAM_URL,
        prefix='stream?streams=',
        proxy=proxy
    )

def get_spot_multi_candlesticks_socket(symbols, time_inteval):
    """
    返回现货单周期多个 symbol K 线 websocket 连接
    """
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=SPOT_STREAM_URL,
        prefix='stream?streams=',
        proxy=proxy
    )
