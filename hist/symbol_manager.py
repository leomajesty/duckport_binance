"""
Symbol management module for data center
Handles symbol listing and filtering
"""

import asyncio
import aiohttp
from lxml import objectify
from utils.log_kit import logger
from utils.config import root_center_url, proxy, blind


async def request_session(session, params):
    """Make HTTP request with retry logic"""
    while True:
        try:
            async with session.get(root_center_url, params=params, proxy=proxy, timeout=20) as response:
                return await response.text()
        except aiohttp.ClientError as ae:
            logger.warning(f'请求失败，继续重试, 错误信息:{ae}')
        except Exception as e:
            logger.warning(f'请求失败，继续重试:{e}')


async def request_session_4_list(session, params):
    """Make HTTP request for list data with pagination"""
    result = []
    while True:
        try:
            async with session.get(root_center_url, params=params, proxy=proxy, timeout=20) as response:
                data = await response.text()
                root = objectify.fromstring(data.encode('ascii'))
                result.append(root)

                if root.IsTruncated:
                    # 还有下一页
                    params = {
                        'delimiter': '/',
                        'prefix': root.Prefix.text,
                        'marker': root.NextMarker.text
                    }
                    continue  # 继续下一页内容请求
                else:
                    return result
        except aiohttp.ClientError as ae:
            if not blind:
                logger.error('请求失败，继续重试', ae)
        except Exception as e:
            if not blind:
                logger.error('请求失败，继续重试', e)


async def get_symbols(params):
    """Get symbols from API"""
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        result = await get_symbols_by_session(session, params)
        return result


async def get_symbols_by_session(session, params):
    """Get symbols using existing session"""
    data = await request_session(session, params)
    root = objectify.fromstring(data.encode('ascii'))
    result = []
    for item in root.CommonPrefixes:
        param = item.Prefix
        s = param.text.split('/')
        result.append(s[len(s) - 2])
    if root.IsTruncated:
        # 下一页的网址
        params['marker'] = root.NextMarker.text
        next = await get_symbols_by_session(session, params)
        result.extend(next)  # 初次循环时，link_lst 包含1000条以上的数据
    return result


def async_get_all_symbols(params):
    """Get all symbols synchronously"""
    return asyncio.run(get_symbols(params))


def async_get_usdt_symbols(params):
    """Get USDT symbols only"""
    all_symbols = async_get_all_symbols(params)
    usdt = set()
    [usdt.add(i) for i in all_symbols if i.endswith('USDT')]
    return usdt


def spot_symbols_filter(symbols):
    """Filter spot symbols to remove stable coins and special pairs"""
    others = []
    stable_symbol = ['BKRW', 'USDC', 'USDP', 'TUSD', 'BUSD', 'FDUSD', 'DAI', 'EUR', 'GBP', 'AEUR']
    # stable_symbols：稳定币交易对
    stable_symbols = [s + 'USDT' for s in stable_symbol]
    # special_symbols：容易误判的特殊交易对
    special_symbols = ['JUPUSDT']
    pure_spot_symbols = []
    
    for symbol in symbols:
        if symbol in special_symbols:
            pure_spot_symbols.append(symbol)
            continue
        if symbol.endswith('UPUSDT') or symbol.endswith('DOWNUSDT') or symbol.endswith('BULLUSDT') or symbol.endswith('BEARUSDT'):
            others.append(symbol)
            continue
        if symbol in stable_symbols:
            others.append(symbol)
            continue
        pure_spot_symbols.append(symbol)
    
    logger.info(f'过滤掉的现货symbol {others}')
    return pure_spot_symbols
