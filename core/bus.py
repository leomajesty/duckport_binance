from .filter_symbol import TradingSpotFilter, TradingCoinFuturesFilter, TradingUsdtFuturesFilter

# 交割合约包含 CURRENT_QUARTER(当季合约), NEXT_QUARTER(次季合约)
DELIVERY_TYPES = ['CURRENT_QUARTER', 'NEXT_QUARTER']

TRADE_TYPE_MAP = {
    # spot
    'usdt_spot': (TradingSpotFilter(quote_asset='USDT', keep_stablecoins=False), 'spot'),
    'usdc_spot': (TradingSpotFilter(quote_asset='USDC', keep_stablecoins=False), 'spot'),
    'btc_spot': (TradingSpotFilter(quote_asset='BTC', keep_stablecoins=False), 'spot'),

    # usdt_futures
    'usdt_perp': (TradingUsdtFuturesFilter(quote_asset='USDT', types=['PERPETUAL']), 'um'),
    'usdt_deli': (TradingUsdtFuturesFilter(quote_asset='USDT', types=DELIVERY_TYPES), 'um'),
    'usdc_perp': (TradingUsdtFuturesFilter(quote_asset='USDC', types=['PERPETUAL']), 'um'),

    # 仅包含 ETHBTC 永续合约，属于 U 本位合约
    'btc_perp': (TradingUsdtFuturesFilter(quote_asset='BTC', types=['PERPETUAL']), 'um'),

    # coin_futures
    'coin_perp': (TradingCoinFuturesFilter(types=['PERPETUAL']), 'um'),
    'coin_deli': (TradingCoinFuturesFilter(types=DELIVERY_TYPES), 'um'),
}