import pandas as pd
import duckdb
from glob import glob
from utils.config import KLINE_INTERVAL, ENABLE_PQT, DUCKDB_DIR
from tqdm import tqdm


def get_all_trading_range(market: str):
    if ENABLE_PQT:
        res = con.execute(f"""
            SELECT symbol, min(open_time) as first_candle, max(open_time) as last_candle FROM read_parquet('data/pqt/{market}_{KLINE_INTERVAL}/*.parquet') where volume > 0 group by symbol 
        """)
    else:
        res = con.execute(f"""
            SELECT symbol, min(open_time) as first_candle, max(open_time) as last_candle FROM {market}_{KLINE_INTERVAL} where volume > 0 group by symbol 
        """)
    return res.df()

def find_useless_symbols(market: str):
    if ENABLE_PQT:
        res = con.execute(f"""
            SELECT symbol, sum(volume) FROM read_parquet('data/pqt/{market}_{KLINE_INTERVAL}/*.parquet') group by symbol having sum(volume) = 0 
        """)
    else:
        res = con.execute(f"""
            SELECT symbol, sum(volume) as first_candle, max(open_time) as last_candle FROM {market}_{KLINE_INTERVAL} group by symbol having sum(volume) = 0
        """)
    return res.df()

# 移除所有在交易时间以外的数据
def remove_out_of_trading_time(market: str):
    trading_range = get_all_trading_range(market)
    useless_symbols = find_useless_symbols(market)
    s = ','.join(useless_symbols['symbol'])
    if ENABLE_PQT:
        files = glob(f'data/pqt/{market}_{KLINE_INTERVAL}/*.parquet')
        for file in tqdm(files, desc=f'处理{market}数据'):
            df = pd.read_parquet(file)
            df = df.merge(trading_range, on='symbol', how='left')
            df = df[df['open_time'] >= df['first_candle']]
            df = df[df['open_time'] <= df['last_candle']]
            if len(useless_symbols) > 0:
                df = df[~df['symbol'].isin(useless_symbols['symbol'])]
            df = df.drop(columns=['first_candle', 'last_candle'])
            df.to_parquet(file, index=False)
    else:
        for idx, row in tqdm(trading_range.iterrows(), desc=f'处理{market}数据'):
            con.execute(f"""
                DELETE FROM {market}_{KLINE_INTERVAL} WHERE symbol = '{row['symbol']}' AND (open_time < '{row['first_candle']}' OR open_time > '{row['last_candle']}')
            """)
        if len(useless_symbols) > 0:
            con.execute(f"""
                DELETE FROM {market}_{KLINE_INTERVAL} WHERE symbol IN ('{'\',\''.join(useless_symbols['symbol'])}')
            """)

if __name__ == "__main__":
    con = duckdb.connect(DUCKDB_DIR)
    remove_out_of_trading_time('usdt_perp')
    remove_out_of_trading_time('usdt_spot')
    con.close()