import os
import time
import csv
from binance.client import Client
from binance.websockets import BinanceSocketManager


def load_env_variables():
    """gets api keys"""
    return (os.environ['BINANCE_API_KEY'], os.environ['BINANCE_API_SECRET'])

def process_orderbook(msg):
    """processes the orderbook data"""
    bid_price = {level: float(price[0]) for level, price in zip(range(-int(orderbook_depth), 0), msg['bids'])}
    ask_price = {level: float(price[0]) for level, price in zip(range(1, int(orderbook_depth)+1), msg['asks'][::-1])}
    bid_volume = {level: float(vol[1]) for level, vol in zip(range(-int(orderbook_depth), 0), msg['bids'])}
    ask_volume = {level: float(vol[1]) for level, vol in zip(range(1, int(orderbook_depth)+1), msg['asks'][::-1])}
    # current_time = {'time': time.time()}
    current_time = {'time': int(round(time.time() * 1000))}  # time in ms
    prices = {**current_time, **ask_price, **bid_price}
    volume = {**current_time, **ask_volume, **bid_volume}
    date = time.strftime("%Y%m%d")
    price_path = '/home/bitnami/ZEROPLUS/data/orderbook/{}_orderbook_price{}'.format(date, '.csv')
    volume_path = '/home/bitnami/ZEROPLUS/data/orderbook/{}_orderbook_volume{}'.format(date, '.csv')
    add_header(price_path, volume_path)
    with open(price_path, 'a') as f1:
        w = csv.DictWriter(f1, prices.keys())
        w.writerow(prices)
        f1.close()
    with open(volume_path, 'a') as f2:
        w = csv.DictWriter(f2, volume.keys())
        w.writerow(volume)
    f2.close()

def process_trade(msg):
    current_time = {'time': int(round(time.time() * 1000))}
    msg = {**current_time, **msg}
    date = time.strftime("%Y%m%d")
    trades_path = '/home/bitnami/ZEROPLUS/data/trades/{}_trades{}'.format(date, '.csv')
    names = ['timestamp', 'event_tpye', 'event_time', 'symbol', 'trade_id', 'price', 'quantity',
             'buyer_order_id', 'seller_order_id', 'trade_time', 'buyer_market_maker', 'Ignore']
    if not os.access(trades_path, os.F_OK):
        with open(trades_path, 'a') as f3:
            writer = csv.writer(f3)
            writer.writerow(names)
            f3.close()
    with open(trades_path, 'a') as f3:
        w = csv.DictWriter(f3, msg.keys())
        w.writerow(msg)
        f3.close()

def add_header(price_path, volume_path):
    col_names = ['time', 'ask_20', 'ask_19', 'ask_18', 'ask_17', 'ask_16', 'ask_15', 'ask_14', 'ask_13',
                 'ask_12', 'ask_11', 'ask_10', 'ask_9', 'ask_8', 'ask_7', 'ask_6', 'ask_5', 'ask_4', 'ask_3',
                 'ask_2', 'ask_1', 'bid_1', 'bid_2', 'bid_3', 'bid_4', 'bid_5', 'bid_6', 'bid_7', 'bid_8',
                 'bid_9', 'bid_10', 'bid_11', 'bid_12', 'bid_13', 'bid_14', 'bid_15', 'bid_16', 'bid_17',
                 'bid_18', 'bid_19', 'bid_20']
    if not os.access(price_path, os.F_OK):
        with open(price_path, 'a') as f1:
            writer = csv.writer(f1)
            writer.writerow(col_names)
            f1.close()
    if not os.access(volume_path, os.F_OK):
        with open(volume_path, 'a') as f2:
            writer = csv.writer(f2)
            writer.writerow(col_names)
            f2.close()

if __name__ == "__main__":

    # date = time.strftime("%Y%m%d")
    # price_path = '/home/bitnami/ZEROPLUS/data/orderbook/{}_orderbook_price{}'.format(date, '.csv')
    # volume_path = '/home/bitnami/ZEROPLUS/data/orderbook/{}_orderbook_volume{}'.format(date, '.csv')
    # trades_path = '/home/bitnami/ZEROPLUS/data/trades/{}_trades{}'.format(date, '.csv')
    api_key, api_secret = load_env_variables()
    client = Client(api_key, api_secret)
    sym = 'BTCUSDT'
    bm = BinanceSocketManager(client)
    orderbook_depth = BinanceSocketManager.WEBSOCKET_DEPTH_20
    conn_key = bm.start_depth_socket(sym, process_orderbook, depth=orderbook_depth)
    partial_key = bm.start_trade_socket(sym, process_trade)
    bm.start()
    print('running')
