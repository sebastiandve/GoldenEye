from cryptofeed.callback import TradeCallback, BookCallback
from cryptofeed import FeedHandler
from cryptofeed.exchanges import (
    Binance,
    Huobi,
    Upbit,
    Bybit,
    KuCoin,
    Bitmex,
    Bitfinex,
    BinanceFutures,
    KrakenFutures,
)
from cryptofeed.defines import TRADES, L2_BOOK
from save_data import book_to_csv
import asyncio
import os


symbol = "BTC-USDT-PERP"
bitmex_symbol = "BTC-USDT-PERP"
kwargs = {
    "max_depth": 10,
    'snapshot_interval': 1000,
    "channels": [L2_BOOK],
    "callbacks": {L2_BOOK: BookCallback(book_to_csv)},
}

f = FeedHandler()


def delete_files():
    files = ["data/" + x for x in os.listdir("data/") if x[0:4] == "book"]
    for file in files:
        os.remove(file)


def stop():
    loop = asyncio.get_event_loop()
    loop.stop()


def main():
    loop = asyncio.get_event_loop()
    f.add_feed(KrakenFutures(symbols=["BTC-USD-PERP"], **kwargs))
    f.add_feed(Bitmex(symbols=[symbol], **kwargs))
    f.add_feed(Bitfinex(symbols=[symbol], **kwargs))
    f.add_feed(BinanceFutures(symbols=[symbol], **kwargs))

    f.run(start_loop=False)
    # loop.call_later(30, stop)
    print("start")
    loop.run_forever()
    print("end")


if __name__ == "__main__":
    delete_files()
    main()
