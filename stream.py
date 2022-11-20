from cryptofeed.callback import TradeCallback, BookCallback
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance, Huobi, Upbit, FTX, Bybit, Kraken, KuCoin, Bitmex, Bitfinex, BinanceFutures
from cryptofeed.defines import TRADES, L2_BOOK
from save_data import book_to_csv
import asyncio
import os


symbol = "BTC-USDT-PERP"
bitmex_symbol = "BTC-USDT-PERP"
kwargs = {
    "max_depth": 10,
    # 'snapshot_interval': 1,
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
    f.add_feed(Binance(symbols=["BTC-USDT"], **kwargs))
    # f.add_feed(Huobi(**kwargs))
    # f.add_feed(OKEx(**kwargs))
    # # AVOID f.add_feed(Upbit(symbols=[symbol], channels=[L2_BOOK], callbacks={L2_BOOK: BookCallback(book)}))
    # f.add_feed(FTX(**kwargs))
    # # AVOID f.add_feed(Bybit(symbols=[symbol], channels=[L2_BOOK], callbacks={L2_BOOK: BookRedis()}))
    # f.add_feed(Kraken(symbols=[symbol], channels=[L2_BOOK], callbacks={L2_BOOK: BookCallback(book)}))
    # f.add_feed(KuCoin(**kwargs))
    f.add_feed(Bitmex(symbols=[bitmex_symbol], **kwargs))
    # f.add_feed(Bitfinex(**kwargs))
    f.add_feed(BinanceFutures(symbols=["BTC-USDT-PERP"], **kwargs))

    f.run(start_loop=False)
    # loop.call_later(30, stop)
    print("start")
    loop.run_forever()
    print("end")


if __name__ == "__main__":
    delete_files()
    main()
