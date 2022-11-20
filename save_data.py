import os
import aiofiles
import csv
from sortedcontainers import SortedDict
from book_features import wide_book


def is_file_empty(file_path):
    """Check if file is empty by confirming if its size is 0 bytes"""
    # Check if file exist and it is empty
    return os.path.exists(file_path) and os.stat(file_path).st_size == 0


async def book_to_csv(ob, receipt):
    file_name = f"data/book-{ob.exchange}-{ob.symbol}.csv"
    async with aiofiles.open(file_name, "a") as csv_file:
        writer = csv.writer(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        line = [
            str(ob.timestamp or receipt),
            ob.exchange,
            ob.symbol,
            str(ob.book.to_dict()["ask"]),
            str(ob.book.to_dict()["bid"]),
        ]
        await writer.writerow(line)
