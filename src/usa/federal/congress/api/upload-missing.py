import os
import time
import datetime
import humanize

from typing import Tuple
from download import scantree


def get_local_bills() -> Tuple[int, set[str]]:
    total: int = 0
    bills: set[str] = set()
    start: float = time.time()
    elapsed: str = humanize.naturaldelta(0)

    if not os.path.exists(os.path.join("data", "local")):
        os.makedirs(os.path.join("data", "local"))

    for file in scantree(path=os.path.join("data", "local")):
        total += 1

        bills.add(file)

        if total % 1000 == 0:
            current: float = time.time()
            elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(current-start)))
            print("\033[KFiles: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\r")
    
    print("\033[KTotal Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\n")
    print("-"*40, end="\n")
    return total, bills

if __name__ == "__main__":
    local_count, local_bills = get_local_bills()