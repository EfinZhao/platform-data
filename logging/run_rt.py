import threading
import time

import pandas as pd
from logger_rt import logger


def main():
    threads = []

    data = pd.read_csv("../udid.csv")
    for row in data.itertuples(index=False):
       t = threading.Thread(target = logger, args=(f"{row.major}_{row.minor}", row.UDID), daemon=True)
       threads.append(t)
       t.start()
       time.sleep(10)


    while True:
        for t in threads:
            print(t.is_alive())

        time.sleep(5)

if __name__ == "__main__":
    main()
