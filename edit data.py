# import time
# import multiprocessing as mp

# def test(counter):
#     while True:
#         print(counter)
#         time.sleep(1)


# if __name__ in "__main__":
#     n_jobs = 5
#     jobs = []
#     counter = 0
#     for i in range(n_jobs):
#         p = mp.Process(target=test, args=(counter,))
#         jobs.append(p)
#         counter += 1
#     for task in jobs:
#         task.start()
#     for task in jobs:
#         task.join()

import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib as plt
import time
import multiprocessing as mp
from datetime import datetime
from tqdm import tqdm


pd.set_option("display.max_rows", None, "display.max_columns", None)
data_path = Path.cwd() / "data"
print(data_path)

datasets = sorted([i for i in  os.listdir(data_path) if i[-4:] == ".csv"])
print(datasets)

for i in datasets:
    print(i)
    
    exec(f"{i[:-4]} = pd.read_csv(data_path / i)")
    exec(f"print({i[:-4]}.head())")
    exec(f"print(len({i[:-4]}))")
for i in datasets:
    print("\n")
    print(i[:-4])
    print(list(eval(f"{i[:-4]}.columns")))
    # print(list(eval(f"{i[:-4]}.unique()")))
    for j in eval(f"{i[:-4]}"):
        print(j)
        #print(list(eval(f"{i[:-4]}{[j]}.unique()")))


order_products = order_products__prior.append(order_products__train)
order_products =order_products.sample(frac = 0.2)
print(order_products.head())
print(len(order_products))

def find_items_in_order_mp(df, data_path, counter):
    start = time.time()
    idxs = list(df.index)
    # df["counter"] = counter
    for i in idxs:
        
        product_id = df.loc[i, "product_id"]
        x = order_products[product_id == order_products["product_id"]]
        orders = list(x["order_id"].unique())
        items = 0
        for j in orders:
            items += len(order_products[j == order_products["order_id"]]["product_id"].unique())
        if len(orders) == 0:
            df.loc[i, "items_in_order_average"] = 0
        else:
            df.loc[i, "items_in_order_average"] = items / len(orders)
        
        
        
        # print(i, counter)
        if counter < 1 and i % 24 == 0:
            now = time.time()
            print(" \n\n\nthe program has elapsed for (mins): " + str(round(((now - start)/60), 2)))
            print("percentage completed: " + str(round(idxs.index(i) * 100 / len(df))))
            df.loc[i, "counter"] = counter
        
        counter += 1e-8
        
        
        
    # queue.put(results)
    df.to_csv(data_path / "augmented" / f"results_{int(counter)}.csv", index = False)\


def find_order_times(products_info, product_id, index):
    x = order_products[product_id == order_products["product_id"]]
    order_days = list(x["order_dow"])
    for i in order_days:
        products_info.loc[index, f"day_of_week:{i}"] += 1


    order_hours = list(x["order_hour_of_day"])
    for i in order_hours:
        products_info.loc[index, f"hour_of_day:{i}"] += 1
    
    return products_info

if __name__ in "__main__":
    products_info = pd.read_csv(data_path / "augmented" / "products_info_part1.csv")

    n_jobs = 7
    # df_split is type list containg 8 pd.DFs
    df_split = np.array_split(products_info, n_jobs)
    # queue = mp.Queue()
    jobs = []
    counter = 0
    for i in range(n_jobs):
        p = mp.Process(target=find_items_in_order_mp, args=(df_split[i], data_path, counter))
        jobs.append(p)
        counter += 1
    for task in jobs:
        task.start()

    for task in jobs:
        task.join()
