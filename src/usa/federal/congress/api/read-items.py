import os
import csv
import json
import time
import yaml
import datetime
import humanize
# import pandasgui
import pandas as pd

from pandas import DataFrame
from download import scantree
from typing import Union, Any


def process_possible_keys(file: str, possible_keys: dict) -> dict:
    fd: int = os.open(file, os.O_RDONLY)
    data: bytes = os.read(fd, os.fstat(fd).st_size)
    contents: dict = json.loads(data)
    os.close(fd=fd)

    if "pagination" in contents:
        del(contents["pagination"])
    
    if "request" in contents:
        del(contents["request"])

    for entry in contents:
        # Skip Custom Lists I've Made
        if type(contents[entry]) is str:
            continue

        if entry not in possible_keys:
            possible_keys[entry] = set()

        if type(contents[entry]) is dict:
            for subentry in contents[entry].keys():
                possible_keys[entry].add(subentry)
        elif type(contents[entry]) is list:
            for subentry in contents[entry]:
                if type(subentry) is not dict:
                    continue
                
                for key in subentry.keys():
                    possible_keys[entry].add(key)
    
    return possible_keys


def get_possible_keys() -> dict:
    total: int = 0
    start: float = time.time()
    elapsed: str = humanize.naturaldelta(0)

    if not os.path.exists(os.path.join("data", "local")):
        os.makedirs(os.path.join("data", "local"))

    possible_keys: dict = {}
    for file in scantree(path=os.path.join("data", "local")):
        total += 1

        possible_keys = process_possible_keys(possible_keys=possible_keys, file=file)

        if total % 1000 == 0:
            current: float = time.time()
            elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(current-start)))
            print("\033[K(Possible Keys) Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\r")
    
    print("\033[K(Possible Keys) Total Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\n")
    print("-"*40, end="\n")

    return possible_keys

def get_records(file: str) -> dict:
    fd: int = os.open(file, os.O_RDONLY)
    data: bytes = os.read(fd, os.fstat(fd).st_size)
    contents: dict = json.loads(data)
    os.close(fd=fd)

    if "pagination" in contents:
        del(contents["pagination"])
    
    if "request" in contents:
        del(contents["request"])

    return contents

def get_dataframes(possible_keys: dict) -> dict:
    total: int = 0
    start: float = time.time()
    elapsed: str = humanize.naturaldelta(0)

    if not os.path.exists(os.path.join("data", "local")):
        os.makedirs(os.path.join("data", "local"))

    records: dict = {}
    for file in scantree(path=os.path.join("data", "local")):
        total += 1

        # TODO: Determine if should change name to be less confusing
        file_records: dict = get_records(file=file)

        # Skip Custom Lists I've Made
        if type(file_records) is list:
            continue

        for entry in file_records.keys():
            if entry not in records:
                records[entry] = []
            
            if type(file_records[entry]) is dict:
                records[entry].append(file_records[entry])
            elif type(file_records[entry]) is list:
                for subentry in file_records[entry]:
                    records[entry].append(subentry)

        if total % 1000 == 0:
            current: float = time.time()
            elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(current-start)))
            print("\033[K(DataFrames[]) Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\r")

    dataframes: dict = {}
    for record in records.keys():
        # if record not in dataframes:
        # print("%s - %s" % (json.dumps(records[record]), json.dumps(list(possible_keys[record]))))
        dataframes[record] = pd.DataFrame.from_records(data=records[record], columns=possible_keys[record])
        dataframes[record].name = record

    print("\033[K(DataFrames[]) Total Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\n")
    print("-"*40, end="\n")

    # print("DataFrames: %s" % dataframes)

    return dataframes

def get_database() -> Union[str, Any]:
    with open(os.path.join("data", "config.yml"), 'r') as fi:
        config: dict = yaml.safe_load(fi)
    
    if "database" not in config:
        raise KeyError('Missing Database Section From Config')
    elif "url" not in config["database"]:
        raise KeyError('Missing url Section From Config["database"]')
    
    url: str = config["database"]["url"]

    return url

def save_dataframes(**kwargs: DataFrame) -> None:
    if not os.path.exists(os.path.join("data", "csv")):
        os.makedirs(os.path.join("data", "csv"))

    for kwarg in kwargs:
        if type(kwargs[kwarg]) is not DataFrame:
            print("Found argument of type: `%s`, Skipping..." % type(kwargs[kwarg]))
            continue

        kwargs[kwarg].to_csv(path_or_buf=os.path.join("data", "csv", "%s.csv" % kwarg), quoting=csv.QUOTE_NONNUMERIC, quotechar='"')

def save_possible_keys(possible_keys: dict) -> None:
    if not os.path.exists("data"):
        os.makedirs("data")

    # default=list tells the json dumper to treat every unknown object as a list
    # The only unknown objects are sets, which are basically lists, but with unique entries.
    # sets convert seamlessly to lists
    with open(file=os.path.join("data", "possible_keys.json"), mode="w") as f:
        f.write(json.dumps(obj=possible_keys, default=list))

def read_sql() -> DataFrame:
    return pd.read_sql(sql='select * from bills;', con=get_database())

if __name__ == "__main__":
    possible_keys: dict = get_possible_keys()  # Get Possible Keys
    save_possible_keys(possible_keys=possible_keys)  # Save Possible Keys To File For Debugging

    # TODO: Consider Only Loading One DataFrame At A Time
    dataframes: dict = get_dataframes(possible_keys=possible_keys)  # Get DataFrames
    del possible_keys  # Delete Possible Keys (To Save RAM)

    save_dataframes(**dataframes)  # Save DataFrames To File For Debugging
    # pandasgui.show(**dataframes)  # Show DataFrames To User

    # df: DataFrame = read_sql()

    # print(df)
    # for row in df.iterrows():
    #     print(row)

    # pandasgui.show(df)