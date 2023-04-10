import os
import yaml
import pandas as pd

from pandas import DataFrame
from typing import Union, Any

def get_database() -> Union[str, Any]:
    with open(os.path.join("data", "config.yml"), 'r') as fi:
        config: dict = yaml.safe_load(fi)
    
    if "database" not in config:
        raise KeyError('Missing Database Section From Config')
    elif "url" not in config["database"]:
        raise KeyError('Missing url Section From Config["database"]')
    
    url: str = config["database"]["url"]

    return url

def read_sql():
    rows: DataFrame = pd.read_sql(sql='select * from bills;', con=get_database())
    print(rows)

if __name__ == "__main__":
    read_sql()