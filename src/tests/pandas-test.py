import os
import yaml
import pandasgui
import pandas as pd

# PyPi version is old, install from https://github.com/adamerose/PandasGUI/issues/220#issuecomment-1464882333

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

def read_sql() -> DataFrame:
    return pd.read_sql(sql='select * from bills;', con=get_database())

if __name__ == "__main__":
    df: DataFrame = read_sql()

    print(df)
    # for row in df.iterrows():
    #     print(row)

    pandasgui.show(df)