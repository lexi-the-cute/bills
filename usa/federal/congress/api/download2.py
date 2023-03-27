import os
import sys
import json
import time
import yaml
import boto3
import dpath
import signal
import datetime
import humanize

from typing import Generator
from urllib.parse import urlparse
from botocore.client import BaseClient

# Note: With Generator[yield, send, return], you can send to a yield via "received = yield ..."

rename: dict = {
    "amendment": "amendments",
    "bill": "bills",
    "member": "members",
    "committee-report": "committee-reports",
    "treaty": "treaties",
    "committee": "committees",
    "nomination": "nominations",
    # "summary": "summaries",
    # "house-communication": "house-communications",
    # "senate-communication": "senate-communications",
    # "congressional-record": "congressional-records",
    # "house-requirement": "house-requirements"
}

def get_key(url: str) -> str:
    path: str = urlparse(url).path
    split = path.split("/")[2:]
    
    # Rename Folder To Our Format
    if split[0] in rename:
        split[0] = rename[split[0]]
    
    return "usa/federal/congress/%s/data.json" % "/".join(split)

# TODO: This global breaks reusability, consider making a class
line: int = 0
def download_file(url: str) -> None:
    global line
    global read_bills_start_time

    key: str = get_key(url=url)

    line += 1
    elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(time.time()-read_bills_start_time)))

    # Skip Download If Already Exists
    if os.path.exists("local/%s" % key):
        print("\033[K%s (%s elapsed) - Skipping %s" % (humanize.intcomma(line), elapsed, key), end="\r")
        return

    print("\033[K%s (%s elapsed) - Downloading %s" % (humanize.intcomma(line), elapsed, key), end="\r")
    pass

def parse_json(data: dict) -> None:
    for (_, value) in dpath.search(data, '**/url', yielded=True):
        # print("%s: %s" % (path, value))
        download_file(url=value)

def scantree(path: str = "local") -> Generator[str, None, None]:
    for entry in os.scandir(path=path):
        if entry.is_dir():
            yield from scantree(path=os.path.join(path, entry.name))
        if entry.name.endswith(".json"):
            yield os.path.join(path, entry.name)

# TODO: This global breaks reusability, consider making a class
read_bills_start_time: int = -1
def read_bills() -> None:
    global read_bills_start_time
    read_bills_start_time = time.time()

    for file in scantree(path='local'):
        try:
            fd: int = os.open(file, os.O_RDONLY)
            data: bytes = os.read(fd, os.fstat(fd).st_size)
            contents: dict = json.loads(data)

            parse_json(data=contents)
        finally:
            os.close(fd)

    print(end="\n")

def count_bills() -> int:
    total: int = 0
    start: int = time.time()
    for _ in scantree(path='local'):
        total += 1

        if total % 1000 == 0:
            current: int = time.time()
            elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(current-start)))
            print("\033[KFiles: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\r")
    
    print("\033[KTotal Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\n")
    print("-"*40, end="\n")
    return total

def hide_cursor(hide: bool) -> None:
    if hide:
        print("\033[?25l", end="\r")
    else:
        print("\033[?25h", end="\r")

def get_s3_client() -> BaseClient:
    with open('config.yml', 'r') as fi:
        config: dict = yaml.safe_load(fi)

    if "s3" not in config:
        raise KeyError('Missing S3 Section From Config')
    elif "access_key_id" not in config["s3"]:
        raise KeyError('Missing access_key_id Section From Config["s3"]')
    elif "secret_access_key" not in config["s3"]:
        raise KeyError('Missing secret_access_key Section From Config["s3"]')
    elif "endpoint" not in config["s3"]:
        raise KeyError('Missing endpoint Section From Config["s3"]')

    # TODO: Determine if there's a type that can allow autocompletion of methods such as s3.put_object(...)
    config = config["s3"]
    s3: BaseClient = boto3.client(
        service_name='s3',
        aws_access_key_id=config['access_key_id'],
        aws_secret_access_key=config['secret_access_key'],
        endpoint_url=config['endpoint']
    )

    return s3

def get_api_key() -> Generator[str, None, None]:
    with open('config.yml', 'r') as fi:
        config: dict = yaml.safe_load(fi)

    if "congress" not in config:
        raise KeyError('Missing Congress Section From Config')
    elif "keys" not in config["congress"]:
        raise KeyError('Missing Keys Section From Config["congress"]')

    config = config["congress"]
    total = len(config["keys"])
    current = 0
    while True:
        yield config["keys"][current]
        
        if current >= total-1:
            current = 0
        else:
            current += 1

def signal_handler(sig, frame):
    # print("\b\b  ", end="\r")  # Note: Hiding Ctrl+C will be difficult without breaking portability
    print("\nExiting...", end="\n")
    hide_cursor(hide=False)
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    hide_cursor(hide=True)
    count_bills()
    read_bills()
    hide_cursor(hide=False)