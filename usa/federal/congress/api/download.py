import os
import csv
import sys
import json
import time
import yaml
import boto3
import dpath
import signal
import datetime
import humanize
import requests

from typing import Generator
from botocore.client import BaseClient
from urllib.parse import urlparse, ParseResult

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

skipped: list = [
    "www.congress.gov",
    "clerk.house.gov",
    "www.senate.gov",
    "www.cbo.gov",
    "api.data.gov",
    # "api.congress.gov"
]

def upload_file(key: str, body: str) -> None:
    s3: BaseClient = get_s3_client()
    bucket: str = get_default_s3_bucket()

    s3.put_object(
        Bucket=bucket,
        Body=body,
        Key=key
    )

# TODO: Figure out if I should optimize this and how to do it
def save_local(key: str, body: str) -> None:
    key: str = "local/%s" % key
    path: str = os.path.dirname(key)
    if not os.path.exists(path):
        os.makedirs(path)
    
    file = open(key, 'w')
    file.write(body)
    file.close()

def get_key(url: str) -> str:
    path: str = urlparse(url).path
    split = path.split("/")[2:]
    
    # Rename Folder To Our Format
    if split[0] in rename:
        split[0] = rename[split[0]]
    
    return "usa/federal/congress/%s/data.json" % "/".join(split)

def log_error(url: str, message: str) -> None:
    # TODO: Determine if we need to optimize writing to this file
    with open('errors.csv', 'a') as fi:
        row = csv.writer(fi)
        row.writerow([url, message])

# TODO: This global breaks reusability, consider making a class
line: int = 0
session = requests.Session()
def download_file(url: str) -> None:
    global line
    global read_bills_start_time
    global session

    key: str = get_key(url=url)

    line += 1
    elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(time.time()-read_bills_start_time)))
    parsed: ParseResult = urlparse(url)

    # TODO: Eventually Add Support For These URLs
    if parsed.netloc in skipped:
        # print("\033[KSkipping Host: %s" % parsed.netloc, end="\n")
        print("\033[K%s (%s elapsed) - Skipping %s" % (humanize.intcomma(line), elapsed, key), end="\r")
        return

    # Skip Download If Already Exists
    if os.path.exists("local/%s" % key):
        print("\033[K%s (%s elapsed) - Skipping %s" % (humanize.intcomma(line), elapsed, key), end="\r")
        return
    
    url: str = "%s://%s%s" % (parsed.scheme, parsed.netloc, parsed.path)
    params: dict = {
        "api_key": next(get_api_key()),
        "format": "json"
    }

    print("\033[K%s (%s elapsed) - Downloading %s - %s" % (humanize.intcomma(line), elapsed, key, url), end="\r")

    # TODO: Figure out how to stream file to s3 bucket and to local filesystem
    response = session.get(url=url, params=params)
    content_type = response.headers.get('content-type')

    # TODO: Implement Handling Unknown File Types
    if content_type != "application/json":
        print("\033[K%s (%s elapsed) - Skipping Unknown File %s" % (humanize.intcomma(line), elapsed, key), end="\r")
    
    try:
        results = response.json()
    except:
        print("\033[K%s (%s elapsed) - Read JSON Error: %s" % (humanize.intcomma(line), elapsed, response.text), end="\r")
        log_error(url=url, message=response.text)
        
    if response.status_code != 200:
        error = results["error"]

        if "message" in error:
            print("\033[K%s (%s elapsed) - Waiting 60 Minutes To Try Again (%s): %s" % (humanize.intcomma(line), elapsed, url, response.text), end="\r")
            time.sleep(60*60)
            download_file(url=url)
        elif "matches the given query" in error:
            print("\033[K%s (%s elapsed) - DJango Error (%s): %s" % (humanize.intcomma(line), elapsed, url, error), end="\r")
            log_error(url=url, message=error)
        else:
            print("\033[K%s (%s elapsed) - Unknown Error (%s): %s" % (humanize.intcomma(line), elapsed, url, response.text), end="\r")
            log_error(url=url, message=error)
        
        return
    
    print("\033[K%s (%s elapsed) - Uploading %s" % (humanize.intcomma(line), elapsed, key), end="\r")

    text: str = json.dumps(results)
    save_local(key=key, body=text)
    upload_file(key=key, body=text)


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
    print("Finished Downloading Bills...", end="\n")

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

def get_default_s3_bucket() -> str:
    with open('config.yml', 'r') as fi:
        config: dict = yaml.safe_load(fi)

    if "s3" not in config:
        raise KeyError('Missing S3 Section From Config')
    elif "default_bucket" not in config["s3"]:
        raise KeyError('Missing default_bucket Section From Config["s3"]')

    return config["s3"]["default_bucket"]

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

def signal_handler(sig, frame) -> None:
    # print("\b\b  ", end="\r")  # Note: Hiding Ctrl+C will be difficult without breaking portability
    print("\nExiting...", end="\n")
    hide_cursor(hide=False)
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    hide_cursor(hide=True)
    # count_bills()
    read_bills()
    hide_cursor(hide=False)