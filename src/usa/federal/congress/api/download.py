import os
import re
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

from typing import Union
from io import BufferedWriter, TextIOWrapper
from re import Pattern, Match
from requests import Response
from mypy_boto3_s3 import S3Client  # This exists purely for strong typing boto3
from typing import Generator, Optional
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
    "house-communication": "house-communications",
    "senate-communication": "senate-communications",
    "house-requirement": "house-requirements",
    "crec": "congressional-records",
    # "summary": "summaries"
}

skipped: list = [
    # "www.congress.gov",  # Non-Downloadable HTML and Downloadable Files (htm, pdf, txt, and xml) (e.g. https://www.congress.gov/amendment/110th-congress/senate-amendment/635 and https://www.congress.gov/110/crec/2007/03/23/CREC-2007-03-23-pt1-PgS3659-7.pdf)
    "clerk.house.gov",  # Non-Download HTML and Downloadable XML (e.g. https://clerk.house.gov/Votes/2007586 and https://clerk.house.gov/evs/2007/roll237.xml)
    "www.senate.gov",  # Non-Downloadable HTML and Downloadable XML (e.g. https://www.senate.gov/legislative/LIS/roll_call_votes/vote1101/vote_110_1_00366.htm and https://www.senate.gov/legislative/LIS/roll_call_votes/vote1101/vote_110_1_00366.xml)
    "www.cbo.gov",  # Non-Downloadable HTML With Link To Downloadable PDF (e.g. https://www.cbo.gov/publication/19453 and https://www.cbo.gov/sites/default/files/110th-congress-2007-2008/costestimate/s10000.pdf)
    "api.data.gov",  # JSON - No Access To API (e.g. https://api.data.gov/congress/v3/amendment/110/samdt/3346?format=json)
    # "api.congress.gov"  # JSON
]

def upload_file(key: str, body: Union[str,bytes]) -> None:
    s3: S3Client = get_s3_client()
    bucket: str = get_default_s3_bucket()

    s3.put_object(
        Bucket=bucket,
        Body=body,
        Key=key
    )

# TODO: Figure out if I should optimize this and how to do it
def save_local(key: str, body: Union[str,bytes]) -> None: # type: ignore
    key: str = os.path.join("data", "local", key)
    path: str = os.path.dirname(key)
    if not os.path.exists(path):
        os.makedirs(path)
    
    if type(body) is str:
        text_file: TextIOWrapper = open(key, 'w')
        text_file.write(body)
        text_file.close()
    elif type(body) is bytes:
        binary_file: BufferedWriter = open(key, 'wb')
        binary_file.write(body)
        binary_file.close()

def get_key(url: str) -> str:
    path: str = urlparse(url).path
    split: list[str] = path.split("/")[2:]
    
    # Rename Folder To Our Format
    if split[0] in rename:
        split[0] = rename[split[0]]
    
    return "usa/federal/congress/%s/data.json" % "/".join(split)

def log_error(url: str, message: str) -> None:
    # TODO: Determine if we need to optimize writing to this file
    with open(os.path.join("data", "errors.csv"), 'a') as fi:
        row = csv.writer(fi)
        row.writerow([url, message])

def get_api_key() -> Generator[str, None, None]:
    with open(os.path.join("data", "config.yml"), 'r') as fi:
        config: dict = yaml.safe_load(fi)

    if "congress" not in config:
        raise KeyError('Missing Congress Section From Config')
    elif "keys" not in config["congress"]:
        raise KeyError('Missing Keys Section From Config["congress"]')

    config = config["congress"]
    total: int = len(config["keys"])
    current = 0
    while True:
        yield config["keys"][current]
        
        if current >= total-1:
            current = 0
        else:
            current += 1

letters_numbers_regex: Pattern[str] = re.compile(r"([a-zA-Z]*)([0-9]*)")
def split_on_letters_numbers(text: str) -> Optional[Match[str]]:
    global letters_numbers_regex

    match: Optional[Match[str]] = re.match(pattern=letters_numbers_regex, string=text)
    return match

def handle_non_json_file(response: Response, line: int, elapsed: str, parent_key: str) -> None:
    url: str = response.url
    parsed: ParseResult = urlparse(url=url)

    if parsed.netloc == "www.congress.gov":
        allowed_extensions: list = ["htm", "pdf", "txt", "xml"]
        extension: str = parsed.path.split(".")[-1]
        if extension in allowed_extensions:
            key: str = os.path.join(parent_key, "files", os.path.split(parsed.path)[1])
            body: bytes = response.content

            print("\033[K%s (%s elapsed) - Downloading File %s" % (humanize.intcomma(line), elapsed, key), end="\r")
            save_local(key=key, body=body)
            upload_file(key=key, body=body)
    else:
        print("\033[K%s (%s elapsed) - Skipping Unknown File %s" % (humanize.intcomma(line), elapsed, url), end="\r")

# TODO: This global breaks reusability, consider making a class
line: int = 0
session: requests.Session = requests.Session()
api_key: Generator[str, None, None] = get_api_key()
def download_file(url: str, parent_key: str) -> None: # type: ignore
    global line
    global start_time
    global session

    key: str = get_key(url=url)

    line += 1
    elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(time.time()-start_time)))
    parsed: ParseResult = urlparse(url)

    # TODO: Eventually Add Support For These URLs
    if parsed.netloc in skipped:
        # print("\033[KSkipping Host: %s" % parsed.netloc, end="\n")
        print("\033[K%s (%s elapsed) - Skipping %s" % (humanize.intcomma(line), elapsed, key), end="\r")
        return

    # Skip Download If Already Exists
    if os.path.exists(os.path.join("data", "local", key)):
        print("\033[K%s (%s elapsed) - Skipping %s" % (humanize.intcomma(line), elapsed, key), end="\r")
        return
    
    url: str = "%s://%s%s" % (parsed.scheme, parsed.netloc, parsed.path)
    params: dict = {
        "api_key": next(api_key),
        "format": "json"
    }

    # TODO: Figure out how to stream file to s3 bucket and to local filesystem
    # TODO: Consider saving to OS' temporary folder and then moving from there, then uploading to S3
    response: Response = session.get(url=url, params=params)
    content_type: Optional[str] = response.headers.get('content-type')

    # TODO: Check if File Exists Before Downloading...
    if content_type != "application/json":
        handle_non_json_file(response=response, line=line, elapsed=elapsed, parent_key=parent_key)
        return

    try:
        results: dict = response.json()

        if response.status_code != 200:
            error = results["error"]

            if "message" in error:
                print("\033[K%s (%s elapsed) - Waiting 60 Minutes To Try Again (%s): %s" % (humanize.intcomma(line), elapsed, url, response.text), end="\r")
                time.sleep(60*60)
                download_file(url=url, parent_key=parent_key)
            elif "matches the given query" in error:
                print("\033[K%s (%s elapsed) - DJango Error (%s): %s" % (humanize.intcomma(line), elapsed, url, error), end="\r")
                log_error(url=url, message=error)
            else:
                print("\033[K%s (%s elapsed) - Unknown Error (%s): %s" % (humanize.intcomma(line), elapsed, url, response.text), end="\r")
                log_error(url=url, message=error)
            
            return
        
        print("\033[K%s (%s elapsed) - Downloading %s" % (humanize.intcomma(line), elapsed, key), end="\r")

        text: str = json.dumps(results)
        save_local(key=key, body=text)
        upload_file(key=key, body=text)
    except:
        print("\033[K%s (%s elapsed) - Read JSON Error: %s" % (humanize.intcomma(line), elapsed, response.text), end="\r")
        log_error(url=url, message=response.text)


def parse_json(data: dict, parent_key: str) -> None:
    for (_, value) in dpath.search(data, '**/url', yielded=True):
        # print("%s: %s" % (path, value))
        download_file(url=value, parent_key=parent_key)

def scantree(path: str = os.path.join("data", "local")) -> Generator[str, None, None]:
    for entry in os.scandir(path=path):
        if entry.is_dir():
            yield from scantree(path=os.path.join(path, entry.name))
        if entry.name.endswith(".json"):
            yield os.path.join(path, entry.name)

# TODO: This global breaks reusability, consider making a class
start_time: float = -1
def read_bills() -> None:
    global start_time
    start_time = time.time()

    if not os.path.exists(os.path.join("data", "local")):
        os.makedirs(os.path.join("data", "local"))

    for file in scantree(path=os.path.join("data", "local")):
        fd: int = os.open(file, os.O_RDONLY)
        data: bytes = os.read(fd, os.fstat(fd).st_size)
        contents: dict = json.loads(data)
        os.close(fd=fd)

        parent_key: str = "/".join(file.split(sep=os.path.sep)[2:-1])
        parse_json(data=contents, parent_key=parent_key)
        os.close(fd)

    print(end="\n")
    print("Finished Downloading Bills...", end="\n")

def count_bills() -> int:
    total: int = 0
    start: float = time.time()
    elapsed: str = humanize.naturaldelta(0)

    if not os.path.exists(os.path.join("data", "local")):
        os.makedirs(os.path.join("data", "local"))

    for _ in scantree(path=os.path.join("data", "local")):
        total += 1

        if total % 1000 == 0:
            current: float = time.time()
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

def get_s3_client() -> S3Client:
    with open(os.path.join("data", "config.yml"), 'r') as fi:
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
    s3: S3Client = boto3.client(
        service_name='s3',
        aws_access_key_id=config['access_key_id'],
        aws_secret_access_key=config['secret_access_key'],
        endpoint_url=config['endpoint']
    )

    return s3

def get_default_s3_bucket() -> str:
    with open(os.path.join("data", "config.yml"), 'r') as fi:
        config: dict = yaml.safe_load(fi)

    if "s3" not in config:
        raise KeyError('Missing S3 Section From Config')
    elif "default_bucket" not in config["s3"]:
        raise KeyError('Missing default_bucket Section From Config["s3"]')

    return config["s3"]["default_bucket"]

def signal_handler(sig, frame) -> None:
    # print("\b\b  ", end="\r")  # Note: Hiding Ctrl+C will be difficult without breaking portability
    print("\nExiting...", end="\n")
    hide_cursor(hide=False)
    sys.exit(0)

def live_download() -> None:
    global start_time
    start_time = time.time()

    endpoints: list = [
        "https://api.congress.gov/v3/congress",  # 118
        "https://api.congress.gov/v3/summaries",  # 375
        "https://api.congress.gov/v3/committee",  # 714
        "https://api.congress.gov/v3/treaty",  # 782
        "https://api.congress.gov/v3/member",  # 2,515
        "https://api.congress.gov/v3/house-requirement",  # 3,226
        # "https://api.congress.gov/v3/congressional-record",  # 5,148 - Uses A Different Pagination System
        "https://api.congress.gov/v3/house-communication",  # 30,487
        "https://api.congress.gov/v3/nomination",  # 41,448
        "https://api.congress.gov/v3/committee-report",  # 47,464
        "https://api.congress.gov/v3/amendment",  # 117,324
        "https://api.congress.gov/v3/senate-communication",  # 164,335
        "https://api.congress.gov/v3/bill"  # 394,438
    ]

    api_key: Generator[str, None, None] = get_api_key()
    session: requests.Session = requests.Session()
    for endpoint in endpoints:
        params: dict = {
            "api_key": next(api_key),
            "offset": 0,
            "limit": 250,
            "format": "json"
        }

        response: requests.Response = session.get(url=endpoint, params=params)

        # TODO: Determine if should put other checks...

        results: dict = response.json()

        # Check in case we get an error message
        total: int = 0  # TODO: Count Array of Items (Variable Name)
        if "pagination" in results:
            total: int = results["pagination"]["count"]
            results.pop("pagination")  # Don't Get Stuck In Loop

        if "offset" not in params or type(params["offset"]) != int:
            return

        while (total-params["offset"])>0:
            if "offset" not in params or type(params["offset"]) != int:
                return

            params["offset"] = params["offset"]+250 # type: ignore

            response: requests.Response = session.get(url=endpoint, params=params)
            results: dict = response.json()

            # Check in case we get an error message
            if "pagination" in results:
                total: int = results["pagination"]["count"]
                results.pop("pagination")  # Don't Get Stuck In Loop

            for (_, value) in dpath.search(results, '**/url', yielded=True):
                # print("%s: %s" % (path, value))
                # print("\033[K%s - %s" % (value, response.url))
                parent_key: str = "/".join(get_key(url=value).split(sep=os.path.sep)[:-1])
                download_file(url=value, parent_key=parent_key)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    hide_cursor(hide=True)
    live_download()
    count_bills()
    read_bills()
    hide_cursor(hide=False)