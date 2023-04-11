import json
import os
import time
import datetime
import humanize

from typing import Tuple
from genericpath import isdir
from botocore.paginate import PageIterator
from mypy_boto3_s3 import S3Client, ListObjectsPaginator
from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef
from download import scantree, get_s3_client, get_default_s3_bucket


def get_local_bills() -> Tuple[int, set[str]]:
    total: int = 0
    bills: set[str] = set()
    start: float = time.time()
    elapsed: str = humanize.naturaldelta(0)

    if not os.path.exists(os.path.join("data", "local")):
        os.makedirs(os.path.join("data", "local"))

    for file in scantree(path=os.path.join("data", "local")):
        total += 1

        cleaned_path: str = os.path.sep.join(file.split(os.path.sep)[2:])
        bills.add(cleaned_path)

        if total % 1000 == 0:
            current: float = time.time()
            elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(current-start)))
            print("\033[KFiles: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\r")
    
    print("\033[KTotal Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\n")
    print("-"*40, end="\n")
    return total, bills

def get_bucket_bills() -> Tuple[int, set[str]]:
    total: int = 0
    bills: set[str] = set()
    start: float = time.time()
    elapsed: str = humanize.naturaldelta(0)

    s3: S3Client = get_s3_client()
    paginator: ListObjectsPaginator = s3.get_paginator('list_objects')
    page_iterator: PageIterator = paginator.paginate(Bucket=get_default_s3_bucket(), Prefix="usa")

    for page in page_iterator:
        for item in page["Contents"]:
            total += 1

            key: str = item["Key"] # type: ignore

            # Skip State and Territory Level Bills
            if not key.startswith("usa/federal"):
                continue

            bills.add(key)
            # print(key)
        
        current: float = time.time()
        elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(current-start)))
        # print("-"*40, end="\n")
        print("\033[KFiles: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\r")

    print("\033[KTotal Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\n")
    print("-"*40, end="\n")
    return total, bills

def find_missing_entries(outer_set: set[str], inner_set: set[str]) -> set[str]:
    missing_items: set[str] = set()
    for outer_item in outer_set:
        if outer_item not in inner_set:
            missing_items.add(outer_item)

    return missing_items

def download_entries(missing_bills: set[str]) -> None:
    s3: S3Client = get_s3_client()
    bucket: str = get_default_s3_bucket()
    
    count: int = 0
    total: int = len(missing_bills)
    for bill in missing_bills:
        file: str = os.path.join("data", "local", bill)
        path: str = os.path.split(file)[0]
        count += 1

        if not os.path.exists(path):
            os.makedirs(path)

        if os.path.isdir(file):
            continue

        print("\033[KDownloading Missing File (%s/%s): %s" % (humanize.intcomma(count), humanize.intcomma(total), file), end="\r")
        with open(file=file, mode="wb") as f:
            contents: GetObjectOutputTypeDef = s3.get_object(
                Bucket=bucket,
                Key=bill
            )

            f.write(contents["Body"].read())
    # print("\n")

def upload_entries(missing_bills: set[str]) -> None:
    s3: S3Client = get_s3_client()
    bucket: str = get_default_s3_bucket()

    count: int = 0
    total: int = len(missing_bills)
    for bill in missing_bills:
        file: str = os.path.join("data", "local", bill)

        count += 1
        print("\033[KUploading Missing File (%s/%s): %s" % (humanize.intcomma(count), humanize.intcomma(total), file), end="\r")
        with open(file=file, mode="r") as f:
            s3.put_object(
                Bucket=bucket,
                Body=f.read(),
                Key=bill
            )
    # print("\n")

if __name__ == "__main__":
    local_count, local_bills = get_local_bills()
    bucket_count, bucket_bills = get_bucket_bills()

    missing_bills_in_bucket: set = find_missing_entries(outer_set=local_bills, inner_set=bucket_bills)
    missing_bills_in_local: set = find_missing_entries(outer_set=bucket_bills, inner_set=local_bills)

    print("-"*40)
    print("Total Local: %s, Total Bucket: %s" % (humanize.intcomma(local_count), humanize.intcomma(bucket_count)))
    print("Total Missing From Bucket: %s" % humanize.intcomma(len(missing_bills_in_bucket)))
    print("Total Missing From Local: %s" % humanize.intcomma(len(missing_bills_in_local)))

    print("-"*40)
    print("Saving Items Missing From Bucket...")
    with open(file="/home/alexis/Desktop/missing-from-bucket.json", mode="w") as f:
        f.write(json.dumps(list(missing_bills_in_bucket)))

    print("Saving Items Missing From Local...")
    with open(file="/home/alexis/Desktop/missing-from-local.json", mode="w") as f:
        f.write(json.dumps(list(missing_bills_in_local)))

    upload_entries(missing_bills=missing_bills_in_bucket)
    download_entries(missing_bills=missing_bills_in_local)

    print("Done...")