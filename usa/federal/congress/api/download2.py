import os
import json
import glob
import dpath
import humanize

def download_file(url: str):
    pass

def parse_json(data: dict):
    for (path, value) in dpath.search(data, '**/url', yielded=True):
        print("%s: %s" % (path, value))
        # download_file(url=value)

def read_bills():
    for file in glob.iglob(pathname="local/**/*.json", recursive=True):
        try:
            fd: int = os.open(file, os.O_RDONLY)
            data: bytes = os.read(fd, os.fstat(fd).st_size)
            contents: dict = json.loads(data)

            parse_json(data=contents)
        finally:
            os.close(fd)

def count_bills():
    total: int = 0
    for file in glob.iglob(pathname="local/**/*.json", recursive=True):
        total += 1

        if total % 1000 == 0:
            print("Files: %s" % humanize.intcomma(total), end="\r")

if __name__ == "__main__":
    # config, s3, api_key = get_config()
    count_bills()
    read_bills()