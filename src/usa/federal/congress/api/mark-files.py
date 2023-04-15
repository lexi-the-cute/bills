import os
import csv
import json
import time
import datetime
import humanize

from download import scantree

def mark_files() -> dict:
    total: int = 0
    start: float = time.time()
    elapsed: str = humanize.naturaldelta(0)

    if not os.path.exists(os.path.join("data", "local")):
        os.makedirs(os.path.join("data", "local"))

    if not os.path.exists(os.path.join("data", "mark")):
        os.makedirs(os.path.join("data", "mark"))

    files: dict = {}
    for file in scantree(path=os.path.join("data", "local")):
        total += 1

        fd: int = os.open(file, os.O_RDONLY)
        data: bytes = os.read(fd, os.fstat(fd).st_size)
        contents: dict = json.loads(data)
        os.close(fd=fd)

        # Skip Custom Lists I've Made
        if type(contents) is list:
            continue

        if "pagination" in contents:
            del(contents["pagination"])
        
        if "request" in contents:
            del(contents["request"])

        for entry in contents:
            if entry not in files:
                file_path: str = os.path.join("data", "mark", "%s.list" % entry)
                files[entry] = {}
                files[entry]["fd"] = open(file_path, mode="w")
                files[entry]["writer"] = csv.writer(files[entry]["fd"])

            files[entry]["writer"].writerow([file])

        if total % 1000 == 0:
            current: float = time.time()
            elapsed: str = humanize.naturaldelta(datetime.timedelta(seconds=(current-start)))
            print("\033[KFiles: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\r")
    
    print("\033[KTotal Files: %s\tElapsed: %s" % (humanize.intcomma(total), elapsed), end="\n")
    print("-"*40, end="\n")

    for file in files.keys():
        files[file]["fd"].close()

if __name__ == "__main__":
    mark_files()