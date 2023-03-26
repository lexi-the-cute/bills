import os, time, glob

def scantree(path: str = "local", n: int = 0):
    for entry in os.scandir(path=path):
        if entry.is_dir():
            n = scantree(path=os.path.join(path, entry.name), n=n)
        if entry.name.endswith(".json"):
            n += 1
    
    return n

def listtree(path: str = "local", n: int = 0):
    for entry in os.listdir(path=path):
        if entry.is_dir():
            n = listtree(path=os.path.join(path, entry.name), n=n)
        if entry.name.endswith(".json"):
            n += 1
    
    return n

def scandir():
    n, t = 0, time.time()
    scantree(path="local", n=0)

    t = time.time() - t
    print("os.scandir (scantree): %.4fs, %d files found\n" % (t, n))

def listdir():
    n, t = 0, time.time()
    listtree(path="local", n=0)

    t = time.time() - t
    print("os.listdir (listtree): %.4fs, %d files found\n" % (t, n))

def iglob():
    n, t = 0, time.time()
    for _ in glob.iglob(pathname="local/**/*.json", recursive=True):
        n += 1

    t = time.time() - t
    print("glob.glob: %.4fs, %d files found\n" % (t, n))

def walk():
    n, t = 0, time.time()
    for _, _, files in os.walk(top="local"):
        for file in files:
            if file.endswith(".json"):
                n += 1

    t = time.time() - t
    print("os.walk: %.4fs, %d files found\n" % (t, n))

if __name__ == "__main__":
    print("Testing os.scandir...")
    scandir()

    print("Testing os.listdir...")
    listdir()

    print("Testing glob.iglob...")
    iglob()

    print("Testing os.walk...")
    walk()