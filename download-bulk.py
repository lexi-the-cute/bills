import os
import json
import requests

# /bulkdata/BILLSTATUS/108/hr/BILLSTATUS-108hr4693.xml
base_url: str = "https://www.govinfo.gov/bulkdata/json"

headers: dict = {
	"Accept": "application/json"
}

def save_local(key: str, body: str):
	path: str = os.path.dirname(key)
	if not os.path.exists(path):
		os.makedirs(path)
	
	file = open(key, 'w')
	file.write(body)
	file.close()

def save_file_local(key: str, body: bytes):
	print("Saving File: %s" % key)
	
	path: str = os.path.dirname(key)
	if not os.path.exists(path):
		os.makedirs(path)
	
	file = open(key, 'wb')
	file.write(body)
	file.close()

def crawl_bulk_download(url: str):
	response = requests.get(url=url, headers=headers)
	results = response.json()
	
	skip = True
	noskipagain = False
	for section in results["files"]:
		# Temporary Resume Solution
		if section["justFileName"] == "BILLSTATUS":
			skip = False
		elif section["justFileName"] == "108":
			skip = False
		elif section["justFileName"] == "hr":
			skip = False
		elif section["justFileName"] == "BILLSTATUS-108hr4693.xml":
			noskipagain = True
			skip = False
		else:
			if not noskipagain:
				skip = True
		
		if skip:
			continue
		
		if section["folder"] == True:
			file_path: str = "bulk/%s/%s/data.json" % ("/".join(url.split("/")[5:]), section["justFileName"])
			save_local(key=file_path, body=json.dumps(section))
			
			crawl_bulk_download(url=section["link"])
		else:
			file_path: str = "bulk/%s/%s" % ("/".join(url.split("/")[5:]), section["justFileName"])
			response = requests.get(url=section["link"])
			save_file_local(key=file_path, body=response.content)

if __name__ == "__main__":
	crawl_bulk_download(url=base_url)
