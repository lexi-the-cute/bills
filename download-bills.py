import os
import yaml
import json
import boto3
import requests

def load_config():
	with open('config.yml', 'r') as fi:
		return yaml.safe_load(fi)

def load_s3_client(config: dict):
	return boto3.client(
		service_name='s3',
		aws_access_key_id=config['access_key_id'],
		aws_secret_access_key=config['secret_access_key'],
		endpoint_url=config['endpoint']
	)

def upload_file(config: dict, key: str, body: str):
	#data = open('test.jpg', 'rb')
	#s3.Bucket('my-bucket').put_object(Key='test.jpg', Body=data)
	s3.put_object(
		Bucket=config['default_bucket'],
		Body=body,
		Key=key
	)

def save_local(key: str, body: str):
	os.makedirs(os.path.dirname(key))
	
	file = open(key, 'w')
	file.write(body)
	file.close()

def get_bills(config: dict):
	offset: int = 0
	limit: int = 250
	loop: bool = True
	
	while loop:
		url = "https://api.congress.gov/v3/bill?api_key=%s&offset=%s&limit=%s&format=json" % (config["api_key"], offset, limit)
		
		response = requests.get(url=url)
		results = response.json()
		
		# We are at the end of the results, stop looping
		if ("next" not in results["pagination"]):
			loop = False
		
		for bill in results["bills"]:
			data = get_bill(url=bill["url"], api_key=config["api_key"])
			session = data["request"]["congress"]
			bill_type = data["request"]["billType"]
			bill_number = data["request"]["billNumber"]
			
			key = "usa/federal/congress/bills/%s/%s/%s/data.json" % (session, bill_type, bill_number)
			
			yield key, data

		offset += limit
	
def get_bill(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)
	
	response = requests.get(url=url)
	return response.json()

def get_bill_actions(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)
	
def get_bill_committees(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_cosponsors(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_text(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_summaries(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_titles(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)

if __name__ == "__main__":
	config = load_config()
	s3 = load_s3_client(config['s3'])
	
	for key, bill in get_bills(config['congress']):
		# usa/federal/congress/bills/$congress/$billType/$billNumber/data.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/actions/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/committees.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/cosponsors/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/text/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/summaries/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/titles/$page.json
		
		bill_text = json.dumps(bill)
		
		print("Uploading: %s" % key)
		save_local(key=key, body=bill_text)
		upload_file(config=config['s3'], key="local/%s" % key, body=bill_text)
