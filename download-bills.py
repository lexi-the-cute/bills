import os
import yaml
import json
import time
import boto3
import requests
import humanize

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
	path: str = os.path.dirname(key)
	if not os.path.exists(path):
		os.makedirs(path)
	
	file = open(key, 'w')
	file.write(body)
	file.close()

def get_rate_limit(response):
	# Monitor Rate Limit - 1,000 Requests Per Hour
	rate_limit = response.headers["x-ratelimit-limit"]
	rate_limit_remaining = response.headers["x-ratelimit-remaining"]
	#print("Rate Limit: %s/%s" % (rate_limit_remaining, rate_limit))
	
	return rate_limit, rate_limit_remaining

def get_bills(config: dict):
	offset: int = 0
	limit: int = 250
	loop: bool = True
	
	while loop:
		url = "https://api.congress.gov/v3/bill?api_key=%s&offset=%s&limit=%s&format=json" % (config["api_key"], offset, limit)
		response = requests.get(url=url)
		results = response.json()
		
		rate_limit, rate_limit_remaining = get_rate_limit(response)
		if response.status_code != 200:
			print(results["error"]["message"])
			print("Waiting 60 Minutes To Try Again...")
			time.sleep(60*60)
			
			response = requests.get(url=url)
			results = response.json()
		
		# We are at the end of the results, stop looping
		if ("next" not in results["pagination"]):
			loop = False
		
		total = results["pagination"]["count"]
		count = offset-1
		
		for bill in results["bills"]:
			count += 1
			session = bill["congress"]
			bill_type = bill["type"].lower()
			bill_number = bill["number"]
			key = "usa/federal/congress/bills/%s/%s/%s/data.json" % (session, bill_type, bill_number)
			
			# TODO: Make Better Restart Check
			if (os.path.exists("local/%s" % key)):
				continue
			
			data = get_bill(url=bill["url"], api_key=config["api_key"])
			
			if "error" in data:
				print(data["error"]["message"])
				print("Waiting 60 Minutes To Try Again...")
				time.sleep(60*60)
			
				data = get_bill(url=bill["url"], api_key=config["api_key"])
			
			yield key, data, count, total

		offset += limit
	
def get_bill(url: str, api_key: str):
	url = "%s&api_key=%s" % (url, api_key)
	
	response = requests.get(url=url)
	return response.json()

def get_bill_actions(url: str, api_key: str):
	# TODO: Paginate
	url = "%s&api_key=%s" % (url, api_key)
	
def get_bill_committees(url: str, api_key: str):
	# TODO: Paginate
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_cosponsors(url: str, api_key: str):
	# TODO: Paginate
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_text(url: str, api_key: str):
	# TODO: Paginate
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_summaries(url: str, api_key: str):
	# TODO: Paginate
	url = "%s&api_key=%s" % (url, api_key)

def get_bill_titles(url: str, api_key: str):
	# TODO: Paginate
	url = "%s&api_key=%s" % (url, api_key)

if __name__ == "__main__":
	config = load_config()
	s3 = load_s3_client(config['s3'])
	
	for key, bill, count, total in get_bills(config['congress']):
		# usa/federal/congress/bills/$congress/$billType/$billNumber/data.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/actions/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/committees.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/cosponsors/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/text/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/summaries/$page.json
		# usa/federal/congress/bills/$congress/$billType/$billNumber/titles/$page.json
		
		bill_text = json.dumps(bill)
		
		print("Uploading (%s, %s): %s" % (humanize.intcomma(count), humanize.intcomma(total), key))
		save_local(key="local/%s" % key, body=bill_text)
		upload_file(config=config['s3'], key=key, body=bill_text)
