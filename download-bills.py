import yaml
import boto3
import requests

def load_config():
	with open('config.yml', 'r') as fi:
		return yaml.safe_load(fi)

def load_bucket(config: dict):
	return boto3.client(
		service_name='s3',
		aws_access_key_id=config['access_key_id'],
		aws_secret_access_key=config['secret_access_key'],
		endpoint_url=config['endpoint']
	)

def upload_file(config: dict):
	#data = open('test.jpg', 'rb')
	#s3.Bucket('my-bucket').put_object(Key='test.jpg', Body=data)
	s3.put_object(
		Bucket=config['default_bucket'],
		Body="Test Body",
		Key="test/hello.txt"
	)

if __name__ == "__main__":
	config = load_config()
	s3 = load_bucket(config['s3'])
	
	#upload_file(config['s3'])
