import yaml
import time
import json
import flask

from urllib.parse import urlparse

hostName = "localhost"
serverPort = 8080

account_name = "demo"
web_hostname = "localhost"
web_domain = "http://%s" % web_hostname

app = flask.Flask(__name__)

if __name__ == "__main__":
	config, ap = get_config()
	hostName = ap["hostname"]
	serverPort = ap["server_port"]

	account_name = ap["account_name"]
	web_hostname = ap["web_hostname"]
	web_domain = ap["web_domain"]
	
	actor["publicKey"]["publicKeyPem"] = ap["activitypub_public_key"]
	
	#app.run(debug=False, ssl_context=(ap["certificate_path"], ap["private_key_path"]), host=hostName, port=serverPort)
	app.run(debug=False, host=hostName, port=serverPort)

actor: dict = {
	"@context": [
		"https://www.w3.org/ns/activitystreams",
		"https://w3id.org/security/v1"
	],
	"id": "%s/actor" % web_domain,
	"type": "Person",
	"preferredUsername": "%s" % account_name,
	"inbox": "%s/inbox" % web_domain,
	"publicKey": {
		"id": "%s/actor#main-key" % web_domain,
		"owner": "%s/actor" % web_domain,
		"publicKeyPem": "-----BEGIN PUBLIC KEY-----...-----END PUBLIC KEY-----"
	}
}

webfinger: dict = {
	"subject": "acct:%s@%s" % (account_name, web_hostname),
	"links": [{
		"rel": "self",
		"type": "application/activity+json",
		"href": "%s/actor" % web_domain
	}]
}

# TODO: Figure out how to host "existing" posts
message: dict = {
	"@context": "https://www.w3.org/ns/activitystreams",
	"id": "%s/create-hello-world" % web_domain,
	"type": "Create",
	"actor": "%s/actor" % web_domain,
	"object": {
		"id": "%s/hello-world" % web_domain,
		"type": "Note",
		"published": "2018-06-23T17:17:11Z",
		"attributedTo": "%s/actor" % web_domain,
		"inReplyTo": "https://mastodon.social/@Gargron/100254678717223630",
		"content": "<p>Hello world</p>",
		"to": "https://www.w3.org/ns/activitystreams#Public"
	}
}

headers: dict = {
	"Signature": 'keyId="%s/actor#main-key",headers="(request-target) host date",signature="..."' % web_domain
}

def load_config():
	with open('config.yml', 'r') as fi:
		return yaml.safe_load(fi)

def get_config():
	config = load_config()
	activitypub = config['activitypub']
	
	return config, activitypub

@app.route("/actor")
def path_actor():
	resp = flask.Response(json.dumps(actor))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/.well-known/webfinger")
def path_webfinger():
	resp = flask.Response(json.dumps(webfinger))
	resp.headers['Content-Type'] = 'application/jrd+json; charset=utf-8'
	
	return resp
