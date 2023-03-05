import yaml
import time
import json
import flask

from waitress import serve
from urllib.parse import urlparse

hostName = "localhost"
serverPort = 8080

account_name = "demo"
web_hostname = "example.com"
web_domain = "http://%s" % web_hostname

app = flask.Flask(__name__)

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

@app.route("/users/<short_id>")
def path_actor(short_id: str):
	global ap
	
	# For Debugging
	disable_redirect: bool = False
	
	accepted: str = flask.request.headers.get('Accept')
	
	actor: dict = {
		"@context": [
			"https://www.w3.org/ns/activitystreams",
			"https://w3id.org/security/v1"
		],
		"id": "%s/users/%s" % (ap["web_domain"], short_id),
		"type": "Service",
		"preferredUsername": "%s" % short_id,
		"url": "%s/@%s" % (ap["web_domain"], short_id),
		"inbox": "%s/users/%s/inbox" % (ap["web_domain"], short_id),
		"outbox": "%s/users/%s/outbox" % (ap["web_domain"], short_id),
		"following": "%s/users/%s/following" % (ap["web_domain"], short_id),
		"followers": "%s/users/%s/followers" % (ap["web_domain"], short_id),
		"publicKey": {
			"id": "%s/users/%s#main-key" % (ap["web_domain"], short_id),
			"owner": "%s/users/%s" % (ap["web_domain"], short_id),
			"publicKeyPem": ap["activitypub_public_key"]
		},
		"endpoints": {
			"sharedInbox": "%s/inbox" % ap["web_domain"]
		},
		
		"name": "%s :bill:" % short_id,
		"summary": """
			<p>I'm a bot that's designed to show bill data from different governments. I was developed by <span class="h-card"><a href="https://chat.alexisart.me/@alexis" class="u-url mention">@<span>alexis</span></a></span>.</p>
			
			<p>Friendly Disclaimer: This bot can generate any response that is not intentional or monitored. The author is not responsible.</p>
		""",
		"published": "1789-03-04T00:00:00Z",
		"icon": {
			"type": "Image",
			"mediaType": "image/png",
			"url": "%s/images/logo" % ap["web_domain"]
		},
		"image": {
			"type": "Image",
			"mediaType": "image/png",
			"url": "%s/images/header" % ap["web_domain"]
		},
		"attachment": [
			{
				"type": "PropertyValue",
				"name": "Donate",
				"value": """
					<a href="https://ko-fi.com/alexisartdesign" target="_blank" rel="nofollow noopener noreferrer me"><span class="invisible">https://</span><span class="">ko-fi.com/alexisartdesign</span><span class="invisible"></span></a>
				"""
			}
		],
		"tag": [
			{
				"id": "%s/emojis/bill" % ap["web_domain"],
				"type": "Emoji",
				"name": ":bill:",
				"updated": "2023-02-25T19:36:09Z",
				"icon": {
					"type": "Image",
					"mediaType": "image/png",
					"url": "%s/images/emojis/bill" % ap["web_domain"]
				}
			}
		]
	}
	
	if "application/activity+json" in accepted or disable_redirect:
		resp = flask.Response(json.dumps(actor))
		resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	else:
		resp = flask.Response("", 302)
		resp.headers['Location'] = "%s/@%s" % (ap["web_domain"], short_id)
	
	return resp

@app.route("/.well-known/webfinger")
def path_webfinger():
	global ap
	
	resource: str = flask.request.args.get('resource', None, type=str)
	
	# Missing Resource
	if resource is None or resource == "":
		resp = flask.Response("", 400)
		return resp
	
	# Not A Known Resource
	if not resource.startswith("acct:"):
		resp = flask.Response("", 400)
		return resp
	
	split = resource.split(":")[1]
	split = split.split("@")
	domain = split[1]
	username = split[0]
	
	# Verify At Least 2 Sections
	if len(split) != 2:
		resp = flask.Response("", 400)
		return resp
	
	webfinger: dict = {
		"subject": resource,
		"links": [
			{
				"rel": "self",
				"type": "application/activity+json",
				"href": "https://%s/users/%s" % (domain, username)
			}
		]
	}
	
	resp = flask.Response(json.dumps(webfinger))
	resp.headers['Content-Type'] = 'application/jrd+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/inbox")
def path_inbox(short_id: str):
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp
	
@app.route("/users/<short_id>/outbox")
def path_outbox(short_id: str):
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/following")
def path_following(short_id: str):
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/followers")
def path_followers(short_id: str):
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/@<short_id>")
def path_actor_html(short_id: str):
	html: str = """
		<!DOCTYPE html>
		<html>
		<head>
			<meta charset="UTF-8"> 
			<title>User: %s</title>
			<meta name="viewport" content="width=device-width, initial-scale=1.0">
		</head>
		<body>
			<h1>User: %s</h1>
		</body>
		</html>
	""" % (short_id, short_id)
	
	resp = flask.Response(html)
	resp.headers['Content-Type'] = 'text/html; charset=utf-8'
	
	return resp

@app.route("/images/logo")
def path_logo():
	global ap
	
	return flask.send_file(ap["logo"])

@app.route("/images/header")
def path_header():
	global ap
	
	return flask.send_file(ap["header"])

@app.route("/images/emojis/bill")
def path_emoji_bill():
	global ap
	
	return flask.send_file(ap["emoji"])

if __name__ == "__main__":
	config, ap = get_config()
	
	#app.run(debug=False, ssl_context=(ap["certificate_path"], ap["private_key_path"]), host=hostName, port=serverPort)
	#app.run(debug=False, host=ap["hostname"], port=ap["server_port"])
	serve(app, host=ap["hostname"], port=ap["server_port"])
