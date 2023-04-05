# https://prisma-client-py.readthedocs.io/en/stable/

import os
import yaml
import json
import flask

from typing import Union
from flask import Response
from waitress import serve


hostName = "localhost"
serverPort = 8080

account_name = "demo"
web_hostname = "example.com"
web_domain: str = "http://%s" % web_hostname

app = flask.Flask(__name__)

headers: dict = {
	"Signature": 'keyId="%s/actor#main-key",headers="(request-target) host date",signature="..."' % web_domain
}

def load_config():
	with open(file=os.path.join("data", "config.yml"), mode='r') as fi:
		return yaml.safe_load(fi)

def get_config():
	config = load_config()
	activitypub = config['activitypub']
	
	return config, activitypub

def has_matching_mime_type(accepted: str) -> bool:
	if accepted is None:
		return False
	
	if 'application/activity+json' in accepted:
		return True
	
	if 'application/ld+json; profile="https://www.w3.org/ns/activitystreams"' in accepted:
		return True
	
	return False

@app.route("/users/<short_id>")
def path_actor(short_id: str) -> Response:
	global ap
	
	accepted: Union[str, None] = flask.request.headers.get('Accept')
	
	actor: dict = {
		"@context": [
			"https://www.w3.org/ns/activitystreams",
			"https://w3id.org/security/v1",
			{
				"isCat": "misskey:isCat"
			}
		],
		"id": "%s/users/%s" % (ap["web_domain"], short_id),
		"type": "Service",
		"preferredUsername": "%s" % short_id,
		"url": "%s/@%s" % (ap["web_domain"], short_id),
		"inbox": "%s/users/%s/inbox" % (ap["web_domain"], short_id),
		"outbox": "%s/users/%s/outbox" % (ap["web_domain"], short_id),
		"following": "%s/users/%s/following" % (ap["web_domain"], short_id),
		"followers": "%s/users/%s/followers" % (ap["web_domain"], short_id),
		"liked": "%s/users/%s/liked" % (ap["web_domain"], short_id),
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
		"isCat": ap["is_cat"],
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
	
	if accepted is not None and has_matching_mime_type(accepted):
		resp = flask.Response(json.dumps(actor))
		resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	else:
		resp = flask.Response("", 302)
		resp.headers['Location'] = "%s/@%s" % (ap["web_domain"], short_id)
	
	return resp

@app.route("/.well-known/webfinger")
def path_webfinger() -> Response:
	global ap
	
	resource: Union[str, None] = flask.request.args.get('resource', None, type=str)
	
	# Missing Resource
	if resource is None or resource == "":
		resp = flask.Response("", 400)
		return resp
	
	# Not A Known Resource
	if not resource.startswith("acct:"):
		resp = flask.Response("", 400)
		return resp
	
	handle: str = resource.split(":")[1]
	split: list[str] = handle.split("@")
	domain: str = split[1]
	username: str = split[0]
	
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

@app.route("/inbox")
def path_globa_inbox(short_id: str) -> Response:
	global ap
	
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/inbox")
def path_inbox(short_id: str) -> Response:
	global ap
	
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/posts/<post_id>")
def path_post(short_id: str, post_id: str) -> Response:
	global ap
	
	message: dict = {
		"@context": [
			"https://www.w3.org/ns/activitystreams",
			{
				"ostatus": "http://ostatus.org#",
				"atomUri": "ostatus:atomUri",
				"inReplyToAtomUri": "ostatus:inReplyToAtomUri",
				"conversation": "ostatus:conversation",
				"sensitive": "as:sensitive",
				"toot": "http://joinmastodon.org/ns#",
				"Emoji": "toot:emoji"
			}
		],
		"id": "%s/users/%s/posts/%s" % (ap["web_domain"], short_id, post_id),
		"type": "Note",
		"summary": None,
		"inReplyTo": None,
		"published": "2023-03-06T00:00:00Z",
		"url": "%s/@%s/%s" % (ap["web_domain"], short_id, post_id),
		"attributedTo": "%s/users/%s" % (ap["web_domain"], short_id),
		"to": [
			"https://www.w3.org/ns/activitystreams#Public"
		],
		"cc": [
			"%s/users/%s/followers" % (ap["web_domain"], short_id),
		],
		"sensitive": False,
		"content": "<p>:bill: This is a test post under the account, \"%s\" and with the id, \"%s\"</p>" % (short_id, post_id),
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
	
	resp = flask.Response(json.dumps(message))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

# TODO: Determine if this is important
#@app.route("/users/<short_id>/posts/<post_id>/activity")
def path_post_activity(short_id: str, post_id: str) -> Response:
	global ap
	
	message: dict = {
		"@context": [
			"https://www.w3.org/ns/activitystreams",
			{
				"ostatus": "http://ostatus.org#",
				"atomUri": "ostatus:atomUri",
				"inReplyToAtomUri": "ostatus:inReplyToAtomUri",
				"conversation": "ostatus:conversation",
				"sensitive": "as:sensitive",
				"toot": "http://joinmastodon.org/ns#",
				"Emoji": "toot:emoji"
			}
		],
		"id": "%s/users/%s/posts/%s/activity" % (ap["web_domain"], short_id, post_id),
		"type": "Create",
		"actor": "%s/users/%s" % (ap["web_domain"], short_id),
		"published": "2023-03-05T00:00:00Z",
		"to": [
			"https://www.w3.org/ns/activitystreams#Public"
		],
		"cc": [
			"%s/users/%s/followers" % (ap["web_domain"], short_id),
		],
		"object": {
			"id": "%s/users/%s/posts/%s" % (ap["web_domain"], short_id, post_id),
			"type": "Note",
			"summary": None,
			"inReplyTo": None,
			"published": "2023-03-06T00:00:00Z",
			"url": "%s/@%s/%s" % (ap["web_domain"], short_id, post_id),
			"attributedTo": "%s/users/%s" % (ap["web_domain"], short_id),
			"to": [
				"https://www.w3.org/ns/activitystreams#Public"
			],
			"cc": [
				"%s/users/%s/followers" % (ap["web_domain"], short_id),
			],
			"sensitive": False,
			"content": "<p>:bill: This is a test post :bill:</p>",
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
	}
	
	resp = flask.Response(json.dumps(message))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp
	
@app.route("/users/<short_id>/outbox")
def path_outbox(short_id: str) -> Response:
	global ap
	
	message: dict = {
		"id": "%s/users/%s/posts/%s/activity" % (ap["web_domain"], short_id, "6d0c2104-4a64-41d3-b337-9718aa6c22e6"),
		"type": "Create",
		"actor": "%s/users/%s" % (ap["web_domain"], short_id),
		"published": "2023-03-05T00:00:00Z",
		"to": [
			"https://www.w3.org/ns/activitystreams#Public"
		],
		"cc": [
			"%s/users/%s/followers" % (ap["web_domain"], short_id),
		],
		"object": {
			"id": "%s/users/%s/posts/%s" % (ap["web_domain"], short_id, "6d0c2104-4a64-41d3-b337-9718aa6c22e6"),
			"type": "Note",
			"summary": None,
			"inReplyTo": None,
			"published": "2023-03-06T00:00:00Z",
			"url": "%s/@%s/%s" % (ap["web_domain"], short_id, "6d0c2104-4a64-41d3-b337-9718aa6c22e6"),
			"attributedTo": "%s/users/%s" % (ap["web_domain"], short_id),
			"to": [
				"https://www.w3.org/ns/activitystreams#Public"
			],
			"cc": [
				"%s/users/%s/followers" % (ap["web_domain"], short_id),
			],
			"sensitive": False,
			"content": "<p>:bill: This is a test post :bill:</p>"
		}
	}
	
	outbox: dict = {
		"@context": [
			"https://www.w3.org/ns/activitystreams",
			{
				"ostatus": "http://ostatus.org#",
				"atomUri": "ostatus:atomUri",
				"inReplyToAtomUri": "ostatus:inReplyToAtomUri",
				"conversation": "ostatus:conversation",
				"sensitive": "as:sensitive",
				"toot": "http://joinmastodon.org/ns#",
				"Emoji": "toot:emoji"
			}
		],
		"id": "%s/users/%s/outbox" % (ap["web_domain"], short_id),
		"type": "OrderedCollectionPage",
		"totalItems": 0,
		#"next": "%s/users/%s/outbox?max_id=%s" % (ap["web_domain"], short_id, 2),
		#"prev": "%s/users/%s/outbox?min_id=%s" % (ap["web_domain"], short_id, 0),
		#"partOf": "%s/users/%s/outbox" % (ap["web_domain"], short_id),
		"orderedItems": [
			# TODO: Determine how to get outbox working
			#message
		]
	}
	
	resp = flask.Response(json.dumps(outbox))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/following")
def path_following(short_id: str) -> Response:
	global ap
	
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/followers")
def path_followers(short_id: str) -> Response:
	global ap
	
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/users/<short_id>/liked")
def path_liked(short_id: str) -> Response:
	global ap
	
	resp = flask.Response(json.dumps({}))
	resp.headers['Content-Type'] = 'application/activity+json; charset=utf-8'
	
	return resp

@app.route("/@<short_id>")
def path_actor_html(short_id: str) -> Response:
	html: str = """
		<!DOCTYPE html>
		<html>
		<head>
			<meta charset="UTF-8"> 
			<title>User: %s</title>
			<link rel="icon" type="image/png" href="/images/emojis/bill">
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

@app.route("/@<short_id>/<post_id>")
def path_post_html(short_id: str, post_id: str) -> Response:
	accepted: Union[str, None] = flask.request.headers.get('Accept')
	if accepted is not None and has_matching_mime_type(accepted):
		resp = flask.Response("", 302)
		resp.headers['Location'] = "%s/users/%s/posts/%s" % (ap["web_domain"], short_id, post_id)
		return resp
	
	html: str = """
		<!DOCTYPE html>
		<html>
		<head>
			<meta charset="UTF-8"> 
			<title>Post: %s</title>
			<link rel="icon" type="image/png" href="/images/emojis/bill">
			<meta name="viewport" content="width=device-width, initial-scale=1.0">
		</head>
		<body>
			<h1>User: %s</h1>
			<h1>Post: %s</h1>
		</body>
		</html>
	""" % (post_id, short_id, post_id)
	
	resp = flask.Response(html)
	resp.headers['Content-Type'] = 'text/html; charset=utf-8'
	
	return resp

@app.route("/images/logo")
def path_logo() -> Response:
	global ap
	
	return flask.send_file(ap["logo"])

@app.route("/images/header")
def path_header() -> Response:
	global ap
	
	return flask.send_file(ap["header"])

@app.route("/images/emojis/bill")
def path_emoji_bill() -> Response:
	global ap
	
	return flask.send_file(ap["emoji"])

if __name__ == "__main__":
	config, ap = get_config()
	
	#app.run(debug=False, ssl_context=(ap["certificate_path"], ap["private_key_path"]), host=hostName, port=serverPort)
	#app.run(debug=True, host=ap["hostname"], port=ap["server_port"])
	serve(app, host=ap["hostname"], port=ap["server_port"])