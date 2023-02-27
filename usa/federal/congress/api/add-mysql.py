import os
import yaml
import json
import mysql.connector

def load_config():
	with open('config.yml', 'r') as fi:
		return yaml.safe_load(fi)

def get_config():
	config = load_config()
	mysql_config = config["mysql"]
	hostname = mysql_config["hostname"]
	username = mysql_config["username"]
	password = mysql_config["password"]
	database = mysql_config["database"]
	
	try:
		connection = mysql.connector.connect(
			host=hostname,
			user=username,
			password=password,
			database=database
		)
	except mysql.connector.Error as err:
		if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
			print("Something is wrong with your username or password")
		elif err.errno == errorcode.ER_BAD_DB_ERROR:
			print("Database does not exist")
		else:
			print(err)
	
	return connection

def setup_table(connection: mysql.connector.connection_cext.CMySQLConnection):
	table_name: str = 'tableofcontents'
	check_exists_sql: str = "SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_NAME = '%s';" % table_name
	create_table_sql: str = "CREATE TABLE %s (country VARCHAR(255), level VARCHAR(255), branch VARCHAR(255), form VARCHAR(255), session VARCHAR(255), type VARCHAR(255), number VARCHAR(255), url VARCHAR(255), CONSTRAINT UC_Entry UNIQUE (country,level,branch,form,session,type,number));" % table_name
	
	cursor = connection.cursor()
	cursor.execute(check_exists_sql)
	
	for x in cursor:
		exists: bool = x[0]
		
		if not exists:
			print("Creating table: %s" % table_name)
			cursor.execute(create_table_sql)  # TODO: Determine if should check if successful
			break  # This shouldn't run more than once, but just in case, we are forcing this to only run once
			
	connection.commit()
	cursor.close()

def add_entry(connection: mysql.connector.connection_cext.CMySQLConnection, path: str):
	table_name: str = 'tableofcontents'
	with open(file=path, mode="r") as f:
		contents: dict = json.load(f)
		
		country: str = "usa"
		level: str = "federal"
		branch: str = "congress"
		
		if "bill" in contents:  # usa/federal/congress/bills/14/hr/10
			form: str = "bill"
			session: int = contents["bill"]["congress"]
			entry_type: str = contents["bill"]["type"].lower()
			number: str = contents["bill"]["number"]
			url: str = "https://s3.us-east-1.wasabisys.com/bills/%s/%s/%s/%s/%s/%s/%s/data.json" % (country, level, branch, "bills", session, entry_type, number)
			
			insert_sql: str = 'INSERT INTO %s (country, level, branch, form, session, type, number, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);' % (table_name, country, level, branch, form, session, entry_type, number, url)
		elif "committeeReports" in contents:  # usa/federal/congress/committee-reports/118/hrpt/1
			form: str = "committee-report"
			session: int = contents["committeeReports"][0]["congress"]
			entry_type: str = contents["committeeReports"][0]["type"].lower()
			number: int = contents["committeeReports"][0]["number"]
			url: str = "https://s3.us-east-1.wasabisys.com/bills/%s/%s/%s/%s/%s/%s/%s/data.json" % (country, level, branch, "committee-reports", session, entry_type, number)
			
			# TODO: Investigate Array
			insert_sql: str = 'INSERT INTO %s (country, level, branch, form, session, type, number, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);' % (table_name, country, level, branch, form, session, entry_type, number, url)
		elif "amendment" in contents:  # usa/federal/congress/amendments/103/hamdt/1
			form: str = "amendment"
			session: int = contents["amendment"]["congress"]
			entry_type: str = contents["amendment"]["type"].lower()
			number: str = contents["amendment"]["number"]
			url: str = "https://s3.us-east-1.wasabisys.com/bills/%s/%s/%s/%s/%s/%s/%s/data.json" % (table_name, country, level, branch, "amendments", session, entry_type, number)
			
			insert_sql: str = 'INSERT INTO %s (country, level, branch, form, session, type, number, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);' % (table_name, country, level, branch, form, session, entry_type, number, url)
		elif "member" in contents:  # usa/federal/congress/members/F000209
			form: str = "member"
			number: str = contents["member"]["identifiers"]["bioguideId"]
			url: str = "https://s3.us-east-1.wasabisys.com/bills/%s/%s/%s/%s/%s/data.json" % (country, level, branch, "members", number)
			
			# TODO: Add Table For Members
			insert_sql: str = 'INSERT INTO %s (country, level, branch, form, number, url) VALUES (%s, %s, %s, %s, %s, %s);' % (table_name, country, level, branch, form, number, url)
			
		print("Inserting Entry For: %s" % url)
		cursor.execute(insert_sql)  # TODO: Determine if should check if successful

def add_entries(connection: mysql.connector.connection_cext.CMySQLConnection):
	path: str = 'local'
	for (dirpath, dirnames, filenames) in os.walk(path):
		for filename in filenames:
			if filename.endswith('.json'):
				file_path: str = os.sep.join([dirpath, filename])
				add_entry(connection=connection, path=file_path)

		connection.commit()

	cursor.close()

if __name__ == "__main__":
	db = get_config()
	setup_table(connection=db)
	add_entries(connection=db)
