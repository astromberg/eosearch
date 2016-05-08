# coding: utf-8
import sqlite3

c = sqlite3.connect('eo.db')
cur = c.cursor()

cur.execute('DROP TABLE IF EXISTS eo')

cur.execute('CREATE TABLE eo ('
	+ 'url text PRIMARY KEY'
	+ ', object_id text'
	+ ', last_crawl timestamp'
	+ ', view_time_millis integer'
	+ ', search_tags text'
	+ ')')
