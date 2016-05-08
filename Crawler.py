# coding: utf-8

import bs4
import datetime as dt
import re
import requests as r
import sqlite3

c = sqlite3.connect('eo.db')
cur = c.cursor()

object_uri_re = re.compile(
	'^/objects/(.{1,5})$')

objects = set()
users = set()

def crawl(start_url):
	urls_skipped_count = 0
	url_fetch_count = 0
	urls_to_crawl = set([start_url])
	urls_crawled = set()
	while len(urls_to_crawl) > 0:
		url_to_crawl = urls_to_crawl.pop()
		if should_crawl_url(url_to_crawl):
			maybe_new_urls = crawl_url(url_to_crawl)
			url_fetch_count += 1
			urls_crawled.add(url_to_crawl)
			new_urls = maybe_new_urls - urls_crawled
			urls_to_crawl.update(new_urls)
			for url in new_urls:
				store_found_url_if_new
			if len(urls_crawled) % 20 == 0:
				print 'made {3} requests, crawled {0} urls, skipped {1}, {2} remaining'.format(
							len(urls_crawled),
							urls_skipped_count,
							len(urls_to_crawl),
							url_fetch_count)
			if len(urls_crawled) > 100:
				break
		else:
			urls_skipped_count += 1

def crawl_url(url):
	additional_urls = set()
	response = r.get(url).text

	soup = bs4.BeautifulSoup(response)
	
	store_page_result(url, soup)

	for link in soup.find_all('a'):
		href = link.get('href')
		if href.startswith('/user'):
			additional_urls.add(normurl(href))
			users.add(href.replace('/users/', ''))
		elif object_uri_re.match(href):
			additional_urls.add(normurl(href))
			objects.add(
				object_uri_re.search(href).group())
	
	return additional_urls

def normurl(uri):
	if not uri.startswith('http'):
		return ('http://www.electricobjects.com'
			+ uri)
	return uri

def should_crawl_url(url):
	url = normurl(url)
	existing = cur.execute(
		'SELECT * FROM eo WHERE url = "{0}"'.format(url))
	res = existing.fetchall()
	if len(res) > 0:
		should_crawl = res[0].timestamp is None
		print 'should crawl {0}, {1}'.format(
			url, should_crawl)
		return should_crawl
	return True

def store_found_url_if_new(url):
	url = normurl(url)
	existing = cur.execute(
		'SELECT * FROM eo WHERE url = "{0}"'.format(url))
	res = existing.fetchall()
	if len(res) > 0:
		print 'already have url {0}'.format(url)
		return
	object_id = None
	timestamp = None
	view_time_millis = 0
	tags = None
	print 'adding new url {0}'.format(url)
	cur.execute('INSERT OR REPLACE INTO eo VALUES (?,?,?,?,?)',
		url, object_id, timestamp,
		view_time_millis, tags)

def store_page_result(url, soup):
	url = normurl(url)
	object_id = ''
	timestamp = dt.datetime.now()
	view_time_millis = 1
	tags = ''
	print 'storing page result'
	cur.execute('INSERT OR REPLACE INTO eo VALUES (?,?,?,?,?)',
		(url, object_id, timestamp, view_time_millis, tags))
			
crawl('https://www.electricobjects.com/objects/ZNz0')

print 'users: {0}'.format(len(users))
print 'objects: {0}'.format(len(objects))
