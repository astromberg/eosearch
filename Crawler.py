# coding: utf-8

from datetime import datetime
from time import mktime

import bs4
import parsedatetime
import re
import requests as r
import sqlite3

c = sqlite3.connect('eo.db')
cur = c.cursor()

object_uri_re = re.compile(
	'.*/objects/(.{1,5})$')
image_re = re.compile(
	'(https://electric.*)\?.*')

URL_INDEX = 0
OBJECT_INDEX = 1
TIMESTAMP_INDEX = 2
VIEW_TIME_INDEX = 3
SEARCH_TAGS_INDEX = 4

objects = set()
users = set()

def crawl(start_url):
	urls_skipped_count = 0
	url_fetch_count = 0
	urls_to_crawl = set([start_url])
	urls_to_crawl.update(get_uncrawled_urls())
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
				store_found_url_if_new(url)
			if len(urls_crawled) % 20 == 0:
				print 'made {3} requests, crawled {0} urls, skipped {1}, {2} remaining'.format(
							len(urls_crawled),
							urls_skipped_count,
							len(urls_to_crawl),
							url_fetch_count)
			if len(urls_crawled) > 1000:
				break
		else:
			print 'skipping url {0}, recently crawled'.format(
				url_to_crawl)
			urls_skipped_count += 1

def crawl_url(url):
	additional_urls = set()
	response = r.get(url).text

	soup = bs4.BeautifulSoup(response, "html.parser")
	
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
		should_crawl = res[0][2] is None
		print 'should crawl {0}, {1}'.format(
			url, should_crawl)
		return should_crawl
	return True

def get_uncrawled_urls():
        existing = cur.execute(
                'SELECT * FROM eo WHERE last_crawl IS NULL')
        res = existing.fetchall()
	urls = set()
	for row in res:
		urls.add(row[URL_INDEX])
	return urls

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
		(url, object_id, timestamp,
		view_time_millis, tags))
	c.commit()

def store_page_result(url, soup):
	url = normurl(url)
	#import pdb; pdb.set_trace()
	object_id = None
	piece_name = None
	attributed_artist = None
	image_url = None
	tags = None
	timestamp = datetime.now()
	view_time_millis = None
	if object_uri_re.match(url):
		object_id = object_uri_re.search(url).group(1)
		p = parsedatetime.Calendar()
		try:
			string_view_time = soup.find('dt',
				string='Total time displayed:').next_sibling.next_sibling.string 
			view_time_millis = (
				datetime.fromtimestamp(
					mktime(p.parse(string_view_time)[0]))
				- datetime.now()).total_seconds() * 1000
		except:
			view_time_millis = 0
			#import pdb; pdb.set_trace()
		piece_name = soup.find(property='og:title')['content']
		attributed_artist = soup.find(
			property='og:description')['content'].replace('By ', '')
		image_url = image_re.search(
			soup.find(property='og:image')['content']).group(1)
		tags = piece_name + ',' + attributed_artist
	print 'storing page result {0}'.format((
		url,
		object_id,
		timestamp,
		piece_name, 
		attributed_artist,
		image_url,
		view_time_millis,
		tags))
	cur.execute('INSERT OR REPLACE INTO eo VALUES (?,?,?,?,?)',
		(url, object_id, timestamp, view_time_millis, tags))
	c.commit()
			
crawl('https://www.electricobjects.com/objects/ZNz0')

print 'users: {0}'.format(len(users))
print 'objects: {0}'.format(len(objects))
