#!/usr/bin/env python3
from bs4 import BeautifulSoup
from datetime import datetime
import elasticsearch
import hashlib
import requests
import time
import sys

def getLinksFromPage(url, ext='7z'):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return set([url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)])

# Configurables.
elasticsearch_host = 'elasticsearch'
elasticsearch_port = '9200'

wiki_host = 'dumps.wikimedia.org'
wiki_languages = ['en', 'fr']
wiki_history_file_index = 'wiki-history-dumps'
wiki_history_file_type = 'wiki-dump'

# Sleep for 60 seconds to let elasticsearch launch.
time.sleep(60)

# Create the indices if they don't exist.
es = elasticsearch.Elasticsearch([ elasticsearch_host + ':' + elasticsearch_port])
es.indices.create(index=wiki_history_file_index, ignore=400)

# Add the files if they don't alrady exist.
for wiki_language in wiki_languages:
    url = 'https://' + wiki_host + '/' + wiki_language + 'wiki/latest/'
    for file in getLinksFromPage(url):
        wiki_history_file_mask = wiki_language + 'wiki-latest-pages-meta-history'
        if wiki_history_file_mask in file:
            cur_id = int(hashlib.sha1(file).hexdigest(), 16) % (10 ** 8)
            try:
                res = es.get(index=wiki_history_file_index, doc_type=wiki_history_file_type, id=cur_id)
                print("Ignoring already indexed file : %s" % file)
            except elasticsearch.exceptions.NotFoundError:
                wiki_dump = {
                    'timestamp': datetime.now(),
                    'file': file,
                    'language': wiki_language,
                    'status': 0
                }
                print es.index(index=wiki_history_file_index, doc_type=wiki_history_file_type, op_type="create", id=cur_id, body=wiki_dump)
            except:
                print "Unexpected error!"
                raise
