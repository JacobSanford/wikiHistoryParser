"""Parse Wikimedia History."""

from elasticsearch import Elasticsearch
from FilterIpRanges import ip_ranges
from WikiHistoryParser import WikiHistoryParser as Parser

local_tmp_dir = '/tmp'

elasticsearch_host = 'elasticsearch'
elasticsearch_port = '9200'

wiki_history_file_index = 'wiki-history-dumps'
wiki_history_file_type = 'wiki-dump'
wiki_history_edit_index = 'gov-wiki-edits'
wiki_history_edit_type = 'wiki-edit'

es = Elasticsearch([elasticsearch_host + ':' + elasticsearch_port])
res = es.search(
    index=wiki_history_file_index,
    doc_type=wiki_history_file_type,
    body={
        "query": {
            "match": {
                "status": 0
            }
        }
    }
)
print("%d archives still need processing" % res['hits']['total'])

for doc in res['hits']['hits']:
    file_to_process = doc['_source']['file']
    file_language = doc['_source']['language']

    parser = Parser.WikiHistoryParser(
        elasticsearch_host,
        elasticsearch_port,
        file_to_process,
        file_language,
        wiki_history_file_index,
        wiki_history_file_type,
        wiki_history_edit_index,
        wiki_history_edit_type,
        local_tmp_dir,
        ip_ranges
    )
    parser.parse()
    break
