"""WikiHistoryParser
Parses the history from a wikimedia page history dump archive for anonmymous edits from a specific IP range.
"""

from datetime import datetime
from elasticsearch import Elasticsearch
from geolite2 import geolite2
from netaddr import IPNetwork, IPAddress
import difflib
import hashlib
import os
import subprocess
import urllib
import xml.etree.cElementTree as ET


class WikiHistoryParser(object):

    def __init__(self, elasticsearch_host, elasticsearch_port, file_to_process, file_language, wiki_history_file_index, wiki_history_file_type, wiki_history_edit_index, wiki_history_edit_type, local_tmp_dir, ip_ranges):
        """Process a wikimedia page history dump archive.

        Args:
            elasticsearch_host (str): The host to the elasticsearch instance.
            elasticsearch_port (str): The port the elasticsearch instance listens on.
            file_to_process (str): The path to the page history dump file to process.
            file_language (str): The language represented in the page history dump file.
            wiki_history_file_index (string): The elasticsearch index for the page history files.
            wiki_history_file_type (string): The elasticsearch doctype for the page history files.
            wiki_history_edit_index (string): The elasticsearch index for the page edits.
            wiki_history_edit_type (string): The elasticsearch doctype for the page edits.
            local_tmp_dir (string): The temporary directory to use when processing the file.
            ip_ranges (dict): A dictionary of IP ranges to filter and store edits with.
        """
        self.local_tmp_dir = local_tmp_dir
        self.ip_ranges = ip_ranges

        self.es = {}
        self.init_elasticsearch(
            elasticsearch_host,
            elasticsearch_port,
            wiki_history_file_index,
            wiki_history_file_type,
            wiki_history_edit_index,
            wiki_history_edit_type
        )

        self.file = {}
        self.init_file(
            file_to_process,
            file_language
        )

    def init_elasticsearch(self, elasticsearch_host, elasticsearch_port, wiki_history_file_index, wiki_history_file_type, wiki_history_edit_index, wiki_history_edit_type):
        """ Connect to and create the ElasticSearch wiki-edits index if it doesn't exist.

        Args:
            elasticsearch_host (string): The hostname of the elasticsearch server.
            elasticsearch_port (string): The port of the elasticsearch server.
            wiki_history_file_index (string): The elasticsearch index for the page history files.
            wiki_history_file_type (string): The elasticsearch doctype for the page history files.
            wiki_history_edit_index (string): The elasticsearch index for the page edits.
            wiki_history_edit_type (string): The elasticsearch doctype for the page edits.
        """
        # Connection information.
        self.es = {
            'host': elasticsearch_host,
            'port': elasticsearch_port,
            'con':  Elasticsearch(
                [
                    elasticsearch_host + ':' + elasticsearch_port
                ],
            ),
            'file_index': wiki_history_file_index,
            'file_doctype': wiki_history_file_type,
            'edit_index': wiki_history_edit_index,
            'edit_doctype': wiki_history_edit_type,
            'mappings': {
                wiki_history_edit_type: {
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                }
            },
        }

        # Connect.
        self.es['con'] = Elasticsearch([elasticsearch_host + ':' + elasticsearch_port])
        self.es['con'].indices.create(
            index=self.es['edit_index'],
            body=self.es['mappings'],
            ignore=400
        )

    def init_file(self, file_name, language):
        """ Set up file properties needed to process the archive.

        Args:
            file_name (string): The path to the page history dump file to process.
            language (string): The language code of the page contents in the history dump.
        """
        self.file = {
            'filename': file_name,
            'basename': os.path.basename(file_name),
            'target': self.local_tmp_dir + "/" + os.path.basename(file_name),
            'hash': int(hashlib.sha1(file_name).hexdigest(), 16) % (10 ** 8),
            'language': language,
        }

    def parse(self):
        """Process the file, storing anonymous edits matching IP ranges found within. """

        # Update file status to in-progress.
        wiki_dump = {
          'timestamp': datetime.now(),
          'file': self.file['filename'],
          'status': 1
        }
        print("Updating status of %s to 'In progress'" % self.file['filename'])
        print self.es['con'].index(
            index=self.es['file_index'],
            doc_type=self.es['file_doctype'],
            id=self.file['hash'],
            body=wiki_dump
        )

        # Process file.
        print("Downloading %s from wikimedia" % self.file['basename'])
        testfile = urllib.URLopener()
        testfile.retrieve(self.file['filename'], self.file['target'])

        print("Extracting XML from %s" % self.file['target'])
        command = ["7z", "-o" + self.local_tmp_dir, "-y", "x", self.file['target']]
        subprocess.call(command)

        file_to_parse, file_to_parse_extension = os.path.splitext(self.file['target'])

        with open(file_to_parse, 'rb') as inputfile:
            print("Parsing Data from %s" % file_to_parse)
            append = False
            inputbuffer = ''

            for line in inputfile:
                if '<page>' in line:
                    inputbuffer = line
                    append = True
                elif '</page>' in line:
                    inputbuffer += line
                    append = False
                    self.process_page(inputbuffer)
                    inputbuffer = None
                    del inputbuffer
                elif append:
                    inputbuffer += line

            # Update file status to complete.
            wiki_dump = {
              'timestamp': datetime.now(),
              'file': self.file['filename'],
              'status': 2
            }
            print("Updating status of %s to 'Done'" % self.file['filename'])
            print self.es['con'].index(
                index=self.es['file_index'],
                doc_type=self.es['file_doctype'],
                id=self.file['hash'],
                body=wiki_dump
            )

        # Remove files
        os.remove(file_to_parse)
        os.remove(self.file['target'])

    def process_page(self, buf):
        """
        Process the page record and add any matching edits to elasticsearch.

        Args:
            buf (str): The page record XML representation.
        """
        revisions = {
            0: '',
        }
        prev_revision = ''

        try:
            # Load the string into an ETree Element
            page = ET.fromstring(buf)

            # Necessary as parent / children may not appear in order for diffing later.
            for revision in page.iter('revision'):
                cur_id = revision.find('id').text
                cur_text = revision.find('text').text
                revisions[cur_id] = cur_text

            # Iterate over revisions
            for revision in page.iter('revision'):
                cur_id = revision.find('id').text
                cur_timestamp = revision.find('timestamp').text
                cur_text = revision.find('text').text

                if not revision.find('parentid') == None:
                    parent_id = revision.find('parentid').text
                else:
                    parent_id = 0

                if not revision.find('comment') == None:
                    cur_comment = revision.find('comment').text
                else:
                    cur_comment = ''

                for contributor in revision.iter('contributor'):
                    for anonedit in contributor.iter('ip'):
                        for gov_org, org_ranges in self.ip_ranges.iteritems():
                            for cur_range in org_ranges:
                                if IPAddress(anonedit.text) in IPNetwork(cur_range):
                                    reader = geolite2.reader()
                                    geo_data = reader.get(anonedit.text)
                                    geolite2.close()

                                    edit_document = {
                                        'timestamp': datetime.now(),
                                        'edit_date': cur_timestamp,
                                        'title': page.find('title').text,
                                        'ip': anonedit.text,
                                        'ip-range-owner': gov_org,
                                        'document': cur_text,
                                        'language': self.file['language'],
                                        'diff': self.unidiff(revisions[parent_id], cur_text),
                                        'location': {
                                            "lat": geo_data['location']['latitude'],
                                            "lon": geo_data['location']['longitude'],
                                        }
                                    }
                                    print self.es['con'].index(
                                        index=self.es['edit_index'],
                                        doc_type=self.es['edit_doctype'],
                                        id=cur_id,
                                        body=edit_document
                                    )
                try:
                    prev_revision = revision.find('text').text
                except:
                    pass
        except:
            print "Parsing Buffer Failed!"

        del revisions

    def unidiff(self, expected, actual):
        """
        Helper method. Returns a string containing the unified diff of two multiline strings.

        Args:
            expected (str): The expected string.
            actual (str): The actual string to compare against.
        """
        expected = expected.splitlines(1)
        actual = actual.splitlines(1)
        diff = difflib.unified_diff(expected, actual)
        return ''.join(diff)
