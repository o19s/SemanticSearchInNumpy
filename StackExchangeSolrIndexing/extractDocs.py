import re
import sys
from optparse import OptionParser
from datetime import datetime
import httplib
from urlparse import urlparse

# pre-compiled regular expressions
rowRe = re.compile('^\s*<row')                  # detects a row
attrRe = re.compile('(\w+)="(.*?)"')            # extracts all attribues and values
cleanupRe = re.compile('<[^<]+?>|[\r\n]+|\s+')  # strips out html and extra whitespace
tagsRe = re.compile('&lt;(.*?)&gt;')            # splits tags into a list
intRe = re.compile('^\d+$')                     # determines if field is an integer
escapeRe = re.compile('&lt;/?.+?&gt;|&[^q][#\w]+;') # pulls our XML excapes eg &lt;


def main(fileName):

    with open(fileName) as f:
        while(True):
            data = get_bulk_solr_doc_set(f,BULK_SIZE)
            if (data):
                post(URL,data)
            else:
                return

def post(url, data):
    url = urlparse(url)

    conn = httplib.HTTPConnection(url.netloc)
    conn.request('POST', url.path, data, {"Content-type": "text/xml"})
    resp = conn.getresponse()

    print resp.read() #TODO do someting smarter


def get_bulk_solr_doc_set(f,bulk_size):
    docs = get_docs(f,bulk_size)
    if(docs):
        return "<add>"+ '\n'.join([make_solr_doc(doc) for doc in docs]) + "</add>"

def get_docs(f,bulk_size):
    """ Parse StackExchange data into ElasticSearch Bulk Format """

    docs = []
    for i, line in enumerate(f):

        # skip line if not a row
        if rowRe.match(line) is None:
            continue

        # build the document to be indexed
        doc = {}
        for field, val in attrRe.findall(line):
            # strip whitespace and skip field if empty value
            val = val.strip()
            if not val:
                continue

            # cleanup title and body by stripping html and whitespace
            if field in ['Body', 'Title']:
                val = escapeRe.sub(' ', val)
                val = cleanupRe.sub(' ', val)

            # make sure dates are in correct format
            elif field in ['CreationDate', 'LastActivityDate', 'LastEditDate']:
                # 2008-07-31T21:42:52.667
                val = '%sZ' % val

                # parse creation month, day, hour, and minute
                if field == 'CreationDate':
                    dateObj = datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%fZ')
                    doc['CreationMonth'] = dateObj.strftime('%B')
                    doc['CreationDay'] = dateObj.strftime('%A')
                    doc['CreationHour'] = dateObj.strftime('%H')
                    doc['CreationMinute'] = dateObj.strftime('%M')

            # split tags into an aray of tags
            elif field == 'Tags':
                val = ' '.join(tagsRe.findall(val))

            doc[field] = val
        docs.append(doc)

        if((i+1) == bulk_size):
            return docs
    return docs


def make_solr_doc(doc):
    return '\n'.join(['<doc>'
        ,'\n'.join(['<field name="%s">%s</field>' % (key, doc[key]) for key in doc])
        ,'</doc>'])


###########################################################################################


if __name__ == '__main__':
    usage = 'usage: %prog [options] file'
    parser = OptionParser(usage)
    parser.add_option('-u', '--url', dest='url', default='localhost:8983/solr/update', help='POST endpoint')
    parser.add_option('-b', '--bulk-size', dest='bulkSize', type='int', default=10000, help='Number of docs to submit in each bulk request.')

    options, args = parser.parse_args()
    if len(args) != 1:
        parser.error('The StackOverflow posts.xml file location must be specified')

    # globals
    URL = "http://"+options.url
    BULK_SIZE = options.bulkSize

    ret = main(args[0])
    sys.exit(ret)
