import requests
from termVector import TermVectorCollection, TermVector


class TermVectorCollector(object):
    """ Query a batch of term vectors for a given field
        using 'id' as the uniqueKey """

    def __pathToTvrh(self, solrUrl, collection):
        import urlparse
        userSpecifiedUrl = urlparse.urlsplit(solrUrl)
        schemeAndNetloc = urlparse.SplitResult(scheme=userSpecifiedUrl.scheme,
                                               netloc=userSpecifiedUrl.netloc,
                                               path='',
                                               query='',
                                               fragment='')
        solrBaseUrl = urlparse.urlunsplit(schemeAndNetloc)
        solrBaseUrl = urlparse.urljoin(solrBaseUrl, 'solr/')
        solrBaseUrl = urlparse.urljoin(solrBaseUrl, collection + '/')
        solrBaseUrl = urlparse.urljoin(solrBaseUrl, 'tvrh')
        return solrBaseUrl

    def __init__(self, solrUrl, collection, tvField):
        super(TermVectorCollector, self).__init__()
        self.solrTvrhUrl = self.__pathToTvrh(solrUrl, collection)
        self.tvField = tvField
        self.sess = requests.Session()
        print "SESS %s" % self.sess

    def collect(self, start, rows):
        params = {"tv.fl": self.tvField,
                  "fl": "id",
                  "wt": "json",
                  "indent": "true",
                  "tv.all": "true",
                  "rows": rows,
                  "start": start,
                  "q": self.tvField + ":[* TO *]"}
        resp = self.sess.get(url=self.solrTvrhUrl,
                             params=params)
        if resp.status_code != 200:
            raise IOError("HTTP Status " + str(resp.status_code))
        return TermVectorCollection(resp.json(), self.tvField)

    def collectMerge(self, existingTvc, start, rows):
        if existingTvc is None:
            return self.collect(start, rows)
        else:
            newTvc = self.collect(start, rows)
            existingTvc.merge(newTvc)
            return existingTvc

    def collectBatch(self, start, totalSize, batchSize):
        tvc = None
        for currStart in range(start, totalSize, batchSize):
            tvc = self.collectMerge(existingTvc=tvc,
                                    start=currStart,
                                    rows=batchSize)
            assert tvc is not None
            print tvc
        return tvc


from gensim import models

if __name__ == "__main__":
    numTopics = 5
    from sys import argv
    from itertools import izip
    import sparsesvd
    import numpy
    #respJson = json.loads(open('tvTest.json').read())
    #tvResp = TermVector(respJson)
    tvc = TermVectorCollector(argv[1], argv[2], argv[3])
    corpus = tvc.collectBatch(start=0, totalSize=50, batchSize=50)
    corpus.setFeature('tf-idf')
    print len(corpus.tvs)
    keyIter = corpus.keyIter()
    #for docId, tv in izip(keyIter, corpus):
    #    print TermVector.fromFeaturePairs(corpus.termDict, docId, tv, "tf")
    tfidf = models.TfidfModel(corpus)
    #lsi = models.LsiModel(corpus, num_topics=4, id2word=corpus.termDict)
    u, s, v = sparsesvd.sparsesvd(corpus.toCsc(), numTopics)

    print u.T
    print s

    us = numpy.zeros(shape=(u[0].size, numTopics))
    print us
    us.reshape(numTopics, -1)

    # multiply in the singular values
    for colNo, col in enumerate(u.T):
        us[colNo] = numpy.multiply(col, s)

    print us.size
    print us

    # Rows in u are topics
    #for topic in u:
    #    print topic

    u = us.T

    topicBags = {i: [] for i in range(0, numTopics)}

    for termCol, term in enumerate(u.T):
        topicBags[term.argmax()].append((corpus.termDict[termCol],
                                         term[term.argmax()]))

    topicBags = {topicRow:
                 sorted(terms, key=lambda termAndScore: termAndScore[1])
                 for topicRow, terms in topicBags.iteritems()}

    print "TOP 5 TOPICS"
    for i in range(0, 5):
        print "Topic %i\n\n" % i
        print topicBags[i][:20]

    #for docId, tv in izip(keyIter, corpus):
    #    print "ORIGINAL %s" % tv
    #    print "BLURRED  %s" % lsi[tv]
    # Scores this document in the 2 topics above
    # print lsi[aTv]
    print "Done"
