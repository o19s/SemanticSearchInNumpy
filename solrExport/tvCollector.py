import requests
from termVector import TermVectorCollection


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

    def collect(self, start, rows):
        from sys import stderr
        params = {"tv.fl": self.tvField,
                  "fl": "id",
                  "wt": "json",
                  "indent": "true",
                  "tv.all": "true",
                  "rows": rows,
                  "start": start,
                  "q": self.tvField + ":[* TO *]"}
        resp = requests.get(url=self.solrTvrhUrl,
                            params=params)
        if resp.status_code != 200:
            raise IOError("HTTP Status " + str(resp.status_code))
        return TermVectorCollection(resp.json())

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


if __name__ == "__main__":
    from sys import argv
    #respJson = json.loads(open('tvTest.json').read())
    #tvResp = TermVector(respJson)
    tvc = TermVectorCollector(argv[1], argv[2], argv[3])
    corpus = tvc.collectBatch(0, 10, 10)
    print len(corpus.tvs)
    for tv in corpus:
        print tv
    print "Done"
