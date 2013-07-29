import requests
from termVector import TermVectorCollection, TermVector
import sparsesvd
from numpy import *

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






def getTermVectorCollection(field,collection,solrUrl):
    tvc = TermVectorCollector(solrUrl,collection,field)
    corpus = tvc.collectBatch(start=0, totalSize=50000, batchSize=5000)
    corpus.setFeature('tf')
    return corpus


class PseudoTermVectors(object):
    def __init__(self,corpus,numTopics,cutoff=0.000001):
        u, s, self.v = sparsesvd.sparsesvd(corpus.toCsc(), numTopics)
        self.uPrime = dot(u.T,diag(s))
        self.corpus = corpus
        self.valueCutoff = cutoff

    def     getPseudoTermVector(self,n):
        return dot(self.uPrime,self.v[:,n:n+1])[:,0]

    def hist(self,n):
        return histogram(self.getPseudoTermVector(n), bins=linspace(-self.valueCutoff*3,self.valueCutoff*3,10))

    def getPseudoTokens(self,n):
        indices = where(self.getPseudoTermVector(n)>self.valueCutoff)[0]
        return [self.corpus.termDict[i] for i in indices]






def main(field,collection,solrUrl):
    corpus =getTermVectorCollection(field,collection,solrUrl)
    numTopics = 10
    pseudoTermVectors = PseudoTermVectors(corpus,numTopics)

if __name__ == "__main__":
    from sys import argv
    if len(argv)==0:
        raise Exception("usage: python tvCollect.py fieldname [collection [solrUrl]]")

    field = argv[1]
    collection = argv[2] if len(argv)>2 else "collection1"
    solrUrl = argv[3] if len(argv)>3 else "http://localhost:8983/solr"
    main(field,collection,solrUrl)
