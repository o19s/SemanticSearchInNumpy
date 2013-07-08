from termDict import TermDictionary

import numpy
import scipy

def corpus2csc(corpus, num_terms=None, dtype=numpy.float64, num_docs=None, num_nnz=None, printprogress=0):
    """
    Convert corpus into a sparse matrix, in scipy.sparse.csc_matrix format,
    with documents as columns.

    If the number of terms, documents and non-zero elements is known, you can pass
    them here as parameters and a more memory efficient code path will be taken.
    """

    try:
        # if the input corpus has the `num_nnz`, `num_docs` and `num_terms` attributes
        # (as is the case with MmCorpus for example), we can use a more efficient code path
        if num_terms is None:
            num_terms = corpus.num_terms
        if num_docs is None:
            num_docs = corpus.num_docs
        if num_nnz is None:
            num_nnz = corpus.num_nnz
    except AttributeError, e:
        pass # not a MmCorpus...
    if printprogress:
        logger.info("creating sparse matrix from corpus")
    if num_terms is not None and num_docs is not None and num_nnz is not None:
        # faster and much more memory-friendly version of creating the sparse csc
        posnow, indptr = 0, [0]
        indices = numpy.empty((num_nnz,), dtype=numpy.int32) # HACK assume feature ids fit in 32bit integer
        data = numpy.empty((num_nnz,), dtype=dtype)
        for docno, doc in enumerate(corpus):
            if printprogress and docno % printprogress == 0:
                logger.info("PROGRESS: at document #%i/%i" % (docno, num_docs))
            posnext = posnow + len(doc)
            indices[posnow : posnext] = [feature_id for feature_id, _ in doc]
            data[posnow : posnext] = [feature_weight for _, feature_weight in doc]
            indptr.append(posnext)
            posnow = posnext
        assert posnow == num_nnz, "mismatch between supplied and computed number of non-zeros"
        result = scipy.sparse.csc_matrix((data, indices, indptr), shape=(num_terms, num_docs), dtype=dtype)
    else:
        # slower version; determine the sparse matrix parameters during iteration
        num_nnz, data, indices, indptr = 0, [], [], [0]
        for docno, doc in enumerate(corpus):
            if printprogress and docno % printprogress == 0:
                logger.info("PROGRESS: at document #%i" % (docno))
            indices.extend([feature_id for feature_id, _ in doc])
            data.extend([feature_weight for _, feature_weight in doc])
            num_nnz += len(doc)
            indptr.append(num_nnz)
        if num_terms is None:
            num_terms = max(indices) + 1 if indices else 0
        num_docs = len(indptr) - 1
        # now num_docs, num_terms and num_nnz contain the correct values
        data = numpy.asarray(data, dtype=dtype)
        indices = numpy.asarray(indices)
        result = scipy.sparse.csc_matrix((data, indices, indptr), shape=(num_terms, num_docs), dtype=dtype)
    return result






def zipListToDict(tvEntry):
    """
    Because Solr gives us stuff in Json lists in alternating
    key->value pairs, not in a nice dictionary
    >>> zipListToDict([0,'a',1,'b'])
    {0: 'a', 1: 'b'}
    """
    tupled = zip(tvEntry[0::2], tvEntry[1::2])
    return dict(tupled)


class TermVector(object):
    """ A single document's term vector, parsed
        from Solr"""
    @staticmethod
    def __zipTermComponents(definitionField):
        return {key: zipListToDict(value)
                for key, value
                in zipListToDict(definitionField).iteritems()}

    def __init__(self):
        """ Construct tv around a documents tv in the Solr response"""
        super(TermVector, self).__init__()
        self.uniqueKey = None
        self.termVector = {}

    def getFeature(self, feature='tf'):
        return {key: value[feature]
                for key, value in self.termVector.iteritems()}

    def toFeaturePairs(self, termDict):
        return {termDict.termToCol[key]: value for
                key, value in self.termVector.iteritems()}

    def __str__(self):
        return str(self.uniqueKey) + "||" + str(self.termVector)

    @staticmethod
    def fromSolr(tvFromSolr, fieldName):
        tv = TermVector()
        zipped = zipListToDict(tvFromSolr)
        tv.uniqueKey = zipped['uniqueKey']
        tv.termVector = tv.__zipTermComponents(zipped[fieldName])
        return tv

    @staticmethod
    def fromFeaturePairs(termDict, uniqueKey, featurePairs, featureName):
        tv = TermVector()
        tv.uniqueKey = uniqueKey
        tv.termVector = {termDict.colToTerm[col]: {featureName: feature}
                         for col, feature in featurePairs}
        return tv


class TermVectorCollection(object):
    """ A collection of term vectors that represents part of a corpus"""
    def __init__(self, solrResp, fieldName):
        """ Construct a collection of termVectors around the Solr resp"""
        super(TermVectorCollection, self).__init__()
        termVectors = solrResp['termVectors']
        self.termDict = TermDictionary()
        self.tvs = {}
        self.feature = 'tf'  # What feature should we emit for each term
        for tv in termVectors:
            if "uniqueKey" in tv and isinstance(tv, list):
                parsedTv = TermVector.fromSolr(tv, fieldName)
                self.tvs[parsedTv.uniqueKey] = parsedTv
                self.termDict.addTerms(parsedTv.termVector.keys())

    def merge(self, tvc):
        """ Merge tvc into self """
        self.tvs = dict(tvc.tvs.items() + self.tvs.items())
        self.termDict.appendTd(tvc.termDict)

    def setFeature(self, feature):
        validSolrFeatures = ('tf-idf', 'tf', 'df')
        if feature in validSolrFeatures:
            self.feature = feature
        else:
            raise ValueError("Solr only exports tf, tf-idf, or df; "
                             "you requested %s" %
                             feature)

    def __str__(self):
        return "Term Vectors %i; Terms %i" % (len(self.tvs),
                                              self.termDict.numTerms())

    def __iter__(self):
        """ Generate feature pairs"""
        for key, tv in self.tvs.iteritems():
            yield {col: value[self.feature] for col, value
                   in tv.toFeaturePairs(self.termDict).iteritems()}.items()

    def toCsc(self):
        """ Get all in csc form"""
        return corpus2csc(self)

    def keyIter(self):
        """ Return an iterator that iterates the names of
            documents in parallel with the feature pairs"""
        return iter(self.tvs.keys())


if __name__ == "__main__":
    import doctest
    doctest.testmod()
