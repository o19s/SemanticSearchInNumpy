from termDict import TermDictionary


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
    """ A single document's term vector"""
    @staticmethod
    def __zipTermComponents(definitionField):
        return {key: zipListToDict(value)
                for key, value
                in zipListToDict(definitionField).iteritems()}

    def __init__(self, tvFromSolr):
        """ Construct tv around a documents tv in the Solr response"""
        super(TermVector, self).__init__()
        zipped = zipListToDict(tvFromSolr)
        self.uniqueKey = zipped['uniqueKey']
        self.termVector = self.__zipTermComponents(zipped['definition'])


class TermVectorCollection(object):
    """ A collection of term vectors that represents part of a corpus"""
    def __init__(self, solrResp):
        """ Construct a collection of termVectors around the Solr resp"""
        super(TermVectorCollection, self).__init__()
        termVectors = solrResp['termVectors']
        self.termDict = TermDictionary()
        self.tvs = {}
        for tv in termVectors:
            if "uniqueKey" in tv and isinstance(tv, list):
                parsedTv = TermVector(tv)
                self.tvs[parsedTv.uniqueKey] = parsedTv
                self.termDict.addTerms(parsedTv.termVector.keys())

    def merge(self, tvc):
        """ Merge tvc into self """
        self.tvs = dict(tvc.tvs.items() + self.tvs.items())
        self.termDict.appendTd(tvc.termDict)

    def __str__(self):
        return "Term Vectors %i; Terms %i" % (len(self.tvs),
                                              self.termDict.numTerms())


if __name__ == "__main__":
    import doctest
    doctest.testmod()
