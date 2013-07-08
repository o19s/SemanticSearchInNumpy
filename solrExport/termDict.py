class TermDictionary(dict):
    """ Assigns an integer id to a term, this id
        is going to correspond to a feature in a 
        column in a term/doc matrix
    """
    def __init__(self, *args):
        """ Init an empty term dictionary"""
        super(TermDictionary, self).__init__(args)
        # terms -> column id
        # columnId -> term
        self.termToCol = {}
        self.colToTerm = {}
        self.counter = 0

    def __getitem__(self, key):
        return self.colToTerm[key]

    def __len__(self):
        return self.numTerms()

    def keys(self):
        return self.colToTerm.keys()

    def addTerms(self, terms):
        """ Add terms to the dictionary"""
        for term in terms:
            if term not in self.termToCol:
                self.termToCol[term] = self.counter
                self.colToTerm[self.counter] = term
                self.counter += 1

    def appendTd(self, td):
        """ Add terms from another term dictionary to this term dict,
            the other term dictionaries terms are appended and given ids
            relative to this term dict, losing their original identities as
            they may overlap here"""
        self.addTerms(td.termToCol.keys())

    def numTerms(self):
        assert len(self.termToCol) == len(self.colToTerm)
        return len(self.termToCol)

    def __str__(self):
        assert len(self.termToCol) == len(self.colToTerm)
        return str(len(self.termToCol)) + ": " + repr(self.termToCol)

if __name__ == "__main__":
    td = TermDictionary()
    td.addTerms(["mary", "had", "a", "little"])
    td2 = TermDictionary()
    td2.addTerms(["a", "little", "lamb", "its"])

    td.appendTd(td2)
    print td.termToCol
    print td.colToTerm
    assert len(td.termToCol) == 6
    assert len(td.colToTerm) == 6
