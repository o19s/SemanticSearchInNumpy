Simple example
==============

* Pull down [this git repo](https://github.com/o19s/StackExchangeSolrIndexing)
* Modify the schema so that Body, Title, and Tags have termVectors="true" termPositions="true" termOffsets="true"
* Run the readme with the example data.
* Run `python tvCollector.py Body`
