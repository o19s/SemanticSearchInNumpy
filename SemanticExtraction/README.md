#Simple example


##Index documents into Solr

In the StackExchangeSolrIndexing folder:

* Download a StackExchange dump of your choosing:
   http://www.clearbits.net/torrents/2076-aug-2012 (or the start
immediatly with the posts.xml.gz file)
* Unzip the data set you're interested in (7-zip format) Or `gunzip
   posts.xml.gz`
* Download Solr: http://lucene.apache.org/solr/
* Start Solr:
```
cd apache-solr-x.x.x/example
java -jar -Dsolr.solr.home=<full_path_to_this_dir>/solr_home start.jar
```
* Index documents (currently only works for posts):
```
python extractDocs.py "<full_path_to_stack_exchange_dump>/posts.xml"
```
(Configuration details can be found in solr_home/collection1/conf/schema.xml)
* Commit the changes
```
http://localhost:8983/solr/update?commit=true
```
* Search!
```
localhost:8983/solr/collection1/select?q=Tags:star-wars
```

##Auto-Generate Synonyms

In the SemanticExtraction folder

* Run `python SemanticAnalyzer.py Body`
* Do some searches! http://localhost:8983/solr/select?q=Tags:harry-potter&fq=Body:*&fl=Body%20BodyBlurred%20Tags%20Id&facet=true&facet.field=Tags
