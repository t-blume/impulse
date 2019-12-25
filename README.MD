# IMPULSE
## Integrate Public Metadata Underneath professional Library SErvices

### Usage
```
usage:   [-c <int>] [-ds <arg>] -f <file> [-fb] [-i] [-if <inputFilter>] -m
       <mapping> -o <folder> [-pld]
Required options: f, m, [-o output folder]
 -c,--cachesize <int>              instances stored in main memory
 -ds,--datasources <arg>           location of datasource URIs (query LODatio if
                                   not provided)
 -f,--files <file>                 read from file(s)
 -fb,--fixBlankNodes               try to fix Blank Nodes
 -i,--inference                    activate inferencing
 -if,--inputFilter <inputFilter>   regex pattern to filter filenames
 -m,--mapping <mapping>            location of mapping file
 -o,--output <folder>              output folder
 -pld,--usePLD                     harvest the complete pay-level domain (also
                                   exports simple harvesting)

```

#### Quickstart with sample-data
```
$ java -jar impulse.jar -f testresources/sample-rdf-data.nt.gz -m testresources/sample-mapping.json -o testresources -ds testresources/sample-datasourceURIs.csv -fb -c 5000

```
#### How to setup the data search experiment.

  1. Download the BTC 2014 dataset from http://km.aifb.kit.edu/projects/btc-2014/
  3. Query the schema-level index available at http://lodatio.informatik.uni-kiel.de/. Alternatively, use the data source URIs provided.
  4. Generate the dataset containing bibliographic metadata only for each query (mapping):
  `java -jar IMPULSE.jar -f path/to/BTC14/ -m mappings/base_mappings/dcterms-mapping.json -ds contexts/base/dcterms/ -o base/dcterms/ -pld -fb -c 10000`
  5. Upload the generated `.json` file to Elasticsearch:
  `java -jar IMPULSE-Experiment.jar -i btc14-base-dcterms base/dcterms/data.json -o experiment1`
  6. Run the deduplication repeatedly until no duplicates can be found:
  `java -jar IMPULSE-Experiment.jar -d btc14-base-dcterms MOVING -o experiment1`
  7. Run the dataset analysis to get statistics about the remaining documents:
  `java -jar IMPULSE-Experiment.jar -a btc14-base-dcterms MOVING -o experiment1`
  8. Repeat steps 4 to 7 for all mappings. Repeat step 5 to 7 for internal dataset, e.g., our ZBW Econonmics dataset.
  9. Link records from two different indices to get the results for the document enrichment experiment:
  `java -jar IMPULSE-Experiment.jar -l btc14-base-dcterms MOVING zbw-economics ZBW -o experiment1`


It is recommended to increase the JVM heap space with, e.g., `-Xmx500G` to use all available resources. To exploit all heap space, adjust the cache size accordingly. Rule of thumb: ~3000 instances per gigabyte.  

#### Technical details of implementation
* Main
   * The framework is implemented as a command line interface.  
* Input
   * In the "input" package, the data set is transposed to the internal data format. Furthermore, the RDF instances are created and added to the instance cache. If the cache limit of instances is reached, additional instances are stored in an disk-cache. instances in memory are stored ind an LRU cache. Instances are processed in a FiFo queue.     
* Processing
   * All data is subject to preprocessing. Invalid URIs, blank nodes, and literals are removed. If the option `-fb` is used, a set of rules is applied to fix them. If they are unsucessful, they are removed anyways.
   * Once preprocessing is complete, the data sources are passed from the harvester to the MOVINGParser. In the MOVINGParser, the RDF instances are mapped to internal `DataItems` using the provided mapping file. For different data formats, new parser and corresponding mapping files need to be defined.
* Output
   * After the parsing is complete, the data items are exported. The data is exported as a JSON object. They can be exported to disk or directly to a previously specified Elasticsearch index. When exported to disk, a text file where each line contains one valid JSON object is created.



#### Mapping File
This mapping file defines which information is used for which attribute.
The mapping file also includes the queries that are send to LODatio.
We defined SPARQL queries to find data sources containing bibliographic metadata. In our experiments we used queries, that are based on three established vocabularies, Bibliographic Ontology [BIBO](http://bibliontology.com/), Semantic Web for Research Communities [SWRC](http://ontoware.org/swrc) and DCMI Metadata Terms [DCTerms](http://dublincore.org/documents/dcmi-terms/).


#### Experiment datasets
The results from our expriments can be found in zendodo
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.2553811.svg)](https://doi.org/10.5281/zenodo.2553811)



#### Compile sources
1. Add MOVINGParser to local maven repository:
 ``mvn install:install-file -Dfile=libs/MOVINGParser-1.5.jar -DgroupId=kd.informatik -DartifactId=moving-parser -Dversion=1.5 -Dpackaging=jar -DgeneratePom=true``

2. Compile sources: ``mvn package``

3. Run jar, e.g., ``java -Xmx800G -jar target/IMPULSE-1.0-SNAPSHOT.jar -f testresources/sample-rdf-data.nt.gz -o experiment -c 12500000 -fe ../../ -ddc`` 

#### Acknowledgments
This research was co-financed by the EU H2020 project [MOVING](http://www.moving-project.eu/) under contract no 693092.
