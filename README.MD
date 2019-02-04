# IMPULSE
## Integrate Public Metadata Underneath professional Library SErvices

### Quickstart

IMPULSE is a command line application.

```
usage: [-f <filename[.gz]>] [-m <mappingfile[.json]>] [-mi] [-dCM] [-ds <contextURIs[.csv]>] [-pld] 
[-o <outputDirectory>] [-e <ElasticsearchIndex>]
  
  [-f <filename[.gz]>]            Location and name of the inputfile, local dataset in N-triple format
  [-m <mappingfile[.json]>]       Location and name of the mappingfile 
  [-mi]                           Use mapping inferencing
  [-dCM]                          Download the Cachemisses in runtime
  [-ds <contextURIs[.csv]>]       Location and name of the contextURI-list, if not set the queries from the mappingfile a send to Lodatio
  [-pld]                          Use PLDFilter, if set the Framework is going to extract the contexts too.
  [-o <outputDirectory>]          Name of NQuad file with output
  [-e <ElasticsearchIndex>]       Name of Elasticsearch index to export output
```

#### Quickstart with sample-data 

```
$ java -jar impulse.jar -f testresources/sample-rdf-data.nt.gz -m testresources/sample-mapping.json -o testresources -ds testresources/sample-datasourceURIs.csv

```
#### How to reprocude the experiments?

  1. Download the BTC 2014 dataset from http://km.aifb.kit.edu/projects/btc-2014/
  2. Use the mapping files in the mappingfiles directory
  3. The seedlist will be downloaded by querying http://lodatio.informatik.uni-kiel.de/. This step can be skipped, when the corresponding seedlist from the Zenodo repository is provided via the -ds command 
  
 #### Technical details of implementation
* Main
   * The framework is implemented as a command line interface.  
* Input
   * In the "input" package, the data set is transferred in the internal data format.  Furthermore, the RDF instances for the FiFo    instance cache are created.    
* Processing
   * All data sources are first added to a pipeline. These are then filtered one after the other by the corresponding filter. With the context filter, all data sources specified by the seed list are filtered out. With the PLD filter, all data sources belonging to a pay-level domain of the seed list are filtered accordingly. 
   * Once prepossecing is complete, the data sources are passed from the harvester to the MOVINGParser. In the MOVINGParser, the information is assigned to the correct attributes according to the mapping file. For different data formats, a new parser and a corresponding mapping files needs to be created.
* Output
   * After the parsing is complete, the data items are exported. The data is exported as a JSON object. They can be exported to disk or directly to a previously specified Elasticsearch index.


  
 #### Mapping File
This mapping file defines which information is used for which attribute. In addition, optional attributes can be defined by the user, e.g. http://purl.org/dc/terms/language. 
The mapping file also provides the queries. In addition to that you can make type restiction in the mapping file. So only documents which fits to the type will be processed. Furthermore, you can define mandatory properties.
  


#### Queries
First of all you need to define SPARQL queries to find bibliographic metadata. In our experiments we used queries, that are based on three established vocabularies, Bibliographic Ontology [BIBO](http://bibliontology.com/), Semantic Web for Research Communities [SWRC](http://ontoware.org/swrc) and DCMI Metadata Terms [DCTerms](http://dublincore.org/documents/dcmi-terms/). 
As a Second step the framework harvests the set of datasources that are returned from each query.

#### Experiment datasets
The results from our expriments can be found in zendodo 
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.2553811.svg)](https://doi.org/10.5281/zenodo.2553811)


### About




#### Acknowledgments
This research was co-financed by the EU H2020 project [MOVING](http://www.moving-project.eu/) under contract no 693092.

