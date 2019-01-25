# IMPULSE
## Integrate Public Metadata Underneath professional Library SErvices

### Quickstart

IMPULSE can be used through a command line application.
```
$ java -jar impulse.jar
```

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
  
 #### Mapping File
This mapping file defines which information is used for which attribute. In addition, optional attributes can be defined by the user, e.g. http://purl.org/dc/terms/language. 
The mapping file also provides the queries. In addition to that you can make type restiction in the mapping file. So only documents which fits to the type will be processed. Furthermore, you can define mandatory properties. 
  


#### Queries
First of all you need to define SPARQL queries to find bibliographic metadata. In our experiments we used queries, that are based on three established vocabularies, Bibliographic Ontology (BIBO)(http://bibliontology.com/), Semantic Web for Research Communities (SWRC) (http://ontoware.org/swrc) and DCMI Metadata Terms (DCTerms)(http://dublincore.org/documents/dcmi-terms/). 
As a Second step the framework harvests the set of datasources that are returned from each query.

#### Experiment datasets
The results from our expriments can be found here: [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.2548392.svg)](https://doi.org/10.5281/zenodo.2548392)

### About

