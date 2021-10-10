## PubMedReference

PubMedReference is a Kotlin backend application that creates a Neo4j 4.3 database of 
a specified PubMed article and that article's references and citations. Input consists
of one (1) or more PubMed Ids which are classified as Origin nodes. The application uses the NCBI eUtils API to 
fetch data, in XML format, for those origin articles. The returned XML data are then
parsed and
loaded as a Neo4j node with PubMedArticle and Origin labels. The XML data for the origin 
articles also contain a list of PubMed articles that were used as references. XML files
for each of these reference articles is fetched from NCBI and mapped to Neo4j nodes with 
PubMedArticle and Reference labels. A Neo4j HAS_REFERENCE relationship is 
created between the appropriate Origin and  each Reference nodes.
In addition, a query is sent to NCBI requesting the PubMed Ids for articles that cite
the origin node. The returned list is then used to fetch data for all the citation articles. 
For
each of these citations, a Neo4j node is created with PubMedArticle and Citation labels.
A Neo4j CITED_BY relationship is created between the Origin node and each Citation node.

This application utilizes the pubmed-parser library available from the thecloudcircle
account on GitHub (https://github.com/thecloudcircle/pubmed-parser) to map XML data
received from PubMed to Java JAXB objects. This library requires a JRE of 15 or lower.

NCBI actively enforces a limit of three (3) API requests per second 
(10 with a registered API key). To accommodate this restriction,
the application pauses for 300 milliseconds after 
each request.

The application requires the user to define two (2) system environment properties, 
NEO4J_ACCOUNT and NEO4J_PASSWORD. This allows for easier code sharing without 
exposing Neo4j credentials. The application logs all Neo4j CYPHER commands to a log
file in the /tmp/logs/neo4j directory. The filenames for these log files contain 
a timestamp component, so they are not overwritten by subsequent executions.

In an effort to reduce delays in accessing NCBI data, PubMedReference nodes are retained
across application invocations. All node relationships are deleted as are all
secondary labels at the start of the application's execution. Any existing
PubMedReference node whose id is specified in a subsequent invocation of the
application is replaced with data fetched from NCBI
But before data requests for reference and citation PubMed Ids are submitted to NCBI, 
the database
is queried to determine if those Ids have existing PubMedReference nodes. If so, the
NCBI data request is skipped and an appropriate relationship to the Origin node is
created and a secondary label is added.

### Support for COSMIC PubMed Ids

The application has been modified to support mining PubMed data from NCBI using
PubMed Ids listing in COSMIC tsv files. The Kotlin class 
org.batteryparkdev.pubmedref.app.CosmicPubMedLoaderApp can be invokes with the full path
name of the COSMIC file. The column name for PubMed Ids should be Pubmed_PMID. Given NCBI's
request rate limit, processing a large COSMIC file will require several hours. 

The CosmicPubMedLoaderApp has been extended to support sending batch requests for multiple PubMed
articles. This significantly reduces the time need to load Origin PubMed articles.