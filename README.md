## PubMedReference

PubMedReference is a Kotlin backend application that creates a Neo4j 4.3 database of 
a specified PubMed article and that article's references and citations. Input consists
of the PubMed Id for the origin article. The application uses the NCBI eUtils API to 
fetch data, in XML format, for that origin article. The returned XML data is then
parsed and
loaded as a Neo4j node with PubMedArticle and Origin labels. The XML data for the origin 
article also contains a list of PubMed articles that were used as references. XML files
for each of these reference articles is fetched from NCBI and mapped to Neo4j nodes with 
PubMedArticle and Reference labels. A Neo4j HAS_REFERENCE relationship is 
created between the Origin and  each Reference node.
In addition, a query is sent to NCBI requesting the PubMed Ids for articles that cite
the origin node. The returned list is then used to fetch data for all the citation articles. 
For
each of these citations, a Neo4j node is created with PubMedArticle and Citation labels.
A Neo4j CITED_BY relationship is created between the Origin node and each Citation node.

This application utilizes the pubmed-parser library available from the thecloudcircle
account on GitHub (https://github.com/thecloudcircle/pubmed-parser) to map XML data
received from PubMed to Java JAXB objects.

NCBI enforces a limit of three (3) API requests per second 
(10 with a registered API key). The application pauses for 300 milliseconds after 
each request.

The application requires the user to define two (2) system environment properties, 
NEO4J_ACCOUNT and NEO4J_PASSWORD. This allows for easier code sharing without 
exposing Neo4j credentials. The application logs all Neo4j CYPHER commands to a log
file in the /tmp/logs/neo4j directory. The filenames for these log files contain 
a timestamp component, so they are not overwritten by subsequent executions.

### Future Development Plans

An immediate goal is to retain PubMedArticle nodes across multiple invocations in order
to eliminate redundant query. All secondary labels (i.e. Origin, Reference, & Citation)
will be removed as well as will all inter-PubMedArticle relationships. Before a PubMed article
request is sent to NCBI, the existing pool of PubMedArticle nodes in the database
will be searched for 
an Id match. If a match is found, the existing node will simply be wired in with the
appropriate label and relationship.
