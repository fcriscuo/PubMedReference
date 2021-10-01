package org.batteryparkdev.pubmedref.neo4j

import com.google.common.flogger.FluentLogger

/*
A collection of Neo4j database constraint definitions in Cypher
These constraints should be defined prior to loading the database with
any initial data.
 */
val constraints = listOf<String>(
    "CREATE CONSTRAINT unique_pubmed_id IF NOT EXISTS ON (p:PubMedArticle) ASSERT p.pubmed_id IS UNIQUE"
)

val logger: FluentLogger = FluentLogger.forEnclosingClass();

fun defineConstraints() {
    constraints.forEach {
        Neo4jConnectionService.defineDatabaseConstraint(it)
        logger.atInfo().log("Constraint: $it  has been defined")
    }
}

// stand-alone invocation
fun main(){
    defineConstraints()
}