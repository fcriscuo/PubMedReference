package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicGeneCensus {
    private val LIMIT = 1000L
    /*
    n.b. file name specification must be full path since it is resolved by Neo4j server
     */
    fun parseCosmicGeneCensusFile(filename: String): Int {
        // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicGeneCensus.parseValueMap(it) }
            .forEach { gene->
                println("Loading gene ${gene.geneSymbol}")
                Neo4jConnectionService.executeCypherCommand(gene.generateCosmicGeneCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicGeneCensus().
            parseCosmicGeneCensusFile("/Volumes/SSD870/COSMIC_rel95/sample/cancer_gene_census.csv")
    println("CosmicGene record count = $recordCount")
}