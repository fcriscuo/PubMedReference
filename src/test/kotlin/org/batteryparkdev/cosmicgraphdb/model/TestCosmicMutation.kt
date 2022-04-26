package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicMutation {
    fun parseCosmicMutationFile(filename: String): Int {
        val LIMIT = 1000L
        // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicMutation.parseValueMap(it) }
            .forEach { mutation->
                println("Loading mutation ${mutation.mutationId}")
                Neo4jConnectionService.executeCypherCommand(mutation.generateCosmicMutationCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicMutation().
        parseCosmicMutationFile("/Volumes/SSD870/COSMIC_rel95/sample/CosmicMutantExportCensus.tsv")
    println("CosmicGene record count = $recordCount")
}