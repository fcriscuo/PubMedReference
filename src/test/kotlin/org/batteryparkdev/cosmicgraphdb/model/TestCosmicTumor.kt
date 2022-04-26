package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicTumor {

    fun parseCosmicTumorFile(filename: String): Int {
        val LIMIT = 1000L
        // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicTumor.parseValueMap(it) }
            .forEach {tumor->
                println("Loading tumor ${tumor.sampleId}")
                val cypher = tumor.generateCosmicTumorCypher().plus(
                    " RETURN ${CosmicTumor.nodename}"
                )
                Neo4jConnectionService.executeCypherCommand(cypher)
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicTumor().
        parseCosmicTumorFile("/Volumes/SSD870/COSMIC_rel95/sample/CosmicMutantExportCensus.tsv")
    println("CosmicSample record count = $recordCount")
}