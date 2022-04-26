package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicClassification {
    fun parseCosmicClassificationFile(filename: String): Int {
        val LIMIT = Long.MAX_VALUE
        // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicClassification.parseValueMap(it) }
            .forEach {classification->
                println("Loading Classification ${classification.cosmicPhenotypeId}")
                Neo4jConnectionService.executeCypherCommand(classification.generateCosmicClassificationCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicClassification().
        parseCosmicClassificationFile("/Volumes/SSD870/COSMIC_rel95/sample/classification.csv")
    println("CosmicClassification record count = $recordCount")
}