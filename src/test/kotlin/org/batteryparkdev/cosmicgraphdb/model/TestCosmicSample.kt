package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicSample {
    fun parseCosmicSampleFile(filename: String): Int {
        val LIMIT = 1000L
        // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicSample.parseValueMap(it) }
            .forEach {sample->
                println("Loading sample ${sample.sampleId}")
                Neo4jConnectionService.executeCypherCommand(sample.generateCosmicSampleCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicSample().
        parseCosmicSampleFile("/Volumes/SSD870/COSMIC_rel95/sample/CosmicSample.tsv")
    println("CosmicSample record count = $recordCount")
}