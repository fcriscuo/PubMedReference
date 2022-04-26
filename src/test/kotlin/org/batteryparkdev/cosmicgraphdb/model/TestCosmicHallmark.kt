package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicHallmark {

    private val LIMIT = 200L

    fun parseCosmicGeneCensusFile(filename: String): Int {
        // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicHallmark.parseValueMap(it) }
            .forEach { hall ->
                //println(hall.generateCosmicHallmarkCypher())
                Neo4jConnectionService.executeCypherCommand(hall.generateCosmicHallmarkCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicHallmark()
            .parseCosmicGeneCensusFile("/Volumes/SSD870/COSMIC_rel95/sample/Cancer_Gene_Census_Hallmarks_Of_Cancer.tsv")
}
