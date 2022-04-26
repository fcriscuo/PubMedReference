package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicHGNC {
    fun parseCosmicHGNCFile(filename: String): Int {
        // limit the number of records processed
        val LIMIT = 1000L
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicHGNC.parseValueMap(it) }
            .forEach { hgnc->
                println("Loading HGNC gene symbol ${hgnc.hgncGeneSymbol}")
                Neo4jConnectionService.executeCypherCommand(hgnc.generateCosmicHGNCCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicHGNC().parseCosmicHGNCFile("/Volumes/SSD870/COSMIC_rel95/sample/CosmicHGNC.tsv")
    println("Breakpoint record count = $recordCount")
}