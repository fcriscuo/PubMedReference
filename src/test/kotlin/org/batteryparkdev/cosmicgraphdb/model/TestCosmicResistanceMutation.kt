package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

class TestCosmicResistanceMutation {
    fun parseCosmicResistanceMutationFile(filename: String): Int {
        val LIMIT = Long.MAX_VALUE
        // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicResistanceMutation.parseValueMap(it) }
            .filter{it.censusGene == "YES"}
            .forEach { resistance ->
                println("Loading resistance mutation ${resistance.resistanceId}")
                Neo4jConnectionService.executeCypherCommand(resistance.generateCosmicResistanceCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
    val recordCount =
        TestCosmicResistanceMutation().
        parseCosmicResistanceMutationFile("/Volumes/SSD870/COSMIC_rel95/sample/CosmicResistanceMutations.tsv")
    println("CosmicGene record count = $recordCount")
}