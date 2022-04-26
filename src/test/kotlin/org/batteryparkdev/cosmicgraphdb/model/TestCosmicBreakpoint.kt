package org.batteryparkdev.cosmicgraphdb.model


import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.cosmicgraphdb.model.CosmicBreakpoint
import org.batteryparkdev.neo4j.service.Neo4jConnectionService


class TestCosmicBreakpoint {
    private val LIMIT = 200L
    /*
    Test parsing sample Cosmic breakpoints TSV file and mapping data to
    CosmicBreakpoint model class
    n.b. file name specification must be full path since it is resolved by Neo4j server
     */
    fun parseBreakpointFile(filename: String): Int {
        // limit the number of records processed

        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename)
            .stream().limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicBreakpoint.parseValueMap(it) }
            .forEach { breakpoint ->
                println("Loading breakpoint  ${breakpoint.mutationId}")
                Neo4jConnectionService.executeCypherCommand(breakpoint.generateBreakpointCypher())
                recordCount += 1
            }
        return recordCount
    }
}
fun main() {
val recordCount =
    TestCosmicBreakpoint().parseBreakpointFile("/Volumes/SSD870/COSMIC_rel95/sample/CosmicBreakpointsExport.tsv")
    println("Breakpoint record count = $recordCount")
}