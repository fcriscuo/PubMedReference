package org.batteryparkdev.cosmicgraphdb.io


import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import org.neo4j.driver.Record
import java.io.File

object ApocFileReader {

    fun processDelimitedFile(filename: String):List<Record>{
        val file = File(filename)
        if (file.exists()){
            return when (file.extension) {
                "csv" -> processCsvFile(filename)
                "tsv" -> processTsvFile(filename)
                else ->  emptyList()
            }
        } else {
          println("File: $filename does not exist on the filesystem")
        }
        return emptyList()
    }

    private fun processCsvFile(filename: String): List<Record> {
        val cypher = "CALL apoc.load.csv(\"$filename\") " +
                "YIELD lineNo, map RETURN map;"
        return Neo4jConnectionService.executeCypherQuery(cypher);
    }

    private fun processTsvFile(filename: String): List<Record> {
        val cypher = "CALL apoc.load.csv(\"$filename\"," +
                "{sep:'TAB' }) " +
                "YIELD lineNo, map RETURN map;"
        return  Neo4jConnectionService.executeCypherQuery(cypher);

    }


}