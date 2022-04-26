package org.batteryparkdev.cosmicgraphdb.loader

import com.google.common.base.Stopwatch
import com.google.common.flogger.FluentLogger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.cosmicgraphdb.model.CosmicBreakpoint
import org.batteryparkdev.cosmicgraphdb.model.CosmicClassification
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

object CosmicClassificationLoader {
    private val logger: FluentLogger = FluentLogger.forEnclosingClass()

    /*
    Function to produce a stream of CosmicClassification model objects via
    a coroutine channel
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.parseCosmicClassificationFile(cosmicClassificationFile: String) =
        produce<CosmicClassification> {
            val cypher = "CALL apoc.load.csv(\"$cosmicClassificationFile\") " +
                    "YIELD lineNo, map RETURN map;"
            val records = Neo4jConnectionService.executeCypherQuery(cypher);
            records.map { record -> record.get("map") }
                .map{ CosmicClassification.parseValueMap(it)}
                .forEach {
                    send (it)
                    delay(20)
                }
        }

    /*
    Function to load CosmicClassification model data into the Neo4j database
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadCosmicClassifications(classifications: ReceiveChannel<CosmicClassification>) =
        produce<String> {
            for (classification in classifications) {
               send (loadCosmicClassification(classification))
                delay(20)
            }
        }

    private fun loadCosmicClassification(classification: CosmicClassification): String {
        println("Loading classification: ${classification.cosmicPhenotypeId}")
        return Neo4jConnectionService.executeCypherCommand(classification.generateCosmicClassificationCypher())
    }
    
    fun loadCosmicClassificationData(filename: String) = runBlocking {
        logger.atInfo().log("Loading CosmicClassification data from file $filename")
        var nodeCount = 0
        val stopwatch = Stopwatch.createStarted()
        val ids = loadCosmicClassifications(
                                parseCosmicClassificationFile(filename)
        )
        for (id in ids) {
            // pipeline stream is lazy - need to consume output
            nodeCount += 1
        }
        logger.atInfo().log(
            "CosmicClassification data loaded " +
                    " $nodeCount nodes in " +
                    " ${stopwatch.elapsed(java.util.concurrent.TimeUnit.SECONDS)} seconds"
        )
    }
}

fun main() {
    CosmicClassificationLoader.loadCosmicClassificationData("/Volumes/SSD870/COSMIC_rel95/sample/classification.csv")
    println("FINIS....")

}