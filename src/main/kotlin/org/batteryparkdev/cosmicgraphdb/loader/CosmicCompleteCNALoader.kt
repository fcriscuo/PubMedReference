package org.batteryparkdev.cosmicgraphdb.loader

import com.google.common.base.Stopwatch
import com.google.common.flogger.FluentLogger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader
import org.batteryparkdev.cosmicgraphdb.model.CosmicCompleteCNA
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

object CosmicCompleteCNALoader {
    private val logger: FluentLogger = FluentLogger.forEnclosingClass()

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.parseCosmicCompleteCNAFile(cosmicCompleteCNAFile: String) =
        produce<CosmicCompleteCNA> {
            ApocFileReader.processDelimitedFile(cosmicCompleteCNAFile)
                .map { record -> record.get("map") }
                .map { CosmicCompleteCNA.parseValueMap(it) }
                .forEach {
                    send(it)
                    delay(20L)
                }
        }

    /*
    Private function to create a coroutine channel that loads
    CosmicCompleteCNA data into the Neo4j database
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadCosmicCompleteCNA(cnas: ReceiveChannel<CosmicCompleteCNA>) =
        produce {
            for (cna in cnas) {
                loadCosmicCompleteCNA(cna)
                send(cna)
                delay(50)
            }
        }

    private fun loadCosmicCompleteCNA(cna: CosmicCompleteCNA): String {
        return Neo4jConnectionService.executeCypherCommand(cna.generateCompleteCNACypher())
    }

/*
Public function to complete parsing of CosmicCompleteCNA file and
loaded data into the Neo4j database
 */
    fun loadCosmicCompleteCNAData(filename: String) = runBlocking {
        logger.atInfo().log("Loading CosmicCompleteCNA data from file: $filename")
        var nodeCount = 0
        val stopwatch = Stopwatch.createStarted()
        val ids =
                loadCosmicCompleteCNA(
                    parseCosmicCompleteCNAFile(filename)
                )
        for (id in ids) {
            // pipeline stream is lazy - need to consume output
            nodeCount += 1
        }
        logger.atInfo().log(
            "CosmicCompleteCNA data loaded " +
                    " $nodeCount nodes in " +
                    " ${stopwatch.elapsed(java.util.concurrent.TimeUnit.SECONDS)} seconds"
        )
    }
}

