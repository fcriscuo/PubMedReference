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
import org.batteryparkdev.cosmicgraphdb.model.CosmicDiffMethylation
import org.batteryparkdev.neo4j.service.Neo4jConnectionService
import java.util.concurrent.TimeUnit

object CosmicDiffMethylationLoader {
    private val logger: FluentLogger = FluentLogger.forEnclosingClass()

    /*
    Coroutine function to produce a channel of CosmicDiffMethylation model objects
    Input is the full file name
    Output is channel of model objects
    */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.parseCosmicDiffMethylationChannel(cosmicDiffMethylFile: String) =
        produce<CosmicDiffMethylation> {
            ApocFileReader.processDelimitedFile(cosmicDiffMethylFile)
                .map { record -> record.get("map") }
                .map { CosmicDiffMethylation.parseValueMap(it) }
                .forEach {
                    send(it)
                    delay(20L)
                }
        }

    /*
   Coroutine function to load CosmicDiffMethylation data into the
   connected Neo4j database
    */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadCosmicDiffMethylationChannel(methyls: ReceiveChannel<CosmicDiffMethylation>) =
        produce {
            for (methyl in methyls) {
                loadCosmicDiffMethylation(methyl)
                send(methyl)
                delay(50)
            }
        }
    private fun loadCosmicDiffMethylation(methyl: CosmicDiffMethylation): String {
        return Neo4jConnectionService.executeCypherCommand(methyl.generateDiffMethylationCypher())
    }

    /*
    Public function to load Cosmic Differential Methylation from a specified file
     */
    fun loadCosmicDiffMethylationData(filename: String) = runBlocking {
        logger.atInfo().log("Loading CosmicDiffMethylation data from file: $filename")
        var nodeCount = 0
        val stopwatch = Stopwatch.createStarted()
        val ids =
            loadCosmicDiffMethylationChannel(
                parseCosmicDiffMethylationChannel(filename)
            )

        for (id in ids) {
            // pipeline stream is lazy - need to consume output
            nodeCount += 1
        }
        logger.atInfo().log(
            "CosmicDifferentialMethylation data loaded " +
                    " $nodeCount nodes in " +
                    " ${stopwatch.elapsed(TimeUnit.SECONDS)} seconds"
        )
    }
}
