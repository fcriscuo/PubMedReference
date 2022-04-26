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
import org.batteryparkdev.cosmicgraphdb.model.CosmicBreakpoint
import org.batteryparkdev.neo4j.service.Neo4jConnectionService

object CosmicBreakpointLoader {
    private val logger: FluentLogger = FluentLogger.forEnclosingClass()
    /*
       Function to produce a stream of CosmicBreakpoint model objects via
       a coroutine channel
        */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.parseCosmicBreakpointFile(cosmicBreakpointFile: String) =
        produce<CosmicBreakpoint> {
            ApocFileReader.processDelimitedFile(cosmicBreakpointFile)
                .map { record -> record.get("map") }
                .map{ CosmicBreakpoint.parseValueMap(it)}
                .forEach {
                    send (it)
                    delay(20)
                }
        }

    /* Function to load CosmicBreakpoint data into Neo4j database
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.loadCosmicBreakpoints(breakpoints: ReceiveChannel<CosmicBreakpoint>)=
        produce<String> {
            for (breakpoint in breakpoints){
                send(executeLoadCypher(breakpoint))
                delay(20)
            }
        }
    private fun executeLoadCypher(breakpoint: CosmicBreakpoint): String =
        Neo4jConnectionService.executeCypherCommand(breakpoint.generateBreakpointCypher())


    fun loadCosmicBreakpointData(filename: String) = runBlocking {
        logger.atInfo().log("Loading CosmicBreakpointData from file: $filename")
        var nodeCount = 0
        val stopwatch = Stopwatch.createStarted()
        val ids = loadCosmicBreakpoints(
            parseCosmicBreakpointFile(filename)
        )
        for (id in ids) {
            // pipeline stream is lazy - need to consume output
            nodeCount += 1
        }
        logger.atInfo().log(
            "CosmicBreakpoint data loaded " +
                    " $nodeCount nodes in " +
                    " ${stopwatch.elapsed(java.util.concurrent.TimeUnit.SECONDS)} seconds"
        )
    }
}