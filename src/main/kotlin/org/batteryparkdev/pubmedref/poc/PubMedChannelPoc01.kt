package org.batteryparkdev.pubmedref.poc

import arrow.core.Either
import com.google.common.flogger.FluentLogger
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.batteryparkdev.pubmedref.service.PubMedRetrievalService
import org.batteryparkdev.pubmedref.service.TsvRecordSequenceSupplier
import java.io.FileReader
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Stream
import kotlin.streams.asStream

/*
Represents a proof-of-concept for using Kotlin channels to perform
time-consuming functions in an asynchronous manner.
This POC will read in a list of PubMed Ids from a sample TSV file using a
buffered Kotlin producer and process these ids using a consumer tha will
retrieve the PubMed entries from NCBI.
The results will be validated by logging a set of properties from the
retrieved entries
As a POC, this code will utilize more timing and logging statements than
seen in production code
 */

private const val bufferSize = 4
private val NCBI_EMAIL = System.getenv("NCBI_EMAIL")
private val NCBI_API_KEY = System.getenv("NCBI_API_KEY")
private const val ncbiBatchSize = 30
private const val pubmedIdCol:String = "Pubmed_PMID"
private const val ncbiDelay = 100L
//private val idChannel = Channel<String>(2)

/*
Function to emit batches of PubMed Ids extracted from a TSV file
as a comma separated string
For this POC the ids will not be filtered based on whether they
are in the Neo4j database
 */

private val logger: FluentLogger = FluentLogger.forEnclosingClass()

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.produceIdBatch(cosmicTsvFile: String) = produce<String> {
    val path = Paths.get(cosmicTsvFile)
    getPubMedIdStream(path)
        .map { it.get(pubmedIdCol) }
        .filter { it.isNotEmpty() }
        .forEach {
            delay(10)
            send(it)
        }
        }

fun getPubMedIdStream(aPath: Path): List<CSVRecord> {
    try {
        FileReader(aPath.toString()).use {
            val parser = CSVParser.parse(
                aPath.toFile(), Charset.defaultCharset(),
                CSVFormat.TDF.withFirstRecordAsHeader().withQuote(null).withIgnoreEmptyLines()
            )
            return parser.records
        }
    } catch (e: IOException) {
        logger.atSevere().log(e.message)
        e.printStackTrace()
    }
    return emptyList()
}

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.processId(pubmedIds: ReceiveChannel<String>) = produce {

    for (pubmedId in pubmedIds){
        //println("PubMed Id: $pubmedId")
            delay(30)
            send(pubmedId)
    }
}

/*
 when (val retEither =PubMedRetrievalService.retrievePubMedArticle("26050619")) {
        is Either.Right -> {
            val article = retEither.value
            println("Title: ${article.medlineCitation.article.articleTitle.getvalue()}")
            PubMedRetrievalService.retrieveCitationIds("26050619").stream()
                .forEach { cit -> println(cit) }
        }
        is Either.Left -> {
            PubMedRetrievalService.logger.atInfo().log(" ${retEither.value.message}")
        }
    }
 */

@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.retrievePubMedArticle(pubmedIds: ReceiveChannel<String>) = produce {
    for (pubmedId in pubmedIds){
        when (val retEither = PubMedRetrievalService.retrievePubMedArticle(pubmedId)) {
            is Either.Right -> {
               send (retEither.value)
                delay(10)
            }
            is Either.Left -> {
                PubMedRetrievalService.logger.atInfo().log(" ${retEither.value.message}")
            }
        }
    }

}

    fun main() = runBlocking<Unit> {
        // val ids = produceIdBatch("./data/sample_CosmicMutantExportCensus.tsv")
        // val id2 = processId(ids)
        var count = 1
        //for (id in id2) {
        for (article in retrievePubMedArticle(produceIdBatch("./data/sample_CosmicMutantExportCensus.tsv"))){
            println("Title: ${article.medlineCitation.article.articleTitle.getvalue()}")
            count += 1
        }
        //delay(30_000)
        println("Cancelling children")
        coroutineContext.cancelChildren()
        println("FINIS....")
    }



