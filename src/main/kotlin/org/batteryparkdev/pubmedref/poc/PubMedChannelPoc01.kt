package org.batteryparkdev.pubmedref.poc

import ai.wisecube.pubmed.PubmedArticle
import arrow.core.Either
import com.google.common.base.Stopwatch
import com.google.common.flogger.FluentLogger
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.batteryparkdev.pubmedref.model.PubMedEntry
import org.batteryparkdev.pubmedref.neo4j.Neo4jConnectionService
import org.batteryparkdev.pubmedref.neo4j.Neo4jUtils
import org.batteryparkdev.pubmedref.service.PubMedRetrievalService
import java.io.FileReader
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

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
private const val pubmedIdCol: String = "Pubmed_PMID"
private const val ncbiDelay = 100L

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
        .filter { createNovelPubMedArticleNode (it,"CosmicReference") }
        .forEach {
            send(it)
            delay(50)
        }
}

private fun createNovelPubMedArticleNode(pubmedId: String, label: String): Boolean  {
    if (Neo4jUtils.pubMedNodeExistsPredicate(pubmedId)) {
        return false
    } else {
        Neo4jConnectionService.executeCypherCommand(
            " MERGE (pma: PubMedArticle{pubmed_id: $pubmedId}) " +
                    " RETURN pma.pubmed_id"
        )
        if (label.isNotEmpty()) {
            Neo4jUtils.addLabel(pubmedId, label)
        }
    }
    return true
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
fun CoroutineScope.retrievePubMedArticle(pubmedIds: ReceiveChannel<String>) = produce {
    for (pubmedId in pubmedIds) {
        when (val retEither = PubMedRetrievalService.retrievePubMedArticle(pubmedId)) {
            is Either.Right -> {
                send(retEither.value)
                delay(200)
            }
            is Either.Left -> {
                PubMedRetrievalService.logger.atInfo().log(" ${retEither.value.message}")
            }
        }
    }
}

private val mergePubMedArticleTemplate = "MERGE (pma:PubMedArticle { pubmed_id: PMAID}) " +
        "SET  pma.pmc_id = \"PMCID\", pma.doi_id = \"DOIID\", " +
        " pma.journal_name = \"JOURNAL_NAME\", pma.journal_issue = \"JOURNAL_ISSUE\", " +
        " pma.article_title = \"TITLE\", pma.abstract = \"ABSTRACT\", " +
        " pma.author = \"AUTHOR\", pma.reference_count = REFCOUNT, " +
        " pma.cited_by_count = CITED_BY, " +
        " pma.keywords = \"KEYWORDS\" " +
        "  RETURN pma.pubmed_id"

private fun mergePubMedEntry(pubMedEntry: PubMedEntry): String {
    val merge = mergePubMedArticleTemplate.replace("PMAID", pubMedEntry.pubmedId)
        .replace("PMCID", pubMedEntry.pmcId)
        .replace("DOIID", pubMedEntry.doiId)
        .replace("JOURNAL_NAME", pubMedEntry.journalName)
        .replace("JOURNAL_ISSUE", pubMedEntry.journalIssue)
        .replace("TITLE", pubMedEntry.articleTitle)
        .replace("ABSTRACT", pubMedEntry.abstract)
        .replace("AUTHOR", pubMedEntry.authorCaption)
        .replace("REFCOUNT", pubMedEntry.referenceSet.size.toString())
        .replace("CITED_BY", pubMedEntry.citedByCount.toString())
        .replace("KEYWORDS", pubMedEntry.keywords)
    return Neo4jConnectionService.executeCypherCommand(merge)
}

/*
Function to map the JAXB PubmedArticle objects to PubMedEntry objects and persist them
 */
fun CoroutineScope.persistPubMedArticle(pubmedArticles: ReceiveChannel<PubmedArticle>) = produce{
    for (pubmedArticle in pubmedArticles) {
        val entry = PubMedEntry.parsePubMedArticle(pubmedArticle)
        val pubmedId = mergePubMedEntry(entry)
        delay(50)
        send(entry) // send out PubMed Entry for additional processing
    }
}

fun main() = runBlocking<Unit> {
    var count = 0
    val stopwatch = Stopwatch.createStarted()
//    for (article in retrievePubMedArticle(produceIdBatch("./data/sample_CosmicMutantExportCensus.tsv"))) {
//        println("PubMed ID: ${article.medlineCitation.pmid.getvalue()} ")
//        println("Title: ${article.medlineCitation.article.articleTitle.getvalue()}")
//
//    }
  //  val articles = retrievePubMedArticle(produceIdBatch("./data/sample_CosmicMutantExportCensus.tsv"))
    for (entry in persistPubMedArticle(retrievePubMedArticle(produceIdBatch("./data/sample_CosmicMutantExportCensus.tsv"))))
    {
        println("PubMedEntry  PubMed Id: ${entry.pubmedId}  ")
        count += 1
    }
    stopwatch.elapsed(TimeUnit.SECONDS)
    println("Article count = $count in ${stopwatch.elapsed(java.util.concurrent.TimeUnit.SECONDS)} seconds")
    delay(20_000)
    println("Cancelling children")
    coroutineContext.cancelChildren()

    println("FINIS....")
}


