package org.batteryparkdev.pubmedref.app

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
This app will read in a list of PubMed Ids from a sample TSV file using a
buffered Kotlin producer and process these ids using a consumer tha will
retrieve the PubMed entries from NCBI.
This app is packaged separately from the main COSMIC codebase in order to
accommodate the requirement that the JRE for ai.wisecube JAXB processor
be level 15 or lower
 */

private val NCBI_EMAIL = System.getenv("NCBI_EMAIL")
private val NCBI_API_KEY = System.getenv("NCBI_API_KEY")
private const val pubmedIdCol: String = "Pubmed_PMID"

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
        .filter {createNovelPubMedArticleNode(it, "CosmicReference") }
        .forEach {
            send(it)
            delay(50)
        }
}

 fun createNovelPubMedArticleNode(pubmedId: String, label: String): Boolean {
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
@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.persistPubMedArticle(pubmedArticles: ReceiveChannel<PubmedArticle>) = produce {
    for (pubmedArticle in pubmedArticles) {
        val entry = PubMedEntry.parsePubMedArticle(pubmedArticle)
        val pubmedId = mergePubMedEntry(entry)
        delay(50)
        send(entry) // send out PubMed Entry for additional processing
    }
}

/*
Function that will process the PubMed articles used as a references in the
original article. Creates a skeleton PubMedArticle node, creates a HAS_REFERENCE relationship
with the original node, and adds a REFERENCE label.
It is possible that the reference node already exists as a CosmicArticle node
 */
@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.createReferenceNodes(pubMedEntries: ReceiveChannel<PubMedEntry>) = produce {
    for (pubmedEntry in pubMedEntries) {
        val refList = mutableListOf<String>()
        val parentId = pubmedEntry.pubmedId
        pubmedEntry.referenceSet.stream().forEach { ref ->
            Neo4jUtils.createPubMedArticleNode(ref)
            Neo4jUtils.addLabel(ref, "Reference")
            Neo4jUtils.createPubMedRelationship("REFERENCE", parentId, ref)
            if (Neo4jUtils.isIncompleteNode(ref)) {
                refList.add(ref)
            }
        }
        //TODO: determine why this has to be a separate stream
        refList.forEach { ref -> send(ref) }
        delay(100)
        refList.clear()
    }
}

fun main(args: Array<String>) = runBlocking {
    val inputFile = when (args.isNotEmpty()){
        true -> args[0]
        false -> "./data/sample_CosmicMutantExportCensus.tsv"
    }
    logger.atInfo().log("Processing PubMed Ids in $inputFile")
    val stopwatch = Stopwatch.createStarted()
    val entryChannel =
        persistPubMedArticle(retrievePubMedArticle(produceIdBatch(inputFile)))
    val refChannel = persistPubMedArticle(retrievePubMedArticle(createReferenceNodes(entryChannel)))
    for (ref in refChannel) {  // consume this channel's entries
        println("Reference Id ${ref.pubmedId}  Title: ${ref.articleTitle}")
    }
    stopwatch.elapsed(TimeUnit.SECONDS)
    println("Elapsed time: ${stopwatch.elapsed(java.util.concurrent.TimeUnit.SECONDS)} seconds")
    delay(20_000)
    println("Cancelling children")
    coroutineContext.cancelChildren()
    println("FINIS....")
}



