package org.batteryparkdev.pubmedref.app

import arrow.core.Either
import com.google.common.base.Stopwatch
import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.model.PubMedEntry
import org.batteryparkdev.pubmedref.neo4j.Neo4jConnectionService
import org.batteryparkdev.pubmedref.neo4j.Neo4jUtils
import org.batteryparkdev.pubmedref.service.PubMedRetrievalService
import org.batteryparkdev.pubmedref.service.TsvRecordSequenceSupplier
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

/*
Represents a Kotlin application that will load data for PubMed articles listed
in a Cosmic TSV-formatted file. In order to expedite data retrieval from NCBI,
data requests will be submitted in batches. The retrieved data in XML format
will be unmarshalled to a JAXB model object and then mapped to a Kotlin model
object. This later object will be used to populate Neo4j Cypher commands to
create PubMedArticle nodes and their relationships.
 */


class PubMedBatchLoader(val loadCitations:Boolean = false) {

    val pubMedToken = "PUBMEDID"
    private val ncbiBatchSize = 100
    val startTag = "<PubmedArticleSet>"
    val UTF8_BOM = "\uFEFF"
    private val originIdList = mutableListOf<String>()
    private val secondaryIdList = mutableListOf<String>()
    private val logger: FluentLogger = FluentLogger.forEnclosingClass();
    private val pubmedIdCol = "Pubmed_PMID"
    // For COSMIC data, The PubMed Ids to be loaded are in a CosmicMutantExport-formatted file

    /*
    Function to read lines from a specified TSV file (with header)
    and extract the PubMed Ids
    These data must be in a column names Pubmed_PMID
    The PubMed Ids read from this file are labeled as Origin entities
     */
    fun loadCosmicPubMedDataBatch(cosmicTsvFile: String) {
        val path = Paths.get(cosmicTsvFile)
        TsvRecordSequenceSupplier(path).get().chunked(30)
            .forEach { it ->
                it.stream()
                    .map { it.get(pubmedIdCol) }
                    .filter { it.isNotEmpty() }  // ignore records w/o pubmedId value
                    .filter { !Neo4jUtils.existingOriginPubMedIdPredicate(it) }
                    .forEach {
                        createPubMedArticleNode(it)
                        Neo4jUtils.addLabel(it,"CosmicArticle")
                        logger.atFine().log("Created PubMedArticle node for $it labeled CosmicArticle")
                        when (originIdList.size < ncbiBatchSize) {
                            true -> originIdList.add(it)
                            false -> {
                                loadOriginNodesByBatch(originIdList.joinToString(separator = ","))
                                originIdList.clear()
                                originIdList.add(it)
                            }
                        }
                    }
            }
        if (originIdList.isNotEmpty()) {  // process residual set of pubmed ids
            logger.atFine().log("Loading residual origin articles, count = ${originIdList.size}")
            loadOriginNodesByBatch(originIdList.joinToString(separator = ","))
        }
    }

    private fun createPubMedArticleNode(pubmedId: String): String =
        Neo4jConnectionService.executeCypherCommand(
            " MERGE (pma: PubMedArticle{pubmed_id: $pubmedId}) " +
                    " RETURN pma.pubmed_id"
        )

    /*
    Retrieve the current batch of origin PubMed Ids from NCBI and load them into the
    into Neo4j database
     */
    private fun loadOriginNodesByBatch(batchIds: String) {
        val loadTimer = Stopwatch.createStarted()
        var nodeCount = 0
        loadPubMedEntryByIdBatch(batchIds).stream()
            .filter { null != it }
            .forEach {
                run {
                    nodeCount += 1
                    if (it.referenceSet.isNotEmpty()) {
                        loadSecondaryNodes("REFERENCE", it.referenceSet, it.pubmedId)
                        nodeCount += it.referenceSet.size
                    }
                    if (loadCitations && it.citationSet.isNotEmpty()) {
                        loadSecondaryNodes("CITATION", it.citationSet, it.pubmedId)
                        nodeCount += it.citationSet.size
                    }
                }
            }
        logger.atInfo().log(
            "+++ Batch load time for $nodeCount nodes =" +
                    "  ${loadTimer.elapsed(TimeUnit.SECONDS)} seconds"
        )
    }

    private fun loadSecondaryNodes(type: String, idSet: Set<String>, parentId: String) {
        logger.atInfo().log("Secondary nodes:  type = $type    count = ${idSet.size}  parent  id = ${parentId}")
        val secStopwatch = Stopwatch.createStarted()
        idSet.stream().forEach { id ->
            createPubMedArticleNode(id) // create the skeleton reference node
            createPubMedRelationship(type, parentId, id)
            Neo4jUtils.addLabel(id,type)
            // check if this PubMed article has already been retrieved
            if (isIncompleteNode(id)) {
                when (secondaryIdList.size < ncbiBatchSize) {
                    true -> secondaryIdList.add(id)
                    false -> {
                        loadPubMedEntryByIdBatch(secondaryIdList.joinToString(separator = ","))
                        secondaryIdList.clear()
                        secondaryIdList.add(id)
                    }
                }
            }
        }
        if (secondaryIdList.isNotEmpty()) {  // process last set of pubmed ids
            logger.atFine().log("Loading residual secondary articles, count = ${secondaryIdList.size}")
            loadPubMedEntryByIdBatch(secondaryIdList.joinToString(separator = ","))
        }
        logger.atInfo().log("Secondary node load required ${secStopwatch.elapsed(TimeUnit.SECONDS)} seconds")
    }

    /*
    determine if a PubMedArticle has been fully loaded by
    looking for a required property
     */
    private fun isIncompleteNode(id: String): Boolean =
        when (Neo4jConnectionService.executeCypherCommand(
            "MATCH (pma:PubMedArticle {pubmed_id:$id}) RETURN COUNT(pma.article_title)"
        )) {
            "0" -> true
            else -> false
        }

    private fun loadPubMedEntryByIdBatch(
        pubmedIdBatch: String,
        label: String = "CosmicArticle",
        parentId: String = ""
    ): List<PubMedEntry> {
        val entryList = mutableListOf<PubMedEntry>()
        return when (val retEither = PubMedRetrievalService.retrievePubMedArticleStream(pubmedIdBatch)) {
            is Either.Right -> {
                val pubmedArticleStream = retEither.value
                pubmedArticleStream.forEach { article ->
                    run {
                        val pubMedEntry = PubMedEntry.parsePubMedArticle(article, label, parentId)
                        mergePubMedEntry(pubMedEntry)
                        logger.atFine().log("Loaded PubMedEntry for ${pubMedEntry.pubmedId}")
                        entryList.add(pubMedEntry)
                    }
                }
                entryList
            }
            is Either.Left -> {
                logger.atSevere().log(retEither.value.message)
                entryList
            }
        }
    }

    fun createPubMedRelationship(label: String, parentPubMedId: String, pubmedId: String): String {
        val command = when (label.uppercase()) {
            "REFERENCE" -> "MATCH (parent:PubMedArticle), (child:PubMedArticle) WHERE " +
                    "parent.pubmed_id = $parentPubMedId AND child.pubmed_id = $pubmedId " +
                    "MERGE (parent) - [r:HAS_REFERENCE] -> (child) " +
                    "ON CREATE SET parent.reference_count = parent.reference_count +1 RETURN r"
            "CITATION" -> "MATCH (parent:PubMedArticle), (child:PubMedArticle) WHERE " +
                    "parent.pubmed_id = $parentPubMedId AND child.pubmed_id = $pubmedId " +
                    "MERGE (parent) - [r:CITED_BY] -> (child) RETURN r"
            else -> ""
        }
        return Neo4jConnectionService.executeCypherCommand(command)
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

}

fun main() {
    PubMedBatchLoader(false).loadCosmicPubMedDataBatch("./data/sample_CosmicMutantExportCensus.tsv")
}