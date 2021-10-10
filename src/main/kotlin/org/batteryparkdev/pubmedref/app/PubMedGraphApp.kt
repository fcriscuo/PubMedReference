package org.batteryparkdev.pubmedref.app

import ai.wisecube.pubmed.PubmedParser
import arrow.core.Either
import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.model.PubMedEntry
import org.batteryparkdev.pubmedref.neo4j.Neo4jPubMedLoader
import org.batteryparkdev.pubmedref.neo4j.Neo4jUtils
import org.batteryparkdev.pubmedref.service.PubMedRetrievalService
import java.util.stream.Stream

const val pubMedTemplate =
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml"
const val pubMedToken = "PUBMEDID"
const val startTag = "<PubmedArticleSet>"
const val UTF8_BOM = "\uFEFF"

class PubMedGraphApp {
    private val logger: FluentLogger = FluentLogger.forEnclosingClass();

    //private val parser = PubmedParser()

    companion object {
        @JvmStatic
        fun processPubMedIdStream(ids: Stream<Int>) {
            val app = PubMedGraphApp()
            Neo4jUtils.clearRelationshipsAndLabels()
            ids.filter { it > 0 }
                .forEach { app.processPubMedNodeById(it) }
        }
    }

    fun processPubMedNodeById(pubmedId: Int) {
        val originEntry = loadOriginNodes(pubmedId.toString())
        if (null != originEntry) {
            loadReferenceNodes(originEntry)
            loadCitationNodes(originEntry)
        }
    }

    fun loadOriginNodesByBatch(batchIds: String) {
//        batchIds.splitToSequence("")
//            .forEach { id -> Neo4jUtils.deleteExistingOriginNode(id) }
        loadPubMedEntryByIdBatch(batchIds).stream()
            .filter { null != it }
            .forEach {
                run {
                    loadReferenceNodes(it)
                    loadCitationNodes(it)
                }
            }
    }

    /*
Load all the Origin nodes individually
 */
    private fun loadOriginNodes(pubmedId: String): PubMedEntry? {
        logger.atFine().log("Processing PubMed Id: $pubmedId")
        Neo4jUtils.deleteExistingOriginNode(pubmedId)
        return loadPubMedEntryById(pubmedId)
    }

    private fun loadCitationNodes(pubMedEntry: PubMedEntry) {
        logger.atInfo().log("Processing Citations")
        val parentId = pubMedEntry.pubmedId
        val label = "Citation"
        pubMedEntry.citationSet.stream().forEach { id ->
            run {
                logger.atFine().log("  Citation id: $id")
                /*
            Only fetch the PubMed data from NCBI if the database does not
            contain a PubMedReference node for this citation id
             */
                if (!Neo4jUtils.pubMedNodeExistsPredicate(id)) {
                    logger.atInfo().log("  Fetching citation  id: $id from NCBI")
                    val citEntry = loadPubMedEntryById(id, label, parentId)
                } else {
                    Neo4jUtils.createPubMedRelationship(label, parentId, id)
                    Neo4jUtils.addLabel(id, label)
                }
            }
        }
    }

    private fun loadReferenceNodes(pubMedEntry: PubMedEntry) {
        logger.atInfo().log("Processing References")
        val parentId = pubMedEntry.pubmedId  // id of origin node
        val label = "Reference"
        pubMedEntry.referenceSet.stream().forEach { id ->
            run {
                logger.atFine().log("  Reference id: $id")
                if (!Neo4jUtils.pubMedNodeExistsPredicate(id)) {
                    logger.atInfo().log("  Fetching reference id: $id from NCBI")
                    val refEntry = loadPubMedEntryById(id, label, parentId)
                } else {
                    Neo4jUtils.createPubMedRelationship(label, parentId, id)
                    Neo4jUtils.addLabel(id, label)
                }
            }
        }
    }

    private fun loadPubMedEntryByIdBatch(
        pubmedIdBatch: String,
        label: String = "Origin",
        parentId: String = ""
    ): List<PubMedEntry> {
        val entryList = mutableListOf<PubMedEntry>()
        return when (val retEither = PubMedRetrievalService.retrievePubMedArticleStream(pubmedIdBatch)) {
            is Either.Right -> {
                val pubmedArticleStream = retEither.value
                pubmedArticleStream.forEach { article ->
                    run {
                        val pubmedEntry = PubMedEntry.parsePubMedArticle(article, label, parentId)
                        Neo4jPubMedLoader.loadPubMedEntry(pubmedEntry)
                        entryList.add(pubmedEntry)
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

    private fun loadPubMedEntryById(pubmedId: String, label: String = "Origin", parentId: String = ""): PubMedEntry? {
        return when (val retEither = PubMedRetrievalService.retrievePubMedArticle(pubmedId)) {
            is Either.Right -> {
                val pubmedArticle = retEither.value
                val pubmedEntry = PubMedEntry.parsePubMedArticle(pubmedArticle, label, parentId)
                Neo4jPubMedLoader.loadPubMedEntry(pubmedEntry)
                pubmedEntry
            }
            is Either.Left -> {
                logger.atSevere().log(retEither.value.message)
                null
            }
        }
    }
}

/*
Add support for multiple origin pubmed ids
*/
fun main() {
    val pubmedIdStream = listOf(25303977, 33024263, 28467829).stream()

    // clear existing relationships and secondary labels
    Neo4jUtils.clearRelationshipsAndLabels()
    PubMedGraphApp.processPubMedIdStream(pubmedIdStream)
}

