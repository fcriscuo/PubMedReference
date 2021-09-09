package org.batteryparkdev.pubmedref.app

import ai.wisecube.pubmed.PubmedParser
import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.model.PubMedEntry
import org.batteryparkdev.pubmedref.neo4j.Neo4jPubMedLoader
import org.batteryparkdev.pubmedref.service.PubMedRetrievalService

private val logger: FluentLogger = FluentLogger.forEnclosingClass();
const val defaultId = "26050619"
const val pubMedTemplate =
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml"
const val pubMedToken = "PUBMEDID"
const val startTag = "<PubmedArticleSet>"
const val UTF8_BOM = "\uFEFF"
private val parser = PubmedParser()

fun main(args: Array<String>) {
    val pubmedId = if (args.isNotEmpty()) args[0] else defaultId
    logger.atInfo().log("Processing PubMed Id: $pubmedId")
    // load the origin node
    val pubmedEntry = loadOriginNode(pubmedId)
    // load the reference nodes
    loadReferenceNodes(pubmedEntry)
    // load citations
    loadCitationNodes(pubmedEntry)
}

fun loadOriginNode(pubmedId: String): PubMedEntry {
    val pubmedEntry = resolvePubMedEntryById(pubmedId)
    Neo4jPubMedLoader.loadPubMedEntry(pubmedEntry) // accept default values for parent anf label
    return pubmedEntry
}

fun loadCitationNodes(pubMedEntry: PubMedEntry){
    logger.atInfo().log("Processing Citations")
    val parentId = pubMedEntry.pubmedId
    pubMedEntry.citationSet.stream().forEach { id ->
        run {
            logger.atInfo().log("  Citation id: $id")
            val citEntry = resolvePubMedEntryById(id, "Citation", parentId)
            Neo4jPubMedLoader.loadPubMedEntry(citEntry)
            Thread.sleep(300L)  // Accommodate NCBI request rate limit
        }
    }
}

fun loadReferenceNodes(pubMedEntry: PubMedEntry) {
    logger.atInfo().log("Processing References")
    val parentId = pubMedEntry.pubmedId  // id of origin node
    pubMedEntry.referenceSet.stream().forEach { id ->
        run {
            logger.atInfo().log("  Reference id: $id")
            val refEntry = resolvePubMedEntryById(id, "Reference", parentId)
            Neo4jPubMedLoader.loadPubMedEntry(refEntry)
            Thread.sleep(300L) // Accommodate NCBI request rate limit
        }
    }
}

fun resolvePubMedEntryById(pubmedId: String, label: String = "Origin", parentId: String = ""): PubMedEntry {
    val pubmedArticle = PubMedRetrievalService.retrievePubMedArticle(pubmedId)
    return PubMedEntry.parsePubMedArticle(pubmedArticle, label, parentId)
}

