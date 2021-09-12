package org.batteryparkdev.pubmedref.app

import ai.wisecube.pubmed.PubmedParser
import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.model.PubMedEntry
import org.batteryparkdev.pubmedref.neo4j.Neo4jPubMedLoader
import org.batteryparkdev.pubmedref.neo4j.Neo4jUtils
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
    // clear existing relationships and secondary labels
    Neo4jUtils.clearRelationshipsAndLabels()
    // load the origin node
    val pubmedEntry = loadOriginNode(pubmedId)
    // load the reference nodes
    loadReferenceNodes(pubmedEntry)
    // load citations
    loadCitationNodes(pubmedEntry)
}

fun loadOriginNode(pubmedId: String): PubMedEntry {
    val pubmedEntry = resolvePubMedEntryById(pubmedId)
    Neo4jUtils.deleteExistingOriginNode(pubmedId)
    Neo4jPubMedLoader.loadPubMedEntry(pubmedEntry) // accept default values for parent any label
    return pubmedEntry
}

fun loadCitationNodes(pubMedEntry: PubMedEntry){
    logger.atInfo().log("Processing Citations")
    val parentId = pubMedEntry.pubmedId
    val label = "Citation"
    pubMedEntry.citationSet.stream().forEach { id ->
        run {
            logger.atInfo().log("  Citation id: $id")
            /*
            Only fetch the PubMed data from NCBI if the database does not
            contain a PubMedReference node for this citation id
             */
            if (!Neo4jUtils.pubMedNodeExistsPredicate(id)) {
                logger.atInfo().log("  Fetching citation  id: $id from NCBI")
                val citEntry = resolvePubMedEntryById(id, label, parentId)
                Neo4jPubMedLoader.loadPubMedEntry(citEntry)
            } else {
                Neo4jUtils.createPubMedRelationship(label, parentId, id)
                Neo4jUtils.addLabel(id,label)
            }
        }
    }
}

fun loadReferenceNodes(pubMedEntry: PubMedEntry) {
    logger.atInfo().log("Processing References")
    val parentId = pubMedEntry.pubmedId  // id of origin node
    val label = "Reference"
    pubMedEntry.referenceSet.stream().forEach { id ->
        run {
            logger.atInfo().log("  Reference id: $id")
            if (!Neo4jUtils.pubMedNodeExistsPredicate(id)) {
                logger.atInfo().log("  Fetching reference id: $id from NCBI")
                val refEntry = resolvePubMedEntryById(id, label, parentId)
                Neo4jPubMedLoader.loadPubMedEntry(refEntry)
            } else {
                Neo4jUtils.createPubMedRelationship(label, parentId, id)
                Neo4jUtils.addLabel(id, label)
            }
        }
    }
}

fun resolvePubMedEntryById(pubmedId: String, label: String = "Origin", parentId: String = ""): PubMedEntry {
    val pubmedArticle = PubMedRetrievalService.retrievePubMedArticle(pubmedId)
    return PubMedEntry.parsePubMedArticle(pubmedArticle, label, parentId)
}

