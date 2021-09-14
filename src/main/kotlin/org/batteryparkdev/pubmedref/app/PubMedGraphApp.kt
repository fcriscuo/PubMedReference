package org.batteryparkdev.pubmedref.app

import ai.wisecube.pubmed.PubmedParser
import arrow.core.Either
import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.model.PubMedEntry
import org.batteryparkdev.pubmedref.neo4j.Neo4jPubMedLoader
import org.batteryparkdev.pubmedref.neo4j.Neo4jUtils
import org.batteryparkdev.pubmedref.service.PubMedRetrievalService

private val logger: FluentLogger = FluentLogger.forEnclosingClass();
val defaultIdList = listOf("26050619","32946445","28875994")

const val pubMedTemplate =
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml"
const val pubMedToken = "PUBMEDID"
const val startTag = "<PubmedArticleSet>"
const val UTF8_BOM = "\uFEFF"
private val parser = PubmedParser()

/*
Add support for multiple origin pubmed ids
 */
fun main(args: Array<String>) {
    val pubmedIdList = if (args.isNotEmpty()) listOf(*args) else defaultIdList
    // clear existing relationships and secondary labels
    Neo4jUtils.clearRelationshipsAndLabels()
    // load the origin node
    val originList = loadOriginNodes(pubmedIdList)
    originList.forEach {
        run {
            loadReferenceNodes(it)
            loadCitationNodes(it)
        }
    }
}

/*
Load all the Origin nodes individually
 */
fun loadOriginNodes(pubmedIdList: List<String>): List<PubMedEntry> {
    val entryList = mutableListOf<PubMedEntry>()
    pubmedIdList.forEach {
        run {
            logger.atInfo().log("Processing PubMed Id: $it")
            Neo4jUtils.deleteExistingOriginNode(it)
            val entry = loadPubMedEntryById(it)
            if (entry != null) {
                entryList.add(entry)
            }
        }
    }
    return entryList.toList()
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
                val citEntry = loadPubMedEntryById(id, label, parentId)
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
                val refEntry = loadPubMedEntryById(id, label, parentId)
            } else {
                Neo4jUtils.createPubMedRelationship(label, parentId, id)
                Neo4jUtils.addLabel(id, label)
            }
        }
    }
}


fun loadPubMedEntryById(pubmedId: String, label: String = "Origin", parentId: String = ""):PubMedEntry? {
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



