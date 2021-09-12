package org.batteryparkdev.pubmedref.neo4j

import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.model.PubMedEntry
import java.util.*

object Neo4jPubMedLoader {

    private val logger: FluentLogger = FluentLogger.forEnclosingClass();

    fun loadPubMedEntry(pubMedEntry: PubMedEntry) {
        /*
        Test if this PubMed Id is already represented
        If so, don't attempt to create another one,
        but the existing node may have a new relationship and
        an additional label
         */
        if (!pubMedNodeExistsPredicate(pubMedEntry.pubmedId)) {
            val newPubMedId = mergePubMedEntry(pubMedEntry)
            logger.atInfo().log("PubMed Id $newPubMedId  loaded into Neo4j")
        } else {
            logger.atInfo().log("PubMed Id ${pubMedEntry.pubmedId}  already loaded into Neo4j")
        }
        if (pubMedEntry.parentPubMedId.isNotEmpty()) {
            val r = Neo4jUtils.createPubMedRelationship(pubMedEntry.label, pubMedEntry.parentPubMedId,
                pubMedEntry.pubmedId)
            logger.atInfo().log(
                "${pubMedEntry.label} relationship between ids ${pubMedEntry.parentPubMedId} " +
                        " and ${pubMedEntry.pubmedId} created"
            )
        }

            val l = Neo4jUtils.addLabel(pubMedEntry.pubmedId, pubMedEntry.label)
            if (l.isNotEmpty()) {
                logger.atFine().log("Added label: ${pubMedEntry.label} to PubMedArticle node for ${pubMedEntry.pubmedId}")

            }
    }

    private const val mergePubMedArticleTemplate = "MERGE (pma:PubMedArticle { pubmed_id: PMAID}) " +
            "SET  pma.pmc_id = \"PMCID\", pma.doi_id = \"DOIID\", " +
            " pma.journal_name = \"JOURNAL_NAME\", pma.journal_issue = \"JOURNAL_ISSUE\", " +
            " pma.article_title = \"TITLE\", pma.abstract = \"ABSTRACT\", " +
            " pma.author = \"AUTHOR\", pma.reference_count = REFCOUNT, " +
            " pma.cited_by_count = CITED_BY " +
            "  RETURN pma.pubmed_id"

    private fun mergePubMedEntry(pubMedEntry: PubMedEntry): String {
        val merge = mergePubMedArticleTemplate.replace("PMAID", pubMedEntry.pubmedId)
            .replace("PMCID", pubMedEntry.pmcId)
            .replace("DOIID", pubMedEntry.doiId)
            .replace("JOURNAL_NAME", pubMedEntry.journalName)
            .replace("JOURNAL_ISSUE", pubMedEntry.journalIssue)
            .replace("TITLE", pubMedEntry.articleTitle)
            .replace("ABSTRACT", modifyInternalQuotes(pubMedEntry.abstract))
            .replace("AUTHOR", pubMedEntry.authorCaption)
            .replace("REFCOUNT", pubMedEntry.referenceSet.size.toString())
            .replace("CITED_BY", pubMedEntry.citedByCount.toString())
        return Neo4jConnectionService.executeCypherCommand(merge)
    }

    /*
    Create a relationship between the origin node and either a reference node or a citation node
    Increment appropriate count in the origin node
     */
    private fun createPubMedRelationship(pubMedEntry: PubMedEntry): String {
        val command = when (pubMedEntry.label.uppercase()) {
            "REFERENCE" -> "MATCH (parent:PubMedArticle), (child:PubMedArticle) WHERE " +
                    "parent.pubmed_id = ${pubMedEntry.parentPubMedId} AND child.pubmed_id = ${pubMedEntry.pubmedId} " +
                    "MERGE (parent) - [r:HAS_REFERENCE] -> (child) " +
                    "ON CREATE SET parent.reference_count = parent.reference_count +1 RETURN r"
            "CITATION" -> "MATCH (parent:PubMedArticle), (child:PubMedArticle) WHERE " +
                    "parent.pubmed_id = ${pubMedEntry.parentPubMedId} AND child.pubmed_id = ${pubMedEntry.pubmedId} " +
                    "MERGE (parent) - [r:CITED_BY] -> (child) RETURN r"
            else -> ""
        }
        return Neo4jConnectionService.executeCypherCommand(command)
    }

    /*
    Function to determine if the PubMed data is already in the database
     */
   fun pubMedNodeExistsPredicate(pubmedId: String): Boolean {
        val cypher = "OPTIONAL MATCH (p:PubMedArticle{pubmed_id: $pubmedId }) " +
                " RETURN p IS NOT NULL AS Predicate"
        try {
            val predicate = Neo4jConnectionService.executeCypherCommand(cypher)
            when (predicate.lowercase(Locale.getDefault())) {
                "true" -> return true
                "false" -> return false
            }
        } catch (e: Exception) {
            logger.atSevere().log(e.message.toString())
            return false
        }
        return false
    }

    /*
    Function to add a second  or third label to a PubMedArticle node if it represents
    either a reference and/or a citation
    Ensure that labels are not duplicated
     */
    private fun addLabel(pubmedId: String, label: String): String {
        // confirm that labels are novel
        val labelExistsQuery = "MATCH (pma:PubMedArticle{pubmed_id: $pubmedId }) " +
                "RETURN apoc.label.exists(pma, \"$label\") AS output;"
        val addLabelCypher = "MATCH (pma:PubMedArticle{pubmed_id: $pubmedId }) " +
                " CALL apoc.create.addLabels(pma, [\"$label\"] ) yield node return node"
        if (Neo4jConnectionService.executeCypherCommand(labelExistsQuery).uppercase() == "FALSE") {
            return Neo4jConnectionService.executeCypherCommand(addLabelCypher)
        }
        logger.atWarning().log("PubMedArticle node $pubmedId  already has label $label")
        return ""
    }

    /*
    Double quotes (i.e. ") inside a text field causes Cypher
    processing errors
     */
    private fun modifyInternalQuotes(text: String): String =
        text.replace("\"", "'")
}