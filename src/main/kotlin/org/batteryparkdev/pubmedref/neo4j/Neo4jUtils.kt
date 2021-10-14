package org.batteryparkdev.pubmedref.neo4j

import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.model.PubMedEntry
import java.util.*

object Neo4jUtils {

    private val logger: FluentLogger = FluentLogger.forEnclosingClass();

    private fun deleteRelationshipsByName(relName: String) {
        val deleteTemplate = "MATCH (:PubMedArticle) -[r:RELATIONSHIP_NAME] ->(:PubMedArticle) DELETE r;"
        val countTemplate = "MATCH (:PubMedArticle) -[:RELATIONSHIP_NAME] -> (:PubMedArticle) RETURN COUNT(*)"
        val delCommand = deleteTemplate.replace("RELATIONSHIP_NAME", relName)
        val countCommand = countTemplate.replace("RELATIONSHIP_NAME", relName)
        val beforeCount = Neo4jConnectionService.executeCypherCommand(countCommand)
        logger.atInfo().log("Deleting relationships: $relName  before count = $beforeCount")
        Neo4jConnectionService.executeCypherCommand(delCommand)
        val afterCount = Neo4jConnectionService.executeCypherCommand(countCommand)
        logger.atInfo().log("After deletion command count = $afterCount")
    }

    private fun removeLabel(label: String) {
        val removeLabelTemplate = "MATCH (p:PubMedArticle) REMOVE p:LABEL RETURN COUNT(p)"
        val countLabelTemplate = "MATCH(l:LABEL) RETURN COUNT(l)"
        val removeLabelCommand = removeLabelTemplate.replace("LABEL", label)
        val countLabelCommand = countLabelTemplate.replace("LABEL", label)
        val beforeCount = Neo4jConnectionService.executeCypherCommand(countLabelCommand)
        logger.atInfo().log("Removing label: $label before count = $beforeCount")
        Neo4jConnectionService.executeCypherCommand(removeLabelCommand)
        val afterCount = Neo4jConnectionService.executeCypherCommand(countLabelCommand)
        logger.atInfo().log("After removal command count = $afterCount")
    }

    private fun deletePubMedRelationships() =
        listOf("HAS_REFERENCE", "CITED_BY").forEach { deleteRelationshipsByName(it) }

    private fun deletePubMedLabels() =
        listOf("Citation", "Reference", "Origin").forEach { removeLabel(it) }

    fun clearRelationshipsAndLabels() {
        deletePubMedRelationships()
        deletePubMedLabels()
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
                "false" -> {
                   logger.atFine().log("Pubmed id $pubmedId does NOT exist")
                    return false
                }
            }

        } catch (e: Exception) {
            logger.atSevere().log(e.message.toString())
            return false
        }
        return false
    }

    /*
    Function to delete a specified PubMedArticle node from the database
    This is to support reloading a node from NCBI to obtain the latest
    list of references and citations
     */
    fun deleteExistingOriginNode(pubMedId: String) {
        if (pubMedNodeExistsPredicate(pubMedId)) {
            val command = "MATCH (p:PubMedArticle{pubmed_id:$pubMedId}) DETACH DELETE p;"
            Neo4jConnectionService.executeCypherCommand(command)
            logger.atInfo().log("PubMedArticle id $pubMedId removed from Neo4j database")
        }
    }

     fun createPubMedArticleNode(pubmedId: String): String =
        Neo4jConnectionService.executeCypherCommand(
            " MERGE (pma: PubMedArticle{pubmed_id: $pubmedId}) " +
                    " RETURN pma.pubmed_id"
        )

    fun deleteExistingPubMedArticleNodes() {
        val nodeCount = Neo4jConnectionService.executeCypherCommand(
            "MATCH (pma:PubMedArticle) RETURN COUNT(pma)"
        )
        Neo4jConnectionService.executeCypherCommand(
            "MATCH (pma: PubMedArticle) DETACH DELETE(pma)"
        )
        logger.atInfo().log(" $nodeCount PubMedArticles deleted")
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

    fun labelExistsPredicate(pubmedId: String, label: String): Boolean {
        val labelExistsQuery = "MATCH (pma:PubMedArticle{pubmed_id: $pubmedId }) " +
                "RETURN apoc.label.exists(pma, \"$label\") AS output;"
        return when (Neo4jConnectionService.executeCypherCommand(labelExistsQuery).uppercase()) {
            "FALSE" -> false
            else -> true
        }
    }

    /*
  determine if a PubMedArticle has been fully loaded by
  looking for a required property
   */
    fun isIncompleteNode(id: String): Boolean =
        when (Neo4jConnectionService.executeCypherCommand(
            "MATCH (pma:PubMedArticle {pubmed_id:$id}) RETURN COUNT(pma.article_title)"
        )) {
            "0" -> true
            else -> false
        }


    /*
   Function to avoid processing the same Origin node more than once
    */
    fun existingOriginPubMedIdPredicate(pubmedId: String): Boolean =
        Neo4jUtils.pubMedNodeExistsPredicate(pubmedId) &&
                Neo4jUtils.labelExistsPredicate(pubmedId, "Origin")

    fun addLabel(pubmedId: String, label: String): String {
        // confirm that label is novel
        val addLabelCypher = "MATCH (pma:PubMedArticle{pubmed_id: $pubmedId }) " +
                " CALL apoc.create.addLabels(pma, [\"$label\"] ) yield node return node"
        if (!labelExistsPredicate(pubmedId, label)) {
            return Neo4jConnectionService.executeCypherCommand(addLabelCypher)
        }
        //logger.atWarning().log("PubMedArticle node $pubmedId  already has label $label")
        return ""
    }
}

fun main() {
    Neo4jUtils.clearRelationshipsAndLabels()
}

