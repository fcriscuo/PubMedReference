package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.neo4j.service.Neo4jUtils

/*
Represents a data class whose properties contain sufficient information to
identify an individual node in a Neo4j graph database
 */
data class NodeIdentifier(
    val primaryLabel: String,
    val idProperty: String,
    val idValue: String,
    val secondaryLabel:String="",
){
    fun isValid():Boolean =
        primaryLabel.isNotBlank().and(idProperty.isNotBlank()).and(idValue.isNotBlank())

    fun mergeNodeIdentifierCypher():String =
        when (secondaryLabel.isNotEmpty()){
            true -> "MERGE (n:$primaryLabel:$secondaryLabel{$idProperty: " +
                    "${Neo4jUtils.formatPropertyValue(idValue)}}) " +
                    "RETURN n.$idProperty"
            false -> "MERGE (n:$primaryLabel {$idProperty: ${Neo4jUtils.formatPropertyValue(idValue)}}) " +
                    "RETURN n.$idProperty"
        }

    fun addNodeLabelCypher():String =
    "MATCH (child:$primaryLabel{$idProperty:" +
            " ${Neo4jUtils.formatPropertyValue(idValue)} }) " +
            " WHERE apoc.label.exists(child,${Neo4jUtils.formatPropertyValue(secondaryLabel)})  = false " +
            " CALL apoc.create.addLabels(child, [${Neo4jUtils.formatPropertyValue(secondaryLabel)}] )" +
            " yield node return node"
}