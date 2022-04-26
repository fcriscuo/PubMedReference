package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.neo4j.service.Neo4jUtils

object CosmicAnnotationFunctions {

    fun generateAnnotationCypher(
        annotationList: List<String>, secondaryLabel: String,
        parentNodeName: String
    ): String {
        var cypher: String = ""
        var index = 0
        val relationship = "HAS_".plus(secondaryLabel.uppercase())
        annotationList.forEach { annon ->
            run {
                index += 1
                val annonName = secondaryLabel.plus(index.toString()).lowercase()
                val relName = "rel_".plus(annonName)
                cypher = cypher.plus(
                    " CALL apoc.merge.node( [\"CosmicAnnotation\"," +
                            " ${Neo4jUtils.formatPropertyValue(secondaryLabel)}]," +
                            " {annotation_value: ${Neo4jUtils.formatPropertyValue(annon)}," +
                            " created: datetime()}) YIELD node as $annonName \n" +
                            " CALL apoc.merge.relationship( $parentNodeName, '$relationship', " +
                            " {}, {created: datetime()}, " +
                            " $annonName, {} ) YIELD rel AS $relName \n"
                )
            }
        }
        return cypher
    }

    fun generateTranslocationCypher(transPartnerList: List<String>): String {
        var index = 0
        val relationship = "HAS_TRANSLOCATION_PARTNER"
        var cypher: String = ""
        transPartnerList.forEach { trans ->
            run {
                index += 1
                val relname = "translocation".plus(index.toString())
                val transName = "trans".plus(index.toString())
                cypher = cypher.plus(
                    " CALL apoc.merge.node([\"CosmicGene\"], " +
                            " {  gene_symbol: ${Neo4jUtils.formatPropertyValue(trans)}, " +
                            " created: datetime()}) YIELD node" +
                            " as $transName \n"
                )
                    .plus(
                        " CALL apoc.merge.relationship(${CosmicGeneCensus.nodename}, " +
                                "'$relationship', {},  {created: datetime()}, " +
                                " $transName, {} ) YIELD rel as $relname \n"
                    )
            }
        }
        return cypher
    }
}