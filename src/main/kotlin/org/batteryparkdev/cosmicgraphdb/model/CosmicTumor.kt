package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.batteryparkdev.nodeidentifier.model.NodeIdentifier
import org.neo4j.driver.Value


data class CosmicTumor(
    val tumorId: Int, val sampleId: Int,
    val site: CosmicType, val histology: CosmicType,
    val tumorOrigin: String, val age: String,
) {

    fun generateCosmicTumorCypher():String {
        val tumorNodeIdentifier = NodeIdentifier("CosmicTumor",
        "tumor_id", tumorId.toString())
        val cypher = when (Neo4jUtils.nodeExistsPredicate(tumorNodeIdentifier)) {
            true -> generateTumorMatchCypher().plus(generateTumorSampleRelationshipCypher())
            false -> generateTumorMergeCypher().plus(generateTumorSampleRelationshipCypher())
        }
        println("Tumor: $cypher")
        return cypher
    }

    /*
    Function to generate Cypher statements to create tumor, site, and histology
    nodes and relationships in the Neo4j database if the tumor id is novel
    n.b. the generated Cypher is intended to be used within a larger
    transaction and as a result it does not have a RETURN component
     */
   private fun generateTumorMergeCypher(): String =
        " CALL apoc.merge.node( [\"CosmicTumor\"], " +
                "{tumor_id: $tumorId,  tumor_origin: " +
                "${Neo4jUtils.formatPropertyValue(tumorOrigin)} ," +
                " age: ${Neo4jUtils.formatPropertyValue(age)}, " +
                "  created: datetime()}) YIELD node as ${CosmicTumor.nodename} \n"
                    .plus(site.generateMergeCypher())
                    .plus(site.generateParentRelationshipCypher(CosmicTumor.nodename))
                    .plus(histology.generateMergeCypher())
                    .plus(histology.generateParentRelationshipCypher(CosmicTumor.nodename))


   private  fun generateTumorMatchCypher(): String =
       "CALL apoc.merge.node ([${CosmicTumor.nodename}],{tumor_id: $tumorId},{} ) " +
               " YIELD node AS ${CosmicTumor.nodename}\n"


    fun generateTumorSampleRelationshipCypher(): String {
        val relationship = "HAS_SAMPLE"
        val relname = "rel_mut_sample"
        return CosmicSample.generateMatchCosmicSampleCypher(sampleId)
            .plus("CALL apoc.merge.relationship(${CosmicTumor.nodename}, '$relationship', " +
                    " {}, {created: datetime()}, ${CosmicSample.nodename},{} )" +
                    " YIELD rel AS $relname \n")
    }

    companion object : AbstractModel {
        const val nodename = "tumor"
        fun parseValueMap(value: Value): CosmicTumor =
            CosmicTumor(
                value["ID_tumour"].asString().toInt(),
                value["ID_sample"].asString().toInt(),
                resolveSiteType(value),
                resolveHistologySite(value),
                value["Tumour origin"].asString(),
                value["Age"].asString()
            )

        private fun resolveSiteType(value: Value): CosmicType =
            CosmicType(
                "Site", value["Primary site"].asString(),
                value["Site subtype 1"].asString(),
                value["Site subtype 2"].asString(),
                value["Site subtype 3"].asString()
            )

        private fun resolveHistologySite(value: Value): CosmicType =
            CosmicType(
                "Histology", value["Primary histology"].asString(),
                value["Histology subtype 1"].asString(),
                value["Histology subtype 2"].asString(),
                value["Histology subtype 3"].asString()
            )


        fun generatePlaceholderCypher(tumorId: Int)  = " CALL apoc.merge.node([\"CosmicTumor\"], " +
                " {tumor_id: $tumorId},{} ) " +
                " YIELD node as ${CosmicTumor.nodename}  \n"

        fun generateChildRelationshipCypher (tumorId: Int, childLabel: String ) :String{
            val relationship = "HAS_".plus(childLabel.uppercase())
            val relname = "rel_tumor"
            return  generatePlaceholderCypher(tumorId).plus(
            " CALL apoc.merge.relationship(${CosmicTumor.nodename}, '$relationship', " +
                    " {}, {created: datetime()}, " +
                    " $childLabel, {} ) YIELD rel AS $relname \n")
        }

    }
}


