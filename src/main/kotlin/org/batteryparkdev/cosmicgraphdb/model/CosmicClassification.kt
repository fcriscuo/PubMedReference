package org.batteryparkdev.cosmicgraphdb.model

import org.neo4j.driver.Value
import java.util.*

data class CosmicClassification(
    val cosmicPhenotypeId: String,
    val siteType: CosmicType,
    val histologyType: CosmicType,
    val cosmicSiteType: CosmicType,
    val nciCode: String,
    val efoUrl: String
) {
    fun resolveClassificationId(): Int =
        UUID.randomUUID().hashCode()

    fun generateCosmicClassificationCypher(): String =
        generateMergeCypher()
            .plus(siteType.generateCosmicTypeCypher(CosmicClassification.nodename))
            .plus(histologyType.generateCosmicTypeCypher(CosmicClassification.nodename))
            .plus(" RETURN ${CosmicClassification.nodename}\n")

    private fun generateMergeCypher(): String = "CALL apoc.merge.node([\"CosmicClassification\"]," +
            "{ classification_id: ${resolveClassificationId()}}," +
            " { phenotype_id: \"${cosmicPhenotypeId}\", " +
            " nci_code: \"${nciCode}\"," +
            " efo_url: \"${efoUrl}\"," +
            " created: datetime() }," +
            "{ last_mod: datetime()}) YIELD node AS $nodename \n "

    companion object : AbstractModel {
        val nodename = "classification"
        fun parseValueMap(value: Value): CosmicClassification {
            val nciCode = value["NCI_CODE"].asString() ?: "NS"
            val efo = value["EFO"].asString() ?: "NS"
            val phenoId = value["COSMIC_PHENOTYPE_ID"].asString() ?: "NS"

            return CosmicClassification(
                phenoId,
                resolveSiteType(value),
                resolveHistologyType(value),
                resolveCosmicSiteType(value),
                nciCode, efo
            )
        }

        private fun resolveSiteType(value: Value): CosmicType =
            CosmicType(
                "Site", value["SITE_PRIMARY"].asString(),
                value["SITE_SUBTYPE1"].asString(),
                value["SITE_SUBTYPE2"].asString(),
                value["SITE_SUBTYPE3"].asString()
            )

        private fun resolveHistologyType(value: Value): CosmicType =
            CosmicType(
                "Histology", value["HISTOLOGY"].asString(),
                value["HIST_SUBTYPE1"].asString(),
                value["HIST_SUBTYPE2"].asString(),
                value["HIST_SUBTYPE3"].asString()
            )

        private fun resolveCosmicSiteType(value: Value): CosmicType =
            CosmicType(
                "CosmicSite", value["SITE_PRIMARY_COSMIC"].asString(),
                value["SITE_SUBTYPE1_COSMIC"].asString(),
                value["SITE_SUBTYPE2_COSMIC"].asString(),
                value["SITE_SUBTYPE3_COSMIC"].asString()
            )

        fun generateChildRelationshipCypher(classificationId: Int, parentNodeName: String): String {
            val relationship = "HAS_COSMIC_CLASSIFICATION"
            val relName = "rel_class"
            return " MATCH (cc:CosmicClassification) WHERE cc.classification_id =  " +
                    " $classificationId  \n" +
                    " CALL apoc.merge.relationship( $parentNodeName, '$relationship', " +
                    " {},  {created: datetime()}, cc, {} ) " +
                    " YIELD rel as $relName \n"
        }
    }
}

