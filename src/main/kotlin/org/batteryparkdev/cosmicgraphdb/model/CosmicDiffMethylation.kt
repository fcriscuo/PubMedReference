package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.neo4j.driver.Value

data class CosmicDiffMethylation(
    val studyId: Int, val sampleId: Int,
    val tumorId: Int, val site: CosmicType,
    val histology: CosmicType, val fragmentId: String,
    val genomeVersion: String, val chromosome: Int, // n.b. numeric values for chromosomes (x=23, y=24)
    val position: Int, val strand: String,
    val geneName: String, val methylation: String,
    val avgBetaValueNormal: Float, val betaValue: Float,
    val twoSidedPValue: Double
) {
    val nodename = "methylation"

    fun generateDiffMethylationCypher():String =
        generateMergeCypher()
            .plus(site.generateCosmicTypeCypher(nodename))
            .plus(histology.generateCosmicTypeCypher(nodename))
            .plus(CosmicTumor.generateChildRelationshipCypher(tumorId, nodename))
            .plus(CosmicSample.generateChildRelationshipCypher(sampleId, nodename))
            .plus(" RETURN $nodename")

    private fun generateMergeCypher(): String = "CALL apoc.merge.node([\"CosmicDiffMethylation\"], " +
            " { key:apoc.create.uuid()}, {study_id: $studyId, " +
            " fragment_id: ${Neo4jUtils.formatPropertyValue(fragmentId)}, genome_version: " +
            " ${Neo4jUtils.formatPropertyValue(genomeVersion)}, chromosome: $chromosome," +
            " position: $position, strand: ${Neo4jUtils.formatPropertyValue(strand)}," +
            " gene_name: ${Neo4jUtils.formatPropertyValue(geneName)}, " +
            " methylation: ${Neo4jUtils.formatPropertyValue(methylation)}," +
            " avg_beta_value_normal: $avgBetaValueNormal, beta_value: $betaValue," +
            " two_sided_p_value: $twoSidedPValue, created: datetime()}, " +
            " { last_mod: datetime()}) YIELD node AS $nodename \n"

    companion object : AbstractModel {

        fun parseValueMap(value: Value): CosmicDiffMethylation =
            CosmicDiffMethylation(
                value["STUDY_ID"].asString().toInt(),
                value["ID_SAMPLE"].asString().toInt(),
                value["ID_TUMOUR"].asString().toInt(),
                resolveSiteType(value),
                resolveHistologySite(value),
                value["FRAGMENT_ID"].asString(),
                value["GENOME_VERSION"].asString(),
                value["CHROMOSOME"].asString().toInt(),  //Integer is OK here (x=23, y=24)
                value["POSITION"].asString().toInt(),
                when (value["STRAND"].asString().toInt()) {
                    1 -> "+"
                    else ->"-"
                },
                value["GENE_NAME"].asString(),
                value["METHYLATION"].asString(),
                value["AVG_BETA_VALUE_NORMAL"].asString().toFloat(),
                value["BETA_VALUE"].asString().toFloat(),
                value["TWO_SIDED_P_VALUE"].asString().toDouble()
            )

        private fun resolveSiteType(value: Value): CosmicType =
            CosmicType(
                "Site", value["PRIMARY_SITE"].asString(),
                value["SITE_SUBTYPE_1"].asString(),
                value["SITE_SUBTYPE_2"].asString(),
                value["SITE_SUBTYPE_3"].asString()
            )

        private fun resolveHistologySite(value: Value): CosmicType =
            CosmicType(
                "Histology", value["PRIMARY_HISTOLOGY"].asString(),
                value["HISTOLOGY_SUBTYPE_1"].asString(),
                value["HISTOLOGY_SUBTYPE_2"].asString(),
                value["HISTOLOGY_SUBTYPE_3"].asString()
            )

        private fun resolveMutationType(value: Value): CosmicType =
            CosmicType(
                "Mutation", value["MUT_TYPE"].asString()
            )
    }
}
