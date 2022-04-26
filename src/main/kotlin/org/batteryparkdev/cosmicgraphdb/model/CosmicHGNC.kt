package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.neo4j.driver.Value

/*
COSMIC_ID	COSMIC_GENE_NAME	Entrez_id	HGNC_ID	Mutated?	Cancer_census?	Expert Curated?
 */
data class CosmicHGNC(
    val cosmicId: Int,
    val hgncGeneSymbol: String,
    val entrezId: Int,
    val hgncId: Int,
    val isMutated: Boolean,
    val isCancerCensus: Boolean,
    val isExpertCurrated: Boolean
) {

    fun generateCosmicHGNCCypher(): String =
        generateMergeCypher()
            .plus(generateGeneRelationshipCypher())
            .plus(generateEntrezRelationshipCypher())
            .plus(" RETURN ${CosmicHGNC.nodename}")

    private fun generateMergeCypher(): String =
        " CALL apoc.merge.node( [\"CosmicHGNC\"], " +
                " {hgnc_id: $hgncId}," +
                " {cosmic_id: $cosmicId, gene_symbol: ${Neo4jUtils.formatPropertyValue(hgncGeneSymbol)}, " +
                " entrez_id: $entrezId, is_mutaated: $isMutated, is_cancer_census: $isCancerCensus, " +
                " is_expert_currated: $isExpertCurrated, " +
                "  created: datetime()}) YIELD node as ${CosmicHGNC.nodename} \n"

    /*
   Function to generate Cypher commands to create a
   HGNC - [HAS_GENE] -> Gene relationship
    */
    private fun generateGeneRelationshipCypher(): String =
        when (isCancerCensus) {
            true -> CosmicGeneCensus.generateHasGeneRelationshipCypher(hgncGeneSymbol, CosmicHGNC.nodename)
            false -> " "
        }

    private fun generateEntrezRelationshipCypher(): String =
        when (entrezId > 0) {
            true -> Entrez.generateHasEntrezRelationship(entrezId, CosmicHGNC.nodename)
            false -> " "
        }

    companion object : AbstractModel {
        const val nodename = "hgnc"

        /*
         convertYNtoBoolean(record.get("Mutated?")),
         */
        fun parseValueMap(value: Value): CosmicHGNC =
            CosmicHGNC(
                value["COSMIC_ID"].asString().toInt(),
                value["COSMIC_GENE_NAME"].asString(),
                value["Entrez_id"].asString().toInt(),
                value["HGNC_ID"].asString().toInt(),
                convertYNtoBoolean(value["Mutated?"].asString()),
                convertYNtoBoolean(value["Cancer_census?"].asString()),
                convertYNtoBoolean(value["Expert Curated?"].asString())
            )
        private fun generatePlaceholderCypher(hgncId: Int): String = " CALL apoc.merge.node([\"CosmicHGNC\"]," +
                " { hgnc_id: $hgncId}, {created: datetime()} ) " +
                " YIELD node as ${CosmicHGNC.nodename} \n"

        fun generateHasHGNCRelationshipCypher(hgncId: Int, parentNodeName: String): String {
            val relationship = "HAS_HGNC"
            val relName = "rel_hgnc"
            return generatePlaceholderCypher(hgncId).plus(
                " CALL apoc.merge.relationship ($parentNodeName, '$relationship' ," +
                        " {}, {created: datetime()}," +
                        " ${CosmicHGNC.nodename}, {}) YIELD rel AS $relName \n"
            )
        }
    }
}
