package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.service.generateNeo4jNodeKey
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.neo4j.driver.Value

data class CosmicResistanceMutation(
    val resistanceId: String,
    val mutationId: Int,
    val sampleId: Int,
    val geneSymbol: String,
    val transcript: String,
    val drugName: String,
    val censusGene: String,
    val pubmedId: Int
) {

    fun generateCosmicResistanceCypher(): String =generateMergeCypher()
        .plus(CosmicMutation.generateChildRelationshipCypher(mutationId, CosmicResistanceMutation.nodename))
        .plus(CosmicSample.generateChildRelationshipCypher(sampleId, CosmicResistanceMutation.nodename))
        .plus(generateDrugRelationshipCypher())
        .plus(" RETURN ${CosmicResistanceMutation.nodename}\n")

    private fun generateMergeCypher(): String =
        " CALL apoc.merge.node([\"DrugResistance\"], " +
                " {resistance_id: ${Neo4jUtils.formatPropertyValue(resistanceId)}}," +
                "  { mutation_id: $mutationId, " +
                " gene_symbol: ${Neo4jUtils.formatPropertyValue(geneSymbol)}," +
                " transcript: ${Neo4jUtils.formatPropertyValue(transcript)}, " +
                " pubmed_id: $pubmedId, " +
                "  created: datetime()}) YIELD node as ${CosmicResistanceMutation.nodename} \n"

    private fun generateDrugMergeCypher(): String  =
        "CALL apoc.merge.node( [\"CosmicDrug\"], " +
                " {drug_name: ${Neo4jUtils.formatPropertyValue(drugName.lowercase())}}, { created: datetime()} )" +
                " YIELD node AS drug \n"
    private fun generateDrugRelationshipCypher(): String {
        val relationship = "RESISTANT_TO"
        val relname = "rel_drug"
        return generateDrugMergeCypher().plus(
            "CALL apoc.merge.relationship(${CosmicResistanceMutation.nodename}, '$relationship', " +
                    " {}, {created: datetime()}, drug,{} )" +
                    " YIELD rel as $relname \n"
        )
    }

    companion object : AbstractModel {
        const val nodename = "resistance"
        fun parseValueMap(value: Value): CosmicResistanceMutation =
            CosmicResistanceMutation(
                generateNeo4jNodeKey(),
                value["MUTATION_ID"].asString().toInt(),
                value["Sample ID"].asString().toInt(),
                value["Gene Name"].asString(),
                value["Transcript"].asString(),
                value["Drug Name"].asString(),
                value["Census Gene"].asString().uppercase(),
                value["Pubmed Id"].asString().toInt()
            )

    }
}

