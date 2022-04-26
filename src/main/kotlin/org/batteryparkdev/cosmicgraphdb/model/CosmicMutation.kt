package org.batteryparkdev.cosmicgraphdb.model

import org.apache.commons.csv.CSVRecord
import java.nio.file.Paths
import org.batteryparkdev.io.TsvRecordSequenceSupplier
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.neo4j.driver.Value

data class CosmicMutation(
    val geneSymbol: String,
    val genomicMutationId: String, val geneCDSLength:Int,
    val hgncId: Int,
    val mutationId: Int, val mutationCds: String,
    val mutationAA: String, val mutationDescription: String, val mutationZygosity: String,
    val LOH: String, val GRCh: String, val mutationGenomePosition: String,
    val mutationStrand: String, val resistanceMutation: String,
    val fathmmPrediction: String, val fathmmScore: Double, val mutationSomaticStatus: String,
    val pubmedId: Int, val genomeWideScreen:Boolean,
    val hgvsp: String, val hgvsc: String, val hgvsg: String, val tier: String,
    val tumor: CosmicTumor


) {
    fun generateCosmicMutationCypher(): String = generateMergeCypher()
        .plus(tumor.generateCosmicTumorCypher())
        .plus(generateTumorMutationRelationshipCypher())
        .plus(generateGeneRelationshipCypher())
        .plus(generateHGNCRelationshipCypher())
        .plus(" RETURN ${CosmicMutation.nodename}")

     private fun generateTumorMutationRelationshipCypher(): String {
         val relationship = "HAS_MUTATION"
         val relname = "rel_tumor_mut"
         return "CALL apoc.merge.relationship(${CosmicTumor.nodename}, '$relationship', " +
                     " {}, {created: datetime()}, ${CosmicMutation.nodename},{} )" +
                     " YIELD rel as $relname \n"
     }

    private fun generateMergeCypher(): String =
        " CALL apoc.merge.node( [\"CosmicMutation\"], " +
                " {mutation_id: $mutationId}, " +
                " {gene_symbol: ${Neo4jUtils.formatPropertyValue(geneSymbol)}, " +
                "  genomic_mutation_id: ${Neo4jUtils.formatPropertyValue(genomicMutationId)}," +
                "  gene_cds_length: $geneCDSLength, " +
                " mutation_cds: ${Neo4jUtils.formatPropertyValue(mutationCds)}," +
                " mutation_aa: ${Neo4jUtils.formatPropertyValue(mutationAA)}, " +
                " description: ${Neo4jUtils.formatPropertyValue(mutationDescription)}," +
                " zygosity: ${Neo4jUtils.formatPropertyValue(mutationZygosity)}, " +
                " loh: ${Neo4jUtils.formatPropertyValue(LOH)}, " +
                " grch: ${Neo4jUtils.formatPropertyValue(GRCh)}, " +
                " genome_position: ${Neo4jUtils.formatPropertyValue(mutationGenomePosition)}, " +
                " strand: ${Neo4jUtils.formatPropertyValue(mutationStrand)}, " +
                " resistance_mutation: ${Neo4jUtils.formatPropertyValue(resistanceMutation)}, " +
                " fathmm_prediction: ${Neo4jUtils.formatPropertyValue(fathmmPrediction)}, " +
                " fathmm_score: $fathmmScore, " +
                " somatic_status: ${Neo4jUtils.formatPropertyValue(mutationSomaticStatus)}, " +
                " pubmed_id: $pubmedId, genome_wide_screen: $genomeWideScreen, " +
                " hgvsp: ${Neo4jUtils.formatPropertyValue(hgvsp)}, " +
                " hgvsc: ${Neo4jUtils.formatPropertyValue(hgvsc)}, " +
                " hgvsq: ${Neo4jUtils.formatPropertyValue(hgvsg)}, " +
                " tier: ${Neo4jUtils.formatPropertyValue(tier)}, " +
                "  created: datetime()}) YIELD node as ${CosmicMutation.nodename} \n"

    /*
   Function to generate Cypher commands to create a
   Mutation - [HAS_GENE] -> Gene   relationship
    */
    private fun generateGeneRelationshipCypher(): String =
        CosmicGeneCensus.generateHasGeneRelationshipCypher(geneSymbol,CosmicMutation.nodename)

  private fun generateHGNCRelationshipCypher(): String =
      CosmicHGNC.generateHasHGNCRelationshipCypher(hgncId, CosmicMutation.nodename)

    companion object : AbstractModel {
        const val nodename = "mutation"

        private fun generateMutationPlaceholderCypher(mutationId: Int): String =
            "CALL apoc.merge.node( [\"CosmicMutation\"], " +
                    " {mutation_id: $mutationId,  created: datetime()}) " +
                    " YIELD node AS ${CosmicMutation.nodename}\n "

        fun generateChildRelationshipCypher(mutationId: Int, childLabel: String): String {
            val relationship = "HAS_".plus(childLabel.uppercase())
            val relname = "rel_mutation"
            return generateMutationPlaceholderCypher(mutationId).plus(
                "CALL apoc.merge.relationship(${CosmicMutation.nodename}, '$relationship', " +
                        " {}, {created: datetime()}, ${childLabel.lowercase()},{} )" +
                        " YIELD rel as $relname \n"
            )
        }

        fun parseValueMap(value: Value): CosmicMutation =
            CosmicMutation(
                value["Gene name"].asString(),   // actually HGNC approved symbol
                value["GENOMIC_MUTATION_ID"].asString(),
                value["Gene CDS length"].asString().toInt(),
                value["HGNC ID"].asString().toInt(),
                value["MUTATION_ID"].asString().toInt(),
                value["Mutation CDS"].asString(),
                value["Mutation AA"].asString(),
                value["Mutation Description"].asString(),
                value["Mutation zygosity"].asString() ?: "",
                value["LOH"].asString() ?: "",
                value["GRCh"].asString() ?: "38",
                value["Mutation genome position"].asString(),
                value["Mutation strand"].asString(),
                value["Resistance Mutation"].asString(),
                value["FATHMM prediction"].asString(),
                parseValidDoubleFromString(value["FATHMM score"].asString()),
                value["Mutation somatic status"].asString(),
                parseValidIntegerFromString(value["Pubmed_PMID"].asString()),
                convertYNtoBoolean(value["Genome-wide screen"].asString()),
                value["HGVSP"].asString(),
                value["HGVSC"].asString(),
                value["HGVSG"].asString(),
                resolveTier(value),
                CosmicTumor.parseValueMap(value)
            )


        /*
               Not all mutation files have a Tier column
          */
        private fun resolveTier(value:Value):String =
            when(value.keys().contains("Tier")) {
                true -> value["Tier"].asString()
                false -> ""
            }

    }
}