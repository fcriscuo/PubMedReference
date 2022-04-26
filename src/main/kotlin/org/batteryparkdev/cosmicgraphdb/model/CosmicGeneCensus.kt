package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.service.TumorTypeService
import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.neo4j.driver.Value

data class CosmicGeneCensus(

    val geneSymbol: String, val geneName: String, val entrezGeneId: String,
    val genomeLocation: String, val tier: Int = 0, val hallmark: Boolean = false,
    val chromosomeBand: String, val somatic: Boolean = false, val germline: Boolean,
    val somaticTumorTypeList: List<String>, val germlineTumorTypeList: List<String>,
    val cancerSyndrome: String, val tissueTypeList: List<String>, val molecularGenetics: String,
    val roleInCancerList: List<String>, val mutationTypeList: List<String>,
    val translocationPartnerList: List<String>,
    val otherGermlineMut: String, val otherSyndromeList: List<String>, val synonymList: List<String>
) {
    fun generateCosmicGeneCypher(): String =
        generateMergeCypher()
            .plus(
                CosmicAnnotationFunctions.generateAnnotationCypher(
                    somaticTumorTypeList,
                    "SomaticTumorType", CosmicGeneCensus.nodename
                )
            )
            .plus(
                CosmicAnnotationFunctions.generateAnnotationCypher(
                    germlineTumorTypeList, "GermlineTumorType",
                    CosmicGeneCensus.nodename
                )
            )
            .plus(
                CosmicAnnotationFunctions.generateAnnotationCypher(
                    tissueTypeList,
                    "TissueType", CosmicGeneCensus.nodename
                )
            )
            .plus(
                CosmicAnnotationFunctions.generateAnnotationCypher(
                    roleInCancerList,
                    "RoleInCancer", CosmicGeneCensus.nodename
                )
            )
            .plus(
                CosmicAnnotationFunctions.generateAnnotationCypher(
                    mutationTypeList, "MutationType",
                    CosmicGeneCensus.nodename
                )
            )
            .plus(
                CosmicAnnotationFunctions.generateAnnotationCypher(
                    otherSyndromeList, "OtherSyndrome",
                    CosmicGeneCensus.nodename
                )
            )
            .plus(
                CosmicAnnotationFunctions.generateAnnotationCypher(
                    synonymList, "Synonym",
                    CosmicGeneCensus.nodename
                )
            )
            .plus(CosmicAnnotationFunctions.generateTranslocationCypher(translocationPartnerList))
            .plus(" RETURN ${CosmicGeneCensus.nodename}")

    private fun generateMergeCypher(): String =
        "CALL apoc.merge.node( [\"CosmicGene\", \"CosmicCensus\"]," +
                "{  gene_symbol: ${Neo4jUtils.formatPropertyValue(geneSymbol)}}," +
                " {gene_name: ${Neo4jUtils.formatPropertyValue(geneName)}," +
                " entrez_gene_id: ${Neo4jUtils.formatPropertyValue(entrezGeneId)}," +
                " genome_location: ${Neo4jUtils.formatPropertyValue(genomeLocation)}," +
                " tier: $tier, hallmark: $hallmark, " +
                " chromosome_band: $chromosomeBand, " +
                " somatic: $somatic, germline: $germline, " +
                " cancer_syndrome: ${Neo4jUtils.formatPropertyValue(cancerSyndrome)}," +
                " molecular_genetics: $molecularGenetics, " +
                " other_germline_mut: ${Neo4jUtils.formatPropertyValue(otherGermlineMut)}," +
                "  created: datetime()}) YIELD node as ${CosmicGeneCensus.nodename} \n"

    companion object : AbstractModel {
        const val nodename = "gene"

        private fun generatePlaceholderCypher(geneSymbol: String): String = " CALL apoc.merge.node([\"CosmicGene\"]," +
                " { gene_symbol: ${Neo4jUtils.formatPropertyValue(geneSymbol)}, created: datetime()} ) " +
                " YIELD node as ${CosmicGeneCensus.nodename} \n"

        fun generateHasGeneRelationshipCypher(geneSymbol: String, parentNodeName: String): String {
            val relationship = "HAS_GENE"
            val relName = "rel_gene"
            return generatePlaceholderCypher(geneSymbol).plus(
                " CALL apoc.merge.relationship($parentNodeName, '$relationship' ," +
                        " {}, {created: datetime()}," +
                        " ${CosmicGeneCensus.nodename}, {}) YIELD rel AS $relName \n"
            )
        }

        fun parseValueMap(value: Value): CosmicGeneCensus =
            CosmicGeneCensus(
                value["Gene Symbol"].asString(),
                value["Name"].asString(),
                value["Entrez GeneId"].asString(),
                value["Genome Location"].asString(),
                value["Tier"].asString().toInt(),
                value["Hallmark"].toString().isNotBlank(),
                value["Chr Band"].toString(),
                value["Somatic"].toString().isNotBlank(),
                value["Germline"].toString().isNotBlank(),
                processTumorTypes(value["Tumour Types(Somatic)"].asString()),
                processTumorTypes(value["Tumour Types(Germline)"].asString()),
                value["Cancer Syndrome"].asString(),
                parseStringOnComma(value["Tissue Type"].asString()),
                value["Molecular Genetics"].toString(),
                parseStringOnComma(value["Role in Cancer"].asString()),
                parseStringOnComma(value["Mutation Types"].asString()),
                parseStringOnComma(value["Translocation Partner"].asString()),
                value["Other Germline Mut"].asString(),
                parseStringOnSemiColon(value["Other Syndrome"].asString()),
                parseStringOnComma(value["Synonyms"].asString())
            )

        /*
    Function to resolve a tumor type abbreviations
     */
        private fun processTumorTypes(tumorTypes: String): List<String> {
            val tumorTypeList = mutableListOf<String>()
            parseStringOnComma(tumorTypes).forEach {
                tumorTypeList.add(TumorTypeService.resolveTumorType(it))
            }
            return tumorTypeList.toList()  // make List immutable
        }
    }
}

