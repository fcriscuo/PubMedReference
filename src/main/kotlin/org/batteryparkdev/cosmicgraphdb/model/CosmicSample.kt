package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.neo4j.service.Neo4jUtils
import org.neo4j.driver.Value

data class CosmicSample(
    val sampleId: Int,
    val sampleName: String,
    val tumorId: Int,
    val site: CosmicType,
    val histology: CosmicType,
    val therapyRelationship: String,
    val sampleDifferentiator: String,
    val mutationAlleleSpecification: String,
    val msi: String,
    val averagePloidy: String,
    val wholeGeneomeScreen: Boolean,
    val wholeExomeScreen: Boolean,
    val sampleRemark: String,
    val drugResponse: String,
    val grade: String,
    val ageAtTumorRecurrence: Int,
    val stage: String,
    val cytogenetics: String,
    val metastaticSite: String,
    val tumorSource: String,
    val tumorRemark: String,
    val age: Int,
    val ethnicity: String,
    val environmentalVariables: String,
    val germlineMutation: String,
    val therapy: String,
    val family: String,
    val normalTissueTested: Boolean,
    val gender: String,
    val individualRemark: String,
    val nciCode: String,
    val sampleType: String,
    val cosmicPhenotypeId: String
) {

    // function to resolve the correct identifier for the CosmicClassification
    private fun resolveClassificationId(): Int =
        (cosmicPhenotypeId.plus(site.primary)
            .plus(site.subtype1)).hashCode()

    fun generateCosmicSampleCypher(): String =
        generateMergeCypher()
            .plus(site.generateCosmicTypeCypher(CosmicSample.nodename))
            .plus(histology.generateCosmicTypeCypher(CosmicSample.nodename))
           // .plus(CosmicTumor.generateChildRelationshipCypher(tumorId, CosmicSample.nodename))
            .plus(CosmicClassification.generateChildRelationshipCypher(resolveClassificationId(),CosmicSample.nodename ))
            .plus(" RETURN ${CosmicSample.nodename}\n")

    private fun generateMergeCypher(): String =
        "CALL apoc.merge.node( [\"CosmicSample\"], " +
                " {sample_id: $sampleId}," +
                " {sample_name: ${Neo4jUtils.formatPropertyValue(sampleName)}," +
                " therapy_relationship: ${Neo4jUtils.formatPropertyValue(therapyRelationship)}," +
                " sample_differentiator: ${Neo4jUtils.formatPropertyValue(therapyRelationship)}, " +
                " mutation_allele_specfication: ${Neo4jUtils.formatPropertyValue(mutationAlleleSpecification)}, " +
                " msi: ${Neo4jUtils.formatPropertyValue(msi)}, average_ploidy: " +
                " ${Neo4jUtils.formatPropertyValue(averagePloidy)}, whole_genome_screen: $wholeGeneomeScreen, " +
                " whole_exome_screen: $wholeExomeScreen, sample_remark: ${Neo4jUtils.formatPropertyValue(sampleRemark)}, " +
                " drug_respose: ${Neo4jUtils.formatPropertyValue(drugResponse)}, " +
                " grade: ${Neo4jUtils.formatPropertyValue(grade)}, age_at_tumor_recurrance: $ageAtTumorRecurrence, " +
                " stage: ${Neo4jUtils.formatPropertyValue(stage)}, cytogenetics: " +
                " ${Neo4jUtils.formatPropertyValue(cytogenetics)}, metastatic_site: " +
                " ${Neo4jUtils.formatPropertyValue(metastaticSite)}, tumor_source: " +
                " ${Neo4jUtils.formatPropertyValue(tumorSource)}, tumor_remark: " +
                " ${Neo4jUtils.formatPropertyValue(tumorRemark)}, age: $age ," +
                " ethnicity: ${Neo4jUtils.formatPropertyValue(ethnicity)}, environmental_variables: " +
                " ${Neo4jUtils.formatPropertyValue(environmentalVariables)}, germline_mutation: " +
                " ${Neo4jUtils.formatPropertyValue(germlineMutation)}, therapy: " +
                " ${Neo4jUtils.formatPropertyValue(therapy)}, family: ${Neo4jUtils.formatPropertyValue(family)}, " +
                " normal_tissue_tested: $normalTissueTested, gender: ${Neo4jUtils.formatPropertyValue(gender)}, " +
                " individual_remark: ${Neo4jUtils.formatPropertyValue(individualRemark)}, " +
                " nci_code: ${Neo4jUtils.formatPropertyValue(nciCode)}, sample_type: " +
                " ${Neo4jUtils.formatPropertyValue(sampleType)}, cosmic_phenotype_id: " +
                " ${Neo4jUtils.formatPropertyValue(cosmicPhenotypeId)}," +
                " created: datetime()}) YIELD node as ${CosmicSample.nodename} \n"


    companion object : AbstractModel {
        const val nodename = "sample"
        const val classificationPrefix = "COSO"  // the classification file uses a prefix, the sample file does not
                                               // COSO36736185  vs  36736185
        fun parseValueMap(value: Value): CosmicSample =
            CosmicSample(
                value["sample_id"].asString().toInt(), value["sample_name"].asString(),
                value["id_tumour"].asString().toInt(),
                resolveSiteType(value),
                resolveHistologyType(value),
                value["therapy_relationship"].asString(),
                value["sample_differentiator"].asString(),
                value["mutation_allele_specification"].asString(),
                value["msi"].asString(), value["average_ploidy"].asString(),
                convertYNtoBoolean(value["whole_genome_screen"].asString()),
                convertYNtoBoolean(value["whole_exome_screen"].asString()),
                removeInternalQuotes(value["sample_remark"].asString()),
                value["drug_response"].asString(),
                value["grade"].asString(),
                parseValidIntegerFromString(value["age_at_tumour_recurrence"].asString()),
                value["stage"].asString(), value["cytogenetics"].asString(),
                value["metastatic_site"].asString(),
                value["tumour_source"].asString(),
                removeInternalQuotes(value["tumour_remark"].asString()),
                parseValidIntegerFromString(value["age"].asString()),
                value["ethnicity"].asString(), value["environmental_variables"].asString(),
                value["germline_mutation"].asString(),
                value["therapy"].asString(), value["family"].asString(),
                convertYNtoBoolean(value["normal_tissue_tested"].asString()),
                value["gender"].asString(), value["individual_remark"].asString(),
                value["nci_code"].asString(),
                value["sample_type"].asString(),
                classificationPrefix.plus(value["cosmic_phenotype_id"].asString())
            )

        private fun resolveSiteType(value: Value): CosmicType =
            CosmicType(
                "Site", value["primary_site"].asString(),
                value["site_subtype_1"].asString(),
                value["site_subtype_2"].asString(),
                value["site_subtype_3"].asString()
            )

        private fun resolveHistologyType(value: Value): CosmicType =
            CosmicType(
                "Histology", value["primary_histology"].asString(),
                value["histology_subtype_1"].asString(),
                value["histology_subtype_2 "].asString(),
                value["histology_subtype_3"].asString()
            )

        fun generateMatchCosmicSampleCypher(sampleId: Int)  =
            "CALL apoc.merge.node ([\"CosmicSample\"],{sample_id: $sampleId},{} ) YIELD node AS sample\n"

        /*
        Functions to generate Cypher a placeholder CosmicSample node if necessary
        and Cypher to create a Sample -[HAS child] -> child  relationship
         */
        private fun generateSamplePlaceholderCypher(sampleId: Int): String =
            " CALL apoc.merge.node( [\"CosmicSample\"], " +
                    "{sample_id: $sampleId, created: datetime()}) " +
                    " YIELD node as ${CosmicSample.nodename} \n"

        fun generateChildRelationshipCypher(sampleId: Int, childLabel: String): String {
            val relationship = "HAS_".plus(childLabel.uppercase())
            val relname = "rel_sample"
            return generateSamplePlaceholderCypher(sampleId).plus(
                "CALL apoc.merge.relationship( ${CosmicSample.nodename}, '$relationship', " +
                        " {}, {created: datetime()}, $childLabel,{} )" +
                        " YIELD rel AS $relname \n"
            )
        }
    }
}

