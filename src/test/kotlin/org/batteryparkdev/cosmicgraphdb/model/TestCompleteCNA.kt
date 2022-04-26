package org.batteryparkdev.cosmicgraphdb.model

import org.batteryparkdev.cosmicgraphdb.io.ApocFileReader


class TestCompleteCNA {
    fun parseCNAFile(filename: String): Int {
        val LIMIT = 100L // limit the number of records processed
        var recordCount = 0
        ApocFileReader.processDelimitedFile(filename).stream()
            .limit(LIMIT)
            .map { record -> record.get("map") }
            .map { CosmicCompleteCNA.parseValueMap(it) }
            .forEach { cna ->
                println(
                    "CosmicCNA Id= ${cna.cnvId}  " +
                            "  Tumor Id = ${cna.tumorId}   \n" +
                            "     Gene: ${cna.geneId}  ${cna.geneSymbol} " +
                            "  ${cna.chromosomeStartStop}  Minor Allele: ${cna.minorAllele} \n" +
                            "     Site: ${cna.site.primary}  Histology: ${cna.histology.primary} " +
                            "  Mutation Type: ${cna.mutationType.primary}   Sample Id: ${cna.sampleId} \n"

                )
                recordCount += 1
            }
        return recordCount
    }
}

fun main() {
    val cosmicCNAFile = "/Volumes/SSD870/COSMIC_rel95/sample/CosmicCompleteCNA.tsv"
    println("Processing COSMIC CNA file $cosmicCNAFile")
    val recordCount = TestCompleteCNA().parseCNAFile(cosmicCNAFile)
    println("Record count = $recordCount")
}