package org.batteryparkdev.cosmicgraphdb.service

import org.batteryparkdev.io.TsvRecordSequenceSupplier
import java.nio.file.Paths

/*
Represents a service class that will provide the full tumor
type name for a specified tumor type abbreviation
Mapping obtained from COSMIC: https://cancer.sanger.ac.uk/census#cl_download
in the Abbreviations section

 */
object TumorTypeService {
    private val abbreviationsFilePath = Paths.get("/Volumes/SSD870/COSMIC_rel95/CosmicTumorTypeAbbreviations.tsv")
    private val abbreviationsMap = mutableMapOf<String, String>()
    init{
        TsvRecordSequenceSupplier(abbreviationsFilePath).get().forEach {
            record -> abbreviationsMap.put(record.get("Abbreviation"), record.get("TumorType"))
        }
    }

    fun resolveTumorType(abbreviation:String): String =
        when(abbreviationsMap.containsKey(abbreviation)) {
            true -> abbreviationsMap.get(abbreviation) ?: ""
            false -> abbreviation  // return the input
        }

}

fun main() {
    println("Abbreviation = NHL   tumor type = ${TumorTypeService.resolveTumorType("NHL")}")
}