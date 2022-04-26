package org.batteryparkdev.cosmicgraphdb.model

class TestCosmicAnnotationFunctions {

    fun runTest(annotations: List<String>, secondaryLabel:String,  parentName: String){
        val cypher = CosmicAnnotationFunctions.generateAnnotationCypher(annotations, secondaryLabel, parentName)
        println(cypher)
    }
}
 fun main() {
     val annotations =  listOf<String>("Gene01", "Gene02", "Gene03", "GeneXX")
     val secondaryLabel = "GeneType"
     val parentName = "gene"
     TestCosmicAnnotationFunctions().runTest(annotations, secondaryLabel, parentName)
 }
