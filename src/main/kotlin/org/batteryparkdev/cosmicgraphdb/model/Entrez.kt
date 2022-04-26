package org.batteryparkdev.cosmicgraphdb.model

data class Entrez (val entrezId: Int ){

    companion object{
        val nodename = "entrez"
        private fun generatePlaceholderCypher(entrezId: Int): String =
            "CALL apoc.merge.node([\"Entrez\"], " +
                    "{entrez_id: $entrezId,  created: datetime()} )" +
                    "YIELD node AS ${Entrez.nodename} \n "
        fun generateHasEntrezRelationship(entrezId: Int, parentNodeName: String): String {
            val relationship = "HAS_ENTREZ"
            val relName = "rel_entrez"
            return generatePlaceholderCypher(entrezId)
                .plus(
                    " CALL apoc.merge.relationship ($parentNodeName, '$relationship' ," +
                            " {}, {created: datetime()}," +
                            " ${Entrez.nodename}, {}) YIELD rel AS $relName \n"
                )
        }
    }
}