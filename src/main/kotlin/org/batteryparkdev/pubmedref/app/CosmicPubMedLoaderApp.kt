package org.batteryparkdev.pubmedref.app

import com.google.common.base.Stopwatch
import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.service.TsvRecordSequenceSupplier
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

/*
Application that will load data for PubMed entries cited in a specified COSMIC
mutant export file. The loaded PubMed entries include the specified PubMed articles,
the PubMed entries referenced by those articles, and the PubMed articles that
cite the specified article.
 */

class CosmicPubMedLoaderApp  (val cosmicTsvFile: String){
    private val logger: FluentLogger = FluentLogger.forEnclosingClass();
    private val pubmedIdCol = "Pubmed_PMID"
    // For COSMIC data, The PubMed Ids to be loaded are in a CosmicMutantExport-formatted file

    fun loadCosmicPubMedData() {
        val path = Paths.get(cosmicTsvFile)
        val app = PubMedGraphApp()
        TsvRecordSequenceSupplier(path).get().chunked(500)
            .forEach { it ->
                it.stream()
                    .map { parseValidIntegerFromString(it.get(pubmedIdCol)) }
                    .filter {it > 0  }  // ignore records w/o pubmedId value
                    .forEach{ app.processPubMedNodeById(it)}
            }
    }
}
private fun parseValidIntegerFromString(s: String): Int =
    when (s.toIntOrNull()) {
        null -> 0
        else  -> s.toInt()
    }

fun main(args: Array<String>) {
    val cosmicTsvFile =
        when (args.size > 0) {
            true -> args[0]
            false -> "./data/sample_CosmicMutantExportCensus.tsv"
        }
    println("This application implements a 300 millisecond delay between NCBI Web-based requests " +
            "to accommodate NCBI's request rate limit")
    println("As a result, loading the PubMed entries referenced in COSMIC will take a considerable amount of time")

    if (cosmicTsvFile.isNotEmpty()) {
        val timer = Stopwatch.createStarted()
        CosmicPubMedLoaderApp(cosmicTsvFile).loadCosmicPubMedData()
        timer.stop()
        println("++++ COSMIC PubMed data loaded  in ${timer.elapsed(TimeUnit.MINUTES)} minutes+++")
    }

}