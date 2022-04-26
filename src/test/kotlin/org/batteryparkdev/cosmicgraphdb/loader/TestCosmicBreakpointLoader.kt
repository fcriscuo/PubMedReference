package org.batteryparkdev.cosmicgraphdb.loader

import org.batteryparkdev.cosmicgraphdb.model.TestCosmicBreakpoint

class TestCosmicBreakpointLoader {
}
fun main() {
    val recordCount =
       CosmicBreakpointLoader.loadCosmicBreakpointData("/Volumes/SSD870/COSMIC_rel95/sample/CosmicBreakpointsExport.tsv")
    println("Breakpoint record count = $recordCount")
}