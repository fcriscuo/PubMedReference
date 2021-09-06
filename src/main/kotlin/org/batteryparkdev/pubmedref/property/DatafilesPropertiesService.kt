package org.batteryparkdev.pubmedref.property

/**
 * * Services responsible for resolving property values from
 * specified properties files
 *
 */
object DatafilesPropertiesService : AbstractPropertiesService() {
    private const val PROPERTIES_FILE = "/datafiles.properties"

    init {
        resolveFrameworkProperties(PROPERTIES_FILE)
    }
}

object FrameworkPropertiesService : AbstractPropertiesService() {
    private const val PROPERTIES_FILE = "/framework.properties"

    init {
        resolveFrameworkProperties(PROPERTIES_FILE)
    }
}

