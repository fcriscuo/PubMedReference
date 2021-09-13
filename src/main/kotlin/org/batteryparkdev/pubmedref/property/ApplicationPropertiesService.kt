package org.batteryparkdev.pubmedref.property

/**
 * * Services responsible for resolving property values from
 * specified properties files
 *
 */
object ApplicationPropertiesService : AbstractPropertiesService() {
    private const val PROPERTIES_FILE = "/application.properties"

    init {
        resolveFrameworkProperties(PROPERTIES_FILE)
    }
}


