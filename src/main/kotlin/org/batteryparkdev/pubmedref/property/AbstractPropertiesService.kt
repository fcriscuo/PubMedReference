package org.batteryparkdev.pubmedref.property

import arrow.core.*
import com.google.common.flogger.FluentLogger
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

/**
 * Created by fcriscuo on 7/27/21.
 */
abstract class AbstractPropertiesService {
     private val properties: Properties = Properties()
    private val logger: FluentLogger = FluentLogger.forEnclosingClass();
    fun resolveFrameworkProperties(propertiesFile: String) {
        val stream = AbstractPropertiesService::class.java.getResourceAsStream(propertiesFile)
        //val p = Properties()
        properties.load(stream)
    }

    fun resolvePropertyAsStringPair(propertyName: String):Pair<String, String> =
        if( properties.contains(propertyName))
           Pair(propertyName, properties.getProperty(propertyName).toString())
        else Pair(propertyName,"")

    //TODO: This should be using arrow-kt Either as a return type
    //      resolve why that no longer works
    fun resolvePropertyAsString(propertyName: String): String? =
        if (properties.containsKey(propertyName)) {
            logger.atInfo().log("Property Value: ${properties.getProperty(propertyName)}")
            properties.getProperty(propertyName).toString()
        } else {
            logger.atWarning().log( "$propertyName is an invalid property name " )
            null
        }


    private fun resolvePropertyAsStringOption(propertyName: String): Option<String> =
        if (properties.containsKey(propertyName)) {
            logger.atInfo().log("Property Value: ${properties.getProperty(propertyName)}")
            Some(properties.getProperty(propertyName).toString())
        } else {
            logger.atWarning().log( "$propertyName is an invalid property name " )
            None
        }

    fun resolvePropertyAsInt(propertyName: String): Int? =
        if (properties.containsKey(propertyName)) {
            properties.getProperty(propertyName).toIntOrNull()
        } else {
            null
        }

    fun resolvePropertyAsLong(propertyName: String): Long =
        if (properties.containsKey(propertyName)) {
            properties.getProperty(propertyName).toLongOrNull() ?: 0L
        } else {
            0L
        }

    fun resolvePropertyAsResourcePathOption(propertyName: String): Option<Path> {
        val propertyOption = resolvePropertyAsStringOption(propertyName)
        if (propertyOption.nonEmpty()) {
            return propertyOption.map { name -> URI(name) }
                .map { uri -> Paths.get(uri) }
        }
        return None
    }

    fun resolvePropertyAsPathOption(propertyName: String): Option<Path> {
        val propertyOption = resolvePropertyAsStringOption(propertyName)
        if (propertyOption.nonEmpty()) {
            return propertyOption.map { path -> Paths.get(path) }
        }
        logger.atInfo().log( "Requested Path property: $propertyName is invalid" )
        return None
    }

    fun filterProperties(filter: String): List<Pair<String,String>> {
        var tmpList = mutableListOf<Pair<String,String>>()
        properties.keys.filter{ it -> it.toString().contains(filter) }
            .map { key -> tmpList.add(Pair(key.toString(),properties.get(key).toString())) }

        return tmpList.toList()
    }
    fun displayProperties() {
        properties.keys.forEach { key ->
            println("key: $key  value: ${properties.get(key)}")
        }
    }
}