package org.batteryparkdev.cosmicgraphdb.service

import arrow.core.Either
import java.io.InputStream
import java.util.*
import kotlin.random.Random

/**
 * Created by fcriscuo on 7/28/21.
 */
fun getEnvVariable(varname:String):String = System.getenv(varname) ?: "undefined"

fun getDrugBankCredentials():Pair<String,String> =
    Pair(getEnvVariable("DRUG_BANK_USER"),
        getEnvVariable("DRUGBANK_PASSWORD"))

fun executeCurlOperation( curlCommand:String): Either<Exception, InputStream> {
    try {
        val proc = Runtime.getRuntime().exec(curlCommand)
        return Either.Right(proc.inputStream)
    } catch (e: Exception) {
        return Either.Left(e)
    }
}

fun generateNeo4jNodeKey():String  = UUID.randomUUID().toString()
/*
Excerpted From
Kotlin Coroutines Deep Dive
Marcin Moskała
 */
private fun generateUniqueString(
    length: Int,
    seed: Long = System.currentTimeMillis()
): Sequence<String> = sequence {
    val random = Random(seed)
    val charPool = ('a'..'z') + ('A'..'Z') + ('0'..'9')
    while (true) {
        val randomString = (1..length)
            .map { i -> random.nextInt(charPool.size) }
            .map(charPool::get)
            .joinToString("");
        yield(randomString)
    }
}.distinct()

