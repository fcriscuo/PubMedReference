package org.batteryparkdev.pubmedref.service

import arrow.core.Either
import java.io.InputStream

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