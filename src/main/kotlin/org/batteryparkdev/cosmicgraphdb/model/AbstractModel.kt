package org.batteryparkdev.cosmicgraphdb.model

/**
 * Created by fcriscuo on 2021Jul29
 */
interface AbstractModel {

    fun convertYNtoBoolean(ynValue:String): Boolean =
        ynValue.lowercase() == "y"
    /*
   def isValidString(x:String):Boolean = x != null && x.trim.length> 0
   */
    fun isValidString(s: String?):Boolean =  !s.isNullOrBlank()

    fun parseStringOnSemiColon(s: String): List<String> = parseStringOnDelimiter(s, ";")

    fun parseStringOnColon(s: String): List<String> = parseStringOnDelimiter(s, ":")

    fun parseStringOnPipe(s: String): List<String> = parseStringOnDelimiter(s, "|")

    fun parseStringOnComma(s: String): List<String> = parseStringOnDelimiter(s, ",")

    fun parseStringOnTab(s: String): List<String> = parseStringOnDelimiter(s, "\t")

    fun parseStringOnEquals(s:String): Pair<String,String>? {
        val list = parseStringOnDelimiter(s,"=")
        if ( list.size == 2)
            return Pair(list[0], list[1])
        return null
    }

    fun isNumeric(str: String) = str.all { it in '0'..'9' }

    private fun parseStringOnDelimiter(s: String, delimiter: String): List<String> =
        s.split(delimiter).map { it.trim() }

    fun isHumanSpeciesId(speciesId: String): Boolean = speciesId.trim().equals("9606")

    fun isEmptyString(s: String): Boolean = s.trim().isNullOrEmpty()

    fun booleanFromInt(i: Int): Boolean = i == 1

    private fun reduceListToDelimitedString(list: List<String>, delimiter: String): String =
        list.joinToString { delimiter }

    fun reduceListToPipeDelimitedString(list: List<String>) = reduceListToDelimitedString(list, "|")

    /*
    Function to return last element in a List
    Needed to support Java clients
     */
    fun <T> getLastListElement(list: List<T>): T = list.last()

    fun parseDoubleString(ds: String): Double = ds.replace(',', '.').toDouble()

    /*
    Function to convert a floating point String into a Float
    returns 0.0 if the String is not in a floating point format
     */
    fun parseValidFloatFromString(fs: String): Float =
        when (Regex("[-+]?[0-9]*\\.?[0-9]+").matches(fs)) {
            true -> fs.toFloat()
            else -> 0.0F
        }

    /*
    Function to convert an Integer String to an Integer
    returns 0 if the field is not numeric
     */

    fun parseValidIntegerFromString(s: String): Int =
        when (Regex(" \"^\\\\d+\$\"").matches(s)) {
            true -> s.toInt()
            else -> 0
        }
    /*
     Quotes (i.e. ", ') inside a text field causes Cypher
     processing errors
      */
    fun removeInternalQuotes(text: String): String =
        text.replace("\"", " ")
            .replace("'"," ")

    fun parseValidDoubleFromString(fs: String): Double =
        when (Regex("[-+]?[0-9]*\\.?[0-9]+").matches(fs)) {
            true -> fs.toDouble()
            else -> 0.0
        }

}