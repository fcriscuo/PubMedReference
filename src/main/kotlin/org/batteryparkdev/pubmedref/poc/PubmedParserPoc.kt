package org.batteryparkdev.pubmedref.poc

import ai.wisecube.pubmed.PubmedArticle
import ai.wisecube.pubmed.PubmedParser
import org.batteryparkdev.pubmedref.model.PubMedEntry
import java.net.URL
import java.nio.charset.Charset

const val pubMedTemplate = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml"
const val pubMedToken = "PUBMEDID"
const val startTag = "<PubmedArticleSet>"
const val UTF8_BOM = "\uFEFF"
const val defaultId = "26050619"

fun main (args: Array<String>) {
    val id = if (args.isNotEmpty()) args[0] else defaultId
    val url = pubMedTemplate.replace(pubMedToken, id)
   // val text = skipXmlHeader( URL(url).readText(Charset.defaultCharset()))
    val text =  URL(url).readText(Charset.defaultCharset())
    val parser = PubmedParser()
    val articleSet = parser.parse(text, ai.wisecube.pubmed.PubmedArticleSet::class.java)
    val pubmedArticle = articleSet.pubmedArticleOrPubmedBookArticle[0] as PubmedArticle
    val medlineCitation = pubmedArticle.medlineCitation
    val pubmedData = pubmedArticle.pubmedData
    val article = medlineCitation.article
    // abstract
    val abstract = article.abstract
    println("Abstract size = ${abstract.abstractText.size}")
   println(abstract.abstractText[0].getvalue())
    // references
    println(pubmedData.referenceList[0].reference.size)
    pubmedData.referenceList[0].reference.stream().forEach { it ->
        val citation = it.citation
        val refId = it.articleIdList.articleId[0].getvalue()
        println("Ref Id = $refId   Citation: $citation")
    }
    val pubmedEntry = PubMedEntry.parsePubMedArticle(pubmedArticle,"Origin")
    // display values
    println("PubMed Id: ${pubmedEntry.pubmedId}")
    println("PMC Id: ${pubmedEntry.pmcId}")
    println("DOI Id: ${pubmedEntry.doiId}")
    println("Article Title: ${pubmedEntry.articleTitle}")
    println("Author(s): ${pubmedEntry.authorCaption}")
    println("Journal: ${pubmedEntry.journalName}")
    println("Journal Issue: ${pubmedEntry.journalIssue}")
    println("Abstract: ${pubmedEntry.abstract}")
    println("Reference count =  ${pubmedEntry.referenceSet.size}")
    pubmedEntry.referenceSet.stream().forEach { println(it) }

}

private fun splitText (text:String?): List<String> =
    text?.chunked(60) ?: emptyList()

private fun skipXmlHeader(xml: String): String =
    xml.substring(xml.indexOf(startTag))

private fun removeBOM(text:String): String = if (text.startsWith(UTF8_BOM)) text.substring(1) else text