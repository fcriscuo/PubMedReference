package org.batteryparkdev.pubmedref.service

import ai.wisecube.pubmed.PubmedArticle
import ai.wisecube.pubmed.PubmedParser
import com.google.common.flogger.FluentLogger
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import java.io.StringReader
import java.net.URL
import java.nio.charset.Charset
import javax.xml.parsers.DocumentBuilderFactory

object PubMedRetrievalService {

    private val logger: FluentLogger = FluentLogger.forEnclosingClass();

    //    private val ncbiEmail = System.getenv("NCBI_EMAIL")
//    private val ncbiApiKey = System.getenv("NCBI_API_KEY")
    private val ncbiEmail = "batteryparkdev@gmail.com"
    private val ncbiApiKey = "8ea2dc1ff16df40319a83d259764f641a208"
    private val dbFactory = DocumentBuilderFactory.newInstance()
    private val dBuilder = dbFactory.newDocumentBuilder()

    private const val pubMedTemplate =
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml"
    private val citationTemplate =
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pubmed_citedin" +
                "&id=PUBMEDID&&tool=my_tool&email=NCBIEMAIL&api_key=APIKEY"
    private const val pubMedToken = "PUBMEDID"

    fun retrievePubMedArticle(pubmedId: String): PubmedArticle {
        val url = pubMedTemplate
            .replace(pubMedToken, pubmedId)
        val text = URL(url).readText(Charset.defaultCharset())
        val parser = PubmedParser()
        val articleSet = parser.parse(text, ai.wisecube.pubmed.PubmedArticleSet::class.java)
        return articleSet.pubmedArticleOrPubmedBookArticle[0] as PubmedArticle
    }

    fun retrieveCitationIds(pubmedId: String): Set<String> {
        val url = citationTemplate.replace(pubMedToken, pubmedId)
            .replace("NCBIEMAIL", ncbiEmail)
            .replace("APIKEY", ncbiApiKey)
        val citationSet = mutableSetOf<String>()
        val text = URL(url).readText(Charset.defaultCharset())
        val xmlDoc = dBuilder.parse(InputSource(StringReader(text)));
        xmlDoc.documentElement.normalize()
        val citationList: NodeList = xmlDoc.getElementsByTagName("Link")
        for (i in 0 until citationList.length) {
            val citationNode = citationList.item(i)
            if (citationNode.nodeType == Node.ELEMENT_NODE) {
                val elem = citationNode as Element
                val id = elem.getElementsByTagName("Id").item(0).textContent
                citationSet.add(id)
            }
        }
        return citationSet.toSet()
    }
}

fun main() {
    // test parsing citations from XML-formatted response
    PubMedRetrievalService.retrieveCitationIds("26050619").stream()
        .forEach { cit -> println(cit) }
}