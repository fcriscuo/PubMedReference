package org.batteryparkdev.pubmedref.service

import ai.wisecube.pubmed.PubmedArticle
import ai.wisecube.pubmed.PubmedParser
import arrow.core.Either
import com.google.common.flogger.FluentLogger
import org.batteryparkdev.pubmedref.neo4j.Neo4jConnectionService
import org.batteryparkdev.pubmedref.neo4j.resolveCurrentTime
import org.batteryparkdev.pubmedref.neo4j.resolveCypherLogFileName
import org.batteryparkdev.pubmedref.property.ApplicationPropertiesService
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import java.io.File
import java.io.StringReader
import java.net.URL
import java.nio.charset.Charset
import java.util.stream.Stream
import javax.xml.parsers.DocumentBuilderFactory

object PubMedRetrievalService {

     val logger: FluentLogger = FluentLogger.forEnclosingClass();
    private val dbFactory = DocumentBuilderFactory.newInstance()
    private val dBuilder = dbFactory.newDocumentBuilder()
    private const val ncbiDelay:Long = 100L  // max NCBI request rate with key
    private val citationPath = resolveCitationLogFileName()
    private val citationFileWriter = File(citationPath).bufferedWriter()
     //   ApplicationPropertiesService.resolvePropertyAsLong("ncbi.request.delay.milliseconds")

    private  val pubMedTemplate =
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&amp;id=PUBMEDID&amp;retmode=xml" +
                "&email=${java.lang.System.getenv("NCBI_EMAIL")}" +
                "&api_key=${java.lang.System.getenv("NCBI_API_KEY")}"
    private val citationTemplate =
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pubmed_citedin" +
                "&id=PUBMEDID&&tool=my_tool" +
                "&email=${java.lang.System.getenv("NCBI_EMAIL")}" +
                "&api_key=${java.lang.System.getenv("NCBI_API_KEY")}"
    private const val pubMedToken = "PUBMEDID"


    /*
    Return an Either<Exception, PubMedArticle to deal with NCBI
    service disruptions
     */
    fun retrievePubMedArticle(pubmedId: String ): Either<Exception, PubmedArticle> {
        Thread.sleep(ncbiDelay)  // Accommodate NCBI maximum request rate
        val url = pubMedTemplate
            .replace(pubMedToken, pubmedId)
        return try {
            val text = URL(url).readText(Charset.defaultCharset())
            val parser = PubmedParser()
            val articleSet = parser.parse(text, ai.wisecube.pubmed.PubmedArticleSet::class.java)
            Either.Right(articleSet.pubmedArticleOrPubmedBookArticle[0] as PubmedArticle)
        } catch (e: Exception) {
            Either.Left(e)
        }
    }

    fun retrievePubMedArticleStream(pubmedIdBatch:String): Either<Exception, Stream<PubmedArticle>> {
        Thread.sleep(ncbiDelay)  // Accommodate NCBI maximum request rate
        val url = pubMedTemplate
            .replace(pubMedToken, pubmedIdBatch)
        return try {
            val text = URL(url).readText(Charset.defaultCharset())
            val parser = PubmedParser()
            val articleSet = parser.parse(text, ai.wisecube.pubmed.PubmedArticleSet::class.java)
            val articleStream = articleSet.pubmedArticleOrPubmedBookArticle.stream() as Stream<PubmedArticle>
            Either.Right(articleStream)
        } catch (e: Exception) {
            Either.Left(e)
        }
    }

    fun retrieveCitationIds(pubmedId: String, repeat:Boolean = true): Set<String> {
        Thread.sleep(200L)
        val url = citationTemplate.replace(pubMedToken, pubmedId)
           // .replace("NCBIEMAIL", ncbiEmail)
          //  .replace("APIKEY", ncbiApiKey)
        val citationSet = mutableSetOf<String>()
        try {
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
        } catch (e: Exception) {
            logger.atWarning().log("++++  EXCEPTION getting citation set for $pubmedId, repeat = $repeat")
            logger.atWarning().log(e.message)
            // sometimes NCBI is not responsive, try one more time
            if (repeat) {
                retrieveCitationIds(pubmedId, false)
            } else {
                citationFileWriter.write("$pubmedId\n")
            }
        }
        return citationSet.toSet()
    }

    fun resolveCitationLogFileName() =
        ApplicationPropertiesService.resolvePropertyAsString("neo4j.log.dir") +"/" +
                "missed_citations" +
                "_" + resolveCurrentTime() + ".log"
}

fun main() {
    // test PubMedArticle retrieval
    when (val retEither =PubMedRetrievalService.retrievePubMedArticle("26050619")) {
        is Either.Right -> {
            val article = retEither.value
            println("Title: ${article.medlineCitation.article.articleTitle.getvalue()}")
            PubMedRetrievalService.retrieveCitationIds("26050619").stream()
                .forEach { cit -> println(cit) }
        }
        is Either.Left -> {
            PubMedRetrievalService.logger.atInfo().log(" ${retEither.value.message}")
        }
    }

}