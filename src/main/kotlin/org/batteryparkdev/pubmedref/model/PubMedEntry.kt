package org.batteryparkdev.pubmedref.model

import ai.wisecube.pubmed.*
import org.batteryparkdev.pubmedref.service.PubMedRetrievalService

data class PubMedEntry(
    val label: String,
    val pubmedId: String,
    val parentPubMedId: String,
    val pmcId: String = "",
    val doiId: String = "",
    val journalName: String,
    val journalIssue: String,
    val articleTitle: String,
    val abstract: String,
    val authorCaption: String,
    val referenceSet: Set<String>,
    val citationSet: Set<String>,
    val citedByCount: Int
) {
    companion object {
        /*
        Function to parse attributes from the PubMedArticle JaXB model object
        label should be one of (Origin, Reference, Citation)
         */
        fun parsePubMedArticle(pubmedArticle: PubmedArticle, label:String = "Origin",
                               parentId:String = ""): PubMedEntry {
            val pmid = pubmedArticle.medlineCitation.pmid.getvalue()
            val pmcid = resolveArticleIdByType(pubmedArticle, "pmc")
            val doiid = resolveArticleIdByType(pubmedArticle, "doi")
            val authors = generateAuthorCaption(pubmedArticle)
            val journalName = pubmedArticle.medlineCitation.article.journal.title
            val journalIssue = resolveJournalIssue(pubmedArticle)
            val title = pubmedArticle.medlineCitation.article.articleTitle.getvalue()
            val abstract = resolveAbstract(pubmedArticle)
            val citations = PubMedRetrievalService.retrieveCitationIds(pmid)

            return PubMedEntry(
                label, pmid, parentId,
                pmcid, doiid, journalName, journalIssue, title,
                abstract, authors, resolveReferenceIdSet(pubmedArticle),
                citations, citations.size)
        }


        private fun resolveReferenceIdSet(pubmedArticle: PubmedArticle): Set<String> {
            val refSet = mutableSetOf<String>()
            pubmedArticle.pubmedData.referenceList.stream().forEach { refL ->
                refL.reference.stream().forEach { ref ->
                    for (articleId in ref.articleIdList.articleId.stream()
                        ) {
                        refSet.add(articleId.getvalue())
                    }
                }
            }

            return refSet.toSet()
        }

        private fun resolveAbstract(pubmedArticle: PubmedArticle): String {

            val absTextList = pubmedArticle.medlineCitation.article.abstract.abstractText
            if (absTextList.isNotEmpty()) {
                return absTextList[0].getvalue()
            }
            return ""
        }

        private fun resolveArticleIdByType(pubmedArticle: PubmedArticle, type: String): String {
            val articleId = pubmedArticle.pubmedData.articleIdList.articleId.filter { it.idType == type }
                .firstOrNull()
            return if (articleId != null) articleId.getvalue() else ""
        }

        /*
        Function to generate a String with th last names of up to the first
        two (authors) plus et al if > 2 authors
        e.g.  Smith, Robert; Jones, Mary, et al
         */
        private fun generateAuthorCaption(pubmedArticle: PubmedArticle): String {
            val authorList = pubmedArticle.medlineCitation.article.authorList.author
            val ret = when (authorList.size) {
                0 -> ""
                1 -> processAuthorName(authorList[0])
                2 -> processAuthorName(authorList[0]) + "; " +
                        processAuthorName(authorList[1])
                else -> processAuthorName(authorList[0]) + "; " +
                        processAuthorName(authorList[1]) + "; et al"


            }
            return ret
        }

        private fun processAuthorName(author: Author): String {
            val authorNameList = author.lastNameOrForeNameOrInitialsOrSuffixOrCollectiveName
            var name = ""
            val lastName: LastName = authorNameList[0] as LastName
            name = lastName.getvalue()
            if (authorNameList.size > 1) {
                val firstName = authorNameList[1] as ForeName
                name = "$name, ${firstName.getvalue()}"
            }
            if (authorNameList.size > 2) {
                val initials = authorNameList[2] as Initials
                name = "$name ${initials.getvalue()}"
            }

            return name
        }

        private fun resolveJournalIssue(pubmedArticle: PubmedArticle): String {
            var ret = ""
            val journalIssue = pubmedArticle.medlineCitation.article.journal.journalIssue
            val year = (journalIssue.pubDate.yearOrMonthOrDayOrSeasonOrMedlineDate[0] as Year).getvalue()
            val vol = journalIssue.volume
            val issue = journalIssue.issue
            val pgn: String = if (pubmedArticle.medlineCitation.article.paginationOrELocationID.size > 0) {
                val page = pubmedArticle.medlineCitation.article.paginationOrELocationID[0] as Pagination
                val medlinePgn = page.startPageOrEndPageOrMedlinePgn[0] as MedlinePgn
                medlinePgn.getvalue()
            } else ""
            if (vol.isNotEmpty()) {
                ret = "$year $vol"
                if (issue.isNotEmpty()) {
                    ret = "$ret($issue)"
                    if (pgn.isNotEmpty()) {
                        ret = "$ret:${pgn.toString()}"
                    }
                }
            }
            return ret
        }
    }

}