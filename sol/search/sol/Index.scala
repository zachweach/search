package search.sol

import search.src.FileIO._
import search.src.StopWords
import search.src.PorterStemmer

import scala.math.{pow, sqrt}
import scala.xml.{Node, NodeSeq}
import scala.collection.mutable
import scala.util.matching.Regex


/**
 * Provides an XML indexer, produces files for a querier
 *
 * @param inputFile - the filename of the XML wiki to be indexed
 */
class Index(val inputFile: String) {

  private val delta: Double = 0.0001
  private val epsilon: Double = 0.15
  val linkRegex: Regex = new Regex("""\[\[[^\[]+?\]\]""")
  val scaryRegex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")

  private val pageTitlesToIDs: mutable.HashMap[String, Int] = mutable.HashMap()
  private val pageTitleTable: mutable.HashMap[Int, String] = parse("title")
  private val n: Int = pageTitleTable.size
  private val pageTextTable: mutable.HashMap[Int, String] = parse("text")

  def parse(tag: String): mutable.HashMap[Int, String] = {
    var output = new mutable.HashMap[Int, String]()
    val mainNode: Node = xml.XML.loadFile(inputFile)
    val pageSeq: NodeSeq = mainNode \ "page"
    for (node <- pageSeq) {
      val pageID: Int = (node \ "id").text.replaceAll("\\s", "").toInt
      val pageTag: String = (node \ tag).text.toLowerCase.trim
      output += (pageID -> pageTag)
      if (tag.equals("title")) {
        pageTitlesToIDs += (pageTag -> pageID)
      }
    }
    output
  }

  def regexToArray(regex: Regex, pageText: String): Array[String] = {
    val matchesIterator = regex.findAllMatchIn(pageText)
    matchesIterator.toArray.map { aMatch => aMatch.matched.toLowerCase }
  }

  private val pageLinksTable: mutable.HashMap[Int, Set[Int]] = {
    val output: mutable.HashMap[Int, Set[Int]] = mutable.HashMap()
    for ((k,v) <- pageTitleTable) {
      output.put(k, Set())
    }
    output
  }

  private val pageTokenTable: mutable.HashMap[Int, Array[String]] = {
    val tokens = new mutable.HashMap[Int, Array[String]]()
    for ((k,v) <- pageTextTable) {
      val allLinks = regexToArray(linkRegex, v)
      var linkWords: Array[String] = Array()
      for (link <- allLinks) {
        var kSet: Set[Int] = pageLinksTable(k)
        var linkedPage: String = ""

        if (link.contains("|")) {
          val splitLink = link.split("""\|""")
          linkedPage = splitLink(0).stripPrefix("[[").toLowerCase
          linkWords = linkWords ++: regexToArray(scaryRegex, splitLink(0))
        } else {
          linkedPage = link.stripPrefix("[[").stripSuffix("]]").toLowerCase
        }

        if (pageTitlesToIDs.contains(linkedPage)) {
          if (pageLinksTable.contains(pageTitlesToIDs(linkedPage)) && pageTitlesToIDs(linkedPage) != k) {
            kSet = kSet + pageTitlesToIDs(linkedPage)
            pageLinksTable.update(k, kSet)
          } else {
            pageLinksTable.put(k, kSet)
          }
        }
      }

      val words = regexToArray(scaryRegex, v).filter(!linkWords.contains(_)).filter(!StopWords.isStopWord(_))
      tokens.put(k, PorterStemmer.stemArray(words))
    }
    tokens
  }

  private def idsToMaxFreqs(): mutable.HashMap[Int, Double] = {
    val output = new mutable.HashMap[Int, Double]()
    val wordsToFreqs: mutable.HashMap[String, Double] = mutable.HashMap()

    for ((k, v) <- pageTokenTable) {
      if (v.nonEmpty) {
        for (word <- v) {
          wordsToFreqs(word) = wordsToFreqs.getOrElse(word, 0.0) + 1.0
        }

        output += (k -> wordsToFreqs.values.max)
        wordsToFreqs.clear()
      }
    }
    output
  }

  def linksTo(j: Int, k: Int): Boolean = {
    if (j.equals(k)) {
      false
    } else if (pageLinksTable(k).isEmpty) {
      true
    } else {
      pageLinksTable(k).contains(j)
    }
  }

  def weight(j: Int, k: Int): Double = {
    if (linksTo(j, k)) {
      if (pageLinksTable(k).isEmpty) {
        (epsilon / n) + ((1.0 - epsilon) * (1.0 / (n - 1.0)))
      } else {
        val nk = pageLinksTable(k).size
        (epsilon / n) + ((1.0 - epsilon) * (1.0 / nk))
      }
    } else {
      epsilon / n
    }
  }

  def distance(prevNext: mutable.HashMap[Int, (Double, Double)]): Double = {
    var output = 0.0
    for ((k,v) <- prevNext) {
      output = output + pow(v._2 - v._1, 2)
    }
    sqrt(output)
  }

  def pageRank(): mutable.HashMap[Int, Double] = {
    val ranksAndIDs: mutable.HashMap[Int, (Double, Double)] = new mutable.HashMap()
    for ((k,v) <- pageTitleTable) {
      ranksAndIDs.put(k, (0.0, 1.0/n))
    }

    while (distance(ranksAndIDs) > delta) {
      for ((k,v) <- pageTitleTable) {
        ranksAndIDs(k) = (ranksAndIDs(k)._2, 0.0)
      }
      for ((k,v) <- pageTitleTable) {
        for ((key,va) <- pageTitleTable) {
          ranksAndIDs(k) = (ranksAndIDs(k)._1, ranksAndIDs(k)._2 + (weight(k, key) * ranksAndIDs(key)._1))
        }
      }
    }

    val rankMap = new mutable.HashMap[Int, Double]
    for ((k,v) <- ranksAndIDs) {
      rankMap += (k -> v._2)
    }
    rankMap
  }

  def wordsToDocumentFrequencies(): mutable.HashMap[String, mutable.HashMap[Int, Double]] = {
    val wordPageCount: mutable.HashMap[String, mutable.HashMap[Int, Double]] = mutable.HashMap()
    for ((k,v) <- pageTokenTable) {
      for (word <- v) {
        if (!wordPageCount.contains(word)) {
          val newWord: mutable.HashMap[Int, Double] = mutable.HashMap()
          newWord.put(k, 1.0)
          wordPageCount.put(word, newWord)
        } else {
          wordPageCount(word).update(k, wordPageCount(word).getOrElse(k, 0.0) + 1)
        }
      }
    }
    wordPageCount
  }
}


object Index {
  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Incorrect usage: please enter the path to the XML file")
      System.exit(1)
    }

    val indexer = new Index(args(0))
    printTitleFile("sol\\search\\sol\\titles.txt", indexer.pageTitleTable)
    printDocumentFile("sol\\search\\sol\\docs.txt", indexer.idsToMaxFreqs(), indexer.pageRank())
    printWordsFile("sol\\search\\sol\\words.txt", indexer.wordsToDocumentFrequencies())
  }
}
