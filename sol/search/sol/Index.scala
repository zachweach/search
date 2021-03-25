package search.sol

import search.src.FileIO._
import search.src.StopWords
import search.src.PorterStemmer

import scala.math.{log, pow}
import scala.xml.{Node, NodeSeq}
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.matching.Regex


/**
 * Provides an XML indexer, produces files for a querier
 *
 * @param inputFile - the filename of the XML wiki to be indexed
 */
class Index(val inputFile: String) {

  def parse(tag: String): mutable.HashMap[Int, String] = {
    var output = new mutable.HashMap[Int, String]()
    val mainNode: Node = xml.XML.loadFile(inputFile)
    val pageSeq: NodeSeq = mainNode \ "page"
    for (node <- pageSeq) {
      val pageID: Int = (node \ "id").text.replaceAll("\\s", "").toInt
      val pageTag: String = (node \ tag).text
      output += (pageID -> pageTag)
    }
    output
  }

  def regexToArray(regex: Regex, pageText: String): Array[String] = {
    val matchesIterator = regex.findAllMatchIn(pageText)
    matchesIterator.toArray.map { aMatch => aMatch.matched }
  }

  def tokenize(pageText: String): Array[String] = {
    val linkRegex = new Regex("""\[\[[^\[]+?\]\]""")
    val scaryRegex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")

    val allLinks = regexToArray(linkRegex, pageText)
    val wordsFromLinks = mutable.ArrayBuffer
    for (link <- allLinks) {
      if (link.contains("|")) {
        val splitLink = link.split("""\|""")
        wordsFromLinks ++: splitLink(1).split(""" |]""")
      }
    }
    wordsFrom

    val words = regexToArray(scaryRegex, pageText)
    //words ++: wordsFromLinks

    val filteredWords = words.filter(!StopWords.isStopWord(_))
    PorterStemmer.stemArray(filteredWords)
  }

  private val delta: Double = 0.0001
  private val epsilon: Double = 0.15
  private val pageTextTable: mutable.HashMap[Int, String] = parse("text")
  private val pageTokenTable: mutable.HashMap[Int, Array[String]] = {
    val output = new mutable.HashMap[Int, Array[String]]()
    for ((k,v) <- pageTextTable) output + (k -> tokenize(v))
    output
  }
  private val idsToMaxFreqs: mutable.HashMap[Int, Double] = {
    val output = new mutable.HashMap[Int, Double]()
    for ((k, v) <- pageTokenTable) {
      val hshMap = new mutable.HashMap[String, Double]()
      for (a <- v) { //find the most common value
        if (hshMap.contains(a)) { // if already exists then update count.
          hshMap(a) = hshMap.getOrElse(a, 0.0) + 1.0
        }
        else hshMap += (a -> 1.0) // else insert it in the map.
      }
      var maxNum = 0.0
      for ((k, v) <- hshMap) { // fetch the # of times most common value occurs
        if (v > maxNum) {
          maxNum = v
        }
      }
      output += (k -> maxNum)
    }
    output
  }

  private val pageTitleTable: mutable.HashMap[Int, String] = parse("title")
  private val n: Int = pageTitleTable.size

  def distance(prev: Array[Double], next: Array[Double]): Double = {
    var output = 0.0
    var i = 0
    for (r <- prev) {
      output = output + pow(next(i) - r, 2)
      i += 1
    }
    output
  }



  def linksTo(j: Int, k: Int): Boolean = {
    pageTextTable(k).contains("[[" + pageTitleTable(j) + "]]") ||
    pageTextTable(k).contains("[[" + pageTitleTable(j) + "|")
  }

  def weight(j: Int, k: Int): Double = {
    if (linksTo(j, k)) {
      (epsilon / n) + (1 - epsilon) * (1 / n)
    } else {
      epsilon / n
    }
  }

  def pageRank(): mutable.HashMap[Int, Double] = {
    var prevRanks = Array.fill(n)(0.0)
    val nextRanks = Array.fill(n)(1.0 / n)

    while (distance(prevRanks, nextRanks) > delta) {
      prevRanks = nextRanks
      for (j <- Range(0, n - 1)) {
        nextRanks(j) = 0
        for (k <- Range(0, n - 1)) {
          nextRanks(j) = nextRanks(j) + (weight(j, k) * prevRanks(k))
        }
      }
    }
    for (j <- range(0, n-1))
  }

  def findIdf(word: String): Double = {
    var n = 0
    var ni = 0
    for ((k, v) <- pageTokenTable) {
      n += 1
      if (v.contains(word)) {
        ni += 1
      }
    }
    log(n / ni)
  }

  def smallHashTable(word: String): mutable.HashMap[Int, Double] = {
    val numHshMap = new mutable.HashMap[Int, Double]()
    for ((k,v) <- pageTokenTable) {
      var occur = 0
      for (b <- v) {
        if (word == b) {
          occur += 1
        }
        val tfIdf = (occur / idsToMaxFreqs(k)) * findIdf(word)
        numHshMap += (k -> tfIdf)
      }
    }
    numHshMap
  }

  def findTf(): mutable.HashMap[String, mutable.HashMap[Int, Double]] = {
    val bigHshMap = new mutable.HashMap[String, mutable.HashMap[Int, Double]]()
    for ((k,v) <- pageTokenTable) {
      for (a <- v) {
        if (!bigHshMap.contains(a)) {
          bigHshMap += (a -> smallHashTable(a))
        }
      }
    }
    bigHshMap
  }
}



object Index {
  def main(args: Array[String]) {

    val indexer = new Index(args(0))
    printTitleFile(titleFile, indexer.pageTitleTable)
    printDocumentFile(documentFile,  indexer.idsToMaxFreqs, indexer.pageRank())
    printWordsFile(wordsFile, indexer.findTf())


    print(indexer.tokenize("THis is the text and words and stuff plus more rouiwbfvweui [[routines]] and [[happy|stuff]] too").mkString("Array(", ", ", ")"))
  }
}
