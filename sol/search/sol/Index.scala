package search.sol

import search.src.FileIO._
import search.src.StopWords
import search.src.PorterStemmer

import scala.math.{log, pow}
import scala.xml.{Node, NodeSeq}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
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

    val allWords = regexToArray(scaryRegex, pageText)

    val allLinks = regexToArray(linkRegex, pageText)
    var linkWords: Array[String] = Array()
    for (link <- allLinks) {
      if (link contains "|") {
        val splitLink = link.split("""\|""")
        linkWords = linkWords ++: splitLink(0).split(""" |\[""")
      }
    }

    val words = allWords.filter(!linkWords.contains(_))
    val filteredWords = words.filter(!StopWords.isStopWord(_))
    PorterStemmer.stemArray(filteredWords)
  }

  private val delta: Double = 0.0001
  private val epsilon: Double = 0.15
  private val pageTextTable: mutable.HashMap[Int, String] = parse("text")

  private val pageTokenTable: mutable.HashMap[Int, Array[String]] = {
    val output = new mutable.HashMap[Int, Array[String]]()
    for ((k,v) <- pageTextTable) {
      output += (k -> tokenize(v))
      print(output(k).mkString("Array(", ", ", ")"))
    }
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
    val rankMap = new mutable.HashMap[Int, Double]
    for (j <- Range(0, n-1)) {
      rankMap += (j -> nextRanks(j))
    }
    rankMap
  }



  def smallHashTable(word: String): mutable.HashMap[Int, Double] = {
    val numHshMap = new mutable.HashMap[Int, Double]()
    for ((k,v) <- pageTokenTable) {
      var occur = 0
      for (b <- v) {
        if (word == b) {
          occur += 1
        }
        if (occur > 0) {
          numHshMap += (k -> occur)
        }
      }
    }
    numHshMap
  }

  val wordsToDocumentFrequencies: mutable.HashMap[String, mutable.HashMap[Int, Double]] = {
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
    // "sol\\search\\sol\\SmallWiki.xml"
    if (args.length != 1) {
      println("Incorrect usage: please enter the directory to the XML file")
      System.exit(1)
    }
    val indexer = new Index(args(0))
    printTitleFile("sol\\search\\sol\\titles.txt", indexer.pageTitleTable)
    printDocumentFile("sol\\search\\sol\\docs.txt",  indexer.idsToMaxFreqs, indexer.pageRank())
    printWordsFile("sol\\search\\sol\\words.txt", indexer.wordsToDocumentFrequencies)
    print(indexer.pageTextTable.toString)
    print(indexer.pageTokenTable.toString)
    //val line = "THis is the text and words and stuff plus more [[Hammer]] [[Presidents|Washington]] [[Category:Computer Science]] rouiwbfvweui [[routines]] and [[happy|stuff]] too"
    //print(indexer.tokenize(line).mkString("Array(", ", ", ")"))
  }
}
