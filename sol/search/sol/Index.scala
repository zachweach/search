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
    }
    output
  }

  private val idsToMaxFreqs: mutable.HashMap[Int, Double] = {
    val output = new mutable.HashMap[Int, Double]()

    for ((k, v) <- pageTokenTable) {
      val wordsToFreqs = new mutable.HashMap[String, Double]()

      for (word <- v) { //find the most common value
        wordsToFreqs(word) = wordsToFreqs.getOrElse(word, 0.0) + 1.0
      }

      var maxNum = 0.0
      for ((k, v) <- wordsToFreqs) { // fetch the # of times most common value occurs
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

  def distance(prevNext: mutable.HashMap[Int, (Double, Double)]): Double = {
    var output = 0.0
    for ((k,v) <- prevNext) {
      output = output + pow(v._2 - v._1, 2)
    }
    sqrt(output)
  }

  def pageRank(): mutable.HashMap[Int, Double] = {
    var ranksAndIDs: mutable.HashMap[Int, (Double, Double)] = new mutable.HashMap()
    for ((k,v) <- pageTitleTable) {
      ranksAndIDs += (k -> (0.0, 1.0/n))
    }

    while (distance(ranksAndIDs) > delta) {
      for ((k,v) <- pageTitleTable) {
        ranksAndIDs(k) = (ranksAndIDs(k)._2, 0)
        for ((key,v) <- pageTitleTable) {
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
    //val line = "THis is the text and words and stuff plus more [[Hammer]] [[Presidents|Washington]] [[Category:Computer Science]] rouiwbfvweui [[routines]] and [[happy|stuff]] too"
    //print(indexer.tokenize(line).mkString("Array(", ", ", ")"))
  }
}
