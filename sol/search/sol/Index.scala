package search.sol

import search.src.FileIO._
import search.src.StopWords
import search.src.PorterStemmer

import java.util
import java.util.Calendar
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

  private val pageTitleTable: mutable.HashMap[Int, String] = parse("title")
  private val n: Int = pageTitleTable.size

  private val pageTextTable: mutable.HashMap[Int, String] = parse("text")
  private val pageTokenTable: mutable.HashMap[Int, Array[String]] = {
    val output = new mutable.HashMap[Int, Array[String]]()
    for ((k, v) <- pageTextTable) {
      output += (k -> tokenize(v))
    }
    val now = Calendar.getInstance()
    println("Finished takenTable (sub): " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    output
  }

  def parse(tag: String): mutable.HashMap[Int, String] = {
    var output = new mutable.HashMap[Int, String]()
    val mainNode: Node = xml.XML.loadFile(inputFile)
    val pageSeq: NodeSeq = mainNode \ "page"
    for (node <- pageSeq) {
      val pageID: Int = (node \ "id").text.replaceAll("\\s", "").toInt
      val pageTag: String = (node \ tag).text.trim
      output += (pageID -> pageTag)
    }
    output
  }

  def regexToArray(regex: Regex, pageText: String): Array[String] = {
    val matchesIterator = regex.findAllMatchIn(pageText)
    matchesIterator.toArray.map { aMatch => aMatch.matched.toLowerCase }
  }

  def tokenize(pageText: String): Array[String] = {
    val allWords = regexToArray(scaryRegex, pageText)

    val allLinks = regexToArray(linkRegex, pageText)
    var linkWords: Array[String] = Array()
    for (link <- allLinks) {
      if (link.contains("|")) {
        val splitLink = link.split("""\|""")
        linkWords = linkWords ++: regexToArray(scaryRegex, splitLink(0))
      }
    }

    val words = allWords.filter(!linkWords.contains(_))
    val filteredWords = words.filter(!StopWords.isStopWord(_))
    PorterStemmer.stemArray(filteredWords)
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
    val now = Calendar.getInstance()
    println("Finished ITMF (sub): " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    output
  }

  def findValidLinks(k: Int): Array[String] = {
    val regexMatches: Array[Regex.Match] = linkRegex.findAllMatchIn(pageTextTable(k)).toArray
    val linkRegexArrayUnfiltered: Array[String] = regexMatches.map(_.matched)
    val linkRegexArray = linkRegexArrayUnfiltered.filter(_ == null)

    val linkRegexMatches = linkRegexArray.toBuffer
    for (link <- linkRegexMatches) {
      if (link.equals("[[" + pageTitleTable(k) + "]]") || link.contains("[[" + pageTitleTable(k) + "|")) {
        linkRegexMatches -= link
      }
    }

    linkRegexMatches.toArray
  }

  def linksTo(j: Int, k: Int): Boolean = {
    val linkRegexMatches = findValidLinks(k)

    if (j.equals(k)) {
      false
    } else if (linkRegexMatches.isEmpty) {
      true
    } else {
      pageTextTable(k).contains("[[" + pageTitleTable(j) + "]]") ||
        pageTextTable(k).contains("[[" + pageTitleTable(j) + "|")
    }
  }

  def weight(j: Int, k: Int): Double = {
    val linkRegexMatches = findValidLinks(k)

    if (linksTo(j, k)) {
      if (linkRegexMatches.isEmpty) {
        (epsilon / n) + ((1.0 - epsilon) * (1.0 / (n - 1.0)))
      } else {
        var linkWords: Set[String] = Set()
        for (link <- linkRegexMatches) {
          if (link contains "|") {
            val splitLink = link.split("""\|""")
            linkWords = linkWords + splitLink(0).stripPrefix("[[").trim
          } else {
            val splitLink = link.stripPrefix("[[").stripSuffix("]]").trim
            linkWords = linkWords + splitLink
          }
        }
        val nk = linkWords.size
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
    var ranksAndIDs: mutable.HashMap[Int, (Double, Double)] = new mutable.HashMap()
    for ((k,v) <- pageTitleTable) {
      ranksAndIDs += (k -> (0.0, 1.0/n))
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
      val now = Calendar.getInstance()
      println("Done time! (sub): " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    }

    val rankMap = new mutable.HashMap[Int, Double]
    for ((k,v) <- ranksAndIDs) {
      rankMap += (k -> v._2)
    }
    val now = Calendar.getInstance()
    println("Finished pageRank (sub): " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    rankMap
  }

  def wordsAh(): mutable.HashMap[String, mutable.HashMap[Int, Double]] = {
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

  val placeHolder: mutable.HashMap[Int, Double] = {
    val smth = new mutable.HashMap[Int, Double]()
    for ((k,v) <- pageTitleTable) {
      val score = (k, 0.0)
      smth += score
    }
    smth
  }
}


object Index {
  def main(args: Array[String]) {
    // "sol\\search\\sol\\SmallWiki.xml"
    if (args.length != 1) {
      println("Incorrect usage: please enter the path to the XML file")
      System.exit(1)
    }
    var now = Calendar.getInstance()
    println("Started: " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))

    val indexer = new Index(args(0))
    now = Calendar.getInstance()
    println("Finished indexer: " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    printTitleFile("sol\\search\\sol\\titles.txt", indexer.pageTitleTable)
    now = Calendar.getInstance()
    println("Finished pageTitleTable: " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    printDocumentFile("sol\\search\\sol\\docs.txt", indexer.idsToMaxFreqs(), indexer.placeHolder)
    now = Calendar.getInstance()
    println("Finished idtoMF and pageRank: " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    printWordsFile("sol\\search\\sol\\words.txt", indexer.wordsAh())
    now = Calendar.getInstance()
    println("Finished wordsToDF (END): " + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":" + now.get(Calendar.SECOND))
    //print(indexer.allWords)
    //val line = "THis is the text and words and stuff plus more [[Hammer]] [[Presidents|Washington]] [[Category:Computer Science]] rouiwbfvweui [[routines]] and [[happy|stuff]] too"
    //print(indexer.tokenize(line).mkString("Array(", ", ", ")"))
  }
}
