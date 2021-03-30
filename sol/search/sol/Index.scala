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

  // Sets constants to be used throughout the program
  private val delta: Double = 0.0001
  private val epsilon: Double = 0.15
  private val linkRegex: Regex = new Regex("""\[\[[^\[]+?\]\]""")
  private val scaryRegex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")

  // HashMap that maps all of the titles of pages in the corpus to their page IDs
  private val pageTitlesToIDs: mutable.HashMap[String, Int] = mutable.HashMap()

  // HashMap that maps all of the page IDs of pages in the corpus to their titles
  private val pageTitleTable: mutable.HashMap[Int, String] = parse("title")

  // Integer representing the number of pages in the corpus
  private val n: Int = pageTitleTable.size

  // HashMap that maps all of the page IDs of pages in the corpus to a string of all the text on that page
  // Used when producing pageLinksTable and pageTokenTable
  private val pageTextTable: mutable.HashMap[Int, String] = parse("text")

  // HashMap that maps all of the page IDs of pages in the corpus to a Set of all the pages that this page links to
  // First, it is instantiated as mapping each page ID to an empty set, which is then filled in alongside pageTokenTable
  private val pageLinksTable: mutable.HashMap[Int, Set[Int]] = {
    val output: mutable.HashMap[Int, Set[Int]] = mutable.HashMap()
    for ((k,v) <- pageTitleTable) {
      output.put(k, Set())
    }
    output
  }

  // HashMap that maps all of the page IDs of pages in the corpus to an array of all of the words on that page
  // The words are all tokenized and stemmed and all of the stop words are removed
  private val pageTokenTable: mutable.HashMap[Int, Array[String]] = {
    val tokens = new mutable.HashMap[Int, Array[String]]()

    // This for loop goes through every page in pageTextTable and creates an array of all of the words.
    // This array is then put in the "tokens" HashMap created above.
    for ((k,v) <- pageTextTable) {
      val allLinks = regexToArray(linkRegex, v) // First, we find all of the links that appear in the text
      var linkWords: Array[String] = Array()

      // This for loop goes through every link and 1) properly extracts the text that should not be included in
      // the list of all words and 2) adds the proper links to pageLinksTable
      for (link <- allLinks) {
        var kSet: Set[Int] = pageLinksTable(k)
        var linkedPage: String = ""

        // If the link has a pipe, add the words that come before the pipe to linkWords
        if (link.contains("|")) {
          val splitLink = link.split("""\|""")
          linkedPage = splitLink(0).stripPrefix("[[").toLowerCase
          linkWords = linkWords ++: regexToArray(scaryRegex, splitLink(0))
        } else {
          linkedPage = link.stripPrefix("[[").stripSuffix("]]").toLowerCase
        }

        // Properly adds the linked page to pageLinksTable
        if (pageTitlesToIDs.contains(linkedPage)) { // Ignore the link if it is outside the corpus
          if (pageTitlesToIDs(linkedPage) != k) { // Ignore if the link is to itself
            kSet = kSet + pageTitlesToIDs(linkedPage)
            pageLinksTable.put(k, kSet) // Add the link to pageLinksTable
          }
        }
      }

      // First, we find all of the words. Then, we get rid of any words that should be excluded from links.
      // Next, stop words are removed. Lastly, all of the remaining words are stemmed and this array is added
      // to the proper spot in "tokens"
      val words = regexToArray(scaryRegex, v).filter(!linkWords.contains(_)).filter(!StopWords.isStopWord(_))
      tokens.put(k, PorterStemmer.stemArray(words))
    }
    tokens
  }

  /**
   * Helper method that parses the input file and maps the page IDs to the contents of the given XML tag.
   * For example, the tag "title" will produce the text all of the titles
   * @param tag - the XML tag to separate the document by
   * @return HashMap that maps page IDs (as Ints) to the String of text for the given tag
   */
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

  /**
   * Converts a string into an array of strings that match a given Regex
   * @param regex - the Regex to match against
   * @param pageText - the string that will be turned into an array
   * @return Array of Strings, representing all of the matches
   */
  def regexToArray(regex: Regex, pageText: String): Array[String] = {
    val matchesIterator = regex.findAllMatchIn(pageText)
    matchesIterator.toArray.map { aMatch => aMatch.matched.toLowerCase }
  }

  /**
   * Creates a HashMap that maps page IDs to the number of times the most commonly used word
   * on the page appears on the page
   * @return the appropriate HashMap based on the description
   */
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

  /**
   * Determines if a page links to another page. Helper for PageRank()
   * @param j - the linked-to page
   * @param k - the main page
   * @return Boolean representing if there is a link
   */
  private def linksTo(j: Int, k: Int): Boolean = {
    if (j.equals(k)) {
      false
    } else if (pageLinksTable(k).isEmpty) {
      true
    } else {
      pageLinksTable(k).contains(j)
    }
  }

  /**
   * Calculates the weight of a page with respect to another page. Helper for PageRank()
   * @param j - the linked-to page
   * @param k - the main page
   * @return Double representing the weight
   */
  private def weight(j: Int, k: Int): Double = {
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

  /**
   * Calculates the distance between the previous iteration of PageRank() and the most recent one
   * @param prevNext HashMap that maps pageIDs to a Tuple containing the previous PageRank score
   *                 and the current PageRank score
   * @return Double representing the total distance between all pages
   */
  private def distance(prevNext: mutable.HashMap[Int, (Double, Double)]): Double = {
    var output = 0.0
    for ((k,v) <- prevNext) {
      output = output + pow(v._2 - v._1, 2)
    }
    sqrt(output)
  }

  /**
   * Calculates the PageRanks of each page in the corpus based on the PageRank algorithm.
   * @return HashMap that maps page IDs to their appropriate PageRanks
   */
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

  /**
   * Creates a HashMap that maps each word that appears in the corpus to a hashtable that maps each
   * page ID to the number of times the word appears on that page
   * @return the appropriate HashMap as detailed in the description
   */
  def wordsToDocumentFrequencies(): mutable.HashMap[String, mutable.HashMap[Int, Double]] = {
    val wordPageCount: mutable.HashMap[String, mutable.HashMap[Int, Double]] = mutable.HashMap()

    for ((k,v) <- pageTokenTable) { // Loop through every page
      for (word <- v) { // Loop through every word on each page
        if (!wordPageCount.contains(word)) { // We have never encountered this word before
          val newWord: mutable.HashMap[Int, Double] = mutable.HashMap()
          newWord.put(k, 1.0)
          wordPageCount.put(word, newWord)
        } else { // We have encountered this word before
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