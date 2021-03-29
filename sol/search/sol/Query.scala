package search.sol

import java.io._
import search.src.{FileIO, PorterStemmer, StopWords}

import java.util.Calendar
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.math.log
import scala.util.matching.Regex

/**
 * Represents a query REPL built off of a specified index
 *
 * @param titleIndex    - the filename of the title index
 * @param documentIndex - the filename of the document index
 * @param wordIndex     - the filename of the word index
 * @param usePageRank   - true if page rank is to be incorporated into scoring
 */
class Query(titleIndex: String, documentIndex: String, wordIndex: String,
            usePageRank: Boolean) {

  // Maps the document ids to the title for each document
  private val idsToTitle = new HashMap[Int, String]

  // Maps the document ids to the euclidean normalization for each document
  private val idsToMaxFreqs = new HashMap[Int, Double]

  // Maps the document ids to the page rank for each document
  private val idsToPageRank = new HashMap[Int, Double]

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]


  /**
   * Handles a single query and prints out results
   *
   * @param userQuery - the query text
   */
  private def query(userQuery: String) {

    def regexToArray(regex: Regex, pageText: String): Array[String] = {
      val matchesIterator = regex.findAllMatchIn(pageText)
      matchesIterator.toArray.map { aMatch => aMatch.matched.toLowerCase }
    }

    val scaryRegex = new Regex("""[^\W_]+'[^\W_]+|[^\W_]+""")
    val justWords = regexToArray(scaryRegex, userQuery).filter(!StopWords.isStopWord(_))
    val words = PorterStemmer.stemArray(justWords)
    calcRank(words)


    def calcScore(page: Int, word: String): Double = {
      (wordsToDocumentFrequencies(word).getOrElse(page, 0.0) / idsToMaxFreqs(page)) *
        log(idsToTitle.size / wordsToDocumentFrequencies(word).size)
    }

    def calcRank(queryWords: Array[String]) {
      val idsToPageScores: mutable.Buffer[(Int, Double)] = mutable.Buffer()

      for ((k, v) <- idsToPageRank) {
        var totalPageScore = 0.0

        for (word <- queryWords) {
          if (wordsToDocumentFrequencies.contains(word)) {
            totalPageScore = totalPageScore + calcScore(k, word)
          }
        }

        if (usePageRank) {
          totalPageScore = totalPageScore * v
        }

        if (totalPageScore != 0.0) {
          val score = (k, totalPageScore)
          idsToPageScores += score
        }
      }

      val rankingList = idsToPageScores.toArray
      rankingList.sortBy(_._2)

      if (rankingList.isEmpty) {
        println("Could not find input on any pages. Try another query.")
      } else {
        printResults(rankingList.map(_._1))
      }
    }
  }

  /**
   * Format and print up to 10 results from the results list
   *
   * @param results - an array of all results to be printed
   */
  private def printResults(results: Array[Int]) {
    for (i <- 0 until Math.min(10, results.size)) {
      println("\t" + (i + 1) + " " + idsToTitle(results(i)))
    }
  }

  /**
   * Reads in the text files.
   */
  def readFiles(): Unit = {
    FileIO.readTitles(titleIndex, idsToTitle)
    FileIO.readDocuments(documentIndex, idsToMaxFreqs, idsToPageRank)
    FileIO.readWords(wordIndex, wordsToDocumentFrequencies)
  }

  /**
   * Starts the read and print loop for queries
   */
  def run() {
    val inputReader = new BufferedReader(new InputStreamReader(System.in))

    // Print the first query prompt and read the first line of input
    print("search> ")
    var userQuery = inputReader.readLine()

    // Loop until there are no more input lines (EOF is reached)
    while (userQuery != null) {
      // If ":quit" is reached, exit the loop
      if (userQuery == ":quit") {
        inputReader.close()
        return
      }

      val pre = Calendar.getInstance()
      // Handle the query for the single line of input
      query(userQuery)
      val done = Calendar.getInstance()
      val timeSec = done.get(Calendar.SECOND) - pre.get(Calendar.SECOND)
      val timeMS = done.get(Calendar.MILLISECOND) - pre.get(Calendar.MILLISECOND)
      println("Ran in " + timeSec + "s and " + timeMS + "ms.")
      // Print next query prompt and read next line of input
      print("search> ")
      userQuery = inputReader.readLine()
    }

    inputReader.close()
  }
}

object Query {
  def main(args: Array[String]) {
    try {
      // Run queries with page rank
      var pageRank = false
      var titleIndex = 0
      var docIndex = 1
      var wordIndex = 2
      if (args.size == 4 && args(0) == "--pagerank") {
        pageRank = true;
        titleIndex = 1
        docIndex = 2
        wordIndex = 3
      } else if (args.size != 3) {
        println("Incorrect arguments. Please use [--pagerank] <titleIndex> "
          + "<documentIndex> <wordIndex>")
        System.exit(1)
      }
      val query: Query = new Query(args(titleIndex), args(docIndex), args(wordIndex), pageRank)
      query.readFiles()
      query.run()
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}
