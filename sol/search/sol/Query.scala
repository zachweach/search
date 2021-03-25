package search.sol

import java.io._
import search.src.FileIO

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.HashMap

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


    def calcRank(words : Array[String]) {
      val idsToPageScores = new mutable.HashMap[Int, Double]()
      for ((k, v) <- idsToPageRank) {
        var totalPageScore = 0.0
        for (a <- words) {
          var dummyVar = wordsToDocumentFrequencies(a)(k)
          totalPageScore += dummyVar
        }
        if (usePageRank) {
          totalPageScore = totalPageScore * v
        }
        idsToPageScores += (k -> totalPageScore)
      }
      val rankingList = new Array[(Int, Double)](10)
      var counter = 0
      for ((k, v) <- idsToPageScores) {
        if (!(v == 0)) {
          if (counter < 10) {
            rankingList(counter) = (k, v)
            counter += 1
          } else {
            rankingList.sortBy(_._2)
            if (v > rankingList(0)._2) {
              rankingList(0) = (k, v)
            }
            rankingList.sortBy(_._2)
          }
        }
      }
      if (counter == 0) {
        print("Fuck u")
      } else {
        for (i <- counter to 1) {
          println(idsToTitle(rankingList(i)._1))
        }
      }
    }
    println("Implement query!")
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

      // Handle the query for the single line of input
      query(userQuery)

      // Print next query prompt and read next line of input
      print("search> ")
      userQuery = inputReader.readLine()
    }

    inputReader.close()
  }

  //AAAAAA THIS IS WHY I WROTE DA CODE IT BE HERE WHO KNOWS IF THIS IS THE RIGHT PLACE

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
