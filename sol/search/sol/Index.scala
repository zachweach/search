package search.sol

import scala.math._

/**
 * Provides an XML indexer, produces files for a querier
 *
 * @param inputFile - the filename of the XML wiki to be indexed
 */
class Index(val inputFile: String) {
  // TODO : Implement!

  val n = 3
  val delta = 0.0001
  val epsilon = 0.15

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
    true //TODO: How do I know if a file links to another file?
  }

  def weight(j: Int, k: Int): Double = { //TODO: Currently, pages are represented as integers. Unsure what they're supposed to be. check handout
    if (linksTo(j, k)) {
      (epsilon/n) + (1-epsilon) * (1/n)
    } else {
      epsilon/n
    }
  }

  def pageRank(): Unit = { //TODO: What should this return? Unit? I'm not sure--look at handout maybe
    var prevRanks = Array.fill(n)(0.0)
    val nextRanks = Array.fill(n)(1.0/n)

    while (distance(prevRanks, nextRanks) > delta) {
      prevRanks = nextRanks
      for (j <- Range(0, n-1)) {
        nextRanks(j) = 0
        for (k <- Range(0, n-1)) {
          nextRanks(j) = nextRanks(j) + (weight(j, k) * prevRanks(k))
        }
      }
    }
  }
}

object Index {
  def main(args: Array[String]) {
    // TODO : Implement!
    System.out.println("Not implemented yet!")
  }
}
