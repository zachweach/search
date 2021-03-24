package search.sol

import scala.collection.mutable
import scala.math.pow
import scala.math.log
import scala.xml.{Node, NodeSeq}

/**
 * Provides an XML indexer, produces files for a querier
 *
 * @param inputFile - the filename of the XML wiki to be indexed
 */
class Index(val inputFile: String) {
  // TODO : Implement!

  def parse(): String /*mutable.HashMap[Int, String]*/ = {
    var s = ""
    val output = new mutable.HashMap[Int, String]()
    val mainNode: Node = xml.XML.loadFile(inputFile)
    val pageSeq: NodeSeq = mainNode \ "page"
    for (node <- pageSeq) {
      val pageID: Int = (node \ "id").text.replaceAll("\\s","").toInt
      val pageText: String = (node \ "text").text
      output + (pageID.toInt -> pageText)
      s + pageID
    }
    output
    s
  }

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

  def findIdf(word : String, X: mutable.HashMap[Int, Array[String]]): Double = {
    var n = 0
    var ni = 0
    for ((k, v) <- X) {
      n += 1
      if (v.contains(word)) {
        ni += 1
      }
    }
    log(n/ni)
  }
}


object Index {
  def main(args: Array[String]) {
    // TODO : Implement!
    System.out.println("Not implemented yet!")
    //System.out.println(new Index("sol\\\\search\\\\sol\\\\SmallWiki.xml").parse())
    //System.out.println((xml.XML.loadFile("sol\\\\search\\\\sol\\\\SmallWiki.xml") \ "page" \ "id").text)
  }
}
