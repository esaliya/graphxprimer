package org.saliya.scalaprimer

/**
  * Saliya Ekanayake on 1/5/17.
  */
object Loops {
  def main(args: Array[String]): Unit = {
    val neighbors = 5
    for (neighbor <- 0 until neighbors) {
      println(neighbor)
    }
  }
}
