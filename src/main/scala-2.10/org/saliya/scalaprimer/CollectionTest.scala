package org.saliya.scalaprimer

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by esaliya on 11/17/16.
  */
object CollectionTest {
  def main(args: Array[String]): Unit = {

    val i = 2
    for (j <- 1 until i){
      println("** " + j)
    }

    val a = 12
    println(a.toBinaryString)
    val b = 9
    println(b.toBinaryString)
    val aAndB = a & b
    println(aAndB.toBinaryString)



    val hm = new mutable.HashMap[Int, Array[Int]]

//    hm += (1 -> Array(1,2))
//    hm += (2 -> Array(2,3))
    hm += (3 -> Array(3,4))

    val keys = hm.keySet
    for (key <- keys){
      println("@@@ key: " + key)
    }






  }

}
