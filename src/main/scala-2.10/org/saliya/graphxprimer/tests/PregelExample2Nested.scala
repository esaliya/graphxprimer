package org.saliya.graphxprimer.tests

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by esaliya on 11/16/16.
  */
object PregelExample2Nested {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pregel Example2")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext

    val k = 5
    val n = 5
    val graph = createGraph(k, n, sc)



    // Check the graph
    graph.vertices.collect.foreach(v => println("ID: "  + v._1 + " color: " + v._2._1 + " array: " + v._2._2.mkString("[", ",", "]")))

    val sumGraph = graph.pregel(initialMsg,
      2,
      EdgeDirection.Both)(
      vprogWrapper(1),
      sendMsg,
      mergeMsg)

    sumGraph.vertices.collect.foreach(v => println("ID: "  + v._1 + " color: " + v._2._1 + " array: " + v._2._2.mkString("[", ",", "]")))
  }

  val initialMsg: scala.collection.mutable.HashMap[Int, Array[Int]] = null

  def vprogWrapper(init: Int) = (vertexId: VertexId, value: (Int, Array[Int]), message: scala.collection.mutable.HashMap[Int, Array[Int]]) => {
    val array = value._2
    if (message != null){
      val neighbors = message.keySet
      for (neighbor <- neighbors){
        val x = message(neighbor)(0)
        array(0) += x
      }
    } else {
      array(0) = init
    }
    value
  }

  def sendMsg(triplet: EdgeTriplet[(Int, Array[Int]), Int]): Iterator[(VertexId, scala.collection.mutable.HashMap[Int, Array[Int]])]  = {
    val hm = new scala.collection.mutable.HashMap[Int, Array[Int]]
    hm += triplet.srcId.toInt -> triplet.srcAttr._2
    Iterator((triplet.dstId, hm))
  }

  def mergeMsg(msg1: scala.collection.mutable.HashMap[Int, Array[Int]], msg2: scala.collection.mutable.HashMap[Int, Array[Int]]): scala.collection.mutable.HashMap[Int, Array[Int]] = {
    val keys = msg2.keys
    for (key <- keys) {
      val array = msg2.get(key)
      if (array.nonEmpty) {
        msg1.put(key, array.get)
      }
    }
    msg1
  }

  /**
    * A quick test method to create a custom graph.
    * A generalized method is necessary in future.
    *
    * @param k motif size
    * @param n the number of nodes
    * @return a Spark Graph object
    */
  def createGraph(k: Int, n: Int, sc: SparkContext): Graph[(Int, Array[Int]), Int] = {

    // from Jose's test of "testReturnsTrueIfAllColorsInGraphAreDifferent" in ColorfulGraphMotifTest //

    val vertices = new Array[(Long, (Int, Array[Int]))](n)
    //    val edges = new Array[Edge[Int]](n-1) // this is one way directed graph
    val edges = new Array[Edge[Int]](2*n-2)
    for (i <- 0 until n){
      // (k+1) elements for the table, last element is to keep track of superstep value,
      // i.e. value of i in the original evaluate circuit code
      vertices(i) = (i.toLong, (i, new Array[Int](k+2)))
      if (i < n-1){
        edges(2*i) = Edge(i.toLong, (i+1).toLong, 0) // The edge value is not necessary here but seems Spark needs to have one
        edges(2*i+1) = Edge((i+1).toLong, i.toLong, 0) // The edge value is not necessary here but seems Spark needs to have one
      }
    }

    val defaultVertex = (-1, Array(-1))

    val verticesRDD: RDD[(VertexId, (Int, Array[Int]))] = sc.parallelize(vertices)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges)

    Graph(verticesRDD, edgesRDD, defaultVertex)
  }

}


