package org.saliya.graphxprimer

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.saliya.graphxprimer.multilinear.{GaloisField, Polynomial}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by esaliya on 11/15/16.
  */
object Program {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("Spark Prop Graph")
      .getOrCreate()

    val sc = spark.sparkContext

//    simpleTests(sc)
    largeTest(sc, args(0), args(1).toInt, args(2).toInt)

  }


  def largeTest(sc: SparkContext, f: String, n: Int, k: Int): Unit ={
//    val graph = createGraphFromFile("/Users/esaliya/Downloads/synthetic-graphs/geometric-0-k10-0-n5000-r10.txt", 10, 5000, sc)

    val seed: Long = 10

    val tup = createGraphFromFile(f, k, n, sc)
    val graph = tup._1
    val numColors = tup._2
    val ret = colorfulGraphMotif(graph, numColors, k, seed)
    println("\n*** Large Test for " + f + " returned " + ret + " numcolors: " + numColors + " k: " + k)
//    graph.vertices.foreach(v => println("vertex: " + v._1 + " color: " + v._2._1))
//    println("\n\n vertex count: " + graph.vertices.count())
//    println()
//    graph.edges.foreach(e => println("edge from: " + e.srcId + " to: " + e.dstId))
//    println("\n\n edge count: " + graph.edges.count())
  }

  def simpleTests (sc: SparkContext): Unit = {
    var allGood = true
    allGood = allGood & testReturnsTrueIfAllColorsInGraphAreDifferent(sc)
    println("\ntestReturnsTrueIfAllColorsInGraphAreDifferent " + (if (allGood) "... SUCCESS" else "... FAILED"))
    allGood = allGood & !testReturnsFalseIfNumberOfColorsIsLessThanK(sc)
    println("\ntestReturnsFalseIfNumberOfColorsIsLessThanK " + (if (allGood) "... SUCCESS" else "... FAILED"))
    allGood = allGood & testReturnsTrueWhenKEqualsOne(sc)
    println("\ntestReturnsTrueWhenKEqualsOne " + (if (allGood) "... SUCCESS" else "... FAILED"))
    allGood = allGood & !testReturnsFalseWhenThereIsNoColorfulMotifInPathGraph(sc)
    println("\ntestReturnsFalseWhenThereIsNoColorfulMotifInPathGraph " + (if (allGood) "... SUCCESS" else "... FAILED"))
    allGood = allGood & testReturnsTrueWhenThereIsOneColorfulMotifAtEndOfPathGraph(sc)
    println("\ntestReturnsTrueWhenThereIsOneColorfulMotifAtEndOfPathGraph " + (if (allGood) "... SUCCESS" else "... FAILED"))
  }



  def testReturnsFalseIfNumberOfColorsIsLessThanK(sc: SparkContext): Boolean = {
    val k = 5
    val n = 5
    val graph = createPathGraph(k, n, Array(0,0,0,0,0), sc)
    val seed: Long = 10
    colorfulGraphMotif(graph, 1, k, seed)
  }

  def testReturnsTrueIfAllColorsInGraphAreDifferent(sc: SparkContext): Boolean = {
    val k = 5
    val n = 5
    val graph = createPathGraph(k, n, null, sc)
    val seed: Long = 10
    colorfulGraphMotif(graph, n, k, seed)
  }

  def testReturnsTrueWhenKEqualsOne(sc: SparkContext): Boolean = {
    val k = 1
    val n = 5
    val graph = createPathGraph(k, n, Array(0,1,2,3,0), sc)
    val seed: Long = 10
    colorfulGraphMotif(graph, 4, k, seed)
  }

  def testReturnsFalseWhenThereIsNoColorfulMotifInPathGraph(sc: SparkContext): Boolean = {
    val k = 3
    val n = 5
    val graph = createPathGraph(k, n, Array(1,1,2,1,1), sc)
    val seed: Long = 0
    colorfulGraphMotif(graph, 2, k, seed)
  }

  def testReturnsTrueWhenThereIsOneColorfulMotifAtEndOfPathGraph(sc: SparkContext): Boolean = {
    val k = 3
    val n = 5
    val graph = createPathGraph(k, n, Array(2,2,2,1,0), sc)
    val seed: Long = 3

    colorfulGraphMotif(graph, 3, k, seed)
  }


  def colorfulGraphMotif(graph: Graph[(Int, Array[Int]), Int], numColors: Int, k: Int, seed: Long): Boolean = {
    // invalid input: k is negative
    if (k <= 0) throw new IllegalArgumentException("k must be a positive integer")
    // trivial case: k = 1
    // any color will do
    if (k == 1) {
      return true
    }
    // trivial case: number of colors is less than k
    // no colorful graph of size k
    if (numColors < k) {
      return false
    }

    val random = new java.util.Random(seed)
    // (1 << k) is 2 raised to the kth power
    val twoRaisedToK: Int = 1 << k
    val degree: Int = 3 + log2(k)
    val gf: GaloisField = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, random).toBigInteger.intValue)

    val randomAssignment: Array[Int] = new Array[Int](numColors)
    randomAssignment.indices.foreach(i => randomAssignment(i) = random.nextInt(twoRaisedToK))

    var totalSum: Int = 0
    val randomSeed: Long = random.nextLong

    for (i <- 0 until twoRaisedToK){
      val s = evaluateCircuit(graph  , randomAssignment, gf, k, i, randomSeed)
      totalSum = gf.add(totalSum, s)
    }

    totalSum > 0
  }



  def evaluateCircuit(graph: Graph[(Int, Array[Int]), Int], randomAssignment: Array[Int], gf: GaloisField, k: Int, iter: Int, randomSeed: Long): Int ={
    val random = new java.util.Random(randomSeed)
    val fieldSize = gf.getFieldSize

    graph.vertices.foreach(v => {
      // First clear the vertex row of table
      val rowOfTable = v._2._2
      rowOfTable.indices.foreach(i => rowOfTable(i) = 0)
      // Set the last element to initial value of i (remember i goes from 2 to k (including))
      rowOfTable(k+1) = 2

      val color = v._2._1
      val dotProduct = randomAssignment(color) & iter
      v._2._2(1) = if (Integer.bitCount(dotProduct) % 2 == 1) 0 else 1
    })

    // Now, we use the pregel operator from 2 to k (including k) times
    val initialMsg: scala.collection.mutable.HashMap[Int, Array[Int]] = null
    val maxIterations = k-1 // (k-2)+1

    val finalGraph = graph.pregel(initialMsg,maxIterations, EdgeDirection.Both)(vprogWrapper(k, random, fieldSize, gf), sendMsg, mergeMsg)

    val products = finalGraph.vertices.mapValues(v => {
      val weight = random.nextInt(fieldSize)
      val product = gf.multiply(weight, v._2(k))
      product
    }).collect()

    var circuitSum = 0
    products.indices.foreach(i => circuitSum = gf.add(circuitSum, products(i)._2))

    circuitSum
  }

  def vprogWrapper(k: Int, random: java.util.Random, fieldSize: Int, gf: GaloisField) = (vertexId: VertexId, value: (Int, Array[Int]), message: scala.collection.mutable.HashMap[Int, Array[Int]]) =>  {
    val myRowOfTable = value._2
    if (message != null) {
      val neighbors = message.keySet
      val i = myRowOfTable(k + 1)
      myRowOfTable(i) = 0

      for (j <- 1 until i) {
        for (neighbor <- neighbors) {
          val weight = random.nextInt(fieldSize)
          val neighborRowOfTable = message.get(neighbor)
          var product = gf.multiply(myRowOfTable(j), neighborRowOfTable.get(i - j))
          product = gf.multiply(weight, product)
          myRowOfTable(i) = gf.add(myRowOfTable(i), product)
        }
      }
      myRowOfTable(k + 1) += 1 // increment i

      (value._1, myRowOfTable.clone())
    } else {
      value
    }
  }

  def sendMsg(triplet: EdgeTriplet[(Int, Array[Int]), Int]): Iterator[(VertexId, scala.collection.mutable.HashMap[Int, Array[Int]])] = {
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
    * Creates a path graph with distinct colors
    *
    * @param k motif size
    * @param n the number of nodes
    * @return a Spark Graph object
    */
  def createPathGraph(k: Int, n: Int, colors: Array[Int], sc: SparkContext): Graph[(Int, Array[Int]), Int] = {

    // from Jose's test of "testReturnsTrueIfAllColorsInGraphAreDifferent" in ColorfulGraphMotifTest //

    val vertices = new Array[(Long, (Int, Array[Int]))](n)
//    val edges = new Array[Edge[Int]](n-1) // this is one way directed graph
    val edges = new Array[Edge[Int]](2*n-2)
    for (i <- 0 until n){
      // (k+1) elements for the table, last element is to keep track of superstep value,
      // i.e. value of i in the original evaluate circuit code
      vertices(i) = ( i.toLong, (if (colors == null) i else colors(i), new Array[Int](k+2)))
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

  def createGraphFromFile(f:String, k: Int, n: Int, sc: SparkContext): (Graph[(Int, Array[Int]), Int], Int) ={
    val vertices = new Array[(Long, (Int, Array[Int]))](n)
    val edges: ArrayBuffer[Edge[Int]] = new ArrayBuffer[Edge[Int]]()
    var edgeCount = 0
    val colors = new mutable.HashSet[Int]()
    var mode = -1
    for (line <- Source.fromFile(f).getLines()){
      if (mode == -1 && "# node color".equals(line)){
        mode = 0
      }

      if (mode == 0 && "# head tail".equals(line)){
        mode = 1
      }

      if (mode == 1 && "# motif".equals(line)){
        mode = 2
      }

      if (!line.startsWith("#") && mode != 2){
        val splits = line.split(" ")
        if (mode == 0){
          val vertexId = splits(0).toInt
          val color = splits(1).toInt
          colors.add(color)
          vertices(vertexId) = (vertexId.toLong, (color, new Array[Int](k+2)))
        } else if (mode == 1){
          edges += Edge(splits(0).toInt, splits(1).toInt, 1)
          edges += Edge(splits(1).toInt, splits(0).toInt, 1) // undirected edges
          edgeCount += 2
        }
      }
    }

    val defaultVertex = (-1, Array(-1))

    val verticesRDD: RDD[(VertexId, (Int, Array[Int]))] = sc.parallelize(vertices)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges)

    (Graph(verticesRDD, edgesRDD, defaultVertex), colors.size)
  }

  private def log2(x: Int): Int = {
    if (x <= 0) throw new IllegalArgumentException("Error. Argument must be greater than 0. Found " + x)
    var result: Int = 0
    var X = x
    X >>= 1
    while (X > 0) {
      result += 1
      X >>= 1
    }
    result
  }

}
