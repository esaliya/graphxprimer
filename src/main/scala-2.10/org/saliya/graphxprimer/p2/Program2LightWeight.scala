package org.saliya.graphxprimer.p2

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.saliya.graphxprimer.multilinear.{GaloisField, Polynomial}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Saliya Ekanayake on 1/14/17.
  */
object Program2LightWeight {
  def main(args: Array[String]): Unit = {
    val fname = args(0)
    val n = args(1).toInt
    val k = args(2).toInt

    val optionsList = args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)


    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    conf.registerKryoClasses(Array(classOf[mutable.HashMap[Int, Array[Int]]]))

    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString)
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString).getOrElse(StorageLevel.MEMORY_ONLY)

    val sc = new SparkContext(conf.setAppName("Multilinear (" + fname + ")"))

    val tup = createGraphFromFile(fname, n, k, sc, vertexStorageLevel, edgeStorageLevel)
    val g = tup._1.cache()
    val numColors = tup._2

    val graph = partitionStrategy.foldLeft(g)(_.partitionBy(_))

    testGraph(fname, k, numColors, graph)

    sc.stop()

  }

  def testGraph(fname: String, k: PartitionID, numColors: PartitionID, graph: Graph[(PartitionID, Array[PartitionID]), PartitionID]): Unit = {
    val seed: VertexId = 10
    val ret = colorfulGraphMotif(graph, numColors, k, seed)
    println("\n*** Test for " + fname + " returned " + ret + " numcolors: " + numColors + " k: " + k)
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

    // TODO - stripping - make just 1 run
//    for (i <- 0 until twoRaisedToK){
    for (i <- 0 until 1){
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
//    val initialMsg: scala.collection.mutable.HashMap[Int, Array[Int]] = null
    // TODO - stripping - let's use a simple array
    val initialMsg: Array[Int] = null
    val maxIterations = k-1 // (k-2)+1

    val finalGraph = graph.pregel(initialMsg,maxIterations, EdgeDirection.Out)(vprogWrapper(k, random, fieldSize, gf), sendMsg, mergeMsg)

    val products = finalGraph.vertices.cache().mapValues(v => {
      val weight = random.nextInt(fieldSize)
      val product = gf.multiply(weight, v._2(k))
      product
    }).collect()

    var circuitSum = 0
    products.indices.foreach(i => circuitSum = gf.add(circuitSum, products(i)._2))

    circuitSum
  }

  // TODO - stripping - just using an array
  //  def vprogWrapper(k: Int, random: java.util.Random, fieldSize: Int, gf: GaloisField) = (vertexId: VertexId, value: (Int, Array[Int]), message: scala.collection.mutable.HashMap[Int, Array[Int]]) =>  {
  def vprogWrapper(k: Int, random: java.util.Random, fieldSize: Int, gf: GaloisField) = (vertexId: VertexId, value: (Int, Array[Int]), message: Array[Int]) =>  {
    val myRowOfTable = value._2
    if (message != null) {

      // TODO - strpping - no computation or lookup of gf field
      /*val neighbors = message.keySet
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
      myRowOfTable(k + 1) += 1 // increment i*/

      (value._1, myRowOfTable.clone())
    } else {
      value
    }
  }

//  def sendMsg(triplet: EdgeTriplet[(Int, Array[Int]), Int]): Iterator[(VertexId, scala.collection.mutable.HashMap[Int, Array[Int]])] = {
//    val hm = new scala.collection.mutable.HashMap[Int, Array[Int]]
//    hm += triplet.srcId.toInt -> triplet.srcAttr._2
//    Iterator((triplet.dstId, hm))
//  }

  // TODO - stripping - just send my array. If this works then we'll have to include vertex ID as an array element
  def sendMsg(triplet: EdgeTriplet[(Int, Array[Int]), Int]): Iterator[(VertexId, Array[Int])] = {
    Iterator((triplet.dstId, triplet.srcAttr._2))
  }

  /*def mergeMsg(msg1: scala.collection.mutable.HashMap[Int, Array[Int]], msg2: scala.collection.mutable.HashMap[Int, Array[Int]]): scala.collection.mutable.HashMap[Int, Array[Int]] = {
    val keys = msg2.keys
    for (key <- keys) {
      val array = msg2.get(key)
      if (array.nonEmpty) {
        msg1.put(key, array.get)
      }
    }
    msg1
  }*/

  // TODO - stripping - don't merge, just send one that you get
  def mergeMsg(msg1: Array[Int], msg2: Array[Int]): Array[Int] = {
    msg1
  }


  def createGraphFromFile(f:String, n: Int, k: Int, sc: SparkContext, vsl: StorageLevel, esl: StorageLevel): (Graph[(Int, Array[Int]), Int], Int) ={
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

    val verticesRDD: RDD[(VertexId, (Int, Array[Int]))] = sc.parallelize(vertices).persist(vsl)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges).persist(esl)

    (Graph(verticesRDD, edgesRDD, defaultVertex), colors.size)
  }

  def log2(x: Int): Int = {
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
