package org.saliya.graphxprimer.p3

import org.apache.log4j.{Level, Logger}
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
object Program2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val fname = args(0)
    val n = args(1).toInt
    val k = args(2).toInt
    val r = args(3).toInt
    val d = args(4).toInt // duplicate computation
    val vp = args(5).toInt // # vertex partitions
    val ep = args(6).toInt // # edge partitions

    val optionsList = args.drop(7).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => opt -> v
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)


    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    GraphXUtils.registerKryoClasses(conf)
    conf.registerKryoClasses(Array(classOf[mutable.HashMap[Int, Array[Int]]]))

    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString)
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString).getOrElse(StorageLevel.MEMORY_ONLY)

    val sc = new SparkContext(conf.setAppName("Multilinear (" + fname + ")"))
//    val sc = new SparkContext(conf.setAppName("Multilinear (" + fname + ")"))

    val tup = createGraphFromFile(fname, n, k, r, vp, ep, sc, vertexStorageLevel, edgeStorageLevel)
    val g = tup._1.cache()
    val numColors = tup._2

    val graph = partitionStrategy.foldLeft(g)(_.partitionBy(_))
    println("Read graph with " + graph.numVertices + " nodes and " + graph.numEdges + " edges")
    testGraph(fname, k, r, d, numColors, graph)

    sc.stop()

  }

  def testGraph(fname: String, k: PartitionID, r: PartitionID, d:Int, numColors: PartitionID, graph: Graph[(PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), PartitionID]): Unit = {
    val seed: VertexId = 10
    val ret = colorfulGraphMotif(graph, k, r, d, seed)
    println("\n*** Test for " + fname + " returned " + ret + " numcolors: " + numColors + " k: " + k)
  }

  def colorfulGraphMotif(graph: Graph[(Int, Array[Array[Int]], Array[Array[Int]]), Int], k: Int, r: Int, d:Int, seed: Long): mutable.HashMap[VertexId, Array[Array[Boolean]]] = {
    // invalid input: k is negative
    if (k <= 0) throw new IllegalArgumentException("k must be a positive integer")
    // trivial case: k = 1
    // any color will do
    //if (k == 1) {
    //  return true
    //}

    val random = new java.util.Random(seed)
    // (1 << k) is 2 raised to the kth power
    val twoRaisedToK: Int = 1 << k
    val degree: Int = 3 + log2(k)
    val gf: GaloisField = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, random).toBigInteger.intValue)

    // now, each node gets a different random vector instead of each color
    val randomAssignment: Array[Int] = new Array[Int](graph.numVertices.toInt)
    randomAssignment.indices.foreach(i => randomAssignment(i) = random.nextInt(twoRaisedToK))
    // auxiliary variables to make a monomial multilinear if it has degree less than k
    val completionVariables: Array[Int] = new Array[Int](k - 1)
    completionVariables.indices.foreach(i => completionVariables(i) = random.nextInt(twoRaisedToK))

    val totalSum: mutable.HashMap[VertexId, Array[Array[Int]]] = new mutable.HashMap[VertexId, Array[Array[Int]]]
    graph.vertices.foreach(v => totalSum.put(v._1, Array.fill(k + 1, r + 1)(0)))
    val randomSeed: Long = random.nextLong

    // TODO change 1 to twoRaisedToK
//    println("Running for " + twoRaisedToK + " iterations")
//    for (i <- 0 until twoRaisedToK) {
    val FIXED_ITR=5
    println("Running for " + FIXED_ITR + " iterations")
    for (i <- 0 until FIXED_ITR) {
      val startTime: Long = System.currentTimeMillis
      val s = evaluateCircuit(graph, randomAssignment, completionVariables, gf, k, r, d, i, randomSeed)
      // TODO Is there a more efficient way to merge?
      for ((node, tableForNode) <- s) {
        val tableTotalSum = totalSum(node)
        for (kPrime <- 0 until (k + 1)) {
          for (rPrime <- 0 until (r + 1)) {
            //int weight = random.nextInt(gf.getFieldSize())
            //int product = gf.ffMultiply(weight, tableForNode[kPrime][rPrime])
            //tableTotalSum[kPrime][rPrime] = gf.add(tableTotalSum[kPrime][rPrime], product)
            tableTotalSum(kPrime)(rPrime) = gf.add(tableTotalSum(kPrime)(rPrime), tableForNode(kPrime)(rPrime))
          }
        }
      }
      val endTime: Long = System.currentTimeMillis
      println("Iteration " + i + ": Took " +  (endTime - startTime) / 1000.0 + " seconds")
    }

    val decisionTable = new mutable.HashMap[VertexId, Array[Array[Boolean]]]
    graph.vertices.foreach(v => totalSum.put(v._1, Array.fill(k + 1, r + 1)(0)))
    for ((node, tableTotalSum) <- totalSum) {
      val decisionTableForNode = decisionTable(node)
      for (kPrime <- 0 until (k + 1)) {
        for (rPrime <- 0 until (r + 1)) {
          //int weight = random.nextInt(gf.getFieldSize());
          //int product = gf.ffMultiply(weight, tableForNode[kPrime][rPrime]);
          //tableTotalSum[kPrime][rPrime] = gf.add(tableTotalSum[kPrime][rPrime], product);
          decisionTableForNode(kPrime)(rPrime) = tableTotalSum(kPrime)(rPrime) > 0
        }
      }
    }
    decisionTable
  }

  def evaluateCircuit(graph: Graph[(Int, Array[Array[Int]], Array[Array[Int]]), Int], randomAssignment: Array[Int], completionVariables: Array[Int], gf: GaloisField, k: Int, r: Int, d:Int, iter: Int, randomSeed: Long): mutable.HashMap[VertexId, Array[Array[Int]]] ={
    val random = new java.util.Random(randomSeed)
    val fieldSize = gf.getFieldSize

    // the ith index of this array contains the polynomial
    // y_1 * y_2 * ... y_i
    // where y_0 is defined as the identity of the finite field, (i.e., 1)
    val cumulativeCompletionVariables: Array[Int] = new Array[Int](k)
    cumulativeCompletionVariables(0) = 1
		for (i <- 1 until k) {
			val dotProduct = completionVariables(i - 1) & iter // dot product is bitwise and
			cumulativeCompletionVariables(i) = if (Integer.bitCount(dotProduct) % 2 == 1) 0 else cumulativeCompletionVariables(i - 1)
		}

    graph.vertices.foreach(v => {
      // First clear the vertex row of table
      val rowOfTable = v._2._2
      rowOfTable.indices.foreach(i => rowOfTable(i).indices.foreach(j => rowOfTable(i)(j) = 0))
      // Set the last element to initial value of i (remember i goes from 2 to k (including))
      rowOfTable(k+1) = Array.fill(r + 1)(2)
      // do the same for the extended table
      val rowOfExtendedTable = v._2._3
      rowOfExtendedTable.indices.foreach(i => rowOfExtendedTable(i).indices.foreach(j => rowOfExtendedTable(i)(j) = 0))
      // Set the last element to initial value of i (remember i goes from 2 to k (including))
      rowOfExtendedTable(k+1) = Array.fill(r + 1)(2)

      val nodePrize = v._2._1
      val dotProduct = randomAssignment(v._1.toInt) & iter
      val eigenvalue = if (Integer.bitCount(dotProduct) % 2 == 1) 0 else 1
      rowOfTable(1)(nodePrize) = eigenvalue
      rowOfExtendedTable(1)(nodePrize) = eigenvalue * cumulativeCompletionVariables(k - 1)
    })


    // Now, we use the pregel operator from 2 to k (including k) times
    val initialMsg: scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])] = null
    val maxIterations = k-1 // (k-2)+1

    val finalGraph = graph.pregel(initialMsg,maxIterations, EdgeDirection.Out)(vprogWrapper(k, r, d,random, fieldSize, gf, cumulativeCompletionVariables), sendMsg, mergeMsg)

//    val products = finalGraph.vertices.cache().mapValues(v => {
//      val weight = random.nextInt(fieldSize)
//      val product = gf.ffMultiply(weight, v._2(k))
//      product
//    }).collect()
//    var circuitSum = 0
//    products.indices.foreach(i => circuitSum = gf.add(circuitSum, products(i)._2))
//    circuitSum

    val extendedTable = new mutable.HashMap[VertexId, Array[Array[Int]]]
    finalGraph.vertices.cache().foreach(v => extendedTable.put(v._1, v._2._3))
    extendedTable
  }

  def vprogWrapper(k: Int, r: Int, d:Int, random: java.util.Random, fieldSize: Int, gf: GaloisField, cumulativeCompletionVariables: Array[Int]): (VertexId, (PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), mutable.HashMap[PartitionID, (Array[Array[PartitionID]], Array[Array[PartitionID]])]) => (PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]) = (vertexId: VertexId, value: (Int, Array[Array[Int]], Array[Array[Int]]), message: scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])]) =>  {
    if (message != null) {
      val myRowOfTable = value._2
      val myRowOfExtendedTable = value._3

      val neighbors = message.keySet
      val i = myRowOfTable(k + 1)(0)
      myRowOfTable(i) = Array.fill(r + 1)(0)

      for (duplicate <- 0 until d) { // The computation duplicate loop
        // for every quota l from 0 to r
        for (l <- 0 until (r + 1)) {
          // initialize the polynomial P_{i,u,l}
          var polynomial = 0
          // recursive step:
          // iterate through all the pairs of polynomials whose sizes add up to i
          for (iPrime <- 1 until i) {
            for (neighbor <- neighbors) {
              for (lPrime <- 0 until (l + 1)) {
                // TODO - (node,neighbor,i,j) will always get the same random number through (2^k) invocations of the evaluate circuit
                // TODO - so it boils down to fixing the order of neighbors that we process
                // TODO - We can keep list of neighbors for each node.
                // TODO - The other option is to have a weight lookup table
                val weight = random.nextInt(fieldSize)
                val neighborRowOfTable = message(neighbor)._1
                var product = gf.ffMultiply(myRowOfTable(iPrime)(lPrime), neighborRowOfTable(i - iPrime)(l - lPrime))
                product = gf.ffMultiply(weight, product)
                polynomial = gf.add(polynomial, product)
              }
            }
          }
          myRowOfTable(i)(l) = polynomial
          if (cumulativeCompletionVariables(k - i) != 0) {
            myRowOfExtendedTable(i)(l) = myRowOfTable(i)(l)
          }
        }
      }
      myRowOfTable(k + 1)(0) += 1 // increment i

      (value._1, myRowOfTable.clone(), myRowOfExtendedTable.clone())
    } else {
      value
    }
  }

  def sendMsg(triplet: EdgeTriplet[(Int, Array[Array[Int]], Array[Array[Int]]), Int]): Iterator[(VertexId, scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])])] = {
    val hm = new scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])]
    hm += triplet.srcId.toInt -> (triplet.srcAttr._2, triplet.srcAttr._3)
    Iterator((triplet.dstId, hm))
  }

  def mergeMsg(msg1: scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])], msg2: scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])]): scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])] = {
    val keys = msg2.keys
    for (key <- keys) {
      val array = msg2.get(key)
      if (array.nonEmpty) {
        msg1.put(key, array.get)
      }
    }
    msg1
  }


  def createGraphFromFile(f:String, n: Int, k: Int, r: Int, vp: Int, ep: Int, sc: SparkContext, vsl: StorageLevel, esl: StorageLevel): (Graph[(PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), PartitionID], PartitionID) ={
    val vertices = new Array[(Long, (Int, Array[Array[Int]], Array[Array[Int]]))](n)
    val edges: ArrayBuffer[Edge[Int]] = new ArrayBuffer[Edge[Int]]()
    var edgeCount = 0
    val alpha = 0.15
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
          val prize = if (splits(1).toFloat < alpha) 1 else 0
          colors.add(prize)
          vertices(vertexId) = (vertexId.toLong, (prize, Array.fill(k+2, r+1)(0), Array.fill(k+2, r+1)(0)))
        } else if (mode == 1) {
          edges += Edge(splits(0).toInt, splits(1).toInt, 1)
          edges += Edge(splits(1).toInt, splits(0).toInt, 1) // undirected edges
          edgeCount += 2
        }
      }
    }

    val defaultVertex = (-1, Array(Array(-1)), Array(Array(-1)))

    val verticesRDD: RDD[(VertexId, (Int, Array[Array[Int]], Array[Array[Int]]))] = sc.parallelize(vertices, vp).persist(vsl)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges, ep).persist(esl)

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
