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

    val optionsList = args.drop(4).map { arg =>
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

    val sc = new SparkContext(conf.setAppName("Multilinear (" + fname + ")").setMaster("local[*]"))
   //val sc = new SparkContext(conf.setAppName("Multilinear (" + fname + ")"))

    //val tup = createGraphFromFile(fname, n, k, r, sc, vertexStorageLevel, edgeStorageLevel)
    val tup = readAPDM(fname, k, r, sc, vertexStorageLevel, edgeStorageLevel)
    val g = tup._1.cache()
    val numColors = tup._2

    val graph = partitionStrategy.foldLeft(g)(_.partitionBy(_))
    println("Read graph with " + graph.numVertices + " nodes and " + graph.numEdges + " edges")
    testGraph(fname, k, r, numColors, graph)

    sc.stop()

  }

  def testGraph(fname: String, k: PartitionID, r: PartitionID, numColors: PartitionID, graph: Graph[(PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), PartitionID]): Unit = {
    val seed: Int = 10
    //val iter: Int = Math.max(10, Math.log(graph.numVertices)).toInt
    val iter: Int = 1
    val alpha: Double = 0.15
    var bestScore: Double = 0
    for (i <- 0 until iter) {
      val existenceTable = colorfulGraphMotif(graph, k, r, seed)
      val bestScoreFromTable = getBestScoreFromTable(graph, alpha, existenceTable);
      bestScore = Math.max(bestScoreFromTable, bestScore)
    }
    println("\n*** Test for " + fname + " returned " + bestScore)
    //println("\n*** Test for " + fname + " returned " + ret + "k: " + k)
  }

  def getBestScoreFromTable(graph: Graph[(PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), PartitionID], alpha: Double, existenceTable: Array[Array[Array[Boolean]]]): Double = {
    var bestScore: Double = 0
    graph.vertices.foreach(v => {
      val existenceForNode = existenceTable(v._1.toInt)
      for (kPrime <- 1 until existenceForNode.length) {
        for (rPrime <- existenceForNode(kPrime).indices) {
          if (existenceForNode(kPrime)(rPrime)) {
            val score = BJ(alpha, rPrime, kPrime)
            bestScore = Math.max(bestScore, score)
          }
        }
      }
    })
    bestScore
  }

  def BJ(alpha: Double, anomalousCount: Int, setSize: Int): Double = {
    setSize * KL(anomalousCount.toDouble / setSize, alpha)
  }

  def KL(a: Double, b: Double): Double = {
    assert(a >= 0 && a <= 1)
    assert(b > 0 && b < 1)

    // corner cases
    if (a == 0) {
      return Math.log(1 / (1 - b))
    }
    if (a == 1) {
      return Math.log(1 / b)
    }

    a * Math.log(a / b) + (1 - a) * Math.log((1 - a) / (1 - b))
  }


  def colorfulGraphMotif(graph: Graph[(Int, Array[Array[Int]], Array[Array[Int]]), Int], k: Int, r: Int, seed: Long): Array[Array[Array[Boolean]]] = {
    // invalid input: k is negative
    if (k <= 0) throw new IllegalArgumentException("k must be a positive integer")

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
    val randomSeed: Long = random.nextLong
    val totalSum: Array[Array[Array[Int]]] = Array.fill(graph.numVertices.toInt, k + 1, r + 1)(0)

    // TODO change 1 to twoRaisedToK
    println("Running for " + twoRaisedToK + " iterations")
    for (i <- 0 until twoRaisedToK) {
      val startTime: Long = System.currentTimeMillis
      val finalGraph = evaluateCircuit(graph, randomAssignment, completionVariables, gf, k, r, i, randomSeed)
      // TODO Is there a more efficient way to merge?
      finalGraph.vertices.cache().foreach(v => {
        val tableForNode = v._2._3
        val tableTotalSum = totalSum(v._1.toInt)
        for (kPrime <- 0 until (k + 1)) {
          for (rPrime <- 0 until (r + 1)) {
            //int weight = random.nextInt(gf.getFieldSize())
            //int product = gf.ffMultiply(weight, tableForNode[kPrime][rPrime])
            //tableTotalSum[kPrime][rPrime] = gf.add(tableTotalSum[kPrime][rPrime], product)
            tableTotalSum(kPrime)(rPrime) = gf.add(tableTotalSum(kPrime)(rPrime), tableForNode(kPrime)(rPrime))
          }
        }
      })
      val endTime: Long = System.currentTimeMillis
      println("Iteration " + i + ": Took " +  (endTime - startTime) / 1000.0 + " seconds")
    }

    val decisionTable: Array[Array[Array[Boolean]]] = Array.fill(graph.numVertices.toInt, k + 1, r + 1)(false)
    totalSum.indices.foreach(node => {
      val tableTotalSum = totalSum(node)
      val decisionTableForNode = decisionTable(node)
      for (kPrime <- 0 until (k + 1)) {
        for (rPrime <- 0 until (r + 1)) {
          //int weight = random.nextInt(gf.getFieldSize());
          //int product = gf.ffMultiply(weight, tableForNode[kPrime][rPrime]);
          //tableTotalSum[kPrime][rPrime] = gf.add(tableTotalSum[kPrime][rPrime], product);
          decisionTableForNode(kPrime)(rPrime) = tableTotalSum(kPrime)(rPrime) > 0
        }
      }
    })
    decisionTable
  }

  def evaluateCircuit(graph: Graph[(Int, Array[Array[Int]], Array[Array[Int]]), Int], randomAssignment: Array[Int], completionVariables: Array[Int], gf: GaloisField, k: Int, r: Int, iter: Int, randomSeed: Long): Graph[(PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), PartitionID] ={
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
      if (nodePrize <= r) {
        rowOfTable(1)(nodePrize) = eigenvalue
        rowOfExtendedTable(1)(nodePrize) = eigenvalue * cumulativeCompletionVariables(k - 1)
      }
    })


    // Now, we use the pregel operator from 2 to k (including k) times
    val initialMsg: scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])] = null
    val maxIterations = k-1 // (k-2)+1

    val finalGraph = graph.pregel(initialMsg,maxIterations, EdgeDirection.Out)(vprogWrapper(k, r, random, fieldSize, gf, cumulativeCompletionVariables), sendMsg, mergeMsg)

//    val products = finalGraph.vertices.cache().mapValues(v => {
//      val weight = random.nextInt(fieldSize)
//      val product = gf.ffMultiply(weight, v._2(k))
//      product
//    }).collect()
//    var circuitSum = 0
//    products.indices.foreach(i => circuitSum = gf.add(circuitSum, products(i)._2))
//    circuitSum
    finalGraph
  }

  def vprogWrapper(k: Int, r: Int, random: java.util.Random, fieldSize: Int, gf: GaloisField, cumulativeCompletionVariables: Array[Int]): (VertexId, (PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), mutable.HashMap[PartitionID, (Array[Array[PartitionID]], Array[Array[PartitionID]])]) => (PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]) = (vertexId: VertexId, value: (Int, Array[Array[Int]], Array[Array[Int]]), message: scala.collection.mutable.HashMap[Int, (Array[Array[Int]], Array[Array[Int]])]) =>  {
    if (message != null) {
      val myRowOfTable = value._2
      val myRowOfExtendedTable = value._3

      val neighbors = message.keySet
      val i = myRowOfTable(k + 1)(0)
      myRowOfTable(i) = Array.fill(r + 1)(0)

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


  def createGraphFromFile(f:String, n: Int, k: Int, r: Int, sc: SparkContext, vsl: StorageLevel, esl: StorageLevel): (Graph[(PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), PartitionID], PartitionID) ={
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
        if (mode == 0) {
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

    val verticesRDD: RDD[(VertexId, (Int, Array[Array[Int]], Array[Array[Int]]))] = sc.parallelize(vertices).persist(vsl)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges).persist(esl)

    (Graph(verticesRDD, edgesRDD, defaultVertex), colors.size)
  }

  def readAPDM(f:String, k: Int, r: Int, sc: SparkContext, vsl: StorageLevel, esl: StorageLevel): (Graph[(PartitionID, Array[Array[PartitionID]], Array[Array[PartitionID]]), PartitionID], PartitionID) ={

    val lines: Array[String] = Source.fromFile(f).getLines.toArray

    // read number of nodes and edges
    var lineNumber: Int = 0
    var line = lines(lineNumber)
    while (! line.startsWith("SECTION1") && lineNumber < lines.length) {
      lineNumber += 1
      line = lines(lineNumber)
    }
    lineNumber += 1
    line = lines(lineNumber)
    val n = line.stripLineEnd.split(" ").last.toInt
    lineNumber += 1
    line = lines(lineNumber)
    val m = line.stripLineEnd.split(" ").last.toInt

    // initialize data structures for nodes and edges
    val vertices = new Array[(Long, (Int, Array[Array[Int]], Array[Array[Int]]))](n)
    val edges = new Array[Edge[Int]](2 * m) // double count because Scala uses directed edges

    // read nodes
    lineNumber += 1
    line = lines(lineNumber)
    while (! line.startsWith("SECTION2") && lineNumber < lines.length) {
      lineNumber += 1
      line = lines(lineNumber)
    }
    lineNumber += 1
    line = lines(lineNumber)
    lineNumber += 1
    line = lines(lineNumber)
    while (! line.startsWith("END")) {
      val tokens = line.stripLineEnd.split(" ")
      val vertexId = tokens(0).toInt
      val prize = tokens(1).toDouble.toInt
      vertices(vertexId) = (vertexId.toLong, (prize, Array.fill(k+2, r+1)(0), Array.fill(k+2, r+1)(0)))
      lineNumber += 1
      line = lines(lineNumber)
    }
    // read edges
    lineNumber += 1
    line = lines(lineNumber)
    lineNumber += 1
    line = lines(lineNumber)
    lineNumber += 1
    line = lines(lineNumber)
    lineNumber += 1
    line = lines(lineNumber)
    var numEdges = 0
    while (! line.startsWith("END")) {
      val tokens = line.stripLineEnd.split(" ")
      val u = tokens(0).toInt
      val v = tokens(1).toInt
      edges(numEdges) = Edge(u.toInt, v.toInt, 1)
      numEdges += 1
      edges(numEdges) = Edge(v.toInt, u.toInt, 1)
      numEdges += 1
      lineNumber += 1
      line = lines(lineNumber)
    }
    val defaultVertex = (-1, Array(Array(-1)), Array(Array(-1)))

    val verticesRDD: RDD[(VertexId, (Int, Array[Array[Int]], Array[Array[Int]]))] = sc.parallelize(vertices).persist(vsl)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges).persist(esl)

    (Graph(verticesRDD, edgesRDD, defaultVertex), 0)
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
