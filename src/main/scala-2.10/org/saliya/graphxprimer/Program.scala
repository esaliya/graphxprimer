package org.saliya.graphxprimer

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.saliya.graphxprimer.multilinear.{GaloisField, Polynomial}

/**
  * Created by esaliya on 11/15/16.
  */
object Program {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Prop Graph").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val k = 5
    val n = 5
    val graph = createGraph(k, n, sc)

    graph.vertices.foreach(v =>
      print("## vertexID: " + v._1 + " color: " + v._2._1 + " arrayVals: " + v._2._2.mkString("[", ",", "]") + "\n"))

    val seed: Int = 10
    val answer = colorfulGraphMotif(graph, n, k, seed)

    print("@@@ answer: " + answer)

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

    /*
    int[] randomAssignment = new int[numColors];
		//System.out.println("Random Assignment:");
		for (int i = 0; i < numColors; i++) {
			randomAssignment[i] = random.nextInt(twoRaisedToK);
			//System.out.println("color: " + i + " vector: " + randomAssignment[i]);
		}

		int totalSum = 0;
		long randomSeed = random.nextLong();

		for (int i = 0; i < twoRaisedToK; i++) {
			//System.out.println("Running circuit for i = " + i);
			int s = evaluateCircuit(G, randomAssignment, gf, k, i, randomSeed);
			//System.out.println("s = " + s);
			totalSum = gf.add(totalSum, s);
			//System.out.println("totalSum = " + totalSum);
		}
		return totalSum > 0;
     */

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

  def evaluateCircuit(graph: Graph[(Int, Array[Int]), Int], randomAssignment: Array[Int], gf: GaloisField, k: Int, iter: Int, randomSeed: Long): Int ={
    val random = new java.util.Random(randomSeed)
    val fieldSize = gf.getFieldSize

    graph.vertices.foreach(v => {
      // First clear the vertex row of table
      val rowOfTable = v._2._2
      rowOfTable.indices.foreach(i => rowOfTable(i) = 0)
      // set the last element to initial value of i (remember i goes from 2 to k (including))
      rowOfTable(k+1) = 2

      val color = v._2._1
      val dotProduct = randomAssignment(color) & iter
      v._2._2(1) = if (Integer.bitCount(dotProduct) % 2 == 1) 0 else 1
    })


    def vprog(vertexId: VertexId, value: (Int, Array[Int]), message: scala.collection.mutable.HashMap[Int, Array[Int]]): (Int, Array[Int]) = {
      if (message != null) {
        val neighbors = message.keys
        val myRowOfTable = value._2
        val i = myRowOfTable(k + 1)
        myRowOfTable(i) = 0
        for (j <- 1 until i) {
          for (neigbor <- neighbors) {
            val weight = random.nextInt(fieldSize)
            val neighborRowOfTable = message.get(neigbor)
            var product = gf.multiply(myRowOfTable(j), neighborRowOfTable.get(i - j))
            product = gf.multiply(weight, product)
            myRowOfTable(i) = gf.add(myRowOfTable(i), product)
          }
        }
        myRowOfTable(k + 1) += 1 // increment i
      }
      value
    }

    // Now, we use the pregel operator from 2 to k (including k) times

    val initialMsg: scala.collection.mutable.HashMap[Int, Array[Int]] = null
    val maxIterations = k-1 // (k-2)+1

    val finalGraph = graph.pregel(initialMsg,maxIterations, EdgeDirection.Both)(vprog, sendMsg, mergeMsg)

    /*
    int circuitSum = 0;
		for (int node: nodes) {
			int weight = random.nextInt(fieldSize);
			int product = gf.multiply(weight, optTable.get(node)[k]);
			circuitSum = gf.add(circuitSum,  product);
		}
		return circuitSum;
     */
    val products = finalGraph.vertices.mapValues(v => {
      val weight = random.nextInt(fieldSize)
      val product = gf.multiply(weight, v._2(k))
      product
    }).collect()

    var circuitSum = 0
    products.indices.foreach(i => circuitSum = gf.add(circuitSum, products(i)._2))

    circuitSum
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
    val edges = new Array[Edge[Int]](n-1)
    for (i <- 0 until n){
      // (k+1) elements for the table, last element is to keep track of superstep value,
      // i.e. value of i in the original evaluate circuit code
      vertices(i) = (i.toLong, (i, new Array[Int](k+2)))
      if (i < n-1){
        edges(i) = Edge(i.toLong, (i+1).toLong, 0) // The edge value is not necessary here but seems Spark needs to have one
      }
    }

    val defaultVertex = (-1, Array(-1))

    val verticesRDD: RDD[(VertexId, (Int, Array[Int]))] = sc.parallelize(vertices)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges)

    Graph(verticesRDD, edgesRDD, defaultVertex)
  }

}
