package org.saliya.graphxprimer.livejournal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.saliya.graphxprimer.multilinear.{GaloisField, Polynomial}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

/**
  * Created by esaliya on 11/15/16.
  */
object ProgramLikePRSimple {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)


    val sc = new SparkContext(conf.setAppName("Multilinear").setMaster("local[1]"))

    largeTest(sc, args(0), args(1).toInt, args(2).toInt)

  }


  def largeTest(sc: SparkContext, f: String, n: Int, k: Int): Unit ={
    val seed: Long = 10

    // TODO - debug - create a simple graph here
    val tup = createLiveJournalGraphFromFile(f, k, n, sc)
//    val tup = createGraphFromFile(f, k, n, sc)
//    val tup = createSimpleGraphFromFile(f, k, n, sc)
    // TODO - debug - add caching here similar to the original PR
    val g = tup._1.cache()
    val graph = g.partitionBy(RandomVertexCut)
    val numColors = tup._2

    // TODO - debug - let's do PageRank from our graph with Arrays as vertex values
//    val ret = colorfulGraphMotif(graph, numColors, k, seed)
//    println("\n*** Large Test for " + f + " returned " + ret + " numcolors: " + numColors + " k: " + k)

    val pr = runUntilConvergence(graph, 0.001F).vertices.cache()

    println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

    sc.stop()


  }

  def createSimpleGraphFromFile(f:String, k: Int, n: Int, sc: SparkContext): (Graph[(Int, Int), Int], Int) ={
    val vertices = new Array[(Long, (Int, Int))](n)
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
          vertices(vertexId) = (vertexId.toLong, (color, 1))
        } else if (mode == 1){
          edges += Edge(splits(0).toInt, splits(1).toInt, 1)
          edges += Edge(splits(1).toInt, splits(0).toInt, 1) // undirected edges
          edgeCount += 2
        }
      }
    }

    val defaultVertex = (-1, -1)

    val verticesRDD: RDD[(VertexId, (Int, Int))] = sc.parallelize(vertices)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges)

    (Graph(verticesRDD, edgesRDD, defaultVertex, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY), colors.size)
  }

  def createLiveJournalGraphFromFile(f:String, k: Int, n: Int, sc: SparkContext): (Graph[(Int, Array[Int]), Int], Int) ={
    val vertices = new Array[(Long, (Int, Array[Int]))](n)
    val edges: ArrayBuffer[Edge[Int]] = new ArrayBuffer[Edge[Int]]()
    var edgeCount = 0
    for (line <- Source.fromFile(f).getLines()){

      if (!line.startsWith("#")){
        val splits = line.split("\t")
        val srcId = splits(0).toInt
        val destId = splits(1).toInt
        vertices(srcId) = (srcId.toLong, (srcId, new Array[Int](k+2)))
        vertices(destId) = (destId.toLong, (destId, new Array[Int](k+2)))
        edges += Edge(srcId, destId, 1) // just one edge
        edgeCount += 1
      }
    }

    val defaultVertex = (-1, Array(-1))

    val verticesRDD: RDD[(VertexId, (Int, Array[Int]))] = sc.parallelize(vertices)
    val edgesRDD: RDD[Edge[Int]] = sc.parallelize(edges)

    (Graph(verticesRDD, edgesRDD, defaultVertex, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY), n)
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

    (Graph(verticesRDD, edgesRDD, defaultVertex, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY), colors.size)
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


  /**
    * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
    * PageRank and edge attributes containing the normalized edge weight.
    *
    * @tparam VD the original vertex attribute (not used)
    * @tparam ED the original edge attribute (not used)
    *
    * @param graph the graph on which to compute PageRank
    * @param tol the tolerance allowed at convergence (smaller => more accurate).
    * @param resetProb the random reset probability (alpha)
    * @param srcId the source vertex for a Personalized Page Rank (optional)
    *
    * @return the graph containing with each vertex containing the PageRank and each edge
    *         containing the normalized weight.
    */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
                                                       graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
                                                       srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
      if (id == src) (1.0, Double.NegativeInfinity) else (0.0, 0.0)
    }
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
                                  msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      var teleport = oldPR
      val delta = if (src==id) resetProb else 0.0
      teleport = oldPR*delta

      val newPR = teleport + (1.0 - resetProb) * msgSum
      val newDelta = if (lastDelta == Double.NegativeInfinity) newPR else newPR - oldPR
      (newPR, newDelta)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = {
      a+b
    }

    // The initial message received by all vertices in PageRank
    val initialMessage = if (personalized) 0.0 else resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp = if (personalized) {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }

    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  }


}
