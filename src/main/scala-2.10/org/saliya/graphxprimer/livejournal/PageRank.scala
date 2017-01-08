package org.saliya.graphxprimer.livejournal

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Pregel, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Saliya Ekanayake on 1/7/17.
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    val fname = args(0)
    val numEParts = args(1).toInt

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val tol = 0.001F

    val sc = new SparkContext(conf.setAppName("PageRank").setMaster("local[1]"))
    val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
      numEdgePartitions = numEParts,
      edgeStorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel = StorageLevel.MEMORY_ONLY).cache()

    val graph = unpartitionedGraph.partitionBy(RandomVertexCut)

    println("*****GRAPHX: Number of vertices " + graph.vertices.count)
    println("*****GRAPHX: Number of edges " + graph.edges.count)

    val pr = runUntilConvergence(graph, tol).vertices.cache()

    println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

    sc.stop()
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

    def messageCombiner(a: Double, b: Double): Double = a + b

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
