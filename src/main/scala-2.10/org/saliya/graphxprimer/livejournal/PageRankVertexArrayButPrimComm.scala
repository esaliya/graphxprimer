package org.saliya.graphxprimer.livejournal

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Pregel, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Saliya Ekanayake on 1/7/17.
  *
  * Direct copy of the Spark's PageRank implementation
  */
object PageRankVertexArrayButPrimComm {
  def main(args: Array[String]): Unit = {
    val taskType = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("PageRank(" + fname + ")"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pr = (numIterOpt match {
          //          case Some(numIter) => PageRank.run(graph, numIter)
          case None => runUntilConvergence(graph, tol)
        }).vertices.cache()

        println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

        if (!outFname.isEmpty) {
          //          logWarning("Saving pageranks of pages to " + outFname)
          pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        }

        sc.stop()
      case _ =>
        println("Invalid task type.")
    }
  }

  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
                                                       graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    runUntilConvergenceWithOptions(graph, tol, resetProb)
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
  def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
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
    val pagerankGraph: Graph[(Double, Array[Double]), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
      if (id == src) (1.0, createArray(Double.NegativeInfinity)) else (0.0, createArray(0.0))
    }
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Array[Double]), msgSum: Double): (Double, Array[Double]) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, createArray(newPR - oldPR))
    }

    def personalizedVertexProgram(id: VertexId, attr: (Double, Array[Double]),
                                  msgSum: Double): (Double, Array[Double]) = {
      val (oldPR, lastDelta) = attr
      var teleport = oldPR
      val delta = if (src==id) resetProb else 0.0
      teleport = oldPR*delta

      val newPR = teleport + (1.0 - resetProb) * msgSum
      val newDelta = if (lastDelta(0) == Double.NegativeInfinity) newPR else newPR - oldPR
      (newPR, createArray(newDelta))
    }

    def sendMessage(edge: EdgeTriplet[(Double, Array[Double]), Double]) = {
      if (edge.srcAttr._2(0) > tol) {
        Iterator((edge.dstId, edge.srcAttr._2(0) * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = if (personalized) 0.0 else resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp = if (personalized) {
      (id: VertexId, attr: (Double, Array[Double]), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Array[Double]), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }

    def createArray(a: Double): Array[Double] = {
      val k: Int = 8
      val arr = new Array[Double](k)
      arr(0) = a
      arr
    }

    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  }
}

