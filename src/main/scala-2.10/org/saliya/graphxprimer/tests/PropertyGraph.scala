package org.saliya.graphxprimer.tests

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Saliya Ekanayake on 11/10/16.
  */
object PropertyGraph {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Prop Graph").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext


    // Create an RDD for the vertices
//    val users: RDD[(VertexId, (String, String))] =
//    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
//      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val users: RDD[(VertexId, (String, Array[Int]))] =
      sc.parallelize(Array((3L, ("rxin", Array(1))), (7L, ("jgonzal", Array(1))),
        (5L, ("franklin", Array(1))), (2L, ("istoica", Array(1)))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", Array(-1))
//    val defaultUser = ("John Doe", "missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    graph.vertices.foreach(v => {v._2._2(0) = 34; print("@@@@@Hi " + v._2._2(0))})

//    val g2 = graph.mapVertices((id, v:(String, String)) => (v._1, v._2+"YES"))


//    graph.mapVertices((id, v:(String, Array[Int])) => (v._1, v._2))

//    val vertices = g2.vertices.collect().mkString("[", ",", "]")
//    print("#########\n" + vertices)

    val vertices:Array[(VertexId, (String, Array[Int]))] = graph.vertices.collect()
    for (v <- vertices) {
      print("#### " + v._2._2(0) + "\n")
    }




    print("******" + graph.edges.filter{case Edge(src, dst, prop) => src > dst }.count)
  }

}
