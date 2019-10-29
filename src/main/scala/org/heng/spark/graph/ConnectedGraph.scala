package org.heng.spark.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ConnectedGraph {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    //build graph
    val mutations: RDD[Edge[Long]] =
      sc.parallelize(Array(
        Edge(4L, 5L, 1L),
        Edge(5L, 6L, 2L),
        Edge(3L, 2L, 3L),
        Edge(2L, 1L, 4L),
        Edge(7L, 8L, 5L),
        Edge(1L, 0L, 6L),
        Edge(10L, 6L, 7L),
        Edge(11L, 12L, 8L),
        Edge(12L, 11L, 9L),
        Edge(13L, 5L, 10L)))


    val graph = Graph.fromEdges(mutations,(-1L)).cache()

    val connectedComponents = graph.connectedComponents()

    connectedComponents.vertices.collect()

    // Print top 5 items from the result
    println(connectedComponents.vertices.take(50).mkString("\n"))


  }

}
