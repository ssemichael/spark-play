package org.heng.spark.graph


// $example on$
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


final case class Path(src: VertexId, dest: VertexId, path: Seq[VertexId])
final case class PathForFilter(src: VertexId, dest: Seq[Array[Edge[String]]], path: Seq[VertexId])


object FindPath {

  /**
    * Assumption on the graph: a vertex only has fan in , no fan out
    * @param graph
    * @param result
    * @param currentLevel
    * @return
    */
  def findPath(graph: Graph[Long, String], result: Array[Path], currentLevel: Array[Path]): Array[Path] = {
    if (currentLevel.length==0) {
      return result
    }
    //find neighbors
    val neighbors = currentLevel.map(t => PathForFilter(t.src, graph.collectEdges(EdgeDirection.Out).lookup(t.dest), t.path))
    //if no more neighbors, put paths to result
    val collected = neighbors.filter(t => t.dest.length == 0).map(t => Path(t.src, t.path.last, t.path))
    val newResult = result ++ collected
    //otherwise, find next level neighbors
    val nextLevel = neighbors.filter(t => t.dest.length > 0 && !t.path.contains(t.dest(0)(0).dstId) ).map(
      t => Path(t.src, t.dest(0)(0).dstId, t.path :+ t.dest(0)(0).dstId))

    findPath(graph, newResult, nextLevel)

  }


  /**
    * algorithm:
    *   1. find vertices with zero in-degree as starting vertices
    *   2. for each vertex find the path to the end using dfs
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    //build graph
    val mutations: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(4L, 5L, "timestamp"),
        Edge(5L, 6L, "timestamp"),
        Edge(3L, 2L, "timestamp"),
        Edge(2L, 1L, "timestamp"),
        Edge(7L, 8L, "timestamp"),
        Edge(1L, 0L, "timestamp"),
        Edge(10L, 6L, "timestamp"),
        Edge(11L, 12L, "timestamp"),
        Edge(12L, 11L, "timestamp")))

    //val graph = GraphLoader.edgeListFile(sc, "in/graphx/followers.txt")
    val graph = Graph.fromEdges(mutations,(0L))




    //create graph with in-degree as vertex property
    val graphWithIndgrees
    = graph.outerJoinVertices(graph.inDegrees)((vid, _, degIpt) => degIpt.getOrElse(0L))

    //find starting Vertex with in degree = 0
    //vertices in cycles will be excluded because as they have non zero in-degree
    val startingVertices = graphWithIndgrees.vertices.filter(_._2 == 0).map(_._1).map(s => Path(s, s, Seq[VertexId](s)))
    ///have to use collect here , or get NPE. but collect should not be used
    //TODO figure out why
    val result = findPath(graph, Array[Path](), startingVertices.collect())

    result.foreach(println)
  }

}
// scalastyle:on println
