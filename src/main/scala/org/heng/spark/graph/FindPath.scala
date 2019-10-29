package org.heng.spark.graph


// $example on$
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


final case class Path(src: VertexId, dest: VertexId, path: Seq[VertexId], lastTimestamp: Long)
final case class PathForFilter(src: VertexId, dest: Seq[Array[Edge[Long]]], path: Seq[VertexId], lastTimestamp: Long)


object FindPath {

  /**
    * Assumption: in degree could be > 1 , and out degree could only be 1
    * @param graph
    * @param result
    * @param currentLevel
    * @return
    */
  def findPath(graph: Graph[Long, Long], result: Array[Path], currentLevel: Array[Path]): Array[Path] = {
    if (currentLevel.isEmpty) {
      return result
    }
    //find neighbors
    val neighbors = currentLevel.map(t => PathForFilter(t.src,
         graph.collectEdges(EdgeDirection.Out).lookup(t.dest), t.path, t.lastTimestamp))
    //if no more neighbors, put paths to result
    val collected = neighbors.filter(t => t.dest.length == 0 || t.dest(0)(0).attr < t.lastTimestamp)
      .map(t => Path(t.src, t.path.last, t.path, t.lastTimestamp))
    val newResult = result ++ collected
    //otherwise, find next level neighbors
    val nextLevel = neighbors.filter(t => t.dest.length > 0
      && !t.path.contains(t.dest(0)(0).dstId)
      && t.lastTimestamp < t.dest(0)(0).attr)
      .map(
        t => Path(t.src, t.dest(0)(0).dstId,
          t.path :+ t.dest(0)(0).dstId,
          t.dest(0)(0).attr))

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

    //val graph = GraphLoader.edgeListFile(sc, "in/graphx/followers.txt")
    val graph = Graph.fromEdges(mutations,(-1L))

    //create graph with in-degree as vertex property
    val graphWithIndgrees
    = graph.outerJoinVertices(graph.inDegrees)((vid, _, degIpt) => degIpt.getOrElse(0L))

    //find starting Vertex with in degree = 0
    //vertices in cycles will be excluded because as they have non zero in-degree
    val startingVertices = graphWithIndgrees.vertices.filter(_._2 == 0).map(_._1)
      .map(s => Path(s, s, Seq[VertexId](s), -1L))
    ///have to use collect here , or get NPE. but collect should not be used
    //TODO figure out why
    val result = findPath(graph, Array[Path](), startingVertices.collect())

    result.foreach(println)
  }

}
// scalastyle:on println
