import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.{SparkConf, Logging, SparkContext}
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.immutable.Map

object ClusterWild_fixes_v04 {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()

    val argmap : Map[String,String] = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    val graphType     : String = argmap.getOrElse("graphtype", "rmat").toString.toLowerCase
    val rMatNumEdges  : Int    = argmap.getOrElse("rmatnumedges", 100000000).toString.toInt
    val path          : String = argmap.getOrElse("path", "graphs/astro.edges").toString
    val numPartitions : Int    = argmap.getOrElse("numpartitions", 640).toString.toInt

    /*
    var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e8.toInt, numEdges = 1e8.toInt).mapVertices( (id, _) => -100.toInt )

//    val path = "/Users/dimitris/Documents/graphs/astro.txt"
//    val numPartitions = 4
//    val graph: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numPartitions)
    */

    val graph: Graph[(Int), Int] =
      if (graphType == "rmat")
        GraphGenerators.rmatGraph(sc, requestedNumVertices = rMatNumEdges.toInt, numEdges = rMatNumEdges.toInt).mapVertices( (id, _) => -100.toInt )
      else
        GraphLoader.edgeListFile(sc, path, false, numPartitions)

    System.out.println(s"Graph has ${graph.vertices.count} vertices (${graph.vertices.partitions.length} partitions), ${graph.edges.count} edges (${graph.edges.partitions.length} partitions)")

    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graph.vertices
    var edgeRDDs: RDD[Edge[Int]] = graph.edges.reverse.union(graph.edges)
    // val graph: Graph[(Int), Int] = Graph(vertexRDDs,edgeRDDs).mapVertices( (id, _) => -100.toInt )


    var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs).mapVertices((id, _) => -100.toInt)
    val n = clusterGraph.numVertices.toFloat

    var prevClusterGraph: Graph[(Int), Int] = clusterGraph
    val epsilon: Double = 0.5
    var x: Int = 1

    var clusterUpdates: VertexRDD[(Int)] = null
    var randomSet: VertexRDD[(Int)] = null
    var newVertices: VertexRDD[(Int)] = null

    var numNewCenters: Long = 0

    var maxDegree: VertexRDD[Int] = clusterGraph.aggregateMessages[Int](
      triplet => {
        if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
          triplet.sendToDst(1)
        }
      }, _ + _).cache()

    var maxDeg: Int = maxDegree.map(x => x._2).fold(0)((a, b) => math.max(a, b))







    //TODO: change for to check for empty RDD
    while (maxDeg >= 1) {

      val time0 = System.currentTimeMillis

      randomSet = clusterGraph.vertices.filter(v => v._2 == -100)
      randomSet = randomSet.mapValues(vId => {
        if (scala.util.Random.nextFloat < epsilon / maxDeg) -1; else -100;
      })
      randomSet = randomSet.filter { case (id, clusterID) => clusterID == -1} // keep the active set
      numNewCenters = randomSet.count

      // prevUnclusterGraph = unclusterGraph
      //
      clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => -1).cache()
      //
      // prevUnclusterGraph.vertices.unpersist(false)
      // prevUnclusterGraph.edges.unpersist(false)

      clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == -1 & triplet.dstAttr == -100) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_, _)
      )

      newVertices = clusterGraph.vertices.leftJoin(clusterUpdates) {
        (id, oldValue, newValue) =>
          newValue match {
            case Some(x: Int) => x
            case None => {
              if (oldValue == -1) id.toInt; else oldValue;
            }
          }
      }

      prevClusterGraph = clusterGraph
      //
      clusterGraph = clusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()
      //
      prevClusterGraph.vertices.unpersist(false)
      prevClusterGraph.edges.unpersist(false)
      clusterGraph.edges.foreachPartition(x => {})

      if (x % 3 == 0) maxDeg = math.round(maxDeg / 2);


      val time1 = System.currentTimeMillis
      System.out.println(
        s"$x\t" +
          s"$maxDeg\t" +
          s"$numNewCenters\t" +
          s"${time1 - time0}\t" +
          "")
      x = x + 1
    }
    clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
    //Take care of degree 0 nodes
    newVertices = clusterGraph.subgraph(vpred = (vId, clusterID) => clusterID == -100).vertices
    newVertices = clusterGraph.vertices.leftJoin(newVertices) {
      (id, oldValue, newValue) =>
        newValue match {
          case Some(x: Int) => id.toInt
          case None => oldValue;
        }
    }
    clusterGraph = clusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()
  }
}
