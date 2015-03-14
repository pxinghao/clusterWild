import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

/**
 * Created by xinghao on 3/10/15.
 */
object ClusterWild_vCheckpoint {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()

    val argmap: Map[String, String] = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    val graphType      : String = argmap.getOrElse("graphtype", "rmat").toLowerCase
    val rMatNumEdges   : Int    = argmap.getOrElse("rmatnumedges", "100000000").toInt
    val path           : String = argmap.getOrElse("path", "graphs/astro.edges")
    val numPartitions  : Int    = argmap.getOrElse("numpartitions", "640").toInt
    val epsilon        : Double = argmap.getOrElse("epsilon", "0.5").toDouble
    val checkpointIter : Int    = argmap.getOrElse("checkpointiter", "20").toInt

    System.out.println(s"graphType      = $graphType")
    System.out.println(s"rMatNumEdges   = $rMatNumEdges")
    System.out.println(s"path           = $path")
    System.out.println(s"numPartitions  = $numPartitions")
    System.out.println(s"epsilon        = $epsilon")
    System.out.println(s"checkpointIter = $checkpointIter")


    /*
    var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e8.toInt, numEdges = 1e8.toInt).mapVertices( (id, _) => -100.toInt )

//    val path = "/Users/dimitris/Documents/graphs/astro.txt"
//    val numPartitions = 4
//    val graph: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numPartitions)
    */

    val graph: Graph[(Int), Int] =
      if (graphType == "rmat")
        GraphGenerators.rmatGraph(sc, requestedNumVertices = rMatNumEdges.toInt, numEdges = rMatNumEdges.toInt).mapVertices((id, _) => -100.toInt)
      else
        GraphLoader.edgeListFile(sc, path, false, numPartitions)

    System.out.println(
        s"Graph has ${graph.vertices.count} vertices (${graph.vertices.partitions.length} partitions),"
      + s"${graph.edges.count} edges (${graph.edges.partitions.length} partitions),"
      + s"eps = $epsilon"
    )

    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graph.vertices
    val edgeRDDs: RDD[Edge[Int]] = graph.edges.reverse.union(graph.edges)
    var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs)
    clusterGraph = clusterGraph.mapVertices((id, _) => -100.toInt)

    var centerID = 0
//    val epsilon: Double = 0.5
    val maxDegree: VertexRDD[Int] = clusterGraph.aggregateMessages[Int](
      triplet => {
        if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
          triplet.sendToDst(1)
        }
      }, _ + _).cache()
    var maxDeg: Int = maxDegree.map(x => x._2).fold(0)((a, b) => math.max(a, b))
    var numNewCenters: Long = 0

    var iteration = 0

    var times : Array[Long] = new Array[Long](100)

//    sc.setCheckpointDir("/Users/xinghao/Documents/tempcheckpoint")
    sc.setCheckpointDir("/mnt/checkpoints/")

    var prevRankGraph: Graph[Int, Int] = null
    while (maxDeg > 0) {
      times(0) = System.currentTimeMillis()
      clusterGraph.cache()

      val randomSet = clusterGraph.vertices.filter(v => (v._2 == -100) && (scala.util.Random.nextFloat < epsilon / maxDeg.toFloat)).cache()
      if ((iteration+1) % checkpointIter == 0) randomSet.checkpoint()

      numNewCenters = randomSet.count

      prevRankGraph = clusterGraph
      clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => centerID).cache()
      clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      clusterGraph.vertices.foreachPartition(_ => {})
      clusterGraph.triplets.foreachPartition(_ => {})
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      val clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == centerID & triplet.dstAttr == -100) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_, _)
      ).cache()

      if ((iteration+1) % checkpointIter == 0) clusterUpdates.checkpoint()

      prevRankGraph = clusterGraph
      clusterGraph = clusterGraph.joinVertices(clusterUpdates) {
        (vId, oldAttr, newAttr) => newAttr
      }.cache()

//      if ((iteration+1) % checkpointIter == 0) {
//        clusterGraph.vertices.checkpoint()
//        clusterGraph.edges.checkpoint()
//        clusterGraph = Graph(clusterGraph.vertices, clusterGraph.edges)
//        clusterGraph.checkpoint()
//      }

      maxDeg = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
            triplet.sendToDst(1)
          }
        }, _ + _).map(x => x._2).fold(0)((a, b) => math.max(a, b))

      clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      clusterGraph.vertices.foreachPartition(_ => {})
      clusterGraph.triplets.foreachPartition(_ => {})
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      times(1) = System.currentTimeMillis()

      System.out.println(
      s"$iteration\t" +
        s"$maxDeg\t" +
        s"$numNewCenters\t" +
      s"${times(1)-times(0)}\t" +
      "")


      iteration += 1
    }
  }
}
