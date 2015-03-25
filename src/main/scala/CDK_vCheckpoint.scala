import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map
import scala.sys.process._

/**
 * Created by xinghao on 3/24/15.
 */
object CDK_vCheckpoint {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    System.setProperty("spark.worker.timeout",                  "30000")
    System.setProperty("spark.akka.timeout",                    "30000")
    System.setProperty("spark.storage.blockManagerHeartBeatMs", "300000")
    System.setProperty("spark.akka.retry.wait",                 "30000")
    System.setProperty("spark.akka.frameSize",                  "10000")
    val sc = new SparkContext()

    val argmap: Map[String, String] = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    val graphType       : String  = argmap.getOrElse("graphtype", "rmat").toLowerCase
    val rMatNumEdges    : Int     = argmap.getOrElse("rmatnumedges", "100000000").toInt
    val path            : String  = argmap.getOrElse("path", "graphs/astro.edges")
    val numPartitions   : Int     = argmap.getOrElse("numpartitions", "640").toInt
    val epsilon         : Double  = argmap.getOrElse("epsilon", "0.5").toDouble
    val checkpointIter  : Int     = argmap.getOrElse("checkpointiter", "20").toInt
    val checkpointDir   : String  = argmap.getOrElse("checkpointdir", "/mnt/checkpoints/")
//    val checkpointDir  : String = argmap.getOrElse("checkpointdir", "/Users/xinghao/Documents/tempcheckpoint")
    val checkpointClean : Boolean = argmap.getOrElse("checkpointclean", "true").toBoolean
    val checkpointLocal : Boolean = argmap.getOrElse("checkpointlocal", "false").toBoolean

    System.out.println(s"graphType      = $graphType")
    System.out.println(s"rMatNumEdges   = $rMatNumEdges")
    System.out.println(s"path           = $path")
    System.out.println(s"numPartitions  = $numPartitions")
    System.out.println(s"epsilon        = $epsilon")
    System.out.println(s"checkpointIter = $checkpointIter")

    /*
    var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e8.toInt, numEdges = 1e8.toInt).mapVertices( (id, _) => initID.toInt )

//    val path = "/Users/dimitris/Documents/graphs/astro.txt"
//    val numPartitions = 4
//    val graph: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numPartitions)
    */

    val initID   : Int = -100
    val centerID : Int = -200

    val graph: Graph[(Int), Int] =
      if (graphType == "rmat")
        GraphGenerators.rmatGraph(sc, requestedNumVertices = rMatNumEdges.toInt, numEdges = rMatNumEdges.toInt).mapVertices((id, _) => initID.toInt)
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
    var hasFriends: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
    clusterGraph = clusterGraph.mapVertices((id, _) => initID.toInt)

//    val epsilon: Double = 0.5
    var maxDeg: Int = clusterGraph.aggregateMessages[Int](
      triplet => {
        if (triplet.dstAttr == initID & triplet.srcAttr == initID) {
          triplet.sendToDst(1)
        }
      }, _ + _).map(x => x._2).fold(0)((a, b) => math.max(a, b))
    var numNewCenters: Long = 0

    var iteration = 0

    val times : Array[Long] = new Array[Long](100)

    var prevRankGraph: Graph[Int, Int] = null
    while (maxDeg > 0) {
      times(0) = System.currentTimeMillis()
      if ((iteration+1) % checkpointIter == 0) sc.setCheckpointDir(checkpointDir + iteration.toString)

      clusterGraph.cache()

      val randomSet = clusterGraph.vertices.filter(v => (v._2 == initID) && (scala.util.Random.nextFloat < epsilon / maxDeg.toFloat)).cache()
//      if ((iteration+1) % checkpointIter == 0) randomSet.checkpoint()

      numNewCenters = randomSet.count

      prevRankGraph = clusterGraph
      clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => centerID).cache()
      clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
//      clusterGraph.vertices.foreachPartition(_ => {})
//      clusterGraph.triplets.foreachPartition(_ => {})
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      //Turn-off active nodes that are friends
      // activeSubgraph = unclusterGraph.subgraph(vpred = (id, attr) => attr == -1).cache()
      // hasFriends = unclusterGraph.degrees.filter{case (id, u) => u > 0}.cache()
      hasFriends = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -1 & triplet.srcAttr == -1) {
            triplet.sendToDst(1)
          }
        }, math.min(_, _)
      )
      clusterGraph = clusterGraph.joinVertices(hasFriends)((vId, attr, active) => -100).cache()


      val clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == centerID & triplet.dstAttr == initID) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_, _)
      ).cache()

//      if ((iteration+1) % checkpointIter == 0) clusterUpdates.checkpoint()

      clusterGraph = clusterGraph.joinVertices(clusterUpdates) {
        (vId, oldAttr, newAttr) => newAttr
      }.cache()

      maxDeg = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == initID & triplet.srcAttr == initID) {
            triplet.sendToDst(1)
          }
        }, _ + _).map(x => x._2).fold(0)((a, b) => math.max(a, b))

      if ((iteration+1) % checkpointIter == 0) {
        clusterGraph.vertices.checkpoint()
        clusterGraph.edges.checkpoint()
        clusterGraph = Graph(clusterGraph.vertices, clusterGraph.edges)
        clusterGraph.checkpoint()
      }

//      prevRankGraph = clusterGraph
//      clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
//      clusterGraph.vertices.foreachPartition(_ => {})
//      clusterGraph.triplets.foreachPartition(_ => {})
//      prevRankGraph.vertices.unpersist(false)
//      prevRankGraph.edges.unpersist(false)

      if ((iteration+1) % checkpointIter == 0){
        if (checkpointClean && iteration-checkpointIter >= 0) {
          if (checkpointLocal)
            Seq("rm", "-rf", checkpointDir + (iteration - checkpointIter).toString).!
          else
            Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir + (iteration - checkpointIter).toString).!
        }
      }


      times(1) = System.currentTimeMillis()

      System.out.println(
          "qq\t" +
          s"$iteration\t" +
          s"$maxDeg\t" +
          s"$numNewCenters\t" +
          s"${times(1)-times(0)}\t" +
          "")


      iteration += 1
    }

    //Take care of degree 0 nodes
    clusterGraph = AuxiliaryFunctions.setZeroDegreeToCenter(clusterGraph, initID, centerID).cache()

    if (checkpointClean) {
      if (checkpointLocal)
        Seq("rm", "-rf", checkpointDir).!
      else
        Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir).!
    }

    System.out.println(s"${AuxiliaryFunctions.computeObjective(clusterGraph)}")
  }
}
