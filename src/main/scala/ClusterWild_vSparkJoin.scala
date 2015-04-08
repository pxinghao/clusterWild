import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

import scala.collection.immutable.Map
import scala.sys.process._




/**
 * Created by xinghao on 3/10/15.
 */
object ClusterWild_vSparkJoin {
  def main(args: Array[String]) = {

    System.setProperty("spark.worker.timeout",                     "30000")
    System.setProperty("spark.storage.blockManagerHeartBeatMs",    "5000")
    System.setProperty("spark.storage.blockManagerSlaveTimeoutMs", "100000")
    System.setProperty("spark.akka.timeout",                       "30000")
    System.setProperty("spark.akka.retry.wait",                    "30000")
    System.setProperty("spark.akka.frameSize",                     "2047")
    System.setProperty("spark.locality.wait",                      "300000000000")
    val sc = new SparkContext(new SparkConf().setAll(List[(String,String)](
      ("spark.worker.timeout",                     "30000"),
      ("spark.storage.blockManagerHeartBeatMs",    "5000"),
      ("spark.storage.blockManagerSlaveTimeoutMs", "100000"),
      ("spark.akka.timeout",                       "30000"),
      ("spark.akka.retry.wait",                    "30000"),
      ("spark.akka.frameSize",                     "2047"),
      ("spark.locality.wait",                      "300000000000"),
      ("spark.logConf",                            "true")
    )))

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

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

    val numVertices = graph.vertices.count
    val logN = math.floor(math.log(numVertices.toDouble))
    val maxDegRecomputeRounds = logN//math.floor(2.0 / epsilon * logN.toDouble)

    System.out.println(
      s"Graph has $numVertices vertices (${graph.vertices.partitions.length} partitions),"
        + s"${graph.edges.count} edges (${graph.edges.partitions.length} partitions),"
        + s"eps = $epsilon"
    )

    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graph.vertices.cache()
    val edgeRDDs: RDD[Edge[Int]] = graph.edges.reverse.union(graph.edges).cache()
    var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs)
    clusterGraph = clusterGraph.mapVertices((id, _) => initID.toInt)

    var unclusteredVertices = vertexRDDs.map(vi => (vi._1, scala.util.Random.nextDouble()))
    var unclusteredEdges    = edgeRDDs.map(e => (e.srcId, e.dstId))

    var maxDeg : Int = unclusteredEdges.map(x => (x._2,x._1))
                        .join(unclusteredVertices).map(x => (x._1,1))
                        .reduceByKey(_+_).reduce((x,y) => (0,math.max(x._2,y._2)))._2

    var threshold : Double = 1.0

    var iteration = 0

    val times : Array[Long] = new Array[Long](100)

    var prevRankGraph: Graph[Int, Int] = null
    while (maxDeg > 0) {
      if ((iteration+1) % checkpointIter == 0) sc.setCheckpointDir(checkpointDir + iteration.toString)

      // Active set of new centers
      threshold *= (1.0 - epsilon / maxDeg.toDouble)
      val newCenters = unclusteredVertices.filter(vi => vi._2 > threshold).map(x => (x._1, centerID)).cache()
      unclusteredVertices = unclusteredVertices.leftOuterJoin(newCenters).filter(x => x._2._2.isEmpty).map(x => (x._1,x._2._1)).cache()
      unclusteredEdges    = unclusteredEdges   .leftOuterJoin(newCenters).filter(x => x._2._2.isEmpty).map(x => (x._1,x._2._1)).cache()

      // New spokes

      // Checkpoint
      if ((iteration+1) % checkpointIter == 0) {
        unclusteredVertices.checkpoint()
        unclusteredEdges.checkpoint()
      }

      // Materialize
      unclusteredVertices.foreachPartition(x => {})
      unclusteredEdges   .foreachPartition(_ => {})

      // Recompute maxDeg
      if (((iteration+1) % maxDegRecomputeRounds) == 0) {
      }

      // Remove old checkpoints
      if ((iteration+1) % checkpointIter == 0){
        if (checkpointClean && iteration-checkpointIter >= 0) {
          if (checkpointLocal)
            Seq("rm", "-rf", checkpointDir + (iteration - checkpointIter).toString).!
          else
            Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir + (iteration - checkpointIter).toString).!
        }
      }

      System.out.println(
        "qq\t" +
          s"$iteration\t" +
          s"$maxDeg\t" +
          s"${times(1)-times(0)}\t" +
          "")


      iteration += 1
    }

    //Take care of degree 0 nodes

    // Compute objective

    if (checkpointClean) {
      if (checkpointLocal)
        Seq("rm", "-rf", checkpointDir).!
      else
        Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir).!
    }

    while (true) {}

  }
}
