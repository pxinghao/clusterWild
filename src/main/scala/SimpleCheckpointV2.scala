import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import scala.collection.immutable.Map
import scala.sys.process._

/**
 * Created by xinghao on 3/10/15.
 */
object SimpleCheckpointV2 {
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

    val initID: Int = -100
    val centerID: Int = -200

    val graph: Graph[(Int), Int] =
      if (graphType == "rmat")
        GraphGenerators.rmatGraph(sc, requestedNumVertices = rMatNumEdges.toInt, numEdges = rMatNumEdges.toInt).mapVertices((id, _) => initID.toInt)
      else
        GraphLoader.edgeListFile(sc, path, false, numPartitions)

    val numVertices = graph.vertices.count
    val logN = math.floor(math.log(numVertices.toDouble))
    val maxDegRecomputeRounds = logN //math.floor(2.0 / epsilon * logN.toDouble)

    System.out.println(
      s"Graph has $numVertices vertices (${graph.vertices.partitions.length} partitions),"
        + s"${graph.edges.count} edges (${graph.edges.partitions.length} partitions),"
        + s"eps = $epsilon"
    )

    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graph.vertices
    val edgeRDDs: RDD[Edge[Int]] = graph.edges.reverse.union(graph.edges)
    var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs)
    clusterGraph = clusterGraph.mapVertices((id, _) => initID.toInt)

//    var vrdd : RDD[(Long,Int)] = clusterGraph.vertices.map(v => (v._1, v._2))
    var vrdd : VertexRDD[Int] = clusterGraph.vertices

    var maxDeg = 100000

    var iteration = 0

    val times : Array[Long] = new Array[Long](100)

    var prevRankGraph: Graph[Int, Int] = null

    var oldVRDD : VertexRDD[Int] = null

    var numNewCenters: Long = 0
    var numNewSpokes: Long = 0

    while (iteration<50){
      times(0) = System.currentTimeMillis()
      if ((iteration+1) % checkpointIter == 0) sc.setCheckpointDir(checkpointDir + iteration.toString)

      oldVRDD = vrdd

//      vrdd = vrdd.join(vrdd).map(x => (x._1, x._2._2))
      vrdd = vrdd.innerJoin(vrdd)((i,x,y) => x).cache()

      // Compute randomSet
//      val randomSet = clusterGraph.vertices.filter(v => (true || v._2 == initID) && (scala.util.Random.nextFloat < 2.0)).cache().setName("r" + iteration)
//      numNewCenters = randomSet.count

      times(1) = System.currentTimeMillis()

      // 1st join with randomSet
//      clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => initID)
//      clusterGraph = clusterGraph.joinVertices(clusterGraph.vertices)((v,i,j) => i)

      // Compute clusterUpdates

      times(2) = System.currentTimeMillis()

      // 2nd join with clusterUpdates


      times(3) = System.currentTimeMillis()

      // Mark for checkpoint
      if ((iteration+1) % checkpointIter == 0) {
//        clusterGraph.checkpoint()
//        clusterGraph.vertices.checkpoint()
//        clusterGraph.edges.checkpoint()
        vrdd.checkpoint()
      }

      times(4) = System.currentTimeMillis()

      // Materialize clusterGraph, make all computations
//      prevRankGraph = clusterGraph
//      clusterGraph.vertices.cache().setName("v" + iteration + ".2")
//      clusterGraph.edges.cache(   ).setName("e" + iteration + ".2")
//      clusterGraph.edges.foreachPartition(x => {})
//      clusterGraph.vertices.foreachPartition(_ => {})
      vrdd.foreachPartition(_ => {})

      // Uncache
//      oldVRDD.unpersist(false)

      times(5) = System.currentTimeMillis()

      // Unpersist from memory
//      randomSet.unpersist(false)
//      prevRankGraph.unpersist(false)

      // Remove old checkpoint
      if ((iteration+1) % checkpointIter == 0){
        if (checkpointClean && iteration-checkpointIter >= 0) {
          if (checkpointLocal)
            Seq("rm", "-rf", checkpointDir + (iteration - checkpointIter).toString).!
          else
            Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir + (iteration - checkpointIter).toString).!
        }
      }

      times(6) = System.currentTimeMillis()


      times(7) = System.currentTimeMillis()

      System.out.println(
        "qq\t" +
          s"$iteration\t" +
          s"$maxDeg\t" +
          s"$numNewCenters\t" +
          s"$numNewSpokes\t" +
          s"${times(7)-times(0)}\t" +
          s"${times(1)-times(0)}\t" +
          s"${times(2)-times(1)}\t" +
          s"${times(3)-times(2)}\t" +
          s"${times(4)-times(3)}\t" +
          s"${times(5)-times(4)}\t" +
          s"${times(6)-times(5)}\t" +
          s"${times(7)-times(6)}\t" +
          "")
      System.out.println(s"Storage level = ${vrdd.getStorageLevel}")
      //      System.out.println(
      //        s"${clusterGraph.vertices.toDebugString.count(_ == '+')}\t" +
      //        s"${clusterGraph.edges.toDebugString.count(_ == '+')}\t" +
      //          "")

      iteration += 1
    }

    System.out.println("Done")
    while (true){}
  }


}