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
object SimpleCheckpointV3 {
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

    val initID  : Long = -100
    val centerID: Long = -200

    val graph: Graph[Int, Int] =
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

    var unclusteredVertices : RDD[(Long, Int)] = vertexRDDs.map(v => (v._1, v._2))
    var clusteredVertices : RDD[(Long, Long)] = sc.parallelize(Array[(Long, Long)]())
    val erdd : RDD[(Long,Long)] = edgeRDDs.map(e => (e.srcId, e.dstId))

    var maxDeg = 1000

    var iteration = 0

    val times : Array[Long] = new Array[Long](100)

    var prevRankGraph: Graph[Int, Int] = null

    var numNewCenters: Long = 0
    var numNewSpokes: Long = 0

    while (true){
      times(0) = System.currentTimeMillis()

      if ((iteration+1) % checkpointIter == 0) sc.setCheckpointDir(checkpointDir + iteration.toString)

      if (true && (iteration==0 || (iteration+1) % maxDegRecomputeRounds == 0)){
        maxDeg = unclusteredVertices
          .join(erdd)
          .map(x => (x._1, 1))
          .reduceByKey(_+_)
          .map(x => x._2)
          .fold(0)((a,b) => math.max(a,b))
      }

      times(1) = System.currentTimeMillis()

      // Compute randomSet
      val randomSet = unclusteredVertices.filter(_ => scala.util.Random.nextFloat() < epsilon / maxDeg.toFloat)
        .map(x => (x._1, centerID))
        .cache().setName("r" + iteration)
      numNewCenters = randomSet.count()

      times(2) = System.currentTimeMillis()

      // 1st join with randomSet
      unclusteredVertices = unclusteredVertices.leftOuterJoin(randomSet).filter(x => x._2._2.isEmpty).map(x => (x._1, x._2._1)).cache()

      times(3) = System.currentTimeMillis()

      // Compute clusterUpdates
      val clusterUpdates = randomSet
        .join(erdd).map(x => (x._2._2, x._1))
        .join(unclusteredVertices).map(x => (x._1, x._2._1))
        .reduceByKey((a,b) => math.min(a,b))
        .cache().setName("u" + iteration)
      numNewSpokes = clusterUpdates.count()

      times(4) = System.currentTimeMillis()

      // 2nd join with clusterUpdates
      unclusteredVertices = unclusteredVertices.leftOuterJoin(clusterUpdates).filter(x => x._2._2.isEmpty).map(x => (x._1, x._2._1)).cache()
      clusteredVertices = clusteredVertices.fullOuterJoin(randomSet     ).map(x => (x._1, if (x._2._1.isDefined) x._2._1.get else x._2._2.get)).cache()
      clusteredVertices = clusteredVertices.fullOuterJoin(clusterUpdates).map(x => (x._1, if (x._2._1.isDefined) x._2._1.get else x._2._2.get)).cache()

      times(5) = System.currentTimeMillis()

      // Mark for checkpoint
      if ((iteration+1) % checkpointIter == 0) {
        unclusteredVertices.checkpoint()
        clusteredVertices.checkpoint()
      }

      times(6) = System.currentTimeMillis()

      // Materialize, make all computations
      unclusteredVertices.foreachPartition(_ => {})
      clusteredVertices.foreachPartition(_ => {})

      times(7) = System.currentTimeMillis()

      // Unpersist from memory

      // Remove old checkpoint
      if ((iteration+1) % checkpointIter == 0){
        if (checkpointClean && iteration-checkpointIter >= 0) {
          if (checkpointLocal)
            Seq("rm", "-rf", checkpointDir + (iteration - checkpointIter).toString).!
          else
            Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir + (iteration - checkpointIter).toString).!
        }
      }

//      System.out.println(s"${unclusteredVertices.count() + clusteredVertices.count()}\t${unclusteredVertices.count()}\t${clusteredVertices.count()}")


      times(8) = System.currentTimeMillis()

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