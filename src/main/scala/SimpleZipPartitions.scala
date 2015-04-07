import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, VertexRDD, GraphLoader, Graph}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Map

import scala.sys.process._

object SimpleZipPartitions{
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

    var vrdd : RDD[(Long,Int)] = clusterGraph.vertices.map(v => (v._1, v._2)).cache()

    val numRands = 10000
    val randArray = new Array[(Long,Int)](numRands)

    val R = vrdd//sc.parallelize(randArray, 160)

    var iteration = 0
    while (iteration < 20){
      R.zipPartitions(R) { (x,y) => x }.cache().setName("r" + iteration)
      if ((iteration+1) % checkpointIter == 0) sc.setCheckpointDir(checkpointDir + iteration.toString)
      if ((iteration+1) % checkpointIter == 0) R.checkpoint()
      R.foreachPartition(_ => {})
      if ((iteration+1) % checkpointIter == 0){
        if (checkpointClean && iteration-checkpointIter >= 0) {
          if (checkpointLocal)
            Seq("rm", "-rf", checkpointDir + (iteration - checkpointIter).toString).!
          else
            Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir + (iteration - checkpointIter).toString).!
        }
      }
      iteration += 1
    }

    println("Done")
    while (true) {}

  }
}










