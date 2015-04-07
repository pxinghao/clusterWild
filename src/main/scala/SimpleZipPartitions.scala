import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import scala.collection.immutable.Map
import scala.sys.process._

object SimpleZipPartitions{
  def main(args: Array[String]) = {

//    System.setProperty("spark.worker.timeout",                     "30000")
//    System.setProperty("spark.storage.blockManagerHeartBeatMs",    "5000")
//    System.setProperty("spark.storage.blockManagerSlaveTimeoutMs", "100000")
//    System.setProperty("spark.akka.timeout",                       "30000")
//    System.setProperty("spark.akka.retry.wait",                    "30000")
//    System.setProperty("spark.akka.frameSize",                     "2047")
//    System.setProperty("spark.locality.wait",                      "300000000000")
//    val sc = new SparkContext(new SparkConf().setAll(List[(String,String)](
//      ("spark.worker.timeout",                     "30000"),
//      ("spark.storage.blockManagerHeartBeatMs",    "5000"),
//      ("spark.storage.blockManagerSlaveTimeoutMs", "100000"),
//      ("spark.akka.timeout",                       "30000"),
//      ("spark.akka.retry.wait",                    "30000"),
//      ("spark.akka.frameSize",                     "2047"),
//      ("spark.locality.wait",                      "300000000000"),
//      ("spark.logConf",                            "true")
//    )))

    val sc = new SparkContext()

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


    var R : RDD[(Long,Int)] = sc.parallelize((0 until numPartitions).toArray, numPartitions).mapPartitions(_ => new Array[(Long,Int)](100000).iterator).cache()

    var iteration = 0
    while (iteration < 50){
      R = R.zipPartitions(R) { (x,y) => x }.cache().setName("r" + iteration)
      if ((iteration+1) % checkpointIter == 0) sc.setCheckpointDir(checkpointDir + iteration.toString)
      if ((iteration+1) % checkpointIter == 0) R.checkpoint()
      R.foreachPartition(_ => {})
      /*
      if ((iteration+1) % checkpointIter == 0){
        if (checkpointClean && iteration-checkpointIter >= 0) {
          if (checkpointLocal)
            Seq("rm", "-rf", checkpointDir + (iteration - checkpointIter).toString).!
          else
            Seq("/root/ephemeral-hdfs/bin/hadoop", "fs", "-rmr", checkpointDir + (iteration - checkpointIter).toString).!
        }
      }
      */
      iteration += 1
    }

    println("Done")
    while (true) {}

  }
}










