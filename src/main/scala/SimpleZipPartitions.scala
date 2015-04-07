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

    val sc = new SparkContext()

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val argmap: Map[String, String] = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    val numPartitions   : Int     = argmap.getOrElse("numpartitions", "640").toInt
    val checkpointIter  : Int     = argmap.getOrElse("checkpointiter", "20").toInt
    val checkpointDir   : String  = argmap.getOrElse("checkpointdir", "/mnt/checkpoints/")

    var R : RDD[(Long,Int)] = sc.parallelize((0 until numPartitions), numPartitions).mapPartitions(_ => new Array[(Long,Int)](10000000).toSeq.iterator).cache()

    var iteration = 0
    while (iteration < 50){
      R = R.zipPartitions(R) { (x,y) => x }.cache().setName("r" + iteration)
      if ((iteration+1) % checkpointIter == 0) sc.setCheckpointDir(checkpointDir + iteration.toString)
      if ((iteration+1) % checkpointIter == 0) R.checkpoint()
      R.foreachPartition(_ => {})
      iteration += 1
    }

    println("Done")
    while (true) {}

  }
}










