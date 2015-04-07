import org.apache.spark.SparkContext
import org.apache.spark.rdd._

object SimpleZipPartitions{
  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val argmap: Map[String, String] = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    val numPartitions   : Int     = argmap.getOrElse("numpartitions", "640").toInt
    val checkpointIter  : Int     = argmap.getOrElse("checkpointiter", "20").toInt
    val checkpointDir   : String  = argmap.getOrElse("checkpointdir", "/mnt/checkpoints/")

    var R    : RDD[(Long,Int)] = sc.parallelize((0 until numPartitions), numPartitions).mapPartitions(_ => new Array[(Long,Int)](10000000).toSeq.iterator).cache()
    var oldR : RDD[(Long,Int)] = null

    sc.setCheckpointDir(checkpointDir)

    var iteration = 0
    while (iteration < 50){
      oldR = R
      R = R.zipPartitions(R) { (x,y) => x }.cache()
      if ((iteration+1) % checkpointIter == 0) R.checkpoint()
      R.foreachPartition(_ => {})
      oldR.unpersist()
      iteration += 1
    }

    println("Done")
    while (true) {}

  }
}










