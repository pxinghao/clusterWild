import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object SimpleZipPartitions{
  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val numPartitions   : Int     = 160
    val checkpointIter  : Int     = 10
    val checkpointDir   : String  = "/mnt/checkpoints/"

    var R : RDD[(Long,Int)]
    = sc.parallelize((0 until numPartitions), numPartitions)
      .mapPartitions(_ => new Array[(Long,Int)](10000000).toSeq.iterator).cache()

    sc.setCheckpointDir(checkpointDir)

    var iteration = 0
    while (iteration < 50){
      R = R.join(R).map(x => (x._1,x._2._1)).cache()
      if ((iteration+1) % checkpointIter == 0) R.checkpoint()
      R.foreachPartition(_ => {})
      iteration += 1
    }

    println("Done")
    while (true) {}

  }
}










