import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object SimpleZipPartitions{
  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val numPartitions   : Int     = 160
    val checkpointIter  : Int     = 10
    val checkpointDir   : String  = "/Users/xinghao/Documents/tempcheckpoint"
//    val checkpointDir   : String  = "/mnt/checkpoints/"

    var R : RDD[(Long,Int)]
    = sc.parallelize((0 until numPartitions), numPartitions)
      .mapPartitions(_ => new Array[(Long,Int)](10000000).map(i => (0L,0)).toSeq.iterator).cache()

    var prevR = R

    sc.setCheckpointDir(checkpointDir)

    var iteration = 0
    while (iteration < 50){
      prevR = R
      R = R.zipPartitions(R)((x,y) => x)
      if ((iteration+1) % checkpointIter == 0) {
        R = R.mapPartitions(x => x)
        R.checkpoint()
      }
      R.cache()
      R.foreachPartition(_ => {})
      prevR.unpersist(false)
      iteration += 1
    }

    println("Done")
    while (true) {}

  }
}










