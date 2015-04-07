import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SimpleZipPartitions{
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()

    val checkpointDir   : String  = "/mnt/checkpoints/"
    sc.setCheckpointDir(checkpointDir)

    val numRands = 10000
    val randArray = new Array[Double](numRands)

    var i = 0
    while (i < numRands){
      randArray(i) = scala.util.Random.nextDouble()
      i += 1
    }

    val R = sc.parallelize(randArray, 160)

    i = 0
    while (i < 20){
      R.zipPartitions(R) { (x,y) => x }.cache()
      if (i == 10) R.checkpoint()
      R.foreachPartition(_ => {})
      i += 1
    }

    while (true) {}

  }
}