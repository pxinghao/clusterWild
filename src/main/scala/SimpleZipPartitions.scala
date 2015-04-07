import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SimpleZipPartitions{
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

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