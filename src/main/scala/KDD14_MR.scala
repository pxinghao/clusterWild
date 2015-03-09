import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KDD14_MR {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext(new SparkConf())



    var numExpts = 10
    var KDD14_MRRunTimes: Array[Long] = new Array[Long](10)
    var expti: Int = 0
    while (expti < numExpts) {
      KDD14_MRRunTimes(expti) = runKDD14_MR(sc, 3e5.toInt, 6e5.toInt)
      expti += 1
    }


  }

  def runKDD14_MR(sc: SparkContext, requestedNumVertices: Int, numEdges: Int): Long = {

    var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(
      sc,
      requestedNumVertices = requestedNumVertices,
      numEdges = numEdges
    ).mapVertices((id, _) => -100.toInt)
    var unclusterGraph: Graph[(Int), Int] = graph
    var activeSubgraph: Graph[(Int), Int] = graph
    val epsilon: Double = 1


    var x: Int = 1
    var prevRankGraph1: Graph[Int, Int] = null
    var prevRankGraph2: Graph[Int, Int] = null
    var maxDegree = graph.vertices.sample(false, 1, 1)
    var clusterUpdates = graph.vertices.sample(false, 1, 1)
    var maxDeg: Int = 0
    var randomSet = graph.vertices.sample(false, 1, 1)
    var newVertices = graph.vertices.sample(false, 1, 1)
    var hasFriends = graph.vertices.sample(false, 1, 1)

    val startTime = System.currentTimeMillis

    while (graph.vertices.filter(v => v._2 == -100).count() > 0) {

      unclusterGraph = graph.subgraph(vpred = (id, attr) => attr == -100)
      maxDegree = unclusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == -100 & triplet.dstAttr == -100) {
            triplet.sendToSrc(1)
          }
        }, _ + _)
      maxDeg = if (maxDegree.count == 0) 1 else maxDegree.toArray.map(x => x._2).max
      randomSet = unclusterGraph.vertices.sample(false, math.min(epsilon / maxDeg, 1), scala.util.Random.nextInt(1000))
      while (randomSet.count == 0) {
        randomSet = unclusterGraph.vertices.sample(false, math.min(epsilon / maxDeg, 1), scala.util.Random.nextInt(1000))
      }


      unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1)
      activeSubgraph = unclusterGraph.subgraph(vpred = (id, attr) => attr == -1)
      hasFriends = activeSubgraph.degrees.filter { case (id, u) => u > 0}
      unclusterGraph = unclusterGraph.joinVertices(hasFriends)((vId, attr, active) => 0)


      // unclusterGraph = unclusterGraph.joinVertices(hasFriends)((vId, attr, active) => 0)
      // unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1)

      clusterUpdates = unclusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -100 // if not clustered
            & triplet.srcAttr == -1 // the source is an active node
          ) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_, _)
      )
      newVertices = unclusterGraph.vertices.leftJoin(clusterUpdates) {
        (id, oldValue, newValue) =>
          newValue match {
            case Some(x: Int) => x
            case None => {
              if (oldValue == -1) -10; else oldValue;
            }
          }
      }

      graph = graph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()

      // prevRankGraph1 = unclusterGraph
      // prevRankGraph2 = graph

      graph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      // unclusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      System.out.println(s"ClusterWild finished iteration $x.")
      System.out.println(s"MaxDegree $maxDeg.")

      // graph.vertices.unpersist(false)
      // prevRankGraph2.vertices.unpersist(false)
      // prevRankGraph1.edges.unpersist(false)
      // prevRankGraph2.edges.unpersist(false)
      x = x + 1
      // graph.vertices.collect()
    }
    // System.out.println(s"ClusterIDs ${graph.vertices.collect().toList}.")


    val endTime = System.currentTimeMillis

    System.out.println(s"Total time: ${endTime - startTime}")

    endTime - startTime

  }
}


