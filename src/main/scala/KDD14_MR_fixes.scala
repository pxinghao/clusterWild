import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

import org.apache.log4j.Logger
import org.apache.log4j.Level



object KDD14_MR_fixes {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()


    // var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e8.toInt, numEdges = 1e8.toInt).mapVertices( (id, _) => -100.toInt )

    val path = "/Users/dimitris/Documents/graphs/dblp.txt"
    val numParitions = 2
    val graphInit: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numParitions)
    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graphInit.vertices
    var edgeRDDs: RDD[Edge[Int]] = graphInit.edges.reverse.union(graphInit.edges)
    // val graph: Graph[(Int), Int] = Graph(vertexRDDs,edgeRDDs).mapVertices( (id, _) => -100.toInt )


    var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs).mapVertices((id, _) => -100.toInt)
    val n = clusterGraph.numVertices.toFloat

    var prevClusterGraph: Graph[(Int), Int] = clusterGraph
    val epsilon: Double = 2
    var x: Int = 1

    var clusterUpdates: VertexRDD[(Int)] = null
    var hasFriends: VertexRDD[(Int)] = null
    var randomSet: VertexRDD[(Int)] = null
    var newVertices: VertexRDD[(Int)] = null

    var numNewCenters: Long = 0

    var maxDegree: VertexRDD[Int] = clusterGraph.aggregateMessages[Int](
      triplet => {
        if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
          triplet.sendToDst(1)
        }
      }, _ + _).cache()

    var maxDeg: Int = maxDegree.map(x => x._2).fold(0)((a, b) => math.max(a, b))







    //TODO: change for to check for empty RDD
    while (maxDeg >= 1) {

      val time0 = System.currentTimeMillis

      randomSet = clusterGraph.vertices.filter(v => v._2 == -100)
      randomSet = randomSet.mapValues(vId => {
        if (scala.util.Random.nextFloat < epsilon / maxDeg) -1; else -100;
      })
      randomSet = randomSet.filter { case (id, clusterID) => clusterID == -1} // keep the active set
      numNewCenters = randomSet.count

      // prevUnclusterGraph = unclusterGraph
      //
      clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => -1).cache()
      //
      // prevUnclusterGraph.vertices.unpersist(false)
      // prevUnclusterGraph.edges.unpersist(false)

      hasFriends = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -1 & triplet.srcAttr == -1) {
            triplet.sendToDst(1)
          }
        }, math.min(_, _)
      )
      // if (hasFriends.count>0)
      clusterGraph = clusterGraph.joinVertices(hasFriends)((vId, attr, active) => -100).cache()



      clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == -1 & triplet.dstAttr == -100) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_, _)
      )

      newVertices = clusterGraph.vertices.leftJoin(clusterUpdates) {
        (id, oldValue, newValue) =>
          newValue match {
            case Some(x: Int) => x
            case None => {
              if (oldValue == -1) id.toInt; else oldValue;
            }
          }
      }

      prevClusterGraph = clusterGraph
      //
      clusterGraph = clusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()
      //
      prevClusterGraph.vertices.unpersist(false)
      prevClusterGraph.edges.unpersist(false)
      clusterGraph.edges.foreachPartition(x => {})
      // clusterGraph.vertices.count


      // logInfo(s"PageRank finished iteration $iteration.")

      // prevUnclusterGraph.edges.unpersist(false)

      // maxDegree = unclusterGraph.aggregateMessages[Int](
      //     triplet => {if ( triplet.dstAttr == -100 & triplet.srcAttr == -100){ triplet.sendToDst(1) }
      //         }, _ + _
      // ).cache()
      // if (x % math.round(math.log10(n)) == 0) {
      if (x % 3 == 0) maxDeg = math.round(maxDeg / 2);


      val time1 = System.currentTimeMillis
      System.out.println(
        s"$x\t" +
          s"$maxDeg\t" +
          s"$numNewCenters\t" +
          s"${time1 - time0}\t" +
          "")
      x = x + 1
    }
    clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
    //Take care of degree 0 nodes
    newVertices = clusterGraph.subgraph(vpred = (vId, clusterID) => clusterID == -100).vertices
    newVertices = clusterGraph.vertices.leftJoin(newVertices) {
      (id, oldValue, newValue) =>
        newValue match {
          case Some(x: Int) => id.toInt
          case None => oldValue;
        }
    }
    clusterGraph = clusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()


    // unclusterGraph = unclusterGraph.mapVertices((id,clusterID) => v == 1)
    // unclusterGraph.vertices.collect


    // // unhappy edges accross clusters
    // val unhappyFriends: Float = unclusterGraph.triplets.filter(e=> e.dstAttr != e.srcAttr).count/2
    // // compute cluster sizes
    // val clusterSizes: List[Float] = unclusterGraph.vertices.map(v=> v._2).countByValue.map(v =>  v._2).toList.map(_.toFloat)
    // // compute missing edges inside clusters
    // val tripletsWithSameID: Float = unclusterGraph.triplets.filter(e=> e.dstAttr == e.srcAttr).count/2

    // //Cost
    // val costClusterWild = (clusterSizes.map( x=> x*(x-1)/2).sum - tripletsWithSameID) + unhappyFriends

    // // Compute the max degrees
    // val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    // val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    // val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)


  }
}