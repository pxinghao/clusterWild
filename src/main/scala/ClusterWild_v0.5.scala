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

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)


// var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e8.toInt, numEdges = 1e8.toInt).mapVertices( (id, _) => -100.toInt )

val path = "/Users/dimitris/Documents/graphs/astro.txt"
val numParitions = 2
val graphInit: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numParitions)
//The following is needed for undirected (bi-directional edge) graphs
val vertexRDDs: VertexRDD[Int] = graphInit.vertices
var edgeRDDs: RDD[Edge[Int]] = graphInit.edges.reverse.union(graphInit.edges)
// val graph: Graph[(Int), Int] = Graph(vertexRDDs,edgeRDDs).mapVertices( (id, _) => -100.toInt )





var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs,edgeRDDs).mapVertices( (id, _) => -100.toInt )
val n = clusterGraph.numVertices.toFloat

var prevClusterGraph: Graph[(Int), Int] = clusterGraph
val epsilon: Double = 2
var x: Int = 1
 
var clusterUpdates: VertexRDD[(Int)] = null
var randomSet: VertexRDD[(Int)] = null
var newVertices: VertexRDD[(Int)] = null

var numNewCenters : Long = 0

var maxDegree: VertexRDD[Int] = clusterGraph.aggregateMessages[Int](
        triplet => {
            if ( triplet.dstAttr == -100& triplet.srcAttr == -100){ triplet.sendToDst(1) }
            }, _ + _).cache()

var maxDeg: Int = maxDegree.map( x => x._2).fold(0)((a,b) => math.max(a, b))



var centerID: Int = 0
        


//TODO: change for to check for empty RDD
while (maxDeg>=1) {

    val time0 = System.currentTimeMillis

    randomSet = clusterGraph.vertices.filter(v => v._2 == -100)  
    centerID = -math.abs(scala.util.Random.nextInt)
    randomSet = randomSet.mapValues( vId => {if (scala.util.Random.nextFloat < epsilon/maxDeg) centerID; else -100;})
    randomSet = randomSet.filter{case (id, clusterID) => clusterID == centerID} // keep the active set      
    numNewCenters = randomSet.count 

    // prevUnclusterGraph = unclusterGraph
    //
    clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => centerID).cache()
    //
    // prevUnclusterGraph.vertices.unpersist(false)
    // prevUnclusterGraph.edges.unpersist(false)
    
    clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
            if (triplet.srcAttr == centerID & triplet.dstAttr == -100){ 
                triplet.sendToDst(triplet.srcId.toInt) 
            }
            }, math.min(_ , _), TripletFields.Src
    )
    
    // newVertices = clusterGraph.vertices.leftJoin(clusterUpdates) {
    //   (id, oldValue, newValue) =>
    //   newValue match {
    //       case Some(x:Int) => x
    //       case None => {if (oldValue == -1) id.toInt; else oldValue;}
    //      }
    // }
    // if (x % math.log(n) == 0) {
    // prevClusterGraph = clusterGraph
    // }
    //
    clusterGraph = clusterGraph.joinVertices(clusterUpdates)((vId, oldAttr, newAttr) => newAttr).cache()    
    clusterGraph.vertices.count()
    clusterGraph.edges.count()
    // clusterGraph.unpersist()
    // clusterGraph = prevClusterGraph
    // clusterGraph.cache()
    //
    // 
    
    // materialize(clusterGraph)
    // g.unpersist()
      // g = gJoinT1
    // clusterGraph.edges.foreachPartition(x => {})
    // clusterGraph.vertices.count

    
    
      // logInfo(s"PageRank finished iteration $iteration.")

    // prevUnclusterGraph.edges.unpersist(false)

    // maxDegree = unclusterGraph.aggregateMessages[Int](
    //     triplet => {if ( triplet.dstAttr == -100 & triplet.srcAttr == -100){ triplet.sendToDst(1) }
    //         }, _ + _
    // ).cache()
    // if (x % math.round(math.log10(n)) == 0) {
    if (x % math.round(math.log(n)) == 0) {
      maxDeg = math.round(maxDeg/2)
      // prevClusterGraph.vertices.unpersist(false)
      // prevClusterGraph.edges.unpersist(false)
    }
  

    val time1 = System.currentTimeMillis
    System.out.println(
      s"$x\t" +
      s"$maxDeg\t" +
      s"$numNewCenters\t" +
      s"${time1-time0}\t" +
      "")
    x = x+1
}
clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
//Take care of degree 0 nodes
newVertices = clusterGraph.subgraph(vpred = (vId, clusterID) => clusterID == -100).vertices
newVertices = clusterGraph.vertices.leftJoin(newVertices) {
      (id, oldValue, newValue) =>
      newValue match {
          case Some(x:Int) => id.toInt
          case None => oldValue;}
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



