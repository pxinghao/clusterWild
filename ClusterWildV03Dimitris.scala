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


// var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e4.toInt, numEdges = 1e4.toInt).mapVertices( (id, _) => -100.toInt )


val path = "hdfs:///uk-2007-05"
val numParitions = 320
val graphInit: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numParitions)
//The following is needed for undirected (bi-directional edge) graphs
val vertexRDDs: VertexRDD[Int] = graphInit.vertices
var edgeRDDs: RDD[Edge[Int]] = graphInit.edges.reverse.union(graphInit.edges)
val graph: Graph[(Int), Int] = Graph(vertexRDDs,edgeRDDs).mapVertices( (id, _) => -100.toInt )


var unclusterGraph: Graph[(Int), Int] = graph
val epsilon: Double = 2
var x: Int = 1
 
var clusterUpdates: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
var randomSet: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
var newVertices: RDD[(org.apache.spark.graphx.VertexId, Int)] = null

var maxDegree: VertexRDD[Int] = unclusterGraph.aggregateMessages[Int](
        triplet => {
            if ( triplet.dstAttr == -100& triplet.srcAttr == -100){ triplet.sendToDst(1) }
            }, _ + _).cache()
          var maxDeg: Int = if (maxDegree.count == 0) 0 else maxDegree.map( x => x._2).reduce((a,b) => math.max(a, b))

while (maxDeg>=1) {

    randomSet = unclusterGraph.vertices.filter(v => v._2 == -100).sample(false, math.min(epsilon/maxDeg,1), scala.util.Random.nextInt(1000))
    while(randomSet.count==0){
        randomSet = unclusterGraph.vertices.filter(v => v._2 == -100).sample(false, math.min(epsilon/maxDeg,1), scala.util.Random.nextInt(1000))
    }
    // System.out.println(s"Cluster Centers ${randomSet.collect().toList}.")

    unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1).cache()
    clusterUpdates = unclusterGraph.aggregateMessages[Int](
        triplet => {
            if (triplet.dstAttr == -100 & triplet.srcAttr == -1){ 
                triplet.sendToDst(triplet.srcId.toInt) 
            }
            }, math.min(_ , _)
    )
    newVertices = unclusterGraph.vertices.leftJoin(clusterUpdates) {
      (id, oldValue, newValue) =>
      newValue match {
          case Some(x:Int) => x
          case None => {if (oldValue == -1) id.toInt; else oldValue;}
         }
    }
    unclusterGraph = unclusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()    

    maxDegree = unclusterGraph.aggregateMessages[Int](
        triplet => {if ( triplet.dstAttr == -100 & triplet.srcAttr == -100){ triplet.sendToDst(1) }
            }, _ + _
    ).cache()
             maxDeg = if (maxDegree.count == 0) 0 else maxDegree.toArray.map( x => x._2).max
    System.out.println(s"new maxDegree $maxDeg.")
    System.out.println(s"ClusterWild! finished iteration $x.")
    x = x+1
}

//Take care of degree 0 nodes
newVertices = unclusterGraph.subgraph(vpred = (vId, clusterID) => clusterID == -100).vertices
newVertices = unclusterGraph.vertices.leftJoin(newVertices) {
      (id, oldValue, newValue) =>
      newValue match {
          case Some(x:Int) => id.toInt
          case None => oldValue;}
    }
unclusterGraph = unclusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()    


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





