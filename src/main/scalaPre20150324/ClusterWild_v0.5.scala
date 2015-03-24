/*
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

val path = "/Users/dimitris/Documents/graphs/dblp.txt"
val numParitions = 8
val graphInit: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numParitions)
// clusterGraph = clusterGraph.mapVertices( (id, _) => -100.toInt )
//The following is needed for undirected (bi-directional edge) graphs
val vertexRDDs: VertexRDD[Int] = graphInit.vertices
val edgeRDDs: RDD[Edge[Int]] = graphInit.edges.reverse.union(graphInit.edges)
var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs,edgeRDDs)
clusterGraph = clusterGraph.mapVertices( (id, _) => -100.toInt )

val n = clusterGraph.numVertices.toFloat
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
        

var time0: Long = 0
var time1: Long = 0



//TODO: change for to check for empty RDD
while (maxDeg>=1) {

    // clusterGraph.cache()

    



    time0 = System.currentTimeMillis

    // randomSet = clusterGraph.vertices.filter(v => v._2 == -100)  
    // centerID = -math.abs(scala.util.Random.nextInt)
    // randomSet = randomSet.mapValues( vId => {if (scala.util.Random.nextFloat < epsilon/maxDeg.toFloat) centerID; else -100;})
    // randomSet = randomSet.filter{case (id, clusterID) => clusterID == centerID} // keep the active set      
    randomSet = clusterGraph.vertices.filter(v => (v._2 == -100) && (scala.util.Random.nextFloat < epsilon/maxDeg.toFloat)).cache()
    numNewCenters = randomSet.count 

    clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => centerID)
    clusterGraph.cache()
    
    clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
            if (triplet.srcAttr == centerID & triplet.dstAttr == -100){ 
                triplet.sendToDst(triplet.srcId.toInt) 
            }
            }, math.min(_ , _)
    )


    
    clusterGraph = clusterGraph.joinVertices(clusterUpdates)((vId, oldAttr, newAttr) => newAttr)    
    // clusterGraph.vertices.count()
    clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
    clusterGraph.cache()

    // if (x % math.round(math.log(n)) == 0) {
    //   maxDeg = math.round(maxDeg/2)
      
    //   // clusterGraph.edges.count()
    // }
    
    maxDegree= clusterGraph.aggregateMessages[Int](
        triplet => {
            if ( triplet.dstAttr == -100& triplet.srcAttr == -100){ triplet.sendToDst(1) }
            }, _ + _).cache()

    maxDeg = maxDegree.map( x => x._2).fold(0)((a,b) => math.max(a, b))


    
    time1 = System.currentTimeMillis
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



*/




import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Map
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

import org.apache.log4j.Logger
import org.apache.log4j.Level


object ClusterWild_v05 {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()

    val argmap: Map[String, String] = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    val graphType: String = argmap.getOrElse("graphtype", "rmat").toString.toLowerCase
    val rMatNumEdges: Int = argmap.getOrElse("rmatnumedges", 100000000).toString.toInt
    val path: String = argmap.getOrElse("path", "graphs/astro.edges").toString
    val numPartitions: Int = argmap.getOrElse("numpartitions", 640).toString.toInt

    /*
    var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e8.toInt, numEdges = 1e8.toInt).mapVertices( (id, _) => -100.toInt )

//    val path = "/Users/dimitris/Documents/graphs/astro.txt"
//    val numPartitions = 4
//    val graph: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numPartitions)
    */

    val graph: Graph[(Int), Int] =
      if (graphType == "rmat")
        GraphGenerators.rmatGraph(sc, requestedNumVertices = rMatNumEdges.toInt, numEdges = rMatNumEdges.toInt).mapVertices((id, _) => -100.toInt)
      else
        GraphLoader.edgeListFile(sc, path, false, numPartitions)

    System.out.println(s"Graph has ${graph.vertices.count} vertices (${graph.vertices.partitions.length} partitions), ${graph.edges.count} edges (${graph.edges.partitions.length} partitions)")

    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graph.vertices
    val edgeRDDs: RDD[Edge[Int]] = graph.edges.reverse.union(graph.edges)
    var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs)
    clusterGraph = clusterGraph.mapVertices((id, _) => -100.toInt)

    val n = clusterGraph.numVertices.toFloat
    val epsilon: Double = 2
    var x: Int = 1

    var clusterUpdates: VertexRDD[(Int)] = null
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



    var centerID: Int = 0



    //TODO: change for to check for empty RDD
    while (maxDeg >= 1) {

      clusterGraph.cache()





      val time0 = System.currentTimeMillis

      // randomSet = clusterGraph.vertices.filter(v => v._2 == -100)
      // centerID = -math.abs(scala.util.Random.nextInt)
      // randomSet = randomSet.mapValues( vId => {if (scala.util.Random.nextFloat < epsilon/maxDeg.toFloat) centerID; else -100;})
      // randomSet = randomSet.filter{case (id, clusterID) => clusterID == centerID} // keep the active set
      randomSet = clusterGraph.vertices.filter(v => (v._2 == -100) && (scala.util.Random.nextFloat < epsilon / maxDeg.toFloat)).cache()
      numNewCenters = randomSet.count

      clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => centerID).cache()

      clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == centerID & triplet.dstAttr == -100) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_, _)
      ).cache()



      clusterGraph = clusterGraph.joinVertices(clusterUpdates)((vId, oldAttr, newAttr) => newAttr).cache()
      clusterGraph.vertices.count()

      if (x % math.round(math.log(n)) == 0) {
        maxDeg = math.round(maxDeg / 2)

        // clusterGraph.edges.count()
      }
      // maxDegree= clusterGraph.aggregateMessages[Int](
      //     triplet => {
      //         if ( triplet.dstAttr == -100& triplet.srcAttr == -100){ triplet.sendToDst(1) }
      //         }, _ + _).cache()

      // maxDeg = maxDegree.map( x => x._2).fold(0)((a,b) => math.max(a, b))


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