import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.{SparkConf, Logging, SparkContext}
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.immutable.Map

object ClusterWild_fixes_v04 {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()

    val argmap : Map[String,String] = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    val graphType     : String = argmap.getOrElse("graphtype", "rmat").toString.toLowerCase
    val rMatNumEdges  : Int    = argmap.getOrElse("rmatnumedges", 100000000).toString.toInt
    val path          : String = argmap.getOrElse("path", "graphs/astro.edges").toString
    val numPartitions : Int    = argmap.getOrElse("numpartitions", 640).toString.toInt

    /*
    var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e8.toInt, numEdges = 1e8.toInt).mapVertices( (id, _) => -100.toInt )

//    val path = "/Users/dimitris/Documents/graphs/astro.txt"
//    val numPartitions = 4
//    val graph: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numPartitions)
    */

    val graph: Graph[(Int), Int] =
      if (graphType == "rmat")
        GraphGenerators.rmatGraph(sc, requestedNumVertices = rMatNumEdges.toInt, numEdges = rMatNumEdges.toInt).mapVertices( (id, _) => -100.toInt )
      else
        GraphLoader.edgeListFile(sc, path, false, numPartitions)

    System.out.println(s"Graph has ${graph.vertices.count} vertices (${graph.vertices.partitions.length} partitions), ${graph.edges.count} edges (${graph.edges.partitions.length} partitions)")

    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graph.vertices
    var edgeRDDs: RDD[Edge[Int]] = graph.edges.reverse.union(graph.edges)
    // val graph: Graph[(Int), Int] = Graph(vertexRDDs,edgeRDDs).mapVertices( (id, _) => -100.toInt )


    var clusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs).mapVertices((id, _) => -100.toInt)
    val n = clusterGraph.numVertices.toFloat

    var prevClusterGraph: Graph[(Int), Int] = clusterGraph
    val epsilon: Double = 1
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



    var centerID : Int = 0

    var time0, time1, time2, time3, time4: Long = 0



    //TODO: change for to check for empty RDD
    while (maxDeg>=1) {

//      clusterGraph.cache()





      time0 = System.currentTimeMillis

      // randomSet = clusterGraph.vertices.filter(v => v._2 == -100)
      // centerID = -math.abs(scala.util.Random.nextInt)
      // randomSet = randomSet.mapValues( vId => {if (scala.util.Random.nextFloat < epsilon/maxDeg.toFloat) centerID; else -100;})
      // randomSet = randomSet.filter{case (id, clusterID) => clusterID == centerID} // keep the active set
      randomSet = clusterGraph.vertices.filter(v => (v._2 == -100) && (scala.util.Random.nextFloat < epsilon/maxDeg.toFloat))
      numNewCenters = randomSet.count

      time1 = System.currentTimeMillis()

      clusterGraph = clusterGraph.joinVertices(randomSet)((vId, attr, active) => centerID)
      clusterGraph.edges.foreachPartition(_ => {})

      time2 = System.currentTimeMillis()

      clusterUpdates = clusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.srcAttr == centerID & triplet.dstAttr == -100){
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_ , _)
      )


//      prevClusterGraph = clusterGraph
      clusterGraph = clusterGraph.joinVertices(clusterUpdates)((vId, oldAttr, newAttr) => newAttr).cache()
      clusterGraph.edges.foreachPartition(_ => {})
//      clusterGraph.vertices.count()
//      prevClusterGraph.vertices.unpersist()
//      prevClusterGraph.edges.unpersist()

      time3 = System.currentTimeMillis()

//      if (x % math.round(math.log(n)) == 0) {
//        maxDeg = math.round(maxDeg/2)
//
//        // clusterGraph.edges.count()
//      }
      maxDeg = clusterGraph.aggregateMessages[Int](
           triplet => {
               if ( triplet.dstAttr == -100& triplet.srcAttr == -100){ triplet.sendToDst(1) }
               }, _ + _).map( x => x._2).fold(0)((a,b) => math.max(a, b))

//      if (x % 10 == 0){
//        clusterGraph.checkpoint()
//      }


//      System.gc()
      time4 = System.currentTimeMillis()


      System.out.println(
        s"$x\t" +
          s"$maxDeg\t" +
          s"$numNewCenters\t" +
          s"${time4-time0}\t" +
          s"${time1-time0}\t" +
          s"${time2-time1}\t" +
          s"${time3-time2}\t" +
          s"${time4-time3}\t" +
          "")
      x = x+1




    }

    //Take care of degree 0 nodes
    val time10 = System.currentTimeMillis
    newVertices = clusterGraph.subgraph(vpred = (vId, clusterID) => clusterID == -100).vertices
    numNewCenters = newVertices.count
    newVertices = clusterGraph.vertices.leftJoin(newVertices) {
      (id, oldValue, newValue) =>
        newValue match {
          case Some(x: Int) => id.toInt
          case None => oldValue;
        }
    }
    clusterGraph = clusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()
    clusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
    val time11 = System.currentTimeMillis

    System.out.println(
      s"$x\t" +
        s"$maxDeg\t" +
        s"${numNewCenters}\t" +
        s"${time11 - time10}\t" +
        "<end>")

  }
}
