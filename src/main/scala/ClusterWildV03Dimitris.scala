import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Map

object ClusterWildV03Dimitris {
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


    var unclusterGraph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs).mapVertices((id, _) => -100.toInt)
    var prevUnclusterGraph: Graph[(Int), Int] = null
    val epsilon: Double = 2
    var x: Int = 1

    var clusterUpdates: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
    var randomSet: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
    var newVertices: RDD[(org.apache.spark.graphx.VertexId, Int)] = null

    var numNewCenters: Long = 0

    var maxDegree: VertexRDD[Int] = unclusterGraph.aggregateMessages[Int](
      triplet => {
        if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
          triplet.sendToDst(1)
        }
      }, _ + _).cache()
    var maxDeg: Int = if (maxDegree.count == 0) 0 else maxDegree.map(x => x._2).reduce((a, b) => math.max(a, b))

    while (maxDeg >= 1) {
      val time0 = System.currentTimeMillis

      numNewCenters = 0
      while (numNewCenters == 0) {
        randomSet = unclusterGraph.vertices.filter(v => v._2 == -100).sample(false, math.min(epsilon / maxDeg, 1), scala.util.Random.nextInt(1000))
        numNewCenters = randomSet.count
      }
      // System.out.println(s"Cluster Centers ${randomSet.collect().toList}.")

      // prevUnclusterGraph = unclusterGraph
      unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1).cache()
      // prevUnclusterGraph.vertices.unpersist(false)
      // prevUnclusterGraph.edges.unpersist(false)

      clusterUpdates = unclusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -100 & triplet.srcAttr == -1) {
            triplet.sendToDst(triplet.srcId.toInt)
          }
        }, math.min(_, _)
      )
      newVertices = unclusterGraph.vertices.leftJoin(clusterUpdates) {
        (id, oldValue, newValue) =>
          newValue match {
            case Some(x: Int) => x
            case None => {
              if (oldValue == -1) id.toInt; else oldValue;
            }
          }
      }

      // prevUnclusterGraph = unclusterGraph
      unclusterGraph = unclusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()
      // prevUnclusterGraph.vertices.unpersist(false)
      // prevUnclusterGraph.edges.unpersist(false)

      maxDegree = unclusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
            triplet.sendToDst(1)
          }
        }, _ + _
      ).cache()
      maxDeg = if (maxDegree.count == 0) 0 else maxDegree.map(x => x._2).reduce((a, b) => math.max(a, b))
      // System.out.println(s"new maxDegree $maxDeg.")
      // System.out.println(s"ClusterWild! finished iteration $x.")

      val time1 = System.currentTimeMillis
      System.out.println(
        s"$x\t" +
          s"$maxDeg\t" +
          s"$numNewCenters\t" +
          s"${time1 - time0}\t" +
          "")
      x = x + 1
    }

    //Take care of degree 0 nodes
    val time10 = System.currentTimeMillis
    newVertices = unclusterGraph.subgraph(vpred = (vId, clusterID) => clusterID == -100).vertices
    numNewCenters = newVertices.count
    newVertices = unclusterGraph.vertices.leftJoin(newVertices) {
      (id, oldValue, newValue) =>
        newValue match {
          case Some(x: Int) => id.toInt
          case None => oldValue;
        }
    }
    unclusterGraph = unclusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()
    unclusterGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
    val time11 = System.currentTimeMillis

    System.out.println(
      s"$x\t" +
        s"$maxDeg\t" +
        s"${newVertices}\t" +
        s"${time11 - time10}\t" +
        "<end>")


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


  }
}
