import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


// val vertexArray = Array(
// 	(1L, (0)),
// 	(2L, (0)),
// 	(3L, (0)),
// 	(4L, (0)),
// 	(5L, (0))
// 	)


// val edgeArray = Array(
// 	Edge(1, 2, 1),
// 	Edge(1, 3, 1),
// 	Edge(1, 5, 1),
// 	Edge(2, 1, 1),
// 	Edge(2, 4, 1),
// 	Edge(2, 5, 1),
// 	Edge(3, 1, 1),
// 	Edge(3, 5, 1),
// 	Edge(4, 2, 1),
// 	Edge(5, 1, 1),
// 	Edge(5, 2, 1),
// 	Edge(5, 3, 1)
// 	)

// val vertexRDD: RDD[(Long,  (Int))] = sc.parallelize(vertexArray)
// val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
// var graph: Graph[(Int), Int] = Graph(vertexRDD, edgeRDD)
var graph = GraphGenerators.rmatGraph(sc, 100, 1000).mapVertices((vid, _) => 0)

var unclusterGraph: Graph[(Int), Int] = graph
val epsilon: Double = 1

val startTime = System.currentTimeMillis

while (graph.vertices.filter(v => v._2 == 0).count()>0) {

	val time0 = System.currentTimeMillis
	
	unclusterGraph = graph.subgraph(vpred = (id, attr) => attr == 0)
	val time1 = System.currentTimeMillis

	val maxDegree = unclusterGraph.aggregateMessages[Int](
		triplet => { if (triplet.srcAttr == 0 & triplet.dstAttr == 0 ) {
						triplet.sendToSrc(1) }
						}, _ + _)
	val time2 = System.currentTimeMillis


	val maxDegInt = if (maxDegree.count == 0) 1 else maxDegree.toArray.map( x => x._2).max	
	val randomSet = unclusterGraph.vertices.sample(false, epsilon/maxDegInt, scala.util.Random.nextInt(1000))	
	System.out.println(s"randomSet.count = ${randomSet.count}")
	val time3 = System.currentTimeMillis



	unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1)
	val time4 = System.currentTimeMillis

	// This is the extra part needed for the KDD14 paper
	val activeSubgraph = unclusterGraph.subgraph(vpred = (id, attr) => attr == -1)
	val time5 = System.currentTimeMillis

	val hasFriends = activeSubgraph.degrees.filter{case (id, u) => u > 0}
	val time6 = System.currentTimeMillis

	unclusterGraph = unclusterGraph.joinVertices(hasFriends)((vId, attr, active) => 0)
	val time7 = System.currentTimeMillis

	// extra code ends here

	val clusterUpdates = unclusterGraph.aggregateMessages[Int](
		triplet => {			
			if ( triplet.dstAttr == 0 // if not clustered
				& triplet.srcAttr == -1 // the source is an active node
				){ triplet.sendToDst(triplet.srcId.toInt) }
			}, math.min(_ , _) 
	)
	val time8 = System.currentTimeMillis


	val newVertices = unclusterGraph.vertices.leftJoin(clusterUpdates) {
      (id, oldValue, newValue) =>
      newValue match {
		  case Some(x:Int) => x 
		  case None => {if (oldValue == -1) -10; else oldValue;}
     	}
     }	
	graph = graph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr)
	val time9 = System.currentTimeMillis

	System.out.println(
		s"${time1 - time0}\t" +
		s"${time2 - time1}\t" +
		s"${time3 - time2}\t" +
		s"${time4 - time3}\t" +
		s"${time5 - time4}\t" +
		s"${time6 - time5}\t" +
		s"${time7 - time6}\t" +
		s"${time8 - time7}\t" +
		s"${time9 - time8}\t" +
		"")


} 

val endTime = System.currentTimeMillis

System.out.println(s"${endTime - startTime} ms taken")
