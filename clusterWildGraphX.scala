import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random


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

var iter = 0

while (graph.vertices.filter(v => v._2 == 0).count()>0 && iter < 50) {

	iter += 1

	val time0 = System.currentTimeMillis

	unclusterGraph = graph.subgraph(vpred = (id, attr) => attr == 0)
	System.out.println(s"unclusterGraph.vertices.count = ${unclusterGraph.vertices.count}")
	System.out.println(s"graph.vertices.count          = ${graph.vertices.count}")
	val time1 = System.currentTimeMillis

	val maxDegree = unclusterGraph.aggregateMessages[Int](
		triplet => { if (triplet.srcAttr == 0 & triplet.dstAttr == 0 ) {
						triplet.sendToSrc(1) }
						}, _ + _)
	val time2 = System.currentTimeMillis

	val maxDegInt = maxDegree.toArray.map( x => x._2).max	
	val randomSet = unclusterGraph.vertices.sample(false, epsilon/maxDegInt, scala.util.Random.nextInt(1000))	
	System.out.println(s"randomSet.count = ${randomSet.count}")
	val time3 = System.currentTimeMillis


	unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1)
	
	val clusterUpdates = unclusterGraph.aggregateMessages[Int](
		triplet => {			
			if ( triplet.dstAttr == 0 // if not clustered
				& triplet.srcAttr == -1 // the source is an active node
				){ triplet.sendToDst(triplet.srcId.toInt) }
			}, math.min(_ , _) 
	)
	val newVertices = unclusterGraph.vertices.leftJoin(clusterUpdates) {
      (id, oldValue, newValue) =>
      newValue match {
		  case Some(x:Int) => x 
		  case None => {if (oldValue == -1) -10; else oldValue;}
     	}
     }	
	graph = graph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr)

	System.out.println(
		s"${time1 - time0}\t" +
		s"${time2 - time1}\t" +
		s"${time3 - time2}\t" +
		"")


}

val endTime = System.currentTimeMillis

System.out.println(s"${endTime - startTime} ms taken")
