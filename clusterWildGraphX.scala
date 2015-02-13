import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random


val vertexArray = Array(
	(1L, (0)),
	(2L, (0)),
	(3L, (0)),
	(4L, (0)),
	(5L, (0))
	)


val edgeArray = Array(
	Edge(1, 2, 1),
	Edge(1, 3, 1),
	Edge(1, 5, 1),
	Edge(2, 1, 1),
	Edge(2, 4, 1),
	Edge(2, 5, 1),
	Edge(3, 1, 1),
	Edge(3, 5, 1),
	Edge(4, 2, 1),
	Edge(5, 1, 1),
	Edge(5, 2, 1),
	Edge(5, 3, 1)
	)

val vertexRDD: RDD[(Long,  (Int))] = sc.parallelize(vertexArray)
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
var graph: Graph[(Int), Int] = Graph(vertexRDD, edgeRDD)
var unclusterGraph: Graph[(Int), Int] = graph
val epsilon: Double = 1

while (graph.vertices.filter(v => v._2 == 0).count()>0) {
	
	unclusterGraph = graph.subgraph(vpred = (id, attr) => attr == 0)
	val maxDegree = unclusterGraph.aggregateMessages[Int](
		triplet => { if (triplet.srcAttr == 0 & triplet.dstAttr == 0 ) {
						triplet.sendToSrc(1) }
						}, _ + _)
	val maxDegInt = maxDegree.toArray.map( x => x._2).max	
	val randomSet = unclusterGraph.vertices.sample(false, epsilon/maxDegInt, scala.util.Random(1).nextInt(1000))	

	unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1)
	// This part is for the KDD14 paper
	// activeSubgraph = graph.subgraph(vpred = (id, attr) => attr == -1)
	// val isFriendsWithOhterActives = activeSubgraph.aggregateMessages[Int](
	// 	triplet => {triplet.sendToDst(triplet.srcId.toInt)}, _ , _)	
	//TODO: find the nonzeros, make a subgraph out of that, and go on

	val clusterUpdates = unclusterGraph.aggregateMessages[Int](
		triplet => {			
			if ( triplet.dstAttr == 0 // if not clustered
				& triplet.srcAttr == -1 // the source is an active node
				){ triplet.sendToDst(triplet.srcId.toInt) }
			}, math.min(_ , _) 
	)

	var newVertices = graph.vertices.leftJoin(clusterUpdates) {
      (id, oldValue, newValueOpt) =>
      newValueOpt match {
		  case Some(x:Int) => x 
		  case _ => oldValue}
     }	
	graph = graph.joinVertices(newVertices)((vId, attr, active) => active)
	graph = graph.joinVertices(randomSet)((vId, attr, active) => -1)
}



// def sample[A](itms:List[A], sampleSize:Int) = {

//         def collect(vect: Vector[A], sampleSize: Int, acc : List[A]) : List[A] = {
//             if (sampleSize == 0) acc
//             else {
//                 val index = Random.nextInt(vect.size)
//                 collect( vect.updated(index, vect(0)) tail, sampleSize - 1, vect(index) :: acc)
//             }
//         }

//         collect(itms toVector, sampleSize, Nil)
//     }    

							
