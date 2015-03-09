import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KDD14_MRV03Dimitris {
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext(new SparkConf())

    // var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e4.toInt, numEdges = 1e4.toInt).mapVertices( (id, _) => -100.toInt )


    val path = "/Users/dimitris/Documents/graphs/amazon.txt"
    val numParitions = 2
    val graphInit: Graph[(Int), Int] = GraphLoader.edgeListFile(sc, path, false, numParitions)
    //The following is needed for undirected (bi-directional edge) graphs
    val vertexRDDs: VertexRDD[Int] = graphInit.vertices
    var edgeRDDs: RDD[Edge[Int]] = graphInit.edges.reverse.union(graphInit.edges)
    val graph: Graph[(Int), Int] = Graph(vertexRDDs, edgeRDDs).mapVertices((id, _) => -100.toInt)


    var unclusterGraph: Graph[(Int), Int] = graph
    var activeSubgraph: Graph[(Int), Int] = null
    val epsilon: Double = 2
    var x: Int = 1

    var clusterUpdates: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
    var randomSet: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
    var newVertices: RDD[(org.apache.spark.graphx.VertexId, Int)] = null
    var hasFriends: RDD[(org.apache.spark.graphx.VertexId, Int)] = null

    var maxDegree: VertexRDD[Int] = unclusterGraph.aggregateMessages[Int](
      triplet => {
        if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
          triplet.sendToDst(1)
        }
      }, _ + _).cache()
    var maxDeg: Int = if (maxDegree.count == 0) 0 else maxDegree.toArray.map(x => x._2).max

    while (maxDeg >= 1) {

      randomSet = unclusterGraph.vertices.filter(v => v._2 == -100).sample(false, math.min(epsilon / maxDeg, 1), scala.util.Random.nextInt(1000))
      while (randomSet.count == 0) {
        randomSet = unclusterGraph.vertices.filter(v => v._2 == -100).sample(false, math.min(epsilon / maxDeg, 1), scala.util.Random.nextInt(1000))
      }
      // System.out.println(s"Cluster Centers ${randomSet.collect().toList}.")

      unclusterGraph = unclusterGraph.joinVertices(randomSet)((vId, attr, active) => -1).cache()

      //Turn-off active nodes that are friends
      // activeSubgraph = unclusterGraph.subgraph(vpred = (id, attr) => attr == -1).cache()
      // hasFriends = unclusterGraph.degrees.filter{case (id, u) => u > 0}.cache()
      hasFriends = unclusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -1 & triplet.srcAttr == -1) {
            triplet.sendToDst(1)
          }
        }, math.min(_, _)
      )
      unclusterGraph = unclusterGraph.joinVertices(hasFriends)((vId, attr, active) => -100).cache()



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
      unclusterGraph = unclusterGraph.joinVertices(newVertices)((vId, oldAttr, newAttr) => newAttr).cache()

      maxDegree = unclusterGraph.aggregateMessages[Int](
        triplet => {
          if (triplet.dstAttr == -100 & triplet.srcAttr == -100) {
            triplet.sendToDst(1)
          }
        }, _ + _
      ).cache()
      maxDeg = if (maxDegree.count == 0) 0 else maxDegree.toArray.map(x => x._2).max
      System.out.println(s"new maxDegree $maxDeg.")
      System.out.println(s"KDD14 finished iteration $x.")
      x = x + 1
    }

    //Take care of degree 0 nodes
    newVertices = unclusterGraph.subgraph(vpred = (vId, clusterID) => clusterID == -100).vertices
    newVertices = unclusterGraph.vertices.leftJoin(newVertices) {
      (id, oldValue, newValue) =>
        newValue match {
          case Some(x: Int) => id.toInt
          case None => oldValue;
        }
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


  }
}

