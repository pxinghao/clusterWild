import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.util.control.Breaks._


import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


var graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, requestedNumVertices = 1e6.toInt, numEdges = 1e6.toInt).mapVertices( (id, _) => -100.toInt )
var rankGraph: Graph[Int, Int] = graph

var iteration = 0
var numIter = 10
// var prevRankGraph: Graph[Int, Int] = null

while (iteration < numIter) {
    rankGraph.cache()

    val rankUpdates = rankGraph.aggregateMessages[Int](
        ctx => ctx.sendToDst(ctx.srcAttr), _ + _, TripletFields.Src)

    // prevRankGraph = rankGraph
    rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => msgSum
    }.cache()

    rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
    System.out.println(s"PowerMethod finished iteration $iteration.")
    // prevRankGraph.vertices.unpersist(false)
    // prevRankGraph.edges.unpersist(false)

    iteration += 1
    
}

rankGraph


 
