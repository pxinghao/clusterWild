import org.apache.spark.graphx.Graph

/**
 * Created by xinghao on 3/13/15.
 */
class AuxiliaryFunctions {

  def setCenterAttrToNegativeID(graph: Graph[Int, Int]) : Graph[Int, Int] = {
    graph.mapVertices[Int]((vid, attr) => if (attr <= 0) -vid.toInt else attr)
  }

  def computeObjective(graph: Graph[Int, Int]) : Long = {
    val cChoose2 : Long = graph.aggregateMessages[Long](
      triplet => {
        if (triplet.srcAttr == triplet.dstId)
          triplet.sendToDst(1)
      },
      _ + _
    ).map(vc => vc._2 * (vc._2 + 1) / 2).reduce(_+_)

    val intraCluster : Long = graph.aggregateMessages[Long](
      triplet => {
        if (math.abs(triplet.srcAttr) == math.abs(triplet.dstAttr))
          triplet.sendToDst(1)
      },
      _ + _
    ).map(vc => vc._2).reduce(_+_) / 2

    val interCluster : Long = graph.aggregateMessages[Long](
      triplet => {
        if (math.abs(triplet.srcAttr) != math.abs(triplet.dstAttr))
          triplet.sendToDst(1)
      },
      _ + _
    ).map(vc => vc._2).reduce(_+_) / 2

    cChoose2 - intraCluster + interCluster
  }

}
