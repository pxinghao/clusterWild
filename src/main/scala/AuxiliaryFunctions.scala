import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * Created by xinghao on 3/13/15.
 */
object AuxiliaryFunctions {

  def setZeroDegreeToCenter(graph: Graph[Int, Int], zeroDegreeID: Int, centerID: Int): Graph[Int, Int] = {
    graph.mapVertices[Int]((vid, attr) => if (attr == zeroDegreeID) centerID else attr)
  }

  def setCenterAttrToNegativeID(graph: Graph[Int, Int]): Graph[Int, Int] = {
    graph.mapVertices[Int]((vid, attr) => if (attr <= 0) -vid.toInt else attr)
  }

  def computeObjective(graph: Graph[Int, Int]): Long = {
    val g = setCenterAttrToNegativeID(graph)
    val cChoose2: Long = g.aggregateMessages[Long](
      triplet => {
        if (triplet.srcAttr == triplet.dstId)
          triplet.sendToDst(1)
      },
      _ + _
    ).map(vc => vc._2 * (vc._2 + 1) / 2).fold(0)(_ + _)

    val intraCluster: Long = g.aggregateMessages[Long](
      triplet => {
        if (math.abs(triplet.srcAttr) == math.abs(triplet.dstAttr))
          triplet.sendToDst(1)
      },
      _ + _
    ).map(vc => vc._2).fold(0)(_ + _) / 2

    val interCluster: Long = g.aggregateMessages[Long](
      triplet => {
        if (math.abs(triplet.srcAttr) != math.abs(triplet.dstAttr))
          triplet.sendToDst(1)
      },
      _ + _
    ).map(vc => vc._2).fold(0)(_ + _) / 2

    cChoose2 - intraCluster + interCluster
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = new SparkContext()

    /* Start with 1, obj = 2 */
    val vertexArray = Array(
      (1L, -999),
      (2L, 1),
      (3L, 1),
      (4L, -999),
      (5L, 1)
    )

    /* Start with 2, obj = 4 */
//    val vertexArray = Array(
//      (1L, 2),
//      (2L, -999),
//      (3L, -999),
//      (4L, 2),
//      (5L, 2)
//    )

    /* Start with 3, obj = 2 */
//    val vertexArray = Array(
//      (1L, 3),
//      (2L, -999),
//      (3L, -999),
//      (4L, 2),
//      (5L, 3)
//    )

    /* Start with 4, obj = 2 */
//    val vertexArray = Array(
//      (1L, 3),
//      (2L, 4),
//      (3L, -999),
//      (4L, -999),
//      (5L, 3)
//    )

    /* Start with 5, obj = 2 */
//    val vertexArray = Array(
//      (1L, 5),
//      (2L, 5),
//      (3L, 5),
//      (4L, -999),
//      (5L, -999)
//    )

    /* Every vertex in its own cluster, obj = 6 */
//    val vertexArray = Array(
//      (1L, -999),
//      (2L, -999),
//      (3L, -999),
//      (4L, -999),
//      (5L, -999)
//    )

    /* {{1,2,3}, {4}, {5}}, obj = 5 */
//    val vertexArray = Array(
//      (1L, -999),
//      (2L, 1),
//      (3L, 1),
//      (4L, -999),
//      (5L, -999)
//    )




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

    val vertexRDD: RDD[(Long,  Int)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    var graph: Graph[Int, Int] = Graph(vertexRDD, edgeRDD)

    val obj = AuxiliaryFunctions.computeObjective(graph)

    System.out.println(s"$obj")
  }

}
