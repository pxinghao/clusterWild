import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by xinghao on 3/10/15.
 */


class TruncatedLineageRDD[T: ClassTag](rdd: RDD[T]) extends RDD[T](rdd.context, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    rdd.iterator(split, context)

  override def getPartitions: Array[Partition] = rdd.partitions

  override val partitioner = rdd.partitioner
}
