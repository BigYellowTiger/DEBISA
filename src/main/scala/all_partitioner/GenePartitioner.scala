package all_partitioner

import org.apache.spark.Partitioner
import org.apache.spark.sql.Row

import scala.collection.mutable.HashMap

class GenePartitioner(parallel_num:Int) extends Partitioner {
  override def numPartitions: Int = parallel_num

  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }
}
