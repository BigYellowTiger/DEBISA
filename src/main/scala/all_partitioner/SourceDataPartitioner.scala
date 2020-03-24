package all_partitioner

import org.apache.spark.Partitioner
import org.apache.spark.sql.Row

import scala.collection.mutable.HashMap

class SourceDataPartitioner(instance_num:Long,parallel_num:Int) extends Partitioner {
  override def numPartitions: Int = parallel_num

  override def getPartition(key: Any): Int = {
    val gap=instance_num.asInstanceOf[Double]/parallel_num.asInstanceOf[Double]
    val keys=key.asInstanceOf[Long].intValue().asInstanceOf[Double]
    (keys/gap).asInstanceOf[Int]
  }
}
