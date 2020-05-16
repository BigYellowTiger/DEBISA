package all_iterator

import org.apache.spark.TaskContext
import scala.collection.mutable.Seq
import org.apache.spark.sql.Row

class CachedIterator(it:Iterator[(Long,Seq[Double])],tk:TaskContext.type ) extends Iterator[(Int,(Long,Seq[Double]))]{
  def hasNext():Boolean=it.hasNext

  def next():(Int,(Long,Seq[Double]))={
    val temp=it.next()
    val key=tk.getPartitionId()
    val value=temp

    (key,value)
  }
}
