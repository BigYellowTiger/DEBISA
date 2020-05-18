package all_iterator

import org.apache.spark.sql.Row

import scala.collection.mutable.Seq

class ToResultIterator(it:Iterator[(Int,(Long,Seq[Double]))]) extends Iterator[Row]{
  def hasNext():Boolean=it.hasNext

  def next():(Row)={
    val temp=it.next()._2._2
    Row.fromSeq(temp)
  }
}
