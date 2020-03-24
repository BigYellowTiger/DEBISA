package all_iterator

import org.apache.spark.sql.Row

class SourceKVIterator(it:Iterator[(Row,Long)]) extends Iterator[(Long,Seq[Double])]{
  def hasNext():Boolean=it.hasNext

  def next():(Long,Seq[Double])={
    val temp=it.next()
    val key=temp._2
    val value=temp._1

    (key,value.toSeq.asInstanceOf[Seq[Double]])
  }
}
