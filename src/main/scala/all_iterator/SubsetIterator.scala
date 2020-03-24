package all_iterator

import java.util

import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class SubsetIterator(it:Iterator[(Int,Array[Int])]
                     ,indexMap:Map[Int,Array[(Int,Int)]]
                     ,soureceDataRdd:RDD[(Long,Seq[Double])]) extends Iterator[RDD[(Long,Seq[Double])]]{
  def hasNext():Boolean=it.hasNext

  def next():RDD[(Long,Seq[Double])]={
    val temp=it.next()
    val currentGene_id=temp._1
    val currentGene=temp._2
    val current_indexMapArr=indexMap(currentGene_id)
    val current_rangeList=new util.ArrayList[(Int,Int)]()
    for(i<-0 to currentGene.length-1){
      current_rangeList.add(current_indexMapArr(currentGene(i)))
    }

    soureceDataRdd.filter(x=>{
      for(i<-0 to current_rangeList.size()-1){
        if(x._1>=current_rangeList.get(i)._1&&x._1<=current_rangeList.get(i)._2){
          true
        }
      }
      false
    })
  }
}
