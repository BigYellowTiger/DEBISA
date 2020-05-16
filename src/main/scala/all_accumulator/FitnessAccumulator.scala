package all_accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{Map, Seq}

class FitnessAccumulator(parallel_num:Int) extends AccumulatorV2[(Int,Seq[((Long,Seq[Double]),(Double,Boolean))]),Map[Int,Array[Int]]]{
  var fitnessRecorder:Map[Int,Array[Int]]=Map[Int,Array[Int]]()

  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[(Int,Seq[((Long,Seq[Double]),(Double,Boolean))]),Map[Int,Array[Int]]] = new FitnessAccumulator(parallel_num)

  override def reset(): Unit = {
    for(i<-0 to parallel_num-1)
      fitnessRecorder.update(i,new Array[Int](2))
  }

  override def add(v: (Int,Seq[((Long,Seq[Double]),(Double,Boolean))])): Unit = {
    val tempRecorder=fitnessRecorder(v._1)
    //数组第0位记录true的个数，第一位记录总个数
    v._2.foreach(x=>{
      tempRecorder.update(1,tempRecorder(1)+1)
      if(x._2._2)
        tempRecorder.update(0,tempRecorder(0)+1)
    })
    fitnessRecorder.update(v._1,tempRecorder)
    val temppppp=1
  }

  override def merge(other: AccumulatorV2[(Int,Seq[((Long,Seq[Double]),(Double,Boolean))]),Map[Int,Array[Int]]]): Unit = {
    for(i<-0 to parallel_num-1){
      if(other.value(i)(1)!=0){
        fitnessRecorder.update(i,other.value(i))
      }
    }
  }

  override def value: Map[Int,Array[Int]] = fitnessRecorder
}
