package all_accumulator

import org.apache.spark.util.AccumulatorV2

class PartNumAccumulator(var counter:Array[Int]) extends AccumulatorV2[Int,Array[Int]] {

  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[Int, Array[Int]] = new PartNumAccumulator(counter.clone())

  override def reset(): Unit = {
    counter=new Array[Int](counter.length)
  }

  override def add(v: Int): Unit = {
    counter.update(v,counter(v)+1)
  }

  override def merge(other: AccumulatorV2[Int, Array[Int]]): Unit = {
    for(i<-0 to counter.length-1){
      counter.update(i,other.value(i)+counter(i))
    }
  }

  override def value: Array[Int] = counter
}
