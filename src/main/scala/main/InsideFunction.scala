package main
import all_accumulator.PartNumAccumulator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object InsideFunction {

  def countPartNum(inputRdd:RDD[(Long,Seq[Double])], accumulator: PartNumAccumulator)={
    inputRdd.map(x=>{
      //println(x)
      accumulator.add(TaskContext.getPartitionId())
    }).count()
  }
}
