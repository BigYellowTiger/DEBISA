package main

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map
import scala.util.control.Breaks

object GetAllSubset {
  def getSubset(inputRdd:RDD[(Long,Seq[Double])],conditionMap:Map[Int,Array[(Int,Int)]],taskContext:TaskContext.type ):RDD[(Long,Seq[Double])]={
    val resultRdd=inputRdd.filter(x=>{
      val currentConditionArray=conditionMap(TaskContext.getPartitionId())
      val breaker=new Breaks
      var result=false
      breaker.breakable{
        for(i<-0 to currentConditionArray.length-1){
          if(x._1.intValue()>=currentConditionArray(i)._1 && x._1.intValue()<=currentConditionArray(i)._2){
            result=true
            breaker.break()
          }
        }
      }
      result
    })
    resultRdd
  }
}
