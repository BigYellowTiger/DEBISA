package main
import all_accumulator.PartNumAccumulator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Seq
import scala.collection.mutable.Map
import scala.util.control.Breaks

object InsideFunction {

  def countPartNum(inputRdd:RDD[(Int,(Long,Seq[Double]))], accumulator: PartNumAccumulator)={
    inputRdd.map(x=>{
      //println(x)
      accumulator.add(TaskContext.getPartitionId())
    }).count()
  }

  //计算1-nn所用函数
  //第一次遇见
  def firstMeet(x:(Long,Seq[Double])):Seq[((Long,Seq[Double]),(Double,Boolean))]={
    var result=Seq[((Long,Seq[Double]),(Double,Boolean))]()
    result=result:+(x,(Double.MaxValue,false))
    result
  }

  //第二次遇见
  def samePartMeet(c:Seq[((Long,Seq[Double]),(Double,Boolean))],x:(Long,Seq[Double]))
  :Seq[((Long,Seq[Double]),(Double,Boolean))]={
    //和已经遇到的点全比一遍距离，然后再把自己加进去
    var newCome=(x,(Double.MaxValue,false))
    for(index<-0 to c.size-1){
      //计算欧式距离，并在两个样本中比较是否是最近的点
      val ciData=c(index)._1._2
      val xData=x._2
      var disSquare=0.0
      for(i<-0 to xData.size-2){
        disSquare+=(ciData(i)-xData(i))*(ciData(i)-xData(i))
      }
      if(disSquare<c(index)._2._1){
        if(ciData(ciData.size-1)==xData(xData.size-1))
          c(index)=(c(index)._1,(disSquare,true))
        else
          c(index)=(c(index)._1,(disSquare,false))
      }
      if(disSquare<newCome._2._1){
        if(ciData(ciData.size-1)==xData(xData.size-1))
          newCome=(x,(disSquare,true))
        else
          newCome=(x,(disSquare,false))
      }
    }
    val result=c:+newCome
    result
  }

  //跨区遇见（按理说遇不见）
  def crossPart(c1:Seq[((Long,Seq[Double]),(Double,Boolean))]
                ,c2:Seq[((Long,Seq[Double]),(Double,Boolean))])
  :Seq[((Long,Seq[Double]),(Double,Boolean))]={
    var result=Seq[((Long,Seq[Double]),(Double,Boolean))]()
    result=result:+((250L,null),(250.0,false))
    result
  }

  //filter函数的
  def belongToGene(condition:Map[Int,Array[(Int,Int)]]):((Int,(Long,Seq[Double])))=>Boolean={
    (x)=>{
      val currentCondition=condition(x._1)
      val breaks=new Breaks
      var result=false
      breaks.breakable{
        currentCondition.foreach(c=>if(x._2._1.intValue()>=c._1&&x._2._1.intValue()<=c._2){
          result=true
          breaks.break()
        })
      }
      result
    }
  }
}
