package test_code

import org.apache.spark.TaskContext

import scala.collection.mutable
import scala.util.Random
import scala.collection.mutable.Map

object Test {
  def main(args: Array[String]): Unit = {
    val parallel_num=3
    val core_min=10
    val allPartNum=mutable.Map[Int,Int]()
    allPartNum(0)=34
    allPartNum(1)=33
    allPartNum(2)=33
    val indexMap=mutable.Map[Int,Array[(Int,Int)]]()
    for(geneId<-0 to parallel_num-1){
      var remainedGroupNum=core_min
      var remainedDataNum=allPartNum(geneId)
      var start=0
      for(i<-0 to geneId-1){
        start+=allPartNum(i)
      }
      val tempIndexArray=new Array[(Int,Int)](core_min)
      for(i<-0 to tempIndexArray.length-1){
        tempIndexArray.update(i,(start,start+remainedDataNum/remainedGroupNum-1))
        start=start+remainedDataNum/remainedGroupNum
        remainedDataNum-=remainedDataNum/remainedGroupNum
        remainedGroupNum-=1
      }
      indexMap(geneId)=tempIndexArray
    }
  }
}
