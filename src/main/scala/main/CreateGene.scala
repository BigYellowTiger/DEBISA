package main
import all_accumulator.PartNumAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{Map,Seq}
import scala.util.Random

object CreateGene {
  def createGene(parallel_num:Int,population_size:Int,one_num:Int,core_min:Int,geneListMap:Map[Int,Array[Array[Int]]])={
    for(geneId<-0 to parallel_num-1){
      val partitionGeneList=new Array[Array[Int]](population_size)
      for(pop<-0 to population_size-1){
        val pickedGeneRecorder=new Array[Boolean](core_min)
        val gene=new Array[Int](core_min)
        for(one_counter<-0 to one_num-1){
          var temp=0
          do{
            temp=Random.nextInt(core_min)
          }while(pickedGeneRecorder(temp))
          pickedGeneRecorder.update(temp,true)
          gene.update(temp,1)
        }
        partitionGeneList.update(pop,gene)
      }
      geneListMap(geneId)=partitionGeneList
//      partitionGeneList.foreach(x=>{
////        x.foreach(xx=>print(xx+","))
//        println()
//      })
    }
  }

  def createGeneIndexMap(parallel_num:Int,core_min:Int,sparkSession: SparkSession,cachedSourceData:RDD[(Int,(Long,Seq[Double]))],indexMap:Map[Int,Array[(Int,Int)]])={
    val allPartNum=new Array[Int](parallel_num)
    val acc=new PartNumAccumulator(allPartNum)
    sparkSession.sparkContext.register(acc)
    InsideFunction.countPartNum(cachedSourceData,acc)

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
