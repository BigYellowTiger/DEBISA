package main

import java.io.Serializable
import java.util

import scala.io.StdIn
import scala.collection.mutable.Map
import all_partitioner.{GenePartitioner, SourceDataPartitioner}
import all_iterator.{SourceKVIterator, SubsetIterator}
import all_accumulator.PartNumAccumulator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext
import org.apache.spark.util.LongAccumulator

import scala.util.Random
import scala.util.control.Breaks

object Main {
  def main(args: Array[String]): Unit = {
    print("Enter the file name:")
    val fileName=StdIn.readLine()
    print("Enter the reduct_rate:")
    val reduct_rate=StdIn.readDouble()
    print("Enter the core_min:")
    var core_min=StdIn.readInt()
    print("Enter the parallel_num:")
    val parallel_num=StdIn.readInt()
    print("Enter the result_num:")
    val result_num=StdIn.readInt()

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",parallel_num)
      .config("spark.shuffle.memoryFraction","0.3")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("kernel")
//      .master("spark://master:7077")
      .master("local")
      .getOrCreate()

    //计算迭代次数
    val fileData=sparkSession.read.parquet(fileName)
    var instance_num=fileData.count()
    var iteration_num_d:Double=0
    if(result_num/parallel_num<=core_min){
      val a=(core_min*parallel_num).asInstanceOf[Double]/instance_num.asInstanceOf[Double]
      iteration_num_d=Math.log(a)/Math.log(reduct_rate)
    }else{
      val a=result_num.asInstanceOf[Double]/instance_num.asInstanceOf[Double]
      iteration_num_d=Math.log(a)/Math.log(reduct_rate)
    }
    val iteration_num=iteration_num_d.asInstanceOf[Int]

    val cachedSourceData=fileData.rdd.zipWithIndex()
      .mapPartitions(it=>new SourceKVIterator(it))
      .partitionBy(new SourceDataPartitioner(instance_num,parallel_num)).persist(StorageLevel.MEMORY_AND_DISK)
    cachedSourceData.foreach(x=>println("pid: "+TaskContext.getPartitionId()+", "+x))

    val one_num=(core_min.asInstanceOf[Double]*reduct_rate).asInstanceOf[Int]
    val population_size=core_min
    val geneListMap=Map[Int,Array[Array[Int]]]()

    //init all genes
    for(geneId<-0 to parallel_num-1){
      val partitionGeneList=new Array[Array[Int]](population_size)
      for(pop<-0 to population_size-1){
        val pickedGeneRecorder=new Array[Boolean](core_min)
        val gene=new Array[Int](one_num)
        for(one_counter<-0 to one_num-1){
          var temp=0
          do{
            temp=Random.nextInt(core_min)
          }while(pickedGeneRecorder(temp))
          pickedGeneRecorder.update(temp,true)
          gene.update(one_counter,temp)
        }
        partitionGeneList.update(pop,gene)
      }
      geneListMap(geneId)=partitionGeneList
    }

    //获取每位基因对应的数据范围
    val indexMap=Map[Int,Array[(Int,Int)]]()
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

    //开始迭代
    for(iterIndex<-0 to iteration_num-1){
      //1-nn，记录每条基因的适应度，每次从每个分区挑一条基因，组成当前要计算的基因组

    }
  }
}
