package main

import scala.io.StdIn
import scala.collection.mutable.Map
import all_partitioner.{SourceDataPartitioner,GenePartitioner}
import all_iterator.{SourceKVIterator,SubsetIterator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext

import scala.util.Random

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

    val one_num=(core_min.asInstanceOf[Double]*reduct_rate).asInstanceOf[Int]
    val population_size=5
    val geneList=new Array[(Int,Array[Int])](parallel_num*population_size)
    var geneList_counter=0

    //init all genes
    for(geneId<-0 to parallel_num-1){
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
        geneList.update(geneList_counter,(geneId,gene))
        geneList_counter+=1
      }
    }
    val indexMap=Map[Int,Array[(Int,Int)]]()
    var start=0
    var gap:Int=instance_num.intValue()/(core_min*parallel_num)
    println(gap)
    var end=start+gap-1
    for(geneId<-0 to parallel_num-1){
      val temp=new Array[(Int,Int)](core_min)
      for(i<-0 to core_min-1){
        temp.update(i,(start,end))
        if(geneId==2 && i==core_min-2){
          gap=instance_num.intValue()-1-end
        }
        start=end+1
        end=start+gap-1
      }
      indexMap(geneId)=temp
    }

    var geneRdd=sparkSession.sparkContext.parallelize(geneList).partitionBy(new GenePartitioner(parallel_num))
//      .foreach(x=>{
//        print("id:"+x._1+", data:")
//        for(i<-0 to x._2.length-1){
//          print(x._2(i)+",")
//        }
//        println()
//      })

    //enter the iteration
    for (current_iterIndex<-0 to iteration_num){
      //init part
      val fitness_array=new Array[Double](geneList.length)

      //starting ga
      val subsetRdd=geneRdd.mapPartitions{
        iter=>new SubsetIterator(iter,indexMap,cachedSourceData)
      }.foreach(x=>println(x))

//      赌轮盘挑选交配
//      生成下一代基因
//      delete data
    }

    //save file

  }
}
