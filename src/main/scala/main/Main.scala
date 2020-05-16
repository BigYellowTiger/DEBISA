package main

import java.io.Serializable
import java.util
import java.util.concurrent.atomic.LongAccumulator

import scala.io.StdIn
import scala.collection.mutable.Map
import scala.collection.mutable.Seq
import all_partitioner.{GenePartitioner, SourceDataPartitioner}
import all_iterator.{CachedIterator, SourceKVIterator, SubsetIterator}
import all_accumulator.{FitnessAccumulator, PartNumAccumulator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
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
    //val fileData=sparkSession.read.parquet(fileName)
    val fileData = sparkSession
      .read.format("csv")
      .option("header","true")
      .load(fileName)
    var sourceSchema = Seq[String]()
    for(i <- 0 to fileData.schema.length-1){
      sourceSchema = sourceSchema :+ fileData.schema(i).name
    }
    val oriData=fileData.select(sourceSchema.map(name => fileData.col(name).cast(DoubleType)):_*)
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

    val cachedSourceData=oriData.rdd.zipWithIndex()
      .mapPartitions(it=>new SourceKVIterator(it))
      .partitionBy(new SourceDataPartitioner(instance_num,parallel_num))
      .mapPartitions(it=>new CachedIterator(it,TaskContext))
    cachedSourceData.foreach(x=>println(x))
      //.persist(StorageLevel.MEMORY_AND_DISK)

    val one_num=(core_min.asInstanceOf[Double]*reduct_rate).asInstanceOf[Int]
    val population_size=core_min
    val geneListMap=Map[Int,Array[Array[Int]]]()

    //init all genes
    println("gene start")
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
      partitionGeneList.foreach(x=>{
        x.foreach(xx=>print(xx+","))
        println()
      })
    }
    println("gene end")

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
    val fitnessAccumulator=new FitnessAccumulator(parallel_num)
    sparkSession.sparkContext.register(fitnessAccumulator)
    val SingleGaIterNum=1
    for(currentGaIterIndex<-1 to SingleGaIterNum){
      //1-nn，记录每条基因的适应度，每次从每个分区挑一条基因，组成当前要计算的基因组
      //每个分区挑选一条基因，开始1-nn
      val allGeneFitness=Map[Int,Seq[Double]]()
      for(allFitnessInitIndex<-0 to parallel_num-1){
        allGeneFitness(allFitnessInitIndex)=Seq[Double]()
      }
      for(i<-0 to core_min-1){
        val currentGeneList=Map[Int,Array[Int]]()
        val currentCondition=Map[Int,Array[(Int,Int)]]()
        for(tempKey<-0 to parallel_num-1){
          val tempGene=geneListMap(tempKey)(i)
          currentGeneList(tempKey)=tempGene
          var tempConditionSeq=Seq[(Int,Int)]()
          for(tempConditionIndex<-0 to tempGene.length-1){
            if(tempGene(tempConditionIndex)==1)
              tempConditionSeq=tempConditionSeq:+indexMap(tempKey)(tempConditionIndex)
          }
          currentCondition(tempKey)=tempConditionSeq.toArray
        }

        //1-nn
        val subSetRdd=cachedSourceData.filter(InsideFunction.belongToGene(currentCondition))
        println("filter start")
        subSetRdd.foreach(x=>println(x))
        println("filter end")
        val calculatedSubset=subSetRdd.combineByKey(InsideFunction.firstMeet
          ,InsideFunction.samePartMeet
          ,InsideFunction.crossPart)

        calculatedSubset.foreach(x=>println(x))
        //计算fitness
        fitnessAccumulator.reset()
        calculatedSubset.foreach(x=>{
          fitnessAccumulator.add(x)
        })
        fitnessAccumulator.value.foreach(x=>{
          print(x._1+": ")
          x._2.foreach(xx=>print(xx+","))
          println()
        })
        //记录fitness
        for(fitnessRecordPIndex<-0 to parallel_num-1){
          var tempFitnessSeq=allGeneFitness(fitnessRecordPIndex)
          tempFitnessSeq=tempFitnessSeq:+fitnessAccumulator
            .value(fitnessRecordPIndex)(0).toDouble/fitnessAccumulator.value(fitnessRecordPIndex)(1).toDouble
          allGeneFitness.update(fitnessRecordPIndex,tempFitnessSeq)
        }
      }
      /**
       * 采用chc算法的遗传准则，最优基因直接进入下一代，剩余基因进行交换时需要满足汉明距离阈值
       * 且前期不变异，当算法陷入收敛时，让某些基因产生大规模变异
       * */
      //最优基因直接进入下一代
      val bestGene=Map[Int,Array[Int]]()
      for(partitionPickIndex<-0 to parallel_num-1){
        val currentPartitionFitness=allGeneFitness(partitionPickIndex)
        var bestScore=0.0
        for(pickedGeneId<-0 to core_min-1){
          if(currentPartitionFitness(pickedGeneId)>bestScore){
            bestScore=currentPartitionFitness(pickedGeneId)
            bestGene.update(partitionPickIndex,geneListMap(partitionPickIndex)(pickedGeneId))
          }
        }
      }

      //进行基因交换

    }
  }
}
