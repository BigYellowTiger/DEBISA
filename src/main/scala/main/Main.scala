package main

import java.util.Date

import scala.io.StdIn
import scala.io.Source
import scala.collection.mutable.Map
import scala.collection.mutable.Seq
import all_partitioner.{GenePartitioner, SourceDataPartitioner}
import all_iterator.{CachedIterator, SourceKVIterator, SubsetIterator, ToResultIterator}
import all_accumulator.{FitnessAccumulator, PartNumAccumulator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext
import org.apache.spark.util.LongAccumulator

import scala.util.Random
import scala.util.control.Breaks

object Main {
  def main(args: Array[String]): Unit = {
    println("start instance selection")
    val start_time = new Date().getTime
    val filepath = args(0)
    val fileName_withoutPath = args(1)
    val fileName = filepath + fileName_withoutPath
    val resultPath = args(2)
    val reduct_rate = args(3).toDouble
    println("reduct_rate="+reduct_rate)
    var core_min=args(4).toInt
    println("core_min="+core_min)
    val parallel_num = args(5).toInt
    println("parallel_num="+parallel_num)
    val iteration_num = args(6).toInt
    println("iteration_num="+iteration_num)
    val singleGaIterNum=args(7).toInt
    println("singe_iter_num="+singleGaIterNum)
//    val max_cal_instance_num = 2000 // 单次采样适应度计算每个种群的最大样本数
    val sample_times = args(8).toInt
    println("sample_times="+sample_times)
    var fitness_sample_fraction = args(9).toDouble // 小于10w的数据集不采样算适应度
    println("sample fraction="+fitness_sample_fraction)
    val mutationThreshold=args(10).toInt // 发生多少次父母基因汉明距离过短就发生变异
    val stop_sample_in_small_dataset = args(11).toBoolean
    val schema_read_on = args(12)
    val resultPostfix = "_"+core_min.toString+"len_"+parallel_num+"p_"+iteration_num+"it_"+sample_times+"samp_00"+(fitness_sample_fraction*1000).toInt+"samp"

    println("当前计算的数据集为："+fileName_withoutPath)
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",parallel_num)
      .config("spark.shuffle.memoryFraction","0.3")
      .config("spark.driver.memory","39g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("instance_selection")
//      .master("spark://master:7077")
//      .master(master_name)
      .master("local[*]")
      .getOrCreate()
    // 打印核心使用数
    println("executor数为："+sparkSession.sparkContext.getExecutorMemoryStatus.toArray.length)
    println("core数："+java.lang.Runtime.getRuntime.availableProcessors)

    //计算迭代次数
//    val fileData=sparkSession.read.parquet(fileName)
    val fileData = sparkSession
      .read.format("csv")
      .option("header",schema_read_on)
      .load(fileName)
    var sourceSchema = Seq[String]()
    for(i <- 0 to fileData.schema.length-1){
      sourceSchema = sourceSchema :+ fileData.schema(i).name
    }

    val oriData=fileData.select(sourceSchema.map(name => fileData.col(name).cast(DoubleType)):_*)
    var instance_num=fileData.count()
    if (instance_num < 100000 && stop_sample_in_small_dataset){
      fitness_sample_fraction = 100
    }

    val cachedSourceData=oriData.rdd.zipWithIndex()
      .mapPartitions(it=>new SourceKVIterator(it))
      .partitionBy(new SourceDataPartitioner(instance_num,parallel_num))
      .mapPartitions(it=>new CachedIterator(it,TaskContext))
    cachedSourceData.cache()
    println("初始样本数："+cachedSourceData.count())

    val one_num=(core_min.asInstanceOf[Double]*reduct_rate).asInstanceOf[Int]
    val population_size=core_min
    val geneListMap=Map[Int,Array[Array[Int]]]()
    val indexMap=Map[Int,Array[(Int,Int)]]()

    //初始化每位基因对应的数据范围
    CreateGene.createGeneIndexMap(parallel_num,core_min,sparkSession,cachedSourceData,indexMap)

    //开始迭代
    var currentGaDataSet=cachedSourceData
    val fitnessAccumulator=new FitnessAccumulator(parallel_num)
    sparkSession.sparkContext.register(fitnessAccumulator)
    for(iterIndex<-1 to iteration_num){
      //init all genes
      println("开始第"+iterIndex.toString+"次遗传算法")
      println("本轮遗传算法数据集包含"+currentGaDataSet.count()+"个样本")
      CreateGene.createGene(parallel_num,population_size,one_num,core_min,geneListMap)
      println("基因初始化完成")

      for(currentGaIterIndex<-1 to singleGaIterNum){
        //根据上一次遗传算法的结果，选出本轮遗传算法所需的初始数据集
        //每个分区挑选一条基因，开始1-nn
        val allGeneFitness=Map[Int,Seq[Double]]()
        for(allFitnessInitIndex<-0 to parallel_num-1){
          allGeneFitness(allFitnessInitIndex)=Seq[Double]()
        }

        //开始对每条基因进行适应度评价
        println("第"+iterIndex+"次遗传算法的第"+currentGaIterIndex+"次迭代的适应度计算开始")
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
          val subSetRdd=currentGaDataSet.filter(InsideFunction.belongToGene(currentCondition))
          if (fitness_sample_fraction <1) {
            subSetRdd.cache()
            val sampleFitnessRecorder = Map[Int,Seq[Double]]()
            for(sampleFitnessInitIndex <-0 to parallel_num-1) {
              sampleFitnessRecorder(sampleFitnessInitIndex) = Seq[Double]()
            }
            for(sampleIndex <-0 to sample_times-1) {
              val sampledRdd = subSetRdd.sample(false, fitness_sample_fraction)
              val calculatedSubset=sampledRdd.combineByKey(
                InsideFunction.firstMeet
                ,InsideFunction.samePartMeet
                ,InsideFunction.crossPart)
//              println("完成采样后的适应度计算RDD")

              fitnessAccumulator.reset()
              calculatedSubset.foreach(x=>{
                fitnessAccumulator.add(x)
              })
              for(curr_sample_record_index <-0 to parallel_num-1){
                var tempFitnessSeq = sampleFitnessRecorder(curr_sample_record_index)
                tempFitnessSeq = tempFitnessSeq :+ fitnessAccumulator
                  .value(curr_sample_record_index)(0).toDouble/fitnessAccumulator.value(curr_sample_record_index)(1).toDouble
                sampleFitnessRecorder.update(curr_sample_record_index,tempFitnessSeq)
              }
            }
            subSetRdd.unpersist()

            //记录fitness
            for(fitnessRecordPIndex<-0 to parallel_num-1){
              var tempFitnessSeq=allGeneFitness(fitnessRecordPIndex)
              val allSampleResult = sampleFitnessRecorder(fitnessRecordPIndex)
              var sum_fitness = 0.0
              for(obj <- allSampleResult){
                sum_fitness += obj
              }
              tempFitnessSeq = tempFitnessSeq :+ sum_fitness / sample_times
              allGeneFitness.update(fitnessRecordPIndex,tempFitnessSeq)
            }
          } else {
            val calculatedSubset=subSetRdd.combineByKey(
              InsideFunction.firstMeet
              ,InsideFunction.samePartMeet
              ,InsideFunction.crossPart)

            //计算fitness
            fitnessAccumulator.reset()
            calculatedSubset.foreach(x=>{
              fitnessAccumulator.add(x)
            })

            //记录fitness
            for(fitnessRecordPIndex<-0 to parallel_num-1){
              var tempFitnessSeq=allGeneFitness(fitnessRecordPIndex)
              tempFitnessSeq=tempFitnessSeq:+fitnessAccumulator
                .value(fitnessRecordPIndex)(0).toDouble/fitnessAccumulator.value(fitnessRecordPIndex)(1).toDouble
              allGeneFitness.update(fitnessRecordPIndex,tempFitnessSeq)
            }
          }
        }

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
        val rePickCounter=Map[Int,Int]()
        for(rePickIndex<-0 to parallel_num-1){
          rePickCounter.update(rePickIndex,0)
        }
        if(currentGaIterIndex!=singleGaIterNum)
          CrossOverAndMutation.crossOver(geneListMap,allGeneFitness,bestGene,rePickCounter)

        //变异
        for(rePickCounterIndex<-0 to parallel_num-1){
          if(rePickCounter(rePickCounterIndex)>=mutationThreshold||currentGaIterIndex==singleGaIterNum*4/5){
            CrossOverAndMutation.mutation(geneListMap)
          }
        }
        //完成当前ga最后一轮进化，让最优基因所代表的样本子集进入下一轮迭代，并改写indexMap的映射
        if(currentGaIterIndex==singleGaIterNum){
          //找到适应度最高的基因
          val finallyBestGene=bestGene

          //获取最好基因对应的映射
          val finallyBestCondition=Map[Int,Array[(Int,Int)]]()
          for(partIndex<-0 to parallel_num-1){
            var tempSeq=Seq[(Int,Int)]()
            val curBestGene=finallyBestGene(partIndex)
            for(tempIndex<-0 to curBestGene.length-1){
              if(curBestGene(tempIndex)==1){
                tempSeq=tempSeq:+indexMap(partIndex)(tempIndex)
              }
            }
            finallyBestCondition.update(partIndex,tempSeq.toArray)
          }
          //获取最好基因对应的数据子集
          currentGaDataSet=cachedSourceData.filter(InsideFunction.belongToGene(finallyBestCondition))

          //重构映射表，将bestCondition里每个区间拆成两个即可
          for(partI<-0 to parallel_num-1){
            var tempSeq=Seq[(Int,Int)]()
            val currentCondition=finallyBestCondition(partI)
            currentCondition.foreach(x=>{
              val mid=x._1+(x._2-x._1)/2
              tempSeq=tempSeq:+(x._1,mid)
              tempSeq=tempSeq:+(mid,x._2)
            })
            indexMap.update(partI,tempSeq.toArray)
          }

          // 统计这一次遗传算法运行了多久
          val exeTime = new Date().getTime
          println("本次遗传算法运行时间为: "+(exeTime-start_time)/1000)
        }
      }

      if(iterIndex==iteration_num){
        val endTime = new Date().getTime
        println("算法整体运行时间为: "+(endTime-start_time)/1000)
        val structField=new Array[StructField](sourceSchema.length)
        for(tempI<-0 to sourceSchema.length-1){
          structField.update(tempI,StructField(sourceSchema(tempI),DoubleType,false))
        }
        val structType=StructType(structField)
        val finallyRdd=currentGaDataSet.mapPartitions(it=>new ToResultIterator(it)).repartition(1)
        val savedName=resultPath+fileName_withoutPath+resultPostfix
        sparkSession.createDataFrame(finallyRdd,structType).write.mode(SaveMode.Overwrite).csv(savedName)
      }
    }
  }
}
