package main

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
    print("Enter the file name:")
//    val inputFile = Source.fromFile("/public/home/hpc182212046/single_spark/input_file")
//    val fileName = inputFile.getLines().next()

    val fileName = "/isData/20_1000000.csv"

//    print("Enter the reduct_rate:")
//    val reduct_rate=StdIn.readDouble()
    val reduct_rate = 0.5
//    print("Enter the core_min:")
//    var core_min=StdIn.readInt()
    var core_min=40
//    print("Enter the parallel_num:")
//    val parallel_num=StdIn.readInt()
    val parallel_num = 20
//    print("Enter the result_num:")
//    val result_num=StdIn.readInt()
    val result_num = 200

    Logger.getLogger("org").setLevel(Level.ERROR)
    // 在中南大学hpc上读取master位置
//    val master_name = Source.fromFile("/public/home/hpc182212046/test_slurm/spark_on_slurm/echo_test").getLines().next()
//    println("master name is "+master_name)
    val sparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",parallel_num)
      .config("spark.shuffle.memoryFraction","0.3")
//      .config("spark.driver.memory","39g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("instance_selection")
      .master("spark://master:7077")
//      .master(master_name)
//      .master("local[*]")
      .getOrCreate()

    //计算迭代次数
//    val fileData=sparkSession.read.parquet(fileName)
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
    var iteration_num=iteration_num_d.asInstanceOf[Int]

    val cachedSourceData=oriData.rdd.zipWithIndex()
      .mapPartitions(it=>new SourceKVIterator(it))
      .partitionBy(new SourceDataPartitioner(instance_num,parallel_num))
      .mapPartitions(it=>new CachedIterator(it,TaskContext))
    cachedSourceData.cache()
    println("初始样本数："+cachedSourceData.count())
    //cachedSourceData.foreach(x=>println(x))
      //.persist(StorageLevel.MEMORY_AND_DISK)

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
    val singleGaIterNum=1
    iteration_num=3
    for(iterIndex<-1 to iteration_num){
      //init all genes
      println("第"+iterIndex.toString+"次迭代")
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
          // 如果这一基因所代表的数据子集超过parallel_num*5000个样本，则进行采样，否则直接计算适应度，采样方式为每个种群随机选5000个样本用于计算
          val max_cal_instance_num = 2000
          val fitness_sample_fraction = max_cal_instance_num / ((instance_num / parallel_num) * math.pow(reduct_rate, iterIndex))
          if (fitness_sample_fraction <1) {
            subSetRdd.cache()
            val sample_times = 10
            val sampleFitnessRecorder = Map[Int,Seq[Double]]()
            for(sampleFitnessInitIndex <-0 to parallel_num) {
              sampleFitnessRecorder(sampleFitnessInitIndex) = Seq[Double]()
            }
            for(sampleIndex <-0 to sample_times-1) {
              val sampledRdd = subSetRdd.sample(false, fitness_sample_fraction)
              val calculatedSubset=sampledRdd.combineByKey(
                InsideFunction.firstMeet
                ,InsideFunction.samePartMeet
                ,InsideFunction.crossPart)

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
              println("完成一次采样适应度计算")
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
            println("第"+i.toString+"次适应度计算完成")
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
            println("第"+i.toString+"次适应度计算完成")
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
        val mutationThreshold=12
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
        }
      }

      if(iterIndex==iteration_num){
        val structField=new Array[StructField](sourceSchema.length)
        for(tempI<-0 to sourceSchema.length-1){
          structField.update(tempI,StructField(sourceSchema(tempI),DoubleType,false))
        }
        val structType=StructType(structField)
        val finallyRdd=currentGaDataSet.mapPartitions(it=>new ToResultIterator(it))
        val savedName="/isData/result"
        sparkSession.createDataFrame(finallyRdd,structType).write.mode(SaveMode.Overwrite).csv(savedName)
      }
    }
  }
}
