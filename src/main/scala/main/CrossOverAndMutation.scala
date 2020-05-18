package main

import scala.collection.mutable.{Map, Seq}
import scala.util.Random

object CrossOverAndMutation {
  def crossOver(allGeneList:Map[Int,Array[Array[Int]]],allFitness:Map[Int,Seq[Double]],bestGene:Map[Int,Array[Int]],repeatCounter:Map[Int,Int])={
    //对于高准确率子集，需要有一个指数级的概率增长
    //构建指数分布赌轮盘，e^(fitness*10)
    for(partId<-0 to allFitness.keySet.size-1){
      val currentGenePartition=allGeneList(partId)
      val currentFitnessPartition=allFitness(partId)
      var start=0.0;
      var fitness_sum=0.0;
      val roulette=new Array[(Double,Double)](currentGenePartition.length)
      for(i<-0 to roulette.length-1){
        val range=math.exp(currentFitnessPartition(i))
        roulette.update(i,(start,start+range))
        start+=range
        fitness_sum+=range
      }

      //挑基因
      var newGenePartitionSeq=Seq[Array[Int]]()
      for(pickedTimes<-1 to currentGenePartition.length/2-1){
        val r1=Random.nextInt((fitness_sum).toInt)
        val r2=Random.nextInt((fitness_sum).toInt)
        var picked1 = -1
        var picked2 = -1
        for(i<-0 to roulette.length-1){
          val currentRou=roulette(i)
          var pickHappened=false
          if(r1>=currentRou._1&&r1<currentRou._2) {
            picked1=i
            pickHappened=true
          }
          if(r2>=currentRou._1&&r2<currentRou._2) {
            picked2=i
            pickHappened=true
          }
          if(picked1==picked2&&pickHappened){
            if(picked1==roulette.length-1){
              picked2=picked1-1
            }else{
              picked2=picked1+1
            }
          }
        }

        //计算汉明距离，不满足就往后选一个基因，如连续3次不满足停止重选，如单轮交换中出现12次重选，则认为算法进入收敛态
        //汉明距离的阈值为40%
        val pickedGeneList1=currentGenePartition(picked1)
        val pickedGeneList2=currentGenePartition(picked2)
        var hammingCalFlag=true
        var hammingReCounter=0
        while (hammingCalFlag){
          var hammingDis=0
          for(i<-0 to pickedGeneList1.length-1){
            if(pickedGeneList1(i)!=pickedGeneList2(i))
              hammingDis+=1
          }
          //重选，谁适应度小重选谁
          if(hammingDis<currentGenePartition.length*0.3&&hammingReCounter<3){
            hammingReCounter+=1
            repeatCounter.update(partId,repeatCounter(partId)+1)
            var tempFlag=true
            while (tempFlag){
              if(currentFitnessPartition(picked1)>currentFitnessPartition(picked2)){
                picked2=Random.nextInt(currentGenePartition.length)
                if(picked2!=picked1){
                  tempFlag=false
                }
              }else{
                picked1=Random.nextInt(currentGenePartition.length)
                if(picked2!=picked1){
                  tempFlag=false
                }
              }
            }
          }else
            hammingCalFlag=false
        }

        //crossover阶段，交换长度三分之一，再在其他位置对齐
        val crossStart=Random.nextInt(currentGenePartition(0).length*2/3)
        val crossEnd=crossStart+currentGenePartition(0).length/3
        /****************/
        println("part id "+partId)
        println("picked 1")
        pickedGeneList1.foreach(x=>print(x+","))
        println()
        println("picked 2")
        pickedGeneList2.foreach(x=>print(x+","))
        println()
        println("cross start: "+crossStart)
        println("cross end: "+crossEnd)
        println("--------------------------")
        /****************/
        var newGene1=new Array[Int](pickedGeneList1.length)
        var newGene2=new Array[Int](pickedGeneList1.length)
        //交换，记录换到new1的1比new1原来的1多多少
        var new1Extra=0
        for(i<-crossStart to crossEnd){
          val tempOri1=pickedGeneList1(i)
          val tempOri2=pickedGeneList2(i)
          if(tempOri1==1&&tempOri2==0){
            new1Extra-=1
          }
          else if(tempOri1==0&&tempOri2==1){
            new1Extra+=1
          }
          newGene1.update(i,pickedGeneList2(i))
          newGene2.update(i,pickedGeneList1(i))
        }
        //对齐
        for(i<-0 to crossStart-1){
          val tempOri1=pickedGeneList1(i)
          val tempOri2=pickedGeneList2(i)
          if(new1Extra<0){
            if(tempOri1==0&&tempOri2==1){
              newGene1.update(i,tempOri2)
              newGene2.update(i,tempOri1)
              new1Extra+=1
            }else{
              newGene1.update(i,tempOri1)
              newGene2.update(i,tempOri2)
            }
          }
          else if(new1Extra>0){
            if(tempOri1==1&&tempOri2==0){
              newGene1.update(i,tempOri2)
              newGene2.update(i,tempOri1)
              new1Extra-=1
            }else{
              newGene1.update(i,tempOri1)
              newGene2.update(i,tempOri2)
            }
          }else{
            newGene1.update(i,tempOri1)
            newGene2.update(i,tempOri2)
          }
        }
        for(i<-crossEnd+1 to newGene1.length-1){
          val tempOri1=pickedGeneList1(i)
          val tempOri2=pickedGeneList2(i)
          if(new1Extra<0){
            if(tempOri1==0&&tempOri2==1){
              newGene1.update(i,tempOri2)
              newGene2.update(i,tempOri1)
              new1Extra+=1
            }else{
              newGene1.update(i,tempOri1)
              newGene2.update(i,tempOri2)
            }
          }
          else if(new1Extra>0){
            if(tempOri1==1&&tempOri2==0){
              newGene1.update(i,tempOri2)
              newGene2.update(i,tempOri1)
              new1Extra-=1
            }else{
              newGene1.update(i,tempOri1)
              newGene2.update(i,tempOri2)
            }
          }else{
            newGene1.update(i,tempOri1)
            newGene2.update(i,tempOri2)
          }
        }

        //将交换完成的基因放入新基因库中
        newGenePartitionSeq=newGenePartitionSeq:+newGene1
        newGenePartitionSeq=newGenePartitionSeq:+newGene2
      }
      newGenePartitionSeq=newGenePartitionSeq:+bestGene(partId)
      allGeneList.update(partId,newGenePartitionSeq.toArray)
    }
  }

  def mutation(allGeneList:Map[Int,Array[Array[Int]]])={
    //除了最优秀的，都发生四分之一位的变异，并在其他位置对齐，最优秀的在最后一个
    allGeneList.keySet.foreach(key=>{
      val currentGenePart=allGeneList(key)
      for(i<-0 to currentGenePart.length-2){
        val currentGene=currentGenePart(i)
        val mutStart=Random.nextInt(currentGene.length*3/4)
        val mutEnd=mutStart+currentGene.length/4
        /*******************/
//        println("---------------------")
//        println("start "+mutStart)
//        println("end "+mutEnd)
//        print("未发生变异前：")
//        currentGene.foreach(x=>print(x+","))
//        println()
        /*******************/

        var oneCounter=0
        for(mutBitIndex<-mutStart to mutEnd){
          if(currentGene(mutBitIndex)==1) {
            currentGene.update(mutBitIndex,0)
            oneCounter-=1
          } else if(currentGene(mutBitIndex)==0) {
            currentGene.update(mutBitIndex,1)
            oneCounter+=1
          }
        }
        /*******************/
//        print("未对齐的变异：")
//        currentGene.foreach(x=>print(x+","))
//        println()
        /*******************/

        //对齐
        var alignCounter=0
        while (oneCounter>0){
          if(currentGene(alignCounter)==1){
            currentGene.update(alignCounter,0)
            oneCounter-=1
          }
          if(alignCounter==mutStart-1){
            alignCounter=mutEnd+1
          }else
            alignCounter+=1
        }
        while (oneCounter<0){
          if(currentGene(alignCounter)==0){
            currentGene.update(alignCounter,1)
            oneCounter+=1
          }
          if(alignCounter==mutStart-1){
            alignCounter=mutEnd+1
          }else
            alignCounter+=1
        }

        /*******************/
//        print("对齐后的变异：")
//        currentGene.foreach(x=>print(x+","))
//        println()
        /*******************/
      }
    })
  }
}
