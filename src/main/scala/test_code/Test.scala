package test_code

import scala.util.Random
import scala.collection.mutable.Map

object Test {
  def main(args: Array[String]): Unit = {
    val instance_num=100
    val core_min=10
    val parallel_num=3
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

    for(i<-0 to parallel_num-1){
      print("id:"+i+", data:")
      for(j<-0 to indexMap(i).length-1){
        print(indexMap(i)(j)+",")
      }
      println()
    }
  }
}
