package test_code
import main.CrossOverAndMutation

import scala.collection.mutable.Seq
import scala.collection.mutable.Map
import scala.util.Random


object Test {
  def main(args: Array[String]): Unit = {
    val parallel_num=2
    val p1Arr=Array(
      Array(1,0,1,0,1,0,1,0,0,1),
      Array(1,1,0,0,1,1,1,0,0,0),
      Array(0,0,0,1,1,1,1,1,0,0),
      Array(1,1,1,0,0,1,1,0,0,0))
    val p2Arr=Array(
      Array(1,1,1,1,1,0,0,0,0,0),
      Array(0,1,0,0,0,0,1,1,1,1),
      Array(0,0,0,1,1,0,0,1,1,1),
      Array(1,1,1,0,0,1,1,0,0,0))
    val allGeneList=Map(0->p1Arr,1->p2Arr)
    CrossOverAndMutation.mutation(allGeneList)
//    allGeneList.keySet.foreach(key=>{
//      println("part id "+key)
//      allGeneList(key).foreach(xx=>{
//        xx.foreach(xxx=>print(xxx+","))
//        println()
//      })
//    })
  }
}
