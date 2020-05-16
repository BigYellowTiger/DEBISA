package test_code
import scala.collection.mutable.Seq
import scala.collection.mutable.Map
import scala.util.Random


object Test {
  def main(args: Array[String]): Unit = {
    val a=Map[Int,Array[Int]]()
    a(0)=new Array[Int](3)
    val temp=a(0)
    temp.update(0,3000)
    a.update(0,temp)
    a(0).foreach(x=>println(x))
  }
}
