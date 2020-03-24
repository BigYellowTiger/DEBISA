package test_code

import scala.io.StdIn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.storage.StorageLevel

object TestSpark {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",1)
      .config("spark.shuffle.memoryFraction","0.3")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("kernel")
      //      .master("spark://master:7077")
      .master("local")
      .getOrCreate()

    val testSeq=Seq(Seq("A","b","c"),Seq("c","a","c"),Seq("A","dd","c"))
    val testData=sparkSession.sparkContext.makeRDD(testSeq)
    val d2=testData.zipWithIndex()
    d2.foreach(x=>print(x))
  }
}
