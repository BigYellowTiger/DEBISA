package datapre

import scala.io.StdIn
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

object CsvToParquet {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",3)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("CsvToParquet")
      .master("spark://master:7077")
      .getOrCreate()

    print("enter readed file path and name:")
    val fileName=StdIn.readLine()
    print("enter saved file path and name:")
    val savedName=StdIn.readLine()

    val fileData = sparkSession
      .read.format("csv")
      .option("header","true")
      .load(fileName)
    fileData.printSchema()
    println(fileData.count())

    //打乱数据s
    import sparkSession.implicits._
    val kv=fileData.withColumn("rand",col=fileData("doors")*Random.nextInt())
    val sortKv=kv.orderBy("rand")
    val sorted=sortKv.drop("rand")

    val result=sorted.write.mode(SaveMode.Overwrite).parquet(savedName)
    val test=sparkSession.read.parquet(savedName)
    test.printSchema()
    test.show(1)
  }
}
