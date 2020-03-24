package datapre

import scala.io.StdIn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

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

    val result=fileData.write.mode(SaveMode.Overwrite).parquet(savedName)
    val test=sparkSession.read.parquet(savedName)
    test.printSchema()
    test.show(1)
  }
}
