package ttt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main_test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.shuffle.memoryFraction","0.3")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("instance_selection")
      .master("spark://master:7077")
      .getOrCreate()
    val fileData = sparkSession
      .read.format("csv")
      .option("header","true")
      .load("/isData/test.csv")
    fileData.show()
  }
}
