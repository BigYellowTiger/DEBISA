import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TMain {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",10)
      .config("spark.shuffle.memoryFraction","0.3")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("instance_selection")
      .master("local[*]")
      .getOrCreate()


    val fileName = "C:/Users/qly/Desktop/100_5.csv"
    val ori_data = sparkSession.read.format("csv").option("header","true").load(fileName)
    val samp1 = ori_data.sample(false, 0.1)
    samp1.show(10)

    val samp2 = ori_data.sample(false, 0.1)
    samp2.show(10)

    val samp3 = ori_data.sample(false, 0.1)
    samp3.show(10)
  }
}
