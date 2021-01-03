import scala.io.Source
object Test {
  def main(args: Array[String]): Unit = {
    println(Source.fromFile("C:\\Users\\qly\\Desktop\\scalaIO.txt").getLines().next())
  }
}
