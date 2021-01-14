
import scala.io.Source
import scala.util.Random
object Test {
  def main(args: Array[String]): Unit = {
    val fitness_sum = 4.5
    while (true) {
      var r1=Random.nextDouble() % (fitness_sum % 1)
      var r2=Random.nextDouble() % (fitness_sum % 1)
      if (fitness_sum.toInt > 0){
        r1 += Random.nextInt(fitness_sum.toInt+1)
        r2 += Random.nextInt(fitness_sum.toInt+1)
      }
      println(r1 + "," + r2)
      if (r1 >4 || r2 > 4)
        while (true)
          println("dayu 4!!!!")
      if (r1 > fitness_sum || r2 > fitness_sum)
        while (true) {
          println("error!!!!")
        }
    }

  }
}
