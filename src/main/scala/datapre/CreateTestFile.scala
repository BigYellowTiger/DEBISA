package datapre

import java.io.{File, PrintWriter}

import scala.util.Random

object CreateTestFile {
  def main(args: Array[String]) {
    val writer = new PrintWriter(new File("C:\\Users\\qly\\Desktop\\100_5.csv"))
    writer.write("a,b,c,d,e\r\n")
    val line_num=100
    val col_num=5
    for(i<-0 to line_num-1){
      var tempLine=""
      for(i<-0 to col_num-1){
        val tempval=Random.nextInt(10).toString
        tempLine+=tempval
        if(i!=col_num-1)
          tempLine+=","
      }
      writer.write(tempLine+"\r\n")
    }
    writer.close()
  }
}
