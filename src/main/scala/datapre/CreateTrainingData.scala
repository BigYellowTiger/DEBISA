package datapre

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter, InputStreamReader}
import java.util.Scanner

object CreateTrainingData {
  def main(args: Array[String]): Unit = {
    val sc = new Scanner(System.in)
    val oriFile=new File("c:/users/qly/desktop/car.data")
    val writeFile=new File("c:/users/qly/desktop/car.csv")
    val bufferedReader=new BufferedReader(new FileReader(oriFile))
    val bufferedWriter=new BufferedWriter(new FileWriter(writeFile))
    bufferedWriter.write("buying,maint,doors,persons,lug_boot,safety,class\r\n")
    var currentLine=bufferedReader.readLine()
    while (currentLine!=null){
      val tempArr=currentLine.split(",")
      var writeLine=""
      for(i<-0 to tempArr.length-1){
        if(tempArr(i).equals("vhigh"))
          writeLine+="4"
        else if(tempArr(i).equals("high"))
          writeLine+="3"
        else if(tempArr(i).equals("med"))
          writeLine+="2"
        else if(tempArr(i).equals("low"))
          writeLine+="1"
        else if(tempArr(i).equals("2"))
          writeLine+="2"
        else if(tempArr(i).equals("3"))
          writeLine+="3"
        else if(tempArr(i).equals("4"))
          writeLine+="4"
        else if(tempArr(i).equals("5"))
          writeLine+="5"
        else if(tempArr(i).equals("5more"))
          writeLine+="6"
        else if(tempArr(i).equals("more"))
          writeLine+="8"
        else if(tempArr(i).equals("small"))
          writeLine+="1"
        else if(tempArr(i).equals("big"))
          writeLine+="3"
        else if(tempArr(i).equals("unacc"))
          writeLine+="0"
        else if(tempArr(i).equals("acc"))
          writeLine+="1"
        else if(tempArr(i).equals("good"))
          writeLine+="2"
        else if(tempArr(i).equals("vgood"))
          writeLine+="3"
        if(i!=tempArr.length-1)
          writeLine+=","
      }
      writeLine += "\r\n"
      bufferedWriter.write(writeLine)
      currentLine=bufferedReader.readLine()
    }
    bufferedWriter.flush()
    bufferedWriter.close()
  }
}
