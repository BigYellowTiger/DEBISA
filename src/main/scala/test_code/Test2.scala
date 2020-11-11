package test_code

object Test2 {
  def main(args: Array[String]): Unit = {
    def test():Boolean={
      for(i<-0 to 20){
        if(i==15)
          return true
      }
      false
    }

    println(test())
  }
}
