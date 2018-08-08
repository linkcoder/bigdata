package cn.fg.scala

object Group {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val conf = new SparkConf
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val path="E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\group"

    val rdd1=sc.textFile(path,4);

    val rdd2=rdd1.map(line=>{
      val key=line.split(" ")(0)
      (key,line)
    })

    val rdd3=rdd2.groupByKey()

    rdd3.collect().foreach(t=>{
      println(t._1 +" =============>>")
      for( e <- t._2){
        println(e)
      }
    })

  }
}
