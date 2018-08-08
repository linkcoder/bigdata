package cn.fg.scala

object Join {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val conf = new SparkConf
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val path="E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\join1"
    val path2="E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\join2"
    val rdd1=sc.textFile(path,4)

    val rdd11=rdd1.map(line=>{
      val arr=line.split(" ")
      (arr(0).toInt,arr(1).toInt)
    })

    val rdd2=sc.textFile(path2,4)

    val rdd21=rdd2.map(line=>{
      val arr=line.split(" ")
      (arr(0).toInt,arr(1))
    })

    rdd21.join(rdd11).collect().foreach(t=>{
      println(t)
    })

  }
}
