package cn.fg.scala

object Sample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val conf = new SparkConf
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val path="E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\test1"
    val rdd1 = sc.textFile(path, 4)
    val rdd2 = rdd1.flatMap(_.split(" "))

    val rdd3 = rdd2.sample(false, 0.5)
    rdd3.collect.foreach(println)
  }
}
