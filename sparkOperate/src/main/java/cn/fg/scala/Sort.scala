package cn.fg.scala

object Sort {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val conf = new SparkConf
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val path="E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\sort"

    val rdd1=sc.textFile(path,4);

    rdd1.map((_," ")).sortByKey().collect().foreach(t=>{
      println(t._1)
    })
  }
}
