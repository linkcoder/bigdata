package cn.fg.scala

object Intersection {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val conf = new SparkConf
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val path="E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\union1"

    val rdd1=sc.textFile(path,4);

    val hello=rdd1.filter( word => {
      !word.toLowerCase.contains("hello")
    })

    val want=rdd1.filter(word=>{
      !word.contains("want")
    })

    val rdd2=hello.intersection(want)

    rdd2.collect().foreach(println)
  }

}
