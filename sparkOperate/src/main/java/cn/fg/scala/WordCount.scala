package cn.fg.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("wordCount")
    conf.setMaster("local")

    val sc=new SparkContext(conf)
    val path="E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\test1"
    val rdd1=sc.textFile(path)

    val rdd2=rdd1.flatMap(line => line.split(" "))

    val rdd3=rdd2.map((_ ,1))

    val rdd4=rdd3.reduceByKey(_ + _);

    val rdd5= rdd4.collect();

    rdd5.foreach(println)
  }
}
