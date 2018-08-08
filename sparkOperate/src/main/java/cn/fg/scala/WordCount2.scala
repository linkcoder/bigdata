package cn.fg.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object WordCount2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("wordCount")
    conf.setMaster("local[3]")

    val sc=new SparkContext(conf)
    val path="D:\\javapro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\test1"
    val rdd1=sc.textFile(path,3)

    val rdd2=rdd1.flatMap(line => line.split(" "))

    val rdd3=rdd2.mapPartitions(it =>{
        println(Thread.currentThread().getName+" map start")
        it
    })

    val rdd4=rdd3.map(word=>{
      println(Thread.currentThread().getName+" mapping :"+word)
      (word,1)
    })

    val rdd5= rdd4.reduceByKey(_ + _)

    val r=rdd5.collect()

    r.foreach(println)
  }
}
