package cn.fg.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("streaming").setMaster("local[2]");//一定要大于或等于2线程，因为一个线程接收数据，一个负责处理
    val scc=new StreamingContext(conf,Seconds(5))

    val lines=scc.socketTextStream("localhost",9999)

    val words=lines.flatMap(line=>line.split(" "))

    val pairs=words.map((_,1))

    pairs.print()

    scc.start();
    scc.awaitTermination();
  }
}
