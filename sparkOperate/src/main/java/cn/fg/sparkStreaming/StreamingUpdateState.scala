package cn.fg.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.runtime.Nothing$

object StreamingUpdateState {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("streaming").setMaster("local[2]");//一定要大于或等于2线程，因为一个线程接收数据，一个负责处理
    val scc=new StreamingContext(conf,Seconds(5))
    scc.checkpoint("E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\check")
    val lines=scc.socketTextStream("localhost",9999)

    val words=lines.flatMap(line=>line.split(" "))

    val pairs=words.map((_,1))

    val newState=pairs.updateStateByKey[Int]((it:Seq[Int],v:Option[Int])=>{
      var newCount=0
      if(v.isEmpty){
        newCount=0
      }else{
        newCount=v.get;
      }
      ;
      for(e <- it){
        newCount=newCount + e;
      }
      Option.apply(newCount)
    })

    val re=newState.reduceByKey(_+_)
    re.print()

    scc.start();
    scc.awaitTermination();
  }
}
