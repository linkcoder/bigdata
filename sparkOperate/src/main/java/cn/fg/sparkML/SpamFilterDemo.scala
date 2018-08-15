package cn.fg.sparkML

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

object SpamFilterDemo {
  def main(args: Array[String]): Unit = {
    val sess=SparkSession.builder().appName("mlMail").master("local[2]").getOrCreate()
    val sc=sess.sparkContext
    //分词器
    val tokenizer=new Tokenizer().setInputCol("message").setOutputCol("words")
    //hash特征
    val hashTF=new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(500)
    //设置管线件训练数据
    val traniningRDD=sess.createDataFrame(Seq(
      ("you@example.com", "hope you are well", 0.0),
      ("raj@example.com", "nice to hear from you", 0.0),
      ("thomas@example.com", "happy holidays", 0.0),
      ("mark@example.com", "see you tomorrow", 0.0),
      ("dog@example.com", "save loan money", 1.0),
      ("xyz@example.com", "save money", 1.0),
      ("top10@example.com", "low interest rate", 1.0),
      ("marketing@example.com", "cheap loan", 1.0))).toDF("email","message","label")

    val lr=new LogisticRegression().setMaxIter(10).setRegParam(0.01)

    val pipeLine=new Pipeline().setStages(Array(tokenizer,hashTF,lr))

    val model=pipeLine.fit(traniningRDD)

    //测试数据，评判model的质量
    val test = sess.createDataFrame(Seq(
      ("you@example.com", "how are you"),
      ("jain@example.com", "hope doing well"),
      ("caren@example.com", "want some money"),
      ("zhou@example.com", "secure loan"),
      ("zhou@example.com", "how are you need secure loan need secure loan"),
      ("ted@example.com", "need loan"))).toDF("email", "message")
    println("==============分词结果===============")
    val wordsDF=tokenizer.transform(test)
    wordsDF.show()
    println("==============hash结果===============")
    val hashDF=hashTF.transform(wordsDF)
    hashDF.show()

    //对测试数据进行模型变换,得到模型的预测结果
    val prediction = model.transform(test).select("email", "message", "prediction")
    println("==============测试结果===============")
    prediction.show()

  }
}
