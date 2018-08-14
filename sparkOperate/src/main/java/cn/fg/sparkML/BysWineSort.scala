package cn.fg.sparkML

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object BysWineSort {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf();
    conf.setAppName("ml_BYSWine")
    conf.setMaster("local[2]")
    val sess=SparkSession.builder().config(conf).getOrCreate()
    val sc=sess.sparkContext

    val dataDir="E:/java-maven-pro/bigdata/sparkOperate/src/main/resources/data/ml/wine/winequality-white.csv"

    case class Wine(FixedAcidity: Double, VolatileAcidity: Double,
                    CitricAcid: Double, ResidualSugar: Double, Chlorides: Double,
                    FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH:
                    Double, Sulphates: Double, Alcohol: Double, Quality: Double)

    val wineRdd=sc.textFile(dataDir).map(_.split(";")).map(w=>Wine(w(0).toDouble, w(1).toDouble,
      w(2).toDouble, w(3).toDouble, w(4).toDouble, w(5).toDouble, w(6).toDouble, w(7).toDouble, w(8).toDouble
      , w(9).toDouble, w(10).toDouble, w(11).toDouble))

    println("============加载数据============")
    wineRdd.foreach(println)

    import sess.implicits._

    val trainningDF=wineRdd.map(w=>(w.Quality,
      Vectors.dense(w.FixedAcidity,
                    w.VolatileAcidity,
                    w.CitricAcid,
                    w.ResidualSugar,
                    w.Chlorides,
                    w.FreeSulfurDioxide,
                    w.TotalSulfurDioxide,
                    w.Density,
                    w.PH,
                    w.Sulphates,
                    w.Alcohol))).toDF("label","features")

    println("============训练数据============")
    trainningDF.show()


    val lr=new LinearRegression()
    //设置迭代数
    lr.setMaxIter(50)
    //拟合训练集
    val model=lr.fit(trainningDF)

    //创建内存测试数据数据框
    val testDF = sess.createDataFrame(Seq((5.0, Vectors.dense(7.4,
      0.7, 0.0, 1.9, 0.076, 25.0, 67.0, 0.9968, 3.2, 0.68, 9.8)), (5.0,
      Vectors.dense(7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0, 0.9978, 3.51, 0.56,
        9.4)), (7.0, Vectors.dense(7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0, 0.9968,
      3.36, 0.57, 9.5)))).toDF("label", "features")
    println("===========测试数据===========")
    testDF.show()

    //创建临时视图
    testDF.createOrReplaceTempView("test")
    //利用model对测试数据进行变化，得到新数据框，查询features", "label", "prediction方面值。
    val tested = model.transform(testDF).select("features", "label", "prediction");
    println("==========结果==========")
    tested.show();
  }
}

