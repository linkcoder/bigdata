/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package cn.fg.sparkML

import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object MovieRecommen {

  case class Rating0(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating0 = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating0(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample").setMaster("local[4]")
    val sess=SparkSession.builder().config(conf).getOrCreate()


    import sess.implicits._
    val textFile=sess.sparkContext.textFile("E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\ml\\movie\\sample_movielens_ratings.txt")
    val movieDF=textFile.map(parseRating).toDF()

    val Array(training,test)=movieDF.randomSplit(Array(0.99,0.01))
    //下面三个值要跟定义的case样例类对应
    val als=new ALS().setMaxIter(50)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model=als.fit(training)

    val predictions=model.transform(test)

    predictions.show()
  }
}

