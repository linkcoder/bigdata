package cn.fg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.function.Consumer;

public class Invertedindex {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf();
        conf.setAppName("invertedIndex");
        conf.setMaster("local[2]");

        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\invertedindex",
                2);

        JavaPairRDD<String, String> rdd2 = rdd1.mapToPair((PairFunction<String, String, String>) s -> {
            Tuple2<String, String> t2 = new Tuple2<>(s.split(":")[1], s.split(":")[0]);
            return t2;
        });

        JavaPairRDD<String, Iterable<String>> rdd3 = rdd2.groupByKey();
        JavaRDD<String> result = rdd3.map((Function<Tuple2<String, Iterable<String>>, String>) v1 -> {
            String word = v1._1;
            StringBuilder builder = new StringBuilder();
            for (String e : v1._2) {
                builder.append(e + "===>");
            }
            return word + " :" + builder.toString();
        });
        result.collect().forEach(s -> System.out.println(s));

    }
}
