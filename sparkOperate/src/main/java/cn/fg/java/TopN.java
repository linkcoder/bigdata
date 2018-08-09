package cn.fg.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class TopN {

    public static void main(String[] args) {
        final int N=3;
        SparkConf conf = new SparkConf();

        conf.setAppName("topN");
        conf.setMaster("local[3]");

        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.textFile("E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\topN",3);

        JavaRDD<Integer> rdd2=rdd1.map((Function<String, Integer>) v1 -> {
//            System.out.println(Thread.currentThread().getName());
            return Integer.parseInt(v1);
        });

        JavaRDD<Integer> rdd3=rdd2.sortBy((Function<Integer, Integer>) v1 -> {
            System.out.println(Thread.currentThread().getName());
           return v1;
        },true,3);

        rdd3.take(N).forEach(v -> System.out.println(Thread.currentThread().getName()+" :"+v));
    }
}
