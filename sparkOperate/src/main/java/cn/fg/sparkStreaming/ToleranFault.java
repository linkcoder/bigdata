package cn.fg.sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ToleranFault {

    public static void main(String[] args) throws InterruptedException {
        //首次创建context时调用该方法。
        Function0<JavaStreamingContext> contextFactory = (Function0<JavaStreamingContext>) () -> {
            SparkConf conf = new SparkConf();
            conf.setMaster("local[4]");
            conf.setAppName("wc");
            JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(2000));
            JavaDStream<String> lines = jssc.socketTextStream("localhost",9999);

            /*******  变换代码放到此处 ***********/
            JavaDStream<Long> dsCount = lines.countByWindow(new Duration(24 * 60 * 60 * 1000),new Duration(2000));
            dsCount.print();
            //设置检察点目录
            jssc.checkpoint("file:///d:/scala/check");
            return jssc;
        };

        JavaStreamingContext context = JavaStreamingContext.getOrCreate("file:///d:/scala/check", contextFactory);

        context.start();
        context.awaitTermination();

    }
}
