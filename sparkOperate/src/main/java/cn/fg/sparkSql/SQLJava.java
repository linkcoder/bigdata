package cn.fg.sparkSql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQLJava {

    public static void main(String[] args) throws AnalysisException {
        SparkSession session=SparkSession.builder()
                            .appName("SQLspark")
                            .config("spark.master","local")
                            .getOrCreate();

        Dataset<Row> ds = session.read().json("E:\\java-maven-pro\\bigdata\\sparkOperate\\src\\main\\resources\\data\\sql\\customer.json");
        ds.createTempView("customers");
        //sql查询
        Dataset<Row> re1 = session.sql("select * from customers where id>2");
        re1.show();
        Dataset<Row> re2 = ds.where("name like 'tomm%'");
        re2.show();


    }
}
