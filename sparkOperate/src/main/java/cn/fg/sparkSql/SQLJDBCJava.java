package cn.fg.sparkSql;

import org.apache.spark.sql.*;

import javax.xml.crypto.Data;

public class SQLJDBCJava {

    public static void main(String[] args) throws AnalysisException {
        SparkSession session=SparkSession.builder()
                            .appName("SQLspark")
                            .config("spark.master","local")
                            .getOrCreate();
        String url="jdbc:mysql://localhost:3306/shiro";
        String table="user";
        Dataset<Row> ds =session.read().format("jdbc")
                .option("url",url)
                .option("dbtable",table)
                .option("user","root")
                .option("password","hellosql")
                .option("driver","com.mysql.jdbc.Driver")
                .load();

//        ds.show();
        ds.select(new Column("username")).show();
    }
}
