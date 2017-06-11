package org.hello;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HelloSparkSQL {
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("Hello Spark SQL - Java");
		JavaSparkContext spark = new JavaSparkContext(conf);
		SQLContext sqlcont = new SQLContext(spark);
		Dataset<Row> df = sqlcont.read().json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companies.json");
		System.out.println("Companies Schema ------------------------------>");
		df.printSchema();
	}
}
