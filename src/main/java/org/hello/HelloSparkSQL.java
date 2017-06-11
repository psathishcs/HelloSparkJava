package org.hello;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HelloSparkSQL {
	public static void main(String[] args){
		SparkSession spark = SparkSession.builder()
										 .appName("HelloSparkSQL-Java")
										 .config("spark.some.config.option", "some-value")
										 .getOrCreate();
		Dataset<Row> df = spark.read().json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companies.json");
		System.out.println("Companies Schema ------------------------------>");
		df.printSchema();
		System.out.println("Companies Schema ------------------------------>");
		df.createOrReplaceTempView("Companies");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM Companies Where _id.$oid = '52cdef7c4bab8bd675297d8a'");
		sqlDF.show();

		
	}
}
