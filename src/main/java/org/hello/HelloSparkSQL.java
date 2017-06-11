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
		Dataset<Row> df = spark.read().json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companie.json");
		System.out.println("Companies Schema ------------------------------>");
		df.printSchema();
		System.out.println("Companies Schema ------------------------------>");
		df.createOrReplaceTempView("Companie");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM Companies");
		sqlDF.show();

		
	}
}
