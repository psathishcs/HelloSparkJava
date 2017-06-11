package org.hello;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HelloSparkSQL {
	public static void main(String[] args) throws AnalysisException{
		SparkSession spark = SparkSession.builder()
										 .appName("HelloSparkSQL-Java")
										 .config("spark.some.config.option", "some-value")
										 .getOrCreate();
		Dataset<Row> df = spark.read().json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companie.json");
		System.out.println("Companies Schema ------------------------------>");
		df.printSchema();
		System.out.println("Companies Schema ------------------------------>");
		df.createOrReplaceTempView("Companie");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM Companie");
		sqlDF.show();
		
		Dataset<Row> nameDF = spark.sql("SELECT * FROM Companie WHERE name='Facebook'");
		nameDF.show();
		
		Dataset<Row> filterDF = spark.sql("SELECT _id, name, category_code, acquisitions, competitions, description, email_address, homepage_url, relationships  FROM Companie WHERE name='Facebook'");
		filterDF.show();
		
		Dataset<Row> relDF = spark.sql("SELECT relationships  FROM Companie WHERE name='Facebook'");
		relDF.show();
		
		relDF = spark.sql("SELECT relationships.person, relationships.title  FROM Companie WHERE name='Facebook'");
		relDF.show();
		
	}
}
