package org.hello;

import org.apache.spark.sql.SparkSession;
import org.hello.entity.Employees;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
public class EmployeesSparkSQL {
	public static void main(String[] args){
		SparkSession spark = SparkSession.builder()
				 .appName("HelloSparkSQL-Java")
				 .config("spark.some.config.option", "some-value")
				 .getOrCreate();
		Encoder<Employees>  employeeEncoder = Encoders.bean(Employees.class);
		Dataset<Employees>  employeesDS = spark.read()
				.option("header", "true")
		        .option("inferSchema", "true")
		        .csv("hdfs://hadoop.master.com:9000/user/psathishcs/Input/csv/Employees.csv").as(employeeEncoder);
		employeesDS.show();
	}
}
