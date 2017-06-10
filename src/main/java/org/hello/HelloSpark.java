package org.hello;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author SathishParthasarathy
 * Jun 9, 2017 8:52:55 PM
 */
public class HelloSpark {
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("HelloSpark-Java").setMaster("192.168.108.138");
		JavaSparkContext spark = new JavaSparkContext(conf);
		JavaRDD<String> textfile = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt");
		long count =  textfile.count();
		System.out.println("No of Lines -> " + count);

	}
}
