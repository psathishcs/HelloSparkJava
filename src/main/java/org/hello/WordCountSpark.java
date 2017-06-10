package org.hello;


import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountSpark {
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("WordCount - Java");
		JavaSparkContext spark = new JavaSparkContext(conf);
		JavaRDD<String> textfile = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt");
		JavaRDD<String> words = textfile.flatMap(line -> Arrays.asList(line.split(" ")));
		JavaPairRDD<String, Integer> counts =
			    words.mapToPair(w -> new Tuple2<String, Integer>(w, 1));
		counts.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/The_Outline_of_Science.txt");
		spark.close();
	}
}
