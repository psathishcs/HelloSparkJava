package org.hello;


import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountSpark {
	public static void main(String[] args) throws IOException{
		SparkConf conf = new SparkConf().setAppName("WordCount - Java");
		Configuration fsConfig = new Configuration();
		fsConfig.set("fs.defaultFS", "hdfs://hadoop.master.com:9000");
		FileSystem fs = FileSystem.get(fsConfig);
		Path outputPath = new Path("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/Science_Java");
		if (fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		JavaSparkContext spark = new JavaSparkContext(conf);
		JavaRDD<String> textfile = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt");
		JavaRDD<String> words = textfile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairRDD<String, Integer> counts = words
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
			    .reduceByKey((x, y) -> x + y);
		counts.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/Science_Java");
		spark.close();
	}
}
