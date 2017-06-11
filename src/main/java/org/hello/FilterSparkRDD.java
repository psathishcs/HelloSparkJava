package org.hello;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterSparkRDD {

	static Boolean filterNewsWord(String line){
		return line.contains(" news ");
	}
	
	public static void main(String[] args) throws IOException{
		SparkConf conf = new SparkConf().setAppName("HelloSpark-Java");
		JavaSparkContext spark = new JavaSparkContext(conf);
		Configuration fsConfig = new Configuration();
		JavaRDD<String> textfile = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt");
		long count =  textfile.count();
		System.out.println("No of Lines -> " + count);
		String first = textfile.first();
		System.out.println("first -> " + first);
		
		fsConfig.set("fs.defaultFS", "hdfs://hadoop.master.com:9000");
		FileSystem fs = FileSystem.get(fsConfig);
		Path outputPath = new Path("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceNews_Java");
		if (fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		JavaRDD<String> newsLines = textfile.filter(line -> line.contains("news"));
		newsLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceNews_Java");
		
		outputPath = new Path("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceNewsWord_Java");
		if (fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		JavaRDD<String> newsWordLines = textfile.filter(line -> filterNewsWord(line));
		newsWordLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceNewsWord_Java");
	}
}
