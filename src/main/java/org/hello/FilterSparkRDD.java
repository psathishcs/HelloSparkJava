package org.hello;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterSparkRDD {

	static Boolean filterScienceWord(String line){
		return line.contains(" Science ");
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
		Path outputPath = new Path("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSci_Java");
		if (fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		JavaRDD<String> sciLines = textfile.filter(line -> line.contains("Science"));
		sciLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSci_Java");
		
		outputPath = new Path("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSciWord_Java");
		if (fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		JavaRDD<String> sciWordLines = textfile.filter(line -> filterScienceWord(line));
		sciWordLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSciWord_Java");
	}
}
