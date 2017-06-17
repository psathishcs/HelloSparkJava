package org.hello;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 
 * @author SathishParthasarathy
 * Jun 17, 2017 9:57:01 AM
 */
public class HelloSparkScream {
	public static void main(String[] args) throws InterruptedException {
		SparkConf spark = new SparkConf().setAppName("Hello Spark Stream - Java Word Count");
		JavaStreamingContext jssc = new JavaStreamingContext(spark, Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop.master.com", 9999);
		JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> counts = words
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
			    .reduceByKey((x, y) -> x + y);
		jssc.start();
		jssc.awaitTermination();
		
				
	}
}
