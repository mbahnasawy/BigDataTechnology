package cs523.SparkWC;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCountjdk8
{

	public static void main(String[] args) throws Exception
	{
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<String, Integer> counts = lines
					.flatMap(line -> Arrays.asList(line.split(" ")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y);
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);
		fs.delete(new Path(args[2]), true);
		fs.delete(new Path(args[3]), true);

		// Save the word count back out to a text file, causing evaluation
		counts.saveAsTextFile(args[1]);
		
		JavaPairRDD<String, Integer> countThreshold = counts
				.filter(t -> t._2 >= Integer.parseInt(args[4]));  
		
		// Save the word count with the threshold back out to a text file.
		countThreshold.saveAsTextFile(args[2]);
		
		JavaPairRDD<Character, Integer> countLetters = countThreshold
				.flatMap(t -> Arrays.asList(StringUtils.repeat(t._1.toLowerCase(), t._2).split("")))
				.mapToPair(w -> new Tuple2<Character, Integer>(w.charAt(0), 1))
				.reduceByKey((x, y) -> x + y)
				.sortByKey();
		
		countLetters.saveAsTextFile(args[3]);
		
		sc.close();
	}
}
