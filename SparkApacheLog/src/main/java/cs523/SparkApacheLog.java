package cs523;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.regex.*;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkApacheLog
{

	/** The number of fields that must be found. */
	public static final int NUM_FIELDS = 9;
	
	public static final String LOG_PATTERN = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";

	public static final String logEntryLine = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";
	
	public static void listIPsThatAccessMoreThan(String logFolder, int threshold, String outputFolder) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ApacheLog").setMaster("local"));

		// Load log file
		JavaRDD<String> lines = sc.textFile(logFolder);
		
		JavaPairRDD<String, Integer> ips= lines
				.filter(line -> isLogEntry(line))
				.map(l -> getIP(l))
				.mapToPair(ip -> new Tuple2<String, Integer>(ip, 1))
				.reduceByKey((x, y) -> x + y)
				.filter(t -> t._2 >= threshold); 
		
		ips.saveAsTextFile(outputFolder);
		
		sc.close();

	}
			
	public static void howManyErrorsBetween(String logFolder,String error, String from, String to, String outputFolder) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ApacheLog").setMaster("local"));

		// Load log file
		JavaRDD<String> lines = sc.textFile(logFolder);
		
		JavaPairRDD<String, Integer> result = lines
				.filter(line -> isMatch(line, error, from, to))
				.mapToPair(l -> new Tuple2<String, Integer>("All", 1))
				.reduceByKey((x,y) -> x+y);
		
		Long count = lines
				.filter(line -> isMatch(line, error, from, to))
				.count();
		
		System.out.println(count);
		
		result.saveAsTextFile(outputFolder);
		
		sc.close();
		
		
	}
	
	private static boolean isMatch(String log, String error, String from, String to) {

		 Pattern p = Pattern.compile(LOG_PATTERN);
		 Matcher matcher = p.matcher(log);
		 if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			     return false; 
		}else {
			if(matcher.group(1) == error && isInRange(matcher.group(4).substring(0, 11), from, to)) {
				return true;
			}
		}
		 
		
		return false;
	}
	
	private static boolean isLogEntry(String log) {

		 Pattern p = Pattern.compile(LOG_PATTERN);
		 Matcher matcher = p.matcher(log);
		 if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			     return false; 
		}
		 
		return true;
	}
	
	private static String getIP(String log) {
		Pattern p = Pattern.compile(LOG_PATTERN);
		Matcher matcher = p.matcher(log);
		return matcher.group(1); 
	}
	
	private static boolean isInRange(String target, String from, String to ) {
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy");
		
	     try
	       {
	           //format() method Formats a Date into a date/time string. 
	           Date fromDate = df.parse(from);
	           Date toDate = df.parse(to);
	           Date targetDate = df.parse(target);
	           
	           if(targetDate.compareTo(fromDate)>=0  && targetDate.compareTo(toDate)<=0 ){
	        	   return true;
	           }
	

	       }
	       catch (Exception ex ){
	          return false;
	       }
	     
	     return false; 
	}

	public static void main(String[] args) throws Exception
	{
		String inputFolder = args[0] ;
		String outputFolder1 = args[1];
		String outputFolder2 = args[2];
		
		String error = args[3];
		String from = args[4];
		String to = args[5];
		
		String threshold = args[6];
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(outputFolder1), true);
		fs.delete(new Path(outputFolder2), true);
		
		// Q1 - How many 401 errors from 10/Mar/2004 to 08/Mar/2004
		howManyErrorsBetween(inputFolder, error, from, to, outputFolder1);
		
		// Q2- list all IPs that access this server more than 20 times
		listIPsThatAccessMoreThan(inputFolder, Integer.parseInt(threshold), outputFolder2);
		
	
		
	}
}
